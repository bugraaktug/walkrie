#include "llama_provider.hpp"
#include "config.hpp"

#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <algorithm>

namespace pgcdc {

LlamaProvider::LlamaProvider(const EmbeddingConfig& cfg)
    : model_path_(cfg.model_path)
    , n_threads_(cfg.n_threads)
    , n_ctx_(cfg.n_ctx)
    , n_batch_(cfg.max_batch_size) {}

LlamaProvider::~LlamaProvider() {
    if (ctx_)   llama_free(ctx_);
    if (model_) llama_model_free(model_);
    llama_backend_free();
}

void LlamaProvider::init()
{
    llama_log_set([](ggml_log_level level, const char* text, void* /*user_data*/) {
        switch (level) {
            case GGML_LOG_LEVEL_ERROR: spdlog::error("[llama] {}", text); break;
            case GGML_LOG_LEVEL_WARN:  spdlog::warn("[llama] {}", text);  break;
            default:                  spdlog::debug("[llama] {}", text); break;
        }
    }, nullptr);

    llama_backend_init();

    llama_model_params mparams = llama_model_default_params();
    mparams.n_gpu_layers = 0; // CPU only; set > 0 when GPU support added

    model_ = llama_model_load_from_file(model_path_.c_str(), mparams);
    if (!model_) {
        throw std::runtime_error(
            "LlamaProvider: failed to load model from '" + model_path_ + "' — "
            "check that the path exists and is a valid GGUF file");
    }
    vocab_ = llama_model_get_vocab(model_);

    // n_seq_max lets one llama_encode() call hold n_batch_ distinct texts as
    // parallel sequences. n_batch/n_ubatch must cover the worst case of
    // n_batch_ sequences each using the full context window; n_ubatch must
    // equal n_batch for non-causal (embedding) models, or llama.cpp refuses
    // to run a batch that mixes multiple sequences into one ubatch.
    const uint32_t n_seq_max = static_cast<uint32_t>(std::max(1, n_batch_));

    llama_context_params cparams = llama_context_default_params();
    cparams.n_ctx           = static_cast<uint32_t>(n_ctx_);
    cparams.n_batch         = static_cast<uint32_t>(n_ctx_) * n_seq_max;
    cparams.n_ubatch        = cparams.n_batch;
    cparams.n_seq_max       = n_seq_max;
    cparams.n_threads       = static_cast<uint32_t>(n_threads_);
    cparams.n_threads_batch = static_cast<uint32_t>(n_threads_);
    cparams.embeddings      = true; // required — without this llama_get_embeddings_seq returns null

    ctx_ = llama_init_from_model(model_, cparams);
    if (!ctx_) {
        llama_model_free(model_);
        model_ = nullptr;
        throw std::runtime_error("LlamaProvider: failed to create llama context");
    }

    spdlog::info("[LlamaProvider] loaded {} ({} dims)", model_path_, dimensions());
    spdlog::info("[LlamaProvider] ACTUAL ctx_ config: n_ctx={} n_batch={} n_ubatch={} n_seq_max={}",
                 llama_n_ctx(ctx_), llama_n_batch(ctx_), llama_n_ubatch(ctx_), llama_n_seq_max(ctx_));
}

std::vector<LlamaTokenizedText> LlamaProvider::tokenize_batch(const std::vector<std::string>& texts)
{
    std::vector<LlamaTokenizedText> results;
    results.reserve(texts.size());

    for (size_t i = 0; i < texts.size(); ++i) {
        LlamaTokenizedText item;
        item.original_index = i;

        std::vector<llama_token> temp_tokens(static_cast<size_t>(n_ctx_));

        int n_tokens = llama_tokenize(
            vocab_,
            texts[i].c_str(),
            static_cast<int32_t>(texts[i].size()),
            temp_tokens.data(),
            static_cast<int32_t>(temp_tokens.size()),
            /*add_special=*/true,
            /*parse_special=*/false
        );

        if (n_tokens < 0) {
            spdlog::error("[LlamaProvider] tokenization failed for text {} (error code {})", i, n_tokens);
            item.ok = false;
            results.push_back(std::move(item));
            continue;
        }
        if (n_tokens > n_ctx_) {
            spdlog::warn("[LlamaProvider] text {} has {} tokens, exceeds n_ctx={} — dropped, not truncated",
                         i, n_tokens, n_ctx_);
            item.ok = false;
            results.push_back(std::move(item));
            continue;
        }

        temp_tokens.resize(static_cast<size_t>(n_tokens));
        item.tokens = std::move(temp_tokens);
        item.ok = true;
        results.push_back(std::move(item));
    }
    return results;
}

void LlamaProvider::build_batch(const std::vector<LlamaTokenizedText>& tokenized, llama_batch& batch)
{
    batch.n_tokens = 0;
    int cursor = 0;

    for (size_t seq_id = 0; seq_id < tokenized.size(); ++seq_id) {
        const auto& item = tokenized[seq_id];
        if (!item.ok || item.tokens.empty()) continue;

        for (int pos = 0; pos < static_cast<int>(item.tokens.size()); ++pos) {
            batch.token[cursor]     = item.tokens[pos];
            batch.pos[cursor]       = pos; // resets per sequence — position WITHIN that sequence
            batch.n_seq_id[cursor]  = 1;
            batch.seq_id[cursor][0] = static_cast<llama_seq_id>(seq_id);
            batch.logits[cursor]    = true; // pooling needs every token's output, not just the last
            ++cursor;
            ++batch.n_tokens;
        }
    }
}

std::vector<std::vector<float>> LlamaProvider::extract_embeddings(llama_context* ctx,
                                                                  LlamaBatch& batch,
                                                                  const std::vector<LlamaTokenizedText>& tokenized,
                                                                  int n_embd)
{
    std::vector<std::vector<float>> results(tokenized.size());

    for (size_t seq_id = 0; seq_id < tokenized.size(); ++seq_id) {
        if (!tokenized[seq_id].ok) continue;

        const float* embd = llama_get_embeddings_seq(ctx, static_cast<llama_seq_id>(seq_id)); // was ctx_ — fixed

        if (!embd) {
            spdlog::warn("[LlamaProvider] no sequence embedding for seq {}, trying ith fallback", seq_id);

            int last_token_idx = -1;
            for (int i = 0; i < batch.batch.n_tokens; ++i) {
                if (batch.batch.seq_id[i][0] == static_cast<llama_seq_id>(seq_id) &&
                    batch.batch.pos[i] == static_cast<int>(tokenized[seq_id].tokens.size()) - 1) {
                    last_token_idx = i;
                    break;
                }
            }
            if (last_token_idx >= 0) {
                embd = llama_get_embeddings_ith(ctx, last_token_idx); // was ctx_ — fixed
            }
        }

        if (!embd) {
            spdlog::warn("[LlamaProvider] no embedding available for sequence {}", seq_id);
            continue;
        }

        // Raw, unnormalized output — matches llama-server's default
        // (normalization is opt-in there via --embd-normalize).
        results[seq_id] = std::vector<float>(embd, embd + n_embd);
    }
    return results;
}

std::vector<float> LlamaProvider::embed(const std::string& text)
{
    auto tokenized = tokenize_batch({text});
    if (tokenized.empty() || !tokenized[0].ok) {
        return {};
    }

    std::lock_guard<std::mutex> lock(ctx_mutex_);
    llama_memory_clear(llama_get_memory(ctx_), true);
    
    int total_tokens = static_cast<int>(tokenized[0].tokens.size());
    LlamaBatch batch(total_tokens, 0, 1);
    build_batch(tokenized, batch.batch);

    if (llama_encode(ctx_, batch.batch) != 0) {
        spdlog::error("[LlamaProvider] llama_encode failed");
        return {};
    }

    auto results = extract_embeddings(ctx_, batch, tokenized, dimensions());
    if (results.empty() || results[0].empty()) {
        return {};
    }
    return std::move(results[0]);
}

std::vector<std::vector<float>> LlamaProvider::embed_batch(const std::vector<std::string>& texts)
{
    if (texts.empty()) return {};

    std::vector<std::vector<float>> results(texts.size());
    auto tokenized = tokenize_batch(texts);

    std::lock_guard<std::mutex> lock(ctx_mutex_);

    // Physical limits of the context we actually got from llama_init_from_model —
    // read back rather than assumed, in case llama.cpp clamped our request.
    const int max_seq_per_chunk   = std::max(1, static_cast<int>(llama_n_seq_max(ctx_)));
    const int max_tokens_per_chunk = static_cast<int>(llama_n_batch(ctx_));

    std::vector<LlamaTokenizedText> chunk;
    int chunk_tokens = 0;

    auto flush_chunk = [&]() {
        if (chunk.empty()) return;

        llama_memory_clear(llama_get_memory(ctx_), true);

        LlamaBatch batch(chunk_tokens, 0, 1);
        build_batch(chunk, batch.batch);

        if (llama_encode(ctx_, batch.batch) != 0) {
            spdlog::error("[LlamaProvider] llama_encode failed for a batch of {} sequences ({} tokens)",
                         chunk.size(), chunk_tokens);
        } else {
            auto chunk_results = extract_embeddings(ctx_, batch, chunk, dimensions());
            for (size_t i = 0; i < chunk.size(); ++i) {
                results[chunk[i].original_index] = std::move(chunk_results[i]);
            }
        }

        chunk.clear();
        chunk_tokens = 0;
    };

    for (auto& item : tokenized) {
        if (!item.ok || item.tokens.empty()) continue; // tokenize_batch already logged why

        const int n_toks = static_cast<int>(item.tokens.size());
        if (n_toks > max_tokens_per_chunk) {
            spdlog::warn("[LlamaProvider] text {} has {} tokens, exceeds llama batch capacity {} — dropped",
                         item.original_index, n_toks, max_tokens_per_chunk);
            continue;
        }

        // Start a new chunk if this text would overflow the current one —
        // by token budget or by number of parallel sequences the context supports.
        if (!chunk.empty() &&
            (chunk_tokens + n_toks > max_tokens_per_chunk ||
             static_cast<int>(chunk.size()) >= max_seq_per_chunk)) {
            flush_chunk();
        }

        chunk_tokens += n_toks;
        chunk.push_back(std::move(item));
    }
    flush_chunk();

    spdlog::debug("[LlamaProvider] embed_batch: {} texts, {} embeddings extracted", texts.size(), results.size());
    return results;
}

int LlamaProvider::dimensions() const 
{
    return static_cast<int>(llama_model_n_embd(model_));
}

std::string LlamaProvider::name() const 
{
    return "llama[" + model_path_ + "]";
}

} // namespace pgcdc
