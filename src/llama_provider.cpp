#include "llama_provider.hpp"
#include "config.hpp"

#include <cstdio>
#include <stdexcept>

namespace pgcdc 
{

LlamaProvider::LlamaProvider(const EmbeddingConfig& cfg)
    : model_path_(cfg.model_path)
    , n_threads_(cfg.n_threads)
    , n_ctx_(cfg.n_ctx) {}

LlamaProvider::~LlamaProvider() 
{
    if (ctx_)   { llama_free(ctx_);         ctx_   = nullptr; }
    if (model_) { llama_model_free(model_); model_ = nullptr; }
}

void LlamaProvider::init() 
{
    llama_backend_init();

    llama_model_params mparams = llama_model_default_params();
    mparams.n_gpu_layers = 0; // CPU only; set > 0 when GPU support added

    model_ = llama_model_load_from_file(model_path_.c_str(), mparams);
    if (!model_) {
        throw std::runtime_error(
            "LlamaProvider: failed to load model from '" + model_path_ + "' — "
            "check that the path exists and is a valid GGUF file");
    }

    llama_context_params cparams = llama_context_default_params();
    cparams.n_ctx      = static_cast<uint32_t>(n_ctx_);
    cparams.n_threads  = static_cast<uint32_t>(n_threads_);
    cparams.embeddings = true; // required — without this llama_get_embeddings_seq returns null

    ctx_ = llama_init_from_model(model_, cparams);
    if (!ctx_) {
        llama_model_free(model_);
        model_ = nullptr;
        throw std::runtime_error("LlamaProvider: failed to create llama context");
    }

    std::fprintf(stderr, "[LlamaProvider] loaded %s (%d dims)\n",
                 model_path_.c_str(), dimensions());
}

std::vector<float> LlamaProvider::embed(const std::string& text) 
{
    const int n_ctx = static_cast<int>(llama_n_ctx(ctx_));
    std::vector<llama_token> tokens(n_ctx);

    const llama_vocab* vocab = llama_model_get_vocab(model_);
    int n_tokens = llama_tokenize(
        vocab,
        text.c_str(),
        static_cast<int32_t>(text.size()),
        tokens.data(),
        static_cast<int32_t>(tokens.size()),
        /*add_special=*/true,
        /*parse_special=*/false
    );

    if (n_tokens < 0) {
        std::fprintf(stderr, "[LlamaProvider] tokenize failed (text too long?): %.60s...\n",
                     text.c_str());
        return {};
    }
    tokens.resize(static_cast<size_t>(n_tokens));

    // Clear KV cache from previous call so sequences don't bleed into each other
    llama_memory_clear(llama_get_memory(ctx_), true);

    llama_batch batch = llama_batch_get_one(tokens.data(), n_tokens);
    if (llama_encode(ctx_, batch) != 0) {
        std::fprintf(stderr, "[LlamaProvider] llama_encode failed\n");
        return {};
    }

    const int    n_embd = dimensions();
    const float* embd   = llama_get_embeddings_seq(ctx_, 0);
    if (!embd) {
        embd = llama_get_embeddings_ith(ctx_, 0); // fallback
    }
    if (!embd) {
        std::fprintf(stderr, "[LlamaProvider] llama_get_embeddings returned null — "
                     "verify cparams.embeddings = true\n");
        return {};
    }

    return std::vector<float>(embd, embd + n_embd);
}

int LlamaProvider::dimensions() const 
{
    return model_ ? llama_model_n_embd(model_) : 0;
}

std::string LlamaProvider::name() const 
{
    // Extract just the filename from the path for concise log lines
    auto pos = model_path_.rfind('/');
    std::string fname = (pos == std::string::npos) ? model_path_ : model_path_.substr(pos + 1);
    return "llama[" + fname + "]";
}

} // namespace pgcdc

