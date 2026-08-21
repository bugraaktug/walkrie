#pragma once

#include <cstdint>
#include <ggml.h>
#include <llama.h>
#include <spdlog/spdlog.h>
#include <string>
#include <vector>

#include "embedding_provider.hpp"

namespace pgcdc {

struct EmbeddingConfig;

struct LlamaTokenizedText 
{
    std::vector<llama_token> tokens;
    size_t original_index;
    bool ok = false;
};

struct LlamaBatch 
{
    llama_batch batch;
    LlamaBatch(int32_t n_tokens_alloc, int32_t embd_size, int32_t seq_ids_per_token)
        : batch(llama_batch_init(n_tokens_alloc, embd_size, seq_ids_per_token)) {}
    ~LlamaBatch() { llama_batch_free(batch); }
    LlamaBatch(const LlamaBatch&) = delete;
    LlamaBatch& operator=(const LlamaBatch&) = delete;
};

class LlamaProvider : public EmbeddingProvider 
{
public:
    explicit LlamaProvider(const EmbeddingConfig& cfg);
    ~LlamaProvider() override;

    LlamaProvider(const LlamaProvider&) = delete;
    LlamaProvider& operator=(const LlamaProvider&) = delete;

    void init() override;
    std::vector<float> embed(const std::string& text) override;
    std::vector<std::vector<float>> embed_batch(const std::vector<std::string>& texts) override;
    int dimensions() const override;
    std::string name() const override;

protected:
    std::vector<LlamaTokenizedText> tokenize_batch(const std::vector<std::string>& texts);
    void build_batch(const std::vector<LlamaTokenizedText>& tokenized, llama_batch& batch);
    std::vector<std::vector<float>> extract_embeddings(llama_context* ctx, 
                                                       LlamaBatch& batch,
                                                       const std::vector<LlamaTokenizedText>& tokenized,
                                                       int n_embd);

private:
    std::string model_path_;
    std::string lora_path_;
    float       lora_scale_;
    std::string text_prefix_;  // from the LoRA adapter's "adapter.lora.prompt_prefix" metadata, if any
    int         n_threads_;
    int         n_ctx_;
    int         n_batch_;
    int         n_gpu_layers_;

    llama_model*        model_ = nullptr;
    llama_context*       ctx_  = nullptr;
    const llama_vocab*  vocab_ = nullptr;
    llama_adapter_lora*  lora_ = nullptr;
    
    std::mutex ctx_mutex_;  // For thread safety
};

} // namespace pgcdc

