#pragma once

#include <ggml.h>
#include <llama.h>
#include <spdlog/spdlog.h>  
#include <string>
#include <vector>

#include "embedding_provider.hpp"

namespace pgcdc {

struct EmbeddingConfig;

class LlamaProvider : public EmbeddingProvider 
{
public:
    explicit LlamaProvider(const EmbeddingConfig& cfg);
    ~LlamaProvider() override;

    LlamaProvider(const LlamaProvider&) = delete;
    LlamaProvider& operator=(const LlamaProvider&) = delete;

    void init() override;
    std::vector<float> embed(const std::string& text) override;
    int dimensions() const override;
    std::string name() const override;

private:
    std::string model_path_;
    int         n_threads_;
    int         n_ctx_;

    llama_model*   model_ = nullptr;
    llama_context* ctx_   = nullptr;
};

} // namespace pgcdc

