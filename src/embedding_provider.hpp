#pragma once

#include <memory>
#include <string>
#include <vector>

namespace pgcdc {

struct EmbeddingConfig; // forward decl — avoid circular include with config.hpp

class EmbeddingProvider 
{
public:
    virtual ~EmbeddingProvider() = default;

    virtual void init() = 0;
    virtual std::vector<float> embed(const std::string& text) = 0;
    virtual std::vector<std::vector<float>> embed_batch(const std::vector<std::string>& texts) ;
    virtual int dimensions() const = 0; 	// <<< Number of dimensions this provider produces (1024 for BGE-M3, 1536 for OpenAI small).
    virtual std::string name() const = 0; 	// <<< Short name for log lines e.g. "llama[bge-m3-Q4_K_M]" or "openai[text-embedding-3-small]"
};

std::shared_ptr<EmbeddingProvider> make_embedding_provider(const EmbeddingConfig& cfg);

std::shared_ptr<EmbeddingProvider> create_initialized_embedding_provider(const EmbeddingConfig& cfg, bool use_cached = true); // <<< make_embedding_provider() + init() + shared error wrapping; use_cached=false for bit-exact fp comparison tests

} // namespace pgcdc

