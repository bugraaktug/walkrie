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
    // Number of dimensions this provider produces. Must match the
    // pgvector column size (1024 for BGE-M3, 1536 for OpenAI small).
    virtual int dimensions() const = 0;
    // Short name for log lines e.g. "llama[bge-m3-Q4_K_M]" or "openai[text-embedding-3-small]"
    virtual std::string name() const = 0;
};

std::shared_ptr<EmbeddingProvider> make_embedding_provider(const EmbeddingConfig& cfg);

} // namespace pgcdc

