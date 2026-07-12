#include "embedding_provider.hpp"

#include "llama_provider.hpp"
#include "openai_provider.hpp"
#include "config.hpp"

#include <stdexcept>

namespace pgcdc 
{

std::shared_ptr<EmbeddingProvider> make_embedding_provider(const EmbeddingConfig& cfg) 
{
    if (cfg.provider == "llama") {
        return std::make_shared<LlamaProvider>(cfg);
    }

    if (cfg.provider == "openai") {
        return std::make_shared<OpenAIProvider>(cfg);
    }

    throw std::runtime_error(
        "make_embedding_provider: unknown provider '" + cfg.provider + "' — "
        "valid values: 'llama', 'openai'");
}

} // namespace pgcdc
