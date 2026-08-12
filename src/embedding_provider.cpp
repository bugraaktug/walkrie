#include "embedding_provider.hpp"

#include "caching_embedding_provider.hpp"
#include "llama_provider.hpp"
#include "openai_provider.hpp"
#include "config.hpp"

#include <spdlog/spdlog.h>
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

std::shared_ptr<EmbeddingProvider> create_initialized_embedding_provider(const EmbeddingConfig& cfg, bool use_cached)
{
    try {
        auto provider = make_embedding_provider(cfg);
        provider->init();
        if (!use_cached) return provider;
        return std::make_shared<CachingEmbeddingProvider>(std::move(provider), cache_size_for_batch(cfg.max_batch_size));
    } catch (const std::exception& e) {
        spdlog::info("[EmbeddingProvider] failed to initialized — {}", e.what());
        throw std::runtime_error("EmbeddingProvider: failed to create context");
    }
}

std::vector<std::vector<float>> EmbeddingProvider::embed_batch(const std::vector<std::string>& texts) 
{
    std::vector<std::vector<float>> results;
    results.reserve(texts.size());
    for (const auto& t : texts) {
        results.push_back(embed(t));
    }
    return results;
}

} // namespace pgcdc
