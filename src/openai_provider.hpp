#pragma once

#include <string>
#include <vector>
#include <memory>

#include "embedding_provider.hpp"
#include "http_client.hpp"

namespace pgcdc 
{

struct EmbeddingConfig;

class OpenAIProvider : public EmbeddingProvider 
{
public:
    explicit OpenAIProvider(const EmbeddingConfig& cfg);
    ~OpenAIProvider() override = default;

    void init() override;
    std::vector<float> embed(const std::string& text) override;
    std::vector<std::vector<float>> embed_batch(const std::vector<std::string>& texts) override; 
    int dimensions() const override;
    std::string name() const override;
protected:
    std::string resolve_api_key(const std::string& configured);

private:
    std::string api_key_;
    std::string model_;        // e.g. "text-embedding-3-small"
    std::string base_url_;     // default "https://api.openai.com/v1/embeddings"
    int         dimensions_;

    std::unique_ptr<HttpClient> http_;
};

} // namespace pgcdc

