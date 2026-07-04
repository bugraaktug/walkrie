#pragma once

#include <string>
#include <vector>

#include "embedding_provider.hpp"

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
    int dimensions() const override;
    std::string name() const override;

private:
    std::string api_key_;
    int         dimensions_;
};

} // namespace pgcdc

