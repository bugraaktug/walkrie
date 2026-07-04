#include "openai_provider.hpp"
#include "config.hpp"

#include <stdexcept>

namespace pgcdc 
{

OpenAIProvider::OpenAIProvider(const EmbeddingConfig& cfg)
    : api_key_(cfg.api_key)
    , dimensions_(cfg.dimensions) {}

void OpenAIProvider::init() 
{
    throw std::runtime_error(
        "OpenAIProvider: not implemented yet. "
        "Set provider = 'llama' in config until the HTTP client is added.");
}

std::vector<float> OpenAIProvider::embed(const std::string& /*text*/) 
{
    return {}; // unreachable — init() throws before this is ever called
}

int OpenAIProvider::dimensions() const 
{
    return dimensions_;
}

std::string OpenAIProvider::name() const 
{
    return "openai[text-embedding-3-small]";
}

} // namespace pgcdc

