#include "openai_provider.hpp"
#include "config.hpp"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace pgcdc 
{

OpenAIProvider::OpenAIProvider(const EmbeddingConfig& cfg)
    : api_key_(resolve_api_key(cfg.api_key))
    , model_(cfg.model.empty() ? "text-embedding-3-small" : cfg.model)
    , base_url_("https://api.openai.com/v1/embeddings")
    , dimensions_(cfg.dimensions) {}

void OpenAIProvider::init() 
{
    if (api_key_.empty()) {
        throw std::runtime_error("OpenAIProvider: no API key configured. Set 'api_key' in [embedding] config.");
    }
    http_ = std::make_unique<HttpClient>(/*timeout_secs=*/30);
    spdlog::info("[OpenAIProvider] initialized, model={}, dimensions={}", model_, dimensions_);
}

std::string OpenAIProvider::resolve_api_key(const std::string& configured) {
    return configured;
}

std::vector<float> OpenAIProvider::embed(const std::string& text) 
{
     if (!http_) {
        spdlog::error("[OpenAIProvider] embed() called before init()");
        return {};
    }

     nlohmann::json request_body = {
        {"model", model_},
        {"input", text},
    };
    if (dimensions_ > 0) {
        request_body["dimensions"] = dimensions_;
    }

    std::vector<std::string> headers = {
        "Authorization: Bearer " + api_key_
    };

    HttpResponse resp = http_->post_json(base_url_, request_body.dump(), headers);

    if (!resp.curl_ok) {
        spdlog::error("[OpenAIProvider] request failed: {}", resp.curl_error);
        return {};
    }
    if (resp.status_code != 200) {
        spdlog::error("[OpenAIProvider] HTTP {} — {}", resp.status_code, resp.body);
        return {};
    }

    try {
        auto parsed = nlohmann::json::parse(resp.body);
        return parsed.at("data").at(0).at("embedding").get<std::vector<float>>();
    } catch (const std::exception& e) {
        spdlog::error("[OpenAIProvider] failed to parse response: {} — body: {}", e.what(), resp.body);
        return {};
    }
}

int OpenAIProvider::dimensions() const 
{
    return dimensions_;
}

std::string OpenAIProvider::name() const 
{
    return "openai[" + model_ + "]";
}

} // namespace pgcdc

