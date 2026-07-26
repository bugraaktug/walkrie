#include "openai_provider.hpp"
#include "config.hpp"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <stdexcept>
namespace pgcdc 
{

namespace 
{
constexpr int kMaxRetries      = 2;
constexpr int kBaseBackoffMs   = 500; // doubles each retry: 500, 1000, 2000ms

bool is_retryable(const pgcdc::HttpResponse& resp) {
    if (!resp.curl_ok) return true;        // network-level: timeout, connection reset, DNS failure
    if (resp.status_code == 429) return true; // rate limited
    if (resp.status_code >= 500) return true; // server-side error
    return false; // 4xx (bad key, bad request) — retrying won't help
}

} // namespace

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

std::vector<std::vector<float>> OpenAIProvider::embed_batch(const std::vector<std::string>& texts) 
{
    if (!http_) {
        spdlog::error("[OpenAIProvider] embed_batch() called before init()");
        return std::vector<std::vector<float>>(texts.size());
    }
    if (texts.empty()) return {};

    nlohmann::json request_body = {
        {"model", model_},
        {"input", texts},
    };

    if (dimensions_ > 0) {
        request_body["dimensions"] = dimensions_;
    }
    
    std::string body_str = request_body.dump();
    std::vector<std::string> headers = { 
        "Authorization: Bearer " + api_key_ 
    };

    std::vector<std::vector<float>> results(texts.size());
    HttpResponse resp;

    for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
        resp = http_->post_json(base_url_, body_str, headers);

        if (resp.curl_ok && resp.status_code == 200) {
            break; // success
        }

        std::string reason = resp.curl_ok ? ("HTTP " + std::to_string(resp.status_code)) : resp.curl_error;

        if (!is_retryable(resp) || attempt == kMaxRetries) {
            spdlog::error("[OpenAIProvider] batch request failed ({}), giving up after {} attempt(s) — "
                          "{} row(s) in this batch will be dropped (source data in Postgres is unaffected; "
                          "the sink copy stays missing until a future UPDATE touches these rows again): {}",
                          reason, attempt + 1, texts.size(), resp.body.empty() ? resp.curl_error : resp.body);
            return results; // all-empty sentinel — PgEmbeddingSink already logs+drops per-row on empty vectors
        }

        int backoff_ms = kBaseBackoffMs * (1 << attempt);
        spdlog::warn("[OpenAIProvider] batch request failed ({}), retrying in {}ms (attempt {}/{})",
                     reason, backoff_ms, attempt + 1, kMaxRetries);
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    }

    try {
        auto parsed = nlohmann::json::parse(resp.body);
        for (auto& item : parsed.at("data")) {
            size_t idx = item.at("index").get<size_t>();
            if (idx < results.size()) {
                results[idx] = item.at("embedding").get<std::vector<float>>();
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("[OpenAIProvider] failed to parse batch response: {} — body: {}", e.what(), resp.body);
    }

    return results;
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

