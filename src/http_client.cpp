#include "http_client.hpp"

#include <curl/curl.h>
#include <spdlog/spdlog.h>

namespace pgcdc
{

namespace
{

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    auto* out = static_cast<std::string*>(userdata);
    out->append(ptr, size * nmemb);
    return size * nmemb;
}

} // namespace

void http_global_init()
{
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

void http_global_cleanup()
{
    curl_global_cleanup();
}

HttpClient::HttpClient(int timeout_secs)
    : timeout_secs_(timeout_secs) {
    curl_ = curl_easy_init();
    if (!curl_) {
        spdlog::error("[HttpClient] curl_easy_init failed");
    }
}

HttpClient::~HttpClient()
{
    if (curl_) {
        curl_easy_cleanup(curl_);
        curl_ = nullptr;
    }
}

HttpResponse HttpClient::perform(HttpMethod method,
                                  const std::string& url,
                                  const std::string& json_body,
                                  const std::vector<std::string>& extra_headers)
{
    HttpResponse resp;

    if (!curl_) {
        resp.curl_ok = false;
        resp.curl_error = "curl handle not initialized";
        return resp;
    }

    curl_easy_reset(curl_);

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    for (const auto& h : extra_headers) {
        headers = curl_slist_append(headers, h.c_str());
    }

    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);

    switch (method) {
        case HttpMethod::Get:
            curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L);
            break;
        case HttpMethod::Post:
            curl_easy_setopt(curl_, CURLOPT_POST, 1L);
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, json_body.c_str());
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
            break;
        case HttpMethod::Put:
            curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "PUT");
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, json_body.c_str());
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
            break;
        case HttpMethod::Delete:
            curl_easy_setopt(curl_, CURLOPT_CUSTOMREQUEST, "DELETE");
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, json_body.c_str());
            curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, static_cast<long>(json_body.size()));
            break;
    }

    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &resp.body);
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, static_cast<long>(timeout_secs_));

    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPIDLE, 60L);  // start probing after 60s idle
    curl_easy_setopt(curl_, CURLOPT_TCP_KEEPINTVL, 10L); // probe every 10s after that

    CURLcode res = curl_easy_perform(curl_);
    curl_slist_free_all(headers);

    if (res != CURLE_OK) {
        resp.curl_ok = false;
        resp.curl_error = curl_easy_strerror(res);
        return resp;
    }

    resp.curl_ok = true;
    curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &resp.status_code);
    return resp;
}

HttpResponse HttpClient::post_json(const std::string& url, const std::string& json_body, const std::vector<std::string>& extra_headers)
{
    return perform(HttpMethod::Post, url, json_body, extra_headers);
}

HttpResponse HttpClient::put_json(const std::string& url, const std::string& json_body, const std::vector<std::string>& extra_headers)
{
    return perform(HttpMethod::Put, url, json_body, extra_headers);
}

HttpResponse HttpClient::delete_json(const std::string& url, const std::string& json_body, const std::vector<std::string>& extra_headers)
{
    return perform(HttpMethod::Delete, url, json_body, extra_headers);
}

HttpResponse HttpClient::get_json(const std::string& url, const std::vector<std::string>& extra_headers)
{
    return perform(HttpMethod::Get, url, "", extra_headers);
}

} // namespace pgcdc
