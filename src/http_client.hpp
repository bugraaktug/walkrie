#pragma once

#include <string>
#include <vector>

// Forward-declare to avoid leaking <curl/curl.h> into every includer.
typedef void CURL;

namespace pgcdc
{

struct HttpResponse
{
    long        status_code = 0;
    std::string body;
    bool        curl_ok     = false;   // false if the transport itself failed (DNS, timeout, etc.) — check this before status_code
    std::string curl_error;            // populated only when curl_ok == false
};

// Not thread-safe — if you need concurrent requests, use one HttpClient
// per thread, or add a mutex around perform_post().
class HttpClient
{
public:
    explicit HttpClient(int timeout_secs = 30);
    ~HttpClient();

    HttpClient(const HttpClient&) = delete;
    HttpClient& operator=(const HttpClient&) = delete;

    // (e.g. {"Authorization: Bearer ..."}). Content-Type: application/json
    // is added automatically — don't include it in `extra_headers`.
    HttpResponse post_json(const std::string& url,
                            const std::string& json_body,
                            const std::vector<std::string>& extra_headers);

private:
    CURL* curl_ = nullptr;
    int   timeout_secs_;
};

// Call from main()
void http_global_init();
void http_global_cleanup();

} // namespace pgcdc
