#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <chrono>
#include <cstdint>

namespace pgcdc::bench 
{

struct ResourceSample 
{
    std::chrono::steady_clock::time_point at;
    int64_t cpu_time_micros;  // cumulative process CPU time at this sample
    long    rss_kb;           // resident set size at this sample
};

// Linux-specific (/proc/self/status) —
// flag for follow-up if a Windows/macOS benchmark build is ever needed.
class ResourceSampler
{
public:
    explicit ResourceSampler(std::chrono::milliseconds interval);
    ~ResourceSampler();

    void start();
    void stop();

    // Only valid after stop() — no locking, since the sampling thread has
    // been joined by then.
    const std::vector<ResourceSample>& samples() const { return samples_; }

    struct Report 
    {
        double avg_cpu_percent = 0;
        double peak_cpu_percent = 0;
        long   avg_rss_kb = 0;
        long   peak_rss_kb = 0;
    };
    Report summarize() const;

private:
    void run();

    std::chrono::milliseconds interval_;
    std::atomic<bool> running_{false};
    std::thread thread_;
    std::vector<ResourceSample> samples_;
};

} // namespace pgcdc::bench
