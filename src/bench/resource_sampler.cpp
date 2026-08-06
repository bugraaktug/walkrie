// resource_sampler.cpp
#include "resource_sampler.hpp"
#include <cstdint>
#include <fstream>
#include <sstream>
#include <sys/resource.h>

namespace pgcdc::bench 
{

namespace 
{

int64_t process_cpu_time_micros() 
{
    struct rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return -1;
    }
    int64_t user = static_cast<int64_t>(usage.ru_utime.tv_sec) * 1'000'000 + usage.ru_utime.tv_usec;
    int64_t sys  = static_cast<int64_t>(usage.ru_stime.tv_sec) * 1'000'000 + usage.ru_stime.tv_usec;
    return user + sys;
}

long current_rss_kb() 
{
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line.substr(6));
            long kb;
            iss >> kb;
            return kb;
        }
    }
    return -1;
}

} // namespace

ResourceSampler::ResourceSampler(std::chrono::milliseconds interval)
    : interval_(interval) {}

ResourceSampler::~ResourceSampler() 
{
    stop();
}

void ResourceSampler::start() 
{
    running_ = true;
    thread_ = std::thread(&ResourceSampler::run, this);
}

void ResourceSampler::stop() 
{
    running_ = false;
    if (thread_.joinable()) thread_.join();
}

void ResourceSampler::run() 
{
    while (running_.load(std::memory_order_relaxed)) {
        ResourceSample s;
        s.at = std::chrono::steady_clock::now();
        s.cpu_time_micros = process_cpu_time_micros();
        s.rss_kb = current_rss_kb();
        samples_.push_back(s);
        std::this_thread::sleep_for(interval_);
    }
}

ResourceSampler::Report ResourceSampler::summarize() const 
{
    Report r;
    if (samples_.size() < 2) return r;

    long rss_sum = 0;
    double cpu_pct_sum = 0;
    size_t cpu_pct_n = 0;

    for (size_t i = 0; i < samples_.size(); ++i) {
        rss_sum += samples_[i].rss_kb;
        r.peak_rss_kb = std::max(r.peak_rss_kb, samples_[i].rss_kb);

        if (i > 0) {
            auto wall_delta_us = std::chrono::duration_cast<std::chrono::microseconds>(
                samples_[i].at - samples_[i - 1].at).count();
            auto cpu_delta_us = samples_[i].cpu_time_micros - samples_[i - 1].cpu_time_micros;
            if (wall_delta_us > 0) {
                double pct = (static_cast<double>(cpu_delta_us) / wall_delta_us) * 100.0;
                cpu_pct_sum += pct;
                ++cpu_pct_n;
                r.peak_cpu_percent = std::max(r.peak_cpu_percent, pct);
            }
        }
    }

    r.avg_rss_kb = rss_sum / static_cast<long>(samples_.size());
    r.avg_cpu_percent = cpu_pct_n > 0 ? cpu_pct_sum / static_cast<double>(cpu_pct_n) : 0;
    return r;
}

} // namespace pgcdc::bench
