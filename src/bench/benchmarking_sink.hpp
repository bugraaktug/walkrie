#pragma once

#include <memory>
#include <chrono>
#include "event_sink.hpp"
#include "bench_stats.hpp"

namespace pgcdc::bench {

class BenchmarkingSink : public pgcdc::EventSink
{
public:
    BenchmarkingSink(std::shared_ptr<pgcdc::EventSink> inner, LagStats& stats)
        : inner_(std::move(inner)), stats_(stats) {}

    void call(const pgcdc::ChangeEvent& event) override {
        auto now = std::chrono::system_clock::now();
        int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();

        if (event.commit_timestamp > 0) {
            int64_t lag_us = now_us - static_cast<int64_t>(event.commit_timestamp);
            stats_.record(lag_us);
        }

        if (first_event_time_.time_since_epoch().count() == 0) {
            first_event_time_ = std::chrono::steady_clock::now();
        }
        last_event_time_ = std::chrono::steady_clock::now();

        inner_->call(event);

        // Increment last, after all work for this row is done — this is
        // the signal a completion-checker should actually wait on.
        processed_count_.fetch_add(1, std::memory_order_release);
    }

    size_t processed_count() const {
        return processed_count_.load(std::memory_order_acquire);
    }

    std::chrono::steady_clock::time_point first_event_time() const { return first_event_time_; }
    std::chrono::steady_clock::time_point last_event_time() const { return last_event_time_; }

private:
    std::shared_ptr<pgcdc::EventSink> inner_;
    LagStats& stats_;
    std::atomic<size_t> processed_count_{0};
    std::chrono::steady_clock::time_point first_event_time_{};
    std::chrono::steady_clock::time_point last_event_time_{};
};

} // namespace pgcdc::bench
