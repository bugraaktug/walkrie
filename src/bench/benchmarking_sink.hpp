#pragma once
#include <memory>
#include <chrono>
#include "event_sink.hpp"
#include "bench_stats.hpp"

namespace pgcdc::bench 
{

class BenchmarkingSink : public pgcdc::EventSink
{
public:
    BenchmarkingSink(std::shared_ptr<pgcdc::EventSink> inner, LagStats& stats)
        : inner_(std::move(inner)), stats_(stats) {}

    void call(const pgcdc::ChangeEvent& event) override 
    {
        auto now = std::chrono::system_clock::now();
        int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

        if (event.commit_timestamp > 0) {
            int64_t lag_us = now_us - static_cast<int64_t>(event.commit_timestamp);
            stats_.record(lag_us);
        }

        ++processed_count_;
        inner_->call(event);
    }

    size_t processed_count() const { return processed_count_; }

private:
    std::shared_ptr<pgcdc::EventSink> inner_;
    LagStats& stats_;
    size_t processed_count_ = 0;
};

} // namespace pgcdc::bench
