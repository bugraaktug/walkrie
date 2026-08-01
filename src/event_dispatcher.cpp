#include "event_dispatcher.hpp"

#include <cstdio>
#include <spdlog/spdlog.h>
#include <sstream>

namespace pgcdc
{

EventDispatcher::~EventDispatcher() 
{
    running_.store(false, std::memory_order_release);
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }	
}

void EventDispatcher::post_job(pgcdc::EventJob job) 
{
    queue_.enqueue(std::move(job));
}

void EventDispatcher::process_jobs()
{
    spdlog::info("[EventDispatcher] started processing events, batch_size={}, batch_timeout_ms={}",
                 max_batch_size_, batch_timeout_.count());
    pgcdc::EventJob current_job;
    while (running_.load(std::memory_order_acquire)) {
        if (!queue_.wait_dequeue_timed(current_job, std::chrono::milliseconds(200))) {
            continue; // nothing arrived in this window — loop back, recheck running_
        }

        std::vector<pgcdc::ChangeEvent> batch_events;
        batch_events.reserve(max_batch_size_);
        batch_events.push_back(std::move(current_job.ev));

        // All jobs currently share one sinks list (built once in main.cpp
        // and copied into every EventJob) — taken from the first job here.
        auto sinks = current_job.sinks;

        auto batch_deadline = std::chrono::steady_clock::now() + batch_timeout_;

        while (batch_events.size() < max_batch_size_) {
            auto now = std::chrono::steady_clock::now();
            if (now >= batch_deadline) break;

            pgcdc::EventJob next_job;
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(batch_deadline - now);
            if (!queue_.wait_dequeue_timed(next_job, remaining)) {
                break; // no more arrived before the deadline — process what we have
            }
            batch_events.push_back(std::move(next_job.ev));
        }

        for (auto& sink : sinks) {
            try {
                sink->call_batch(batch_events);
            } catch (const std::exception& e) {
                spdlog::error("[EventDispatcher] sink batch error: {}", e.what());
            }
        }
    }
    // drain any remaining queued jobs before the thread exits
    drain_remaining();
}

void EventDispatcher::drain_remaining()
{
    pgcdc::EventJob current;
    std::vector<pgcdc::ChangeEvent> batch_events;
    std::vector<std::shared_ptr<pgcdc::EventSink>> sinks;

    auto flush = [&]() {
        if (batch_events.empty()) return;
        for (auto& sink : sinks) {
            try {
                sink->call_batch(batch_events);
            } catch (const std::exception& e) {
                spdlog::error("[EventDispatcher] sink batch error (drain): {}", e.what());
            }
        }
        batch_events.clear();
    };

    while (queue_.try_dequeue(current)) {
        if (sinks.empty()) {
            sinks = current.sinks;
        }
        batch_events.push_back(std::move(current.ev));
        if (batch_events.size() >= max_batch_size_) {
            flush();
        }
    }
    flush(); // flush any partial batch left over at the end
}

} // namespace
