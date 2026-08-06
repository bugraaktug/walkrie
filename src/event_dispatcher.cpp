#include "event_dispatcher.hpp"

#include <algorithm>
#include <cstdint>
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

bool EventDispatcher::dispatch_with_retry(const SinkHandle& handle, const std::vector<pgcdc::ChangeEvent>& batch_events)
{
    auto backoff = required_retry_initial_backoff_;
    auto start = std::chrono::steady_clock::now();
    for (;;) {
        try {
            handle.sink->call_batch(batch_events);
            return true;
        } catch (const std::exception& e) {
            if (!handle.required) {
                spdlog::error("[EventDispatcher] best-effort sink '{}' batch error: {}", handle.sink->name(), e.what());
                return true; // <<< best-effort: log and move on, not a dispatch failure
            }
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (elapsed >= required_retry_ceiling_) {
                spdlog::critical("[EventDispatcher] required sink '{}' stalled past {}ms, giving up: {}",
                                  handle.sink->name(), required_retry_ceiling_.count(), e.what());
                return false;
            }
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(required_retry_ceiling_ - elapsed);
            auto sleep_for = std::min(remaining, backoff);
            spdlog::warn("[EventDispatcher] required sink '{}' batch error, retrying in {}ms: {}",
                          handle.sink->name(), sleep_for.count(), e.what());
            std::this_thread::sleep_for(sleep_for);
            backoff *= 2;
        }
    }
}

bool EventDispatcher::dispatch(const std::vector<pgcdc::ChangeEvent>& batch_events, const std::vector<SinkHandle>& sinks)
{
    for (const auto& handle : sinks) {
        if (!dispatch_with_retry(handle, batch_events)) {
            if (!fatal_triggered_ && on_fatal_) {
                fatal_triggered_ = true;
                on_fatal_();
            }
            return false;
        }
    }
    return true;
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
        std::unordered_map<SourceId, uint64_t> batch_max_lsn;
        batch_max_lsn[current_job.source_id] = current_job.ev.commit_lsn;
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
            auto& lsn = batch_max_lsn[next_job.source_id];
            lsn = std::max(lsn, next_job.ev.commit_lsn);
            batch_events.push_back(std::move(next_job.ev));
        }

        if (!dispatch(batch_events, sinks)) {
            break; // <<< required sink fatally stalled; don't confirm this batch, stop taking more work
        }
        notify_confirmed(batch_max_lsn);
    }
    if (!fatal_triggered_) {
        drain_remaining(); // drain any remaining queued jobs before the thread exits
    }
}

void EventDispatcher::drain_remaining()
{
    pgcdc::EventJob current;
    std::vector<pgcdc::ChangeEvent> batch_events;
    std::vector<SinkHandle> sinks;
    std::unordered_map<SourceId, uint64_t> batch_max_lsn;

    auto flush = [&]() -> bool {
        if (batch_events.empty()) return true;
        bool ok = dispatch(batch_events, sinks);
        if (ok) notify_confirmed(batch_max_lsn);
        batch_events.clear();
        batch_max_lsn.clear();
        return ok;
    };

    while (queue_.try_dequeue(current)) {
        if (sinks.empty()) {
            sinks = current.sinks;
        }
        auto& lsn = batch_max_lsn[current.source_id];
        lsn = std::max(lsn, current.ev.commit_lsn);
        batch_events.push_back(std::move(current.ev));
        if (batch_events.size() >= max_batch_size_) {
            if (!flush()) return; // <<< required sink fatally stalled during drain; stop
        }
    }
    flush(); // flush any partial batch left over at the end
}

void EventDispatcher::notify_confirmed(const std::unordered_map<SourceId, uint64_t>& batch_max_lsn)
{
    if (!on_confirmed_) return;
    for (const auto& [source_id, lsn] : batch_max_lsn) {
        on_confirmed_(source_id, lsn);
    }
}

} // namespace
