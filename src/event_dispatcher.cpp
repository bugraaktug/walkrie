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
    pgcdc::EventJob current_job;
    while (running_.load(std::memory_order_acquire)) {
        if (queue_.wait_dequeue_timed(current_job, std::chrono::milliseconds(200))) {
            for (auto& sink : current_job.sinks) {
                try {
                    sink->call(current_job.ev);
                } catch (const std::exception& e) {
                    spdlog::error("[EventDispatcher] sink error: {}", e.what());
                }
            }
        }
    }
    // drain any remaining queued jobs before the thread exits
    while (queue_.try_dequeue(current_job)) {
        for (auto& sink : current_job.sinks) {
            try {
                sink->call(current_job.ev);
            } catch (const std::exception& e) {
                spdlog::error("[EventDispatcher] sink error: {}", e.what());
            }
        }
    }
}

} // namespace
