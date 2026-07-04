#include "event_dispatcher.hpp"

#include <cstdio>
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
    while (running_.load(std::memory_order_acquire) || queue_.size_approx() > 0) {
        if (queue_.try_dequeue(current_job)) {
	    for (auto& sink : current_job.sinks) {
                try {
                    sink->call(current_job.ev);
                } catch (const std::exception& e) {
                    std::fprintf(stderr, "sink error: %s\n", e.what());
                    // continue to next sink — one bad sink doesn't kill the others
                }
            }
	}
	else {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

} // namespace
