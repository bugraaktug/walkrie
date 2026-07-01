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
	        std::cout << "received change event job \n";
            current_job.sink->call(current_job.ev);    
	    }
	    else {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

} // namespace
