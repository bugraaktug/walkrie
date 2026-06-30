#include "event_dispatcher.hpp"

#include <cstdio>
#include <sstream>

namespace pgcdc
{

EventDispatcher::~EventDispatcher() 
{
    if (worker_thread_.joinable()) {
	worker_thread_.join();
    }	
}

void EventDispatcher::post_job(const pgcdc::ChangeEvent job) 
{
    queue_.enqueue(std::move(job));
}

void EventDispatcher::process_jobs()
{
    pgcdc::ChangeEvent current_job;
    while (running_) {
        if (queue_.try_dequeue(current_job)) {
	    std::cout << "received change event \n";	
	}
	else {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

} // namespace
