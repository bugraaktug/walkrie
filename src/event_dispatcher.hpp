#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <variant>
#include <memory>
#include "readerwriterqueue.hpp"

#include "event_sink.hpp"
#include "pgoutput_parser.hpp"

namespace pgcdc 
{

struct EventJob 
{
    ChangeEvent ev;
    std::vector<std::shared_ptr<EventSink>> sinks;
};

class EventDispatcher 
{
public:
    EventDispatcher() : running_(true) {
        worker_thread_ = std::thread(&EventDispatcher::process_jobs, this);
    }
    
    ~EventDispatcher();
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;
    
    void post_job(pgcdc::EventJob job);

private:
    void process_jobs(); 
    
    moodycamel::BlockingReaderWriterQueue<EventJob> queue_;
    std::thread worker_thread_;
    std::atomic<bool> running_;
};

} // namespace 
