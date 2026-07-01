#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <variant>
#include <memory>
#include "readerwriterqueue.hpp"

#include "pgoutput_parser.hpp"

using ChangeEventFn = std::function<void(const pgcdc::ChangeEvent&)>;

namespace pgcdc 
{

struct EventSink 
{
    virtual void call(const pgcdc::ChangeEvent&) = 0;
    virtual ~EventSink() = default;
};

struct EventJob 
{
    ChangeEvent ev;
    std::shared_ptr<EventSink> sink; // shared, not per-job allocated
};

class EventDispatcher 
{
public:
    EventDispatcher() : running_(true) {
        worker_thread_ = std::thread(&EventDispatcher::process_jobs, this);
    }
    
    ~EventDispatcher() ;
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;
    
    void post_job(pgcdc::EventJob job);

private:
    void process_jobs(); 
    
    moodycamel::ReaderWriterQueue<EventJob> queue_;
    std::thread worker_thread_;
    std::atomic<bool> running_;
};

} // namespace 
