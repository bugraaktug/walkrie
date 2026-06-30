#pragma once

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <variant>
#include <memory>
#include "readerwriterqueue.hpp"

#include "pgoutput_parser.hpp"

namespace pgcdc 
{

class EventDispatcher 
{
public:
    EventDispatcher() : running_(true) {
        worker_thread_ = std::thread(&EventDispatcher::process_jobs, this);
    }
    
    ~EventDispatcher() ;
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;
    
    void post_job(const pgcdc::ChangeEvent job);

private:
    void process_jobs(); 
    
    moodycamel::ReaderWriterQueue<ChangeEvent> queue_;
    std::thread worker_thread_;
    bool running_;
};

} // namespace 
