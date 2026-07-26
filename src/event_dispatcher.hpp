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
    explicit EventDispatcher(size_t max_batch_size = 1,
                             std::chrono::milliseconds batch_timeout = std::chrono::milliseconds(50))
        : running_(true)
        , max_batch_size_(max_batch_size)
        , batch_timeout_(batch_timeout)
    {
        worker_thread_ = std::thread(&EventDispatcher::process_jobs, this);
    }
   
    ~EventDispatcher();
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;
    
    void post_job(pgcdc::EventJob job);

private:
    void process_jobs(); 
    void drain_remaining(); // process remining jobs on system stop etc

    moodycamel::BlockingReaderWriterQueue<EventJob> queue_;
    std::thread worker_thread_;
    std::atomic<bool> running_;

    size_t max_batch_size_;
    std::chrono::milliseconds batch_timeout_;
};

} // namespace 
