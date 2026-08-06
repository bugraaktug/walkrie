#pragma once

#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <variant>
#include <memory>
#include <functional>
#include <unordered_map>
#include "readerwriterqueue.hpp"

#include "event_sink.hpp"
#include "pgoutput_parser.hpp"
#include "replication_source.hpp"

namespace pgcdc
{

struct SinkHandle
{
    std::shared_ptr<EventSink> sink;
    bool required;

    template <typename T>
    SinkHandle(std::shared_ptr<T> s) : sink(std::move(s)), required(sink->default_required()) {}
    
    template <typename T>
    SinkHandle(std::shared_ptr<T> s, bool req) : sink(std::move(s)), required(req) {}
};

struct EventJob
{
    ChangeEvent ev;
    SourceId source_id = 0;
    std::vector<SinkHandle> sinks;
};

class EventDispatcher
{
public:
    using ConfirmFn = std::function<void(SourceId, uint64_t)>;
    using FatalFn = std::function<void()>;

    explicit EventDispatcher(size_t max_batch_size = 1,
                             std::chrono::milliseconds batch_timeout = std::chrono::milliseconds(50),
                             std::chrono::milliseconds required_retry_initial_backoff = std::chrono::milliseconds(500),
                             std::chrono::milliseconds required_retry_ceiling = std::chrono::seconds(60))
        : running_(true)
        , max_batch_size_(max_batch_size)
        , batch_timeout_(batch_timeout)
        , required_retry_initial_backoff_(required_retry_initial_backoff)
        , required_retry_ceiling_(required_retry_ceiling)
    {
        worker_thread_ = std::thread(&EventDispatcher::process_jobs, this);
    }

    ~EventDispatcher();
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;

    void post_job(pgcdc::EventJob job);
    void set_on_confirmed(ConfirmFn fn) { on_confirmed_ = std::move(fn); }
    void set_on_fatal(FatalFn fn) { on_fatal_ = std::move(fn); }

private:
    void process_jobs();
    void drain_remaining(); // process remining jobs on system stop etc
    void notify_confirmed(const std::unordered_map<SourceId, uint64_t>& batch_max_lsn);
    bool dispatch(const std::vector<ChangeEvent>& batch_events, const std::vector<SinkHandle>& sinks);
    bool dispatch_with_retry(const SinkHandle& handle, const std::vector<ChangeEvent>& batch_events); // <<< false iff this sink was required and breached the ceiling

    moodycamel::BlockingReaderWriterQueue<EventJob> queue_;
    std::thread worker_thread_;
    std::atomic<bool> running_;

    size_t max_batch_size_;
    std::chrono::milliseconds batch_timeout_;
    std::chrono::milliseconds required_retry_initial_backoff_;
    std::chrono::milliseconds required_retry_ceiling_;

    ConfirmFn on_confirmed_;
    FatalFn on_fatal_;
    bool fatal_triggered_ = false; // <<< worker-thread-only; ensures on_fatal_ fires once
};

} // namespace
