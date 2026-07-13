// test_event_dispatcher.cpp
#include "doctest.h"
#include "event_dispatcher.hpp"
#include "event_sink.hpp"
#include "event.hpp"

#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <vector>

namespace 
{

class RecordingSink : public pgcdc::EventSink 
{
public:
    void call(const pgcdc::ChangeEvent& ev) override 
    {
        std::lock_guard<std::mutex> lock(mu_);
        received_.push_back(ev);
        ++call_count_;
    }

    size_t call_count() const { return call_count_.load(); }

    std::vector<pgcdc::ChangeEvent> received() const 
    {
        std::lock_guard<std::mutex> lock(mu_);
        return received_;
    }

private:
    mutable std::mutex mu_;
    std::vector<pgcdc::ChangeEvent> received_;
    std::atomic<size_t> call_count_{0};
};

// Sink that always throws — verifies one bad sink doesn't take down the rest.
class ThrowingSink : public pgcdc::EventSink 
{
public:
    void call(const pgcdc::ChangeEvent&) override 
    {
        throw std::runtime_error("simulated sink failure");
    }
};

pgcdc::ChangeEvent make_test_event(uint64_t lsn) 
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Insert;
    ev.schema_name = "public";
    ev.table_name = "test_table";
    ev.commit_lsn = lsn;
    return ev;
}

// Polls with a short sleep until `pred` is true or timeout elapses.
// Avoids sleeping a fixed "long enough" duration in every test.
template <typename Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout) 
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

} // namespace

TEST_SUITE("EventDispatcher") 
{

    TEST_CASE("dispatches a single job to its sink") 
    {
        pgcdc::EventDispatcher dispatcher;
        auto sink = std::make_shared<RecordingSink>();

        pgcdc::EventJob job;
        job.ev = make_test_event(100);
        job.sinks = {sink};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return sink->call_count() == 1; },
                            std::chrono::milliseconds(500)));
        CHECK(sink->received()[0].commit_lsn == 100);
    }

    TEST_CASE("dispatches multiple jobs, each to all its sinks") 
    {
        pgcdc::EventDispatcher dispatcher;
        auto sink_a = std::make_shared<RecordingSink>();
        auto sink_b = std::make_shared<RecordingSink>();

        for (uint64_t lsn : {1, 2, 3}) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {sink_a, sink_b};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return sink_a->call_count() == 3; },
                            std::chrono::milliseconds(500)));
        CHECK(sink_b->call_count() == 3);
    }

    TEST_CASE("a throwing sink does not prevent other sinks from being called") 
    {
        pgcdc::EventDispatcher dispatcher;
        auto bad_sink = std::make_shared<ThrowingSink>();
        auto good_sink = std::make_shared<RecordingSink>();

        pgcdc::EventJob job;
        job.ev = make_test_event(42);
        job.sinks = {bad_sink, good_sink};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return good_sink->call_count() == 1; },
                            std::chrono::milliseconds(500)));
        CHECK(good_sink->received()[0].commit_lsn == 42);
    }

    TEST_CASE("a throwing sink does not prevent subsequent jobs from being processed") 
    {
        pgcdc::EventDispatcher dispatcher;
        auto bad_sink = std::make_shared<ThrowingSink>();
        auto good_sink = std::make_shared<RecordingSink>();

        for (uint64_t lsn : {10, 20}) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {bad_sink, good_sink};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return good_sink->call_count() == 2; },
                            std::chrono::milliseconds(500)));
    }

    TEST_CASE("destructor drains remaining queued jobs before exiting") 
    {
        auto sink = std::make_shared<RecordingSink>();
        {
            pgcdc::EventDispatcher dispatcher;
            // Post several jobs immediately before destruction — the worker
            // thread may not have processed any of them yet.
            for (uint64_t lsn = 0; lsn < 20; ++lsn) {
                pgcdc::EventJob job;
                job.ev = make_test_event(lsn);
                job.sinks = {sink};
                dispatcher.post_job(std::move(job));
            }
            // dispatcher destructs here — must drain, not drop, pending jobs
        }
        CHECK(sink->call_count() == 20);
    }
}
