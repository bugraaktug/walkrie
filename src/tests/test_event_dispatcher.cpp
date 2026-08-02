// test_event_dispatcher.cpp
#include "doctest.h"
#include "event_dispatcher.hpp"
#include "event_sink.hpp"
#include "event.hpp"

#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_map>
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

// Fails the first `fail_times` calls, then succeeds — verifies retry-then-recover.
class FlakySink : public pgcdc::EventSink
{
public:
    explicit FlakySink(int fail_times) : fail_times_(fail_times) {}

    void call(const pgcdc::ChangeEvent&) override
    {
        if (++call_count_ <= fail_times_) throw std::runtime_error("simulated transient failure");
    }

    int call_count() const { return call_count_.load(); }

private:
    int fail_times_;
    std::atomic<int> call_count_{0};
};

// Records on_confirmed(source_id, lsn) calls, keyed by source. Tracks only
// the latest LSN reported per source — confirmation is meant to be
// monotonic, so "did we eventually reach LSN X for source S" is the
// meaningful assertion, not the exact number/grouping of calls it took to
// get there (that's a batching-timing detail tests shouldn't depend on).
class ConfirmRecorder
{
public:
    void record(SourceId id, uint64_t lsn)
    {
        std::lock_guard<std::mutex> lock(mu_);
        last_lsn_[id] = lsn;
        ++call_count_;
    }

    uint64_t last_lsn(SourceId id) const
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = last_lsn_.find(id);
        return it == last_lsn_.end() ? 0 : it->second;
    }

    size_t call_count() const
    {
        std::lock_guard<std::mutex> lock(mu_);
        return call_count_;
    }

private:
    mutable std::mutex mu_;
    std::unordered_map<SourceId, uint64_t> last_lsn_;
    size_t call_count_ = 0;
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

    TEST_CASE("a throwing best-effort sink does not prevent other sinks from being called")
    {
        pgcdc::EventDispatcher dispatcher;
        auto bad_sink = std::make_shared<ThrowingSink>();
        auto good_sink = std::make_shared<RecordingSink>();

        pgcdc::EventJob job;
        job.ev = make_test_event(42);
        job.sinks = {pgcdc::SinkHandle(bad_sink, /*required=*/false), good_sink};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return good_sink->call_count() == 1; },
                            std::chrono::milliseconds(500)));
        CHECK(good_sink->received()[0].commit_lsn == 42);
    }

    TEST_CASE("a throwing best-effort sink does not prevent subsequent jobs from being processed")
    {
        pgcdc::EventDispatcher dispatcher;
        auto bad_sink = std::make_shared<ThrowingSink>();
        auto good_sink = std::make_shared<RecordingSink>();

        for (uint64_t lsn : {10, 20}) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {pgcdc::SinkHandle(bad_sink, /*required=*/false), good_sink};
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

    TEST_CASE("on_confirmed is called with the job's source and LSN")
    {
        pgcdc::EventDispatcher dispatcher;
        ConfirmRecorder recorder;
        dispatcher.set_on_confirmed([&recorder](SourceId id, uint64_t lsn) { recorder.record(id, lsn); });

        auto sink = std::make_shared<RecordingSink>();
        pgcdc::EventJob job;
        job.ev = make_test_event(100);
        job.source_id = 7;
        job.sinks = {sink};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return recorder.last_lsn(7) == 100; },
                            std::chrono::milliseconds(500)));
    }

    TEST_CASE("on_confirmed tracks each source's LSN independently")
    {
        pgcdc::EventDispatcher dispatcher;
        ConfirmRecorder recorder;
        dispatcher.set_on_confirmed([&recorder](SourceId id, uint64_t lsn) { recorder.record(id, lsn); });

        auto sink = std::make_shared<RecordingSink>();
        std::vector<std::pair<SourceId, uint64_t>> posted = {
            {1, 10}, {2, 5}, {1, 20}, {2, 15}, {1, 30}
        };
        for (auto [source_id, lsn] : posted) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.source_id = source_id;
            job.sinks = {sink};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return recorder.last_lsn(1) == 30 && recorder.last_lsn(2) == 15; },
                            std::chrono::milliseconds(500)));
    }

    TEST_CASE("on_confirmed fires for jobs drained on destruction")
    {
        auto sink = std::make_shared<RecordingSink>();
        ConfirmRecorder recorder;
        {
            pgcdc::EventDispatcher dispatcher;
            dispatcher.set_on_confirmed([&recorder](SourceId id, uint64_t lsn) { recorder.record(id, lsn); });
            for (uint64_t lsn = 0; lsn < 20; ++lsn) {
                pgcdc::EventJob job;
                job.ev = make_test_event(lsn);
                job.source_id = 3;
                job.sinks = {sink};
                dispatcher.post_job(std::move(job));
            }
            // dispatcher destructs here — drain_remaining() must still notify_confirmed,
            // exercising the same path main.cpp relies on to write back a final LSN on shutdown.
        }
        CHECK(recorder.last_lsn(3) == 19);
    }

    TEST_CASE("a throwing best-effort sink does not retry and does not block confirmation")
    {
        pgcdc::EventDispatcher dispatcher;
        ConfirmRecorder recorder;
        dispatcher.set_on_confirmed([&recorder](SourceId id, uint64_t lsn) { recorder.record(id, lsn); });

        auto bad_sink = std::make_shared<ThrowingSink>();
        pgcdc::EventJob job;
        job.ev = make_test_event(50);
        job.source_id = 9;
        job.sinks = {pgcdc::SinkHandle(bad_sink, /*required=*/false)};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return recorder.last_lsn(9) == 50; },
                            std::chrono::milliseconds(500)));
    }

    TEST_CASE("a required sink is retried with backoff and on_confirmed fires once it recovers")
    {
        // Tiny backoff/ceiling — keeps the retry loop's real sleeps well under the test timeout.
        pgcdc::EventDispatcher dispatcher(1, std::chrono::milliseconds(50),
                                           std::chrono::milliseconds(5), std::chrono::milliseconds(500));
        ConfirmRecorder recorder;
        dispatcher.set_on_confirmed([&recorder](SourceId id, uint64_t lsn) { recorder.record(id, lsn); });

        auto flaky = std::make_shared<FlakySink>(2); // fails twice, then succeeds — required by default

        pgcdc::EventJob job;
        job.ev = make_test_event(50);
        job.source_id = 11;
        job.sinks = {flaky};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return recorder.last_lsn(11) == 50; },
                            std::chrono::milliseconds(1000)));
        CHECK(flaky->call_count() == 3);
    }

    TEST_CASE("a required sink that never recovers triggers on_fatal and withholds confirmation")
    {
        pgcdc::EventDispatcher dispatcher(1, std::chrono::milliseconds(50),
                                           std::chrono::milliseconds(5), std::chrono::milliseconds(20));
        ConfirmRecorder recorder;
        dispatcher.set_on_confirmed([&recorder](SourceId id, uint64_t lsn) { recorder.record(id, lsn); });

        std::atomic<int> fatal_count{0};
        dispatcher.set_on_fatal([&fatal_count] { ++fatal_count; });

        auto bad_sink = std::make_shared<ThrowingSink>(); // required by default

        pgcdc::EventJob job;
        job.ev = make_test_event(99);
        job.source_id = 12;
        job.sinks = {bad_sink};

        dispatcher.post_job(std::move(job));

        REQUIRE(wait_until([&] { return fatal_count.load() == 1; },
                            std::chrono::milliseconds(1000)));
        CHECK(recorder.last_lsn(12) == 0);
    }
}
