// test_event_dispatcher_batching.cpp
#include <doctest.h>
#include "event_dispatcher.hpp"
#include "event_sink.hpp"
#include "event.hpp"

#include <chrono>
#include <thread>
#include <mutex>
#include <vector>
#include <atomic>

namespace 
{

pgcdc::ChangeEvent make_test_event(uint64_t lsn) 
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Insert;
    ev.schema_name = "public";
    ev.table_name = "test_table";
    ev.commit_lsn = lsn;
    return ev;
}

// Records every call_batch() invocation as a distinct batch — lets tests
// inspect exactly how the dispatcher grouped events, not just the total
// count. Thread-safe: call_batch runs on the dispatcher's worker thread,
// tests read from the main thread.
class BatchRecordingSink : public pgcdc::EventSink 
{
public:
    void call(const pgcdc::ChangeEvent& event) override {
        // Should not normally be hit — the dispatcher always routes
        // through call_batch(), even for single-event batches. Recorded
        // separately so a test can assert this path stays unused.
        std::lock_guard<std::mutex> lock(mu_);
        single_call_count_++;
        (void)event;
    }

    void call_batch(const std::vector<pgcdc::ChangeEvent>& events) override {
        std::lock_guard<std::mutex> lock(mu_);
        batches_.push_back(events);
        total_events_ += events.size();
    }

    size_t batch_count() const {
        std::lock_guard<std::mutex> lock(mu_);
        return batches_.size();
    }

    size_t total_events() const {
        std::lock_guard<std::mutex> lock(mu_);
        return total_events_;
    }

    size_t single_call_count() const {
        std::lock_guard<std::mutex> lock(mu_);
        return single_call_count_;
    }

    size_t max_batch_seen() const {
        std::lock_guard<std::mutex> lock(mu_);
        size_t m = 0;
        for (auto& b : batches_) m = std::max(m, b.size());
        return m;
    }

    std::vector<size_t> batch_sizes() const {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<size_t> sizes;
        for (auto& b : batches_) sizes.push_back(b.size());
        return sizes;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::vector<pgcdc::ChangeEvent>> batches_;
    size_t total_events_ = 0;
    size_t single_call_count_ = 0;
};

class ThrowingBatchSink : public pgcdc::EventSink 
{
public:
    void call(const pgcdc::ChangeEvent&) override {
        throw std::runtime_error("simulated single-call failure");
    }
    void call_batch(const std::vector<pgcdc::ChangeEvent>&) override {
        throw std::runtime_error("simulated batch failure");
    }
};

template <typename Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

} // namespace

TEST_SUITE("EventDispatcher batching") 
{

    TEST_CASE("default construction (batch_size=1) processes every event as its own batch of one") 
    {
        pgcdc::EventDispatcher dispatcher; // defaults: max_batch_size=1
        auto sink = std::make_shared<BatchRecordingSink>();

        for (uint64_t lsn : {1, 2, 3}) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {sink};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return sink->total_events() == 3; },
                            std::chrono::milliseconds(1000)));

        // With batch_size=1, every recorded batch must have exactly one event.
        for (auto size : sink->batch_sizes()) {
            CHECK(size == 1);
        }
        // call() (the unbatched path) should never be hit — the dispatcher
        // always routes through call_batch(), even for size-1 batches.
        CHECK(sink->single_call_count() == 0);
    }

    TEST_CASE("events posted faster than the timeout are grouped up to max_batch_size") 
    {
        // Long timeout relative to how fast we post, so the timeout branch
        // shouldn't be what triggers the flush here — max_batch_size should.
        pgcdc::EventDispatcher dispatcher(4, std::chrono::milliseconds(500));
        auto sink = std::make_shared<BatchRecordingSink>();

        for (uint64_t lsn = 0; lsn < 12; ++lsn) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {sink};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return sink->total_events() == 12; },
                            std::chrono::milliseconds(2000)));

        // No batch should ever exceed the configured max.
        CHECK(sink->max_batch_seen() <= 4);
        // At least one batch should have actually reached the max — this
        // is the real evidence that grouping (not just per-event dispatch)
        // is happening, given the posting rate here is much faster than
        // the 500ms timeout.
        CHECK(sink->max_batch_seen() == 4);
    }

    TEST_CASE("a lone event still gets processed promptly via the timeout, not held forever") 
    {
        // max_batch_size is large; with only one event ever posted, the
        // ONLY way it gets flushed is the timeout branch.
        pgcdc::EventDispatcher dispatcher(10, std::chrono::milliseconds(100));
        auto sink = std::make_shared<BatchRecordingSink>();

        pgcdc::EventJob job;
        job.ev = make_test_event(42);
        job.sinks = {sink};
        dispatcher.post_job(std::move(job));

        // Should flush at or shortly after the 100ms timeout — well under
        // a generous ceiling, not stuck waiting for 9 more events that
        // will never arrive.
        REQUIRE(wait_until([&] { return sink->total_events() == 1; },
                            std::chrono::milliseconds(500)));
        CHECK(sink->batch_count() == 1);
        CHECK(sink->batch_sizes()[0] == 1);
    }

    TEST_CASE("all events are eventually delivered, none dropped, across multiple batches") 
    {
        pgcdc::EventDispatcher dispatcher(5, std::chrono::milliseconds(50));
        auto sink = std::make_shared<BatchRecordingSink>();

        constexpr int kTotal = 37; // deliberately not a clean multiple of batch size
        for (uint64_t lsn = 0; lsn < kTotal; ++lsn) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {sink};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return sink->total_events() == kTotal; },
                            std::chrono::milliseconds(2000)));
        CHECK(sink->max_batch_seen() <= 5);
    }

    TEST_CASE("multiple sinks each receive the full batch independently") 
    {
        pgcdc::EventDispatcher dispatcher(3, std::chrono::milliseconds(200));
        auto sink_a = std::make_shared<BatchRecordingSink>();
        auto sink_b = std::make_shared<BatchRecordingSink>();

        for (uint64_t lsn = 0; lsn < 6; ++lsn) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {sink_a, sink_b};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return sink_a->total_events() == 6 && sink_b->total_events() == 6; },
                            std::chrono::milliseconds(1000)));
        CHECK(sink_a->batch_sizes() == sink_b->batch_sizes());
    }

    TEST_CASE("a throwing best-effort sink's call_batch does not prevent other sinks or subsequent batches")
    {
        pgcdc::EventDispatcher dispatcher(3, std::chrono::milliseconds(100));
        auto bad_sink  = std::make_shared<ThrowingBatchSink>();
        auto good_sink = std::make_shared<BatchRecordingSink>();

        for (uint64_t lsn = 0; lsn < 5; ++lsn) {
            pgcdc::EventJob job;
            job.ev = make_test_event(lsn);
            job.sinks = {pgcdc::SinkHandle(bad_sink, /*required=*/false), good_sink};
            dispatcher.post_job(std::move(job));
        }

        REQUIRE(wait_until([&] { return good_sink->total_events() == 5; },
                            std::chrono::milliseconds(1000)));
    }

    TEST_CASE("destructor drains remaining queued jobs in batches respecting max_batch_size") 
    {
        auto sink = std::make_shared<BatchRecordingSink>();
        {
            pgcdc::EventDispatcher dispatcher(4, std::chrono::milliseconds(50));
            // Post immediately before destruction — the worker thread may
            // not have processed any of these yet.
            for (uint64_t lsn = 0; lsn < 15; ++lsn) {
                pgcdc::EventJob job;
                job.ev = make_test_event(lsn);
                job.sinks = {sink};
                dispatcher.post_job(std::move(job));
            }
            // dispatcher destructs here — must drain everything, batched.
        }
        CHECK(sink->total_events() == 15);
        CHECK(sink->max_batch_seen() <= 4);
    }
}
