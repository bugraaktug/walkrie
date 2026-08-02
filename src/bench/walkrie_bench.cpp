#include <iostream>
#include <iomanip>
#include <memory>
#include <vector>
#include <atomic>
#include <csignal>
#include <thread>
#include <chrono>

#include <event2/event.h>
#include <spdlog/spdlog.h>

#include "config.hpp"
#include "event_dispatcher.hpp"
#include "http_client.hpp"
#include "pgembedding_sink.hpp"
#include "pgreplication_source.hpp"

#include "bench/bench_stats.hpp"
#include "bench/resource_sampler.hpp"
#include "bench/benchmarking_sink.hpp"

namespace 
{

std::atomic<bool> g_stop{false};
event_base* g_base = nullptr;

void handle_sigint(int) 
{
    g_stop = true;
    if (g_base) {
        event_base_loopbreak(g_base);
    }
}

} // namespace

int main(int argc, char** argv)
{
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--target-count N] [--duration-secs S]\n";
        return 1;
    }

    std::string config_path = argv[1];
    size_t target_count = 0;
    int duration_secs = 0;

    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--target-count" && i + 1 < argc) {
            target_count = static_cast<size_t>(std::stoul(argv[++i]));
        } else if (arg == "--duration-secs" && i + 1 < argc) {
            duration_secs = std::stoi(argv[++i]);
        }
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }

    auto errors = cfg.validate();
    if (!errors.empty()) {
        for (auto& e : errors) std::cerr << "config error: " << e << "\n";
        return 1;
    }

    if (cfg.sinks.empty()) {
        std::cerr << "benchmark requires exactly one configured sink\n";
        return 1;
    }
    if (cfg.sources.empty()) {
        std::cerr << "benchmark requires at least one configured source\n";
        return 1;
    }

    // spdlog not initialized here deliberately — file logging I/O would
    // pollute the resource measurements this binary exists to produce.
    // Sink/source internals may still call spdlog; if that turns out to
    // add noticeable overhead, redirect to a null sink for the benchmark.
    spdlog::set_level(spdlog::level::off);
    //spdlog::set_level(spdlog::level::from_str("debug"));

    pgcdc::bench::LagStats lag_stats;

    // Only the first configured sink is benchmarked — this binary is for
    // isolating one sink type's contribution to lag/CPU/RAM at a time
    // (json vs pg), not measuring a multi-sink fan-out.
    auto real_sink = cfg.sinks.front()->create_sink(cfg.embedding);
    auto bench_sink = std::make_shared<pgcdc::bench::BenchmarkingSink>(real_sink, lag_stats);
    std::vector<pgcdc::SinkHandle> sinks{ bench_sink };

    pgcdc::http_global_init();
    event_base* base = event_base_new();
    if (!base) {
        std::cerr << "event_base_new() failed\n";
        return 1;
    }
    g_base = base;
    signal(SIGINT, handle_sigint);

    pgcdc::EventDispatcher dispatcher;

    std::vector<std::unique_ptr<pgcdc::PgReplicationSource>> sources;
    SourceId next_source_id = 1;
    for (const auto& src : cfg.sources) {
        pgcdc::PgReplicationConfig src_config;
        src_config.host             = src.host;
        src_config.port             = src.port;
        src_config.dbname           = src.dbname;
        src_config.user             = src.user;
        src_config.password         = src.password;
        src_config.slot_name        = src.slot_name;
        src_config.publication_name = src.publication;
        sources.push_back(std::make_unique<pgcdc::PgReplicationSource>(next_source_id++, src_config));
    }

    auto dispatch_handle = [&](const pgcdc::ChangeEvent& event, SourceId source_id) {
        pgcdc::EventJob job;
        job.ev = event;
        job.source_id = source_id;
        job.sinks = sinks;
        dispatcher.post_job(std::move(job));
    };

    for (auto& source : sources) {
        if (!source->connect()) {
            std::cerr << "source connect failed: " << source->last_error() << "\n";
            event_base_free(base);
            return 1;
        }
        if (!source->start_streaming()) {
            std::cerr << "source start_streaming failed: " << source->last_error() << "\n";
            event_base_free(base);
            return 1;
        }
        if (!source->register_event_loop(base, dispatch_handle)) {
            std::cerr << "source register_event_loop failed: " << source->last_error() << "\n";
            event_base_free(base);
            return 1;
        }
    }

    pgcdc::bench::ResourceSampler sampler(std::chrono::milliseconds(200));
    sampler.start();

    std::thread duration_timer;
    if (duration_secs > 0) {
        duration_timer = std::thread([&]() {
            std::this_thread::sleep_for(std::chrono::seconds(duration_secs));
            g_stop = true;
            event_base_loopbreak(base);
        });
    }

    std::cout << "walkrie_bench running (sink=" << cfg.sinks.front()->type() << ")"
              << (target_count > 0 ? ", target=" + std::to_string(target_count) + " events" : "")
              << (duration_secs > 0 ? ", duration=" + std::to_string(duration_secs) + "s" : "")
              << " — Ctrl-C to stop early\n";


    struct CompletionCheckCtx {
        pgcdc::bench::BenchmarkingSink* sink;
        size_t target_count;
        event_base* base;
    };
    CompletionCheckCtx check_ctx{ bench_sink.get(), target_count, base };

    event* completion_timer = nullptr;
    if (target_count > 0) {
        completion_timer = event_new(base, -1, EV_PERSIST,
            [](evutil_socket_t, short, void* arg) {
                auto* ctx = static_cast<CompletionCheckCtx*>(arg);
                if (ctx->sink->processed_count() >= ctx->target_count) {
                    event_base_loopbreak(ctx->base);
                }
            },
            &check_ctx);
        struct timeval interval{0, 20000}; // check every 20ms — cheap, frequent enough
        event_add(completion_timer, &interval);
    }

    
    auto bench_start = std::chrono::steady_clock::now();
    spdlog::debug("[walkrie_bench] event loop starting now");
    event_base_dispatch(base);
    auto bench_end = std::chrono::steady_clock::now();

    if (duration_timer.joinable()) duration_timer.join();

    sampler.stop();
    // dispatcher's destructor drains remaining queued jobs and joins its
    // worker thread when it goes out of scope below.

    if (completion_timer) {
        event_free(completion_timer);
    }

    double wall_secs = std::chrono::duration<double>(bench_end - bench_start).count();
    size_t processed = bench_sink->processed_count();
    auto lag_summary = lag_stats.summarize();
    auto res_report = sampler.summarize();

    auto processing_secs = std::chrono::duration<double>(
    bench_sink->last_event_time() - bench_sink->first_event_time()).count();

   std::cout << "\n=== walkrie_bench report ===\n";
    std::cout << "sink type:         " << cfg.sinks.front()->type() << "\n";
    std::cout << "events processed:  " << processed << "\n";
    std::cout << "wall time:         " << std::fixed << std::setprecision(2) << wall_secs << " s\n";
    std::cout << "throughput:        " << std::fixed << std::setprecision(1)
               << (processed / std::max(wall_secs, 0.001)) << " events/sec\n";
    std::cout << "total wall time:      " << wall_secs << " s (includes connection setup)\n";
    std::cout << "pure processing time: " << processing_secs << " s (first row -> last row)\n";
    std::cout << "processing throughput: " << (processed / std::max(processing_secs, 0.001)) << " events/sec\n";
 
    if (lag_summary_count_warning(lag_stats)) {
        std::cout << "\n(no lag samples recorded — commit_timestamp may be 0; "
                     "confirm PgOutputParser::handle_begin fix is in place)\n";
    } else {
        std::cout << "\n-- lag (commit time -> sink processed) --\n";
        std::cout << "min:  " << lag_summary.min_us / 1000.0 << " ms\n";
        std::cout << "avg:  " << lag_summary.avg_us / 1000.0 << " ms\n";
        std::cout << "p50:  " << lag_summary.p50_us / 1000.0 << " ms\n";
        std::cout << "p95:  " << lag_summary.p95_us / 1000.0 << " ms\n";
        std::cout << "p99:  " << lag_summary.p99_us / 1000.0 << " ms\n";
        std::cout << "max:  " << lag_summary.max_us / 1000.0 << " ms\n";
    }

    std::cout << "\n-- resources (this process) --\n";
    std::cout << "avg CPU:  " << res_report.avg_cpu_percent << " % (may exceed 100% — multiple threads)\n";
    std::cout << "peak CPU: " << res_report.peak_cpu_percent << " %\n";
    std::cout << "avg RSS:  " << res_report.avg_rss_kb / 1024.0 << " MB\n";
    std::cout << "peak RSS: " << res_report.peak_rss_kb / 1024.0 << " MB\n";

    event_base_free(base);
    pgcdc::http_global_cleanup();
    return 0;
}
