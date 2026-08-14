#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>
#include <cerrno>
#include <cstring>
#include <csignal>
#include <getopt.h>

#include <sys/wait.h>
#include <unistd.h>

#include <event2/event.h>
#include <spdlog/spdlog.h>
#include <spdlog/async.h> // Required for async thread pool
#include <spdlog/sinks/rotating_file_sink.h>

#include "backfill_manager.hpp"
#include "config.hpp"
#include "daemon_utils.hpp"
#include "event_dispatcher.hpp"
#include "http_client.hpp"
#include "pgembedding_sink.hpp"
#include "pgreplication_source.hpp"
#include "readerwriterqueue.hpp"

namespace 
{

struct Options 
{
    std::string config_path;
    std::string pid_file = "/run/walkrie/walkrie.pid";
    bool foreground = false;
    bool show_help = false;
};

void print_usage(const char* argv0) 
{
    std::cerr
        << "usage: " << argv0 << " -c <config.toml> [-f] [--pid-file <path>]\n"
        << "\n"
        << "  -c, --config <path>    path to config.toml (required)\n"
        << "  -f, --foreground       run in the foreground; do not daemonize\n"
        << "                         (use this under systemd — see walkrie.service)\n"
        << "      --pid-file <path> PID file path when daemonizing (default: /run/walkrie/walkrie.pid)\n"
        << "  -h, --help              show this message\n";
}

Options parse_args(int argc, char** argv) 
{
    Options opts;

    static struct option long_opts[] = {
        {"config",    required_argument, nullptr, 'c'},
        {"foreground", no_argument,      nullptr, 'f'},
        {"pid-file",  required_argument, nullptr, 'p'},
        {"help",      no_argument,       nullptr, 'h'},
        {nullptr, 0, nullptr, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "c:fh", long_opts, nullptr)) != -1) {
        switch (c) {
            case 'c': opts.config_path = optarg; break;
            case 'f': opts.foreground = true; break;
            case 'p': opts.pid_file = optarg; break;
            case 'h': opts.show_help = true; break;
            default:
                print_usage(argv[0]);
                std::exit(1);
        }
    }
    return opts;
}

}

void init_logger(const pgcdc::AppSettings& settings) 
{
    try {
        spdlog::init_thread_pool(8192, 1);
        auto async_file_logger = spdlog::create_async<spdlog::sinks::rotating_file_sink_mt> (
            "walkrie",
            settings.log_file,
            static_cast<size_t>(settings.log_max_size_mb) * 1024 * 1024,
            static_cast<size_t>(settings.log_max_files)
        );
        spdlog::set_default_logger(async_file_logger);
        spdlog::set_level(spdlog::level::from_str(settings.log_level));
        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%l] %v"); // [%t] prints thread ID
        spdlog::flush_every(std::chrono::seconds(3));

        spdlog::info("Walkrie logger initialized — level={} file={}",
                     settings.log_level, 
                     settings.log_file);
    } catch (const spdlog::spdlog_ex& e) {
        std::cerr << "Walkrie log initialization failed " << e.what() << "\n";
    }
}

struct SignalHandle
{
    event_base* base;
    std::string pid_file;
    bool daemonized;
    std::atomic<bool>* terminate = nullptr;
};

void on_shutdown_signal(evutil_socket_t sig, short, void* arg)
{
    auto* ctx = static_cast<SignalHandle*>(arg);
    spdlog::info("Walkrie received signal {} — shutting down", sig);
    event_base_loopbreak(ctx->base);
}

void on_terminate_poll(evutil_socket_t, short, void* arg)
{
    auto* ctx = static_cast<SignalHandle*>(arg);
    if (ctx->terminate->load(std::memory_order_acquire)) {
        spdlog::critical("Walkrie shutting down — a required sink stalled past its retry ceiling");
        event_base_loopbreak(ctx->base);
    }
}

std::filesystem::path resolve_backfill_worker_path(const char* argv0)
{
    std::error_code ec;
    auto self_exe = std::filesystem::canonical("/proc/self/exe", ec);
    return (!ec ? self_exe.parent_path() : std::filesystem::path(argv0).parent_path()) / "walkrie_worker";
}

int main(int argc, char** argv)
{
    Options opts = parse_args(argc, argv);

    if (opts.show_help || opts.config_path.empty()) {
        print_usage(argv[0]);
        return opts.show_help ? 0 : 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(opts.config_path);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }

    auto errors = cfg.validate();
    if (!errors.empty()) {
        std::cerr << "[Configuration] config validation failed:\n";
        for (auto& e : errors) { 
	        spdlog::error(e);
            std::cerr << "  - " << e << "\n";
        }
        return 1;
    }
    
    const std::filesystem::path backfill_worker_path = resolve_backfill_worker_path(argv[0]);

    if (!opts.foreground) {
        pgcdc::daemonize(opts.pid_file);
    }

    signal(SIGPIPE, SIG_IGN);

    init_logger(cfg.settings);
    
    pgcdc::http_global_init();
    event_base* base = event_base_new();
    if (!base) {
        spdlog::error("[Dispatcher] event_base_new() failed to initialize...");
        return 1;
    }
    
    // Graceful shutdown on SIGTERM (systemctl stop / kill) and SIGINT
    // (Ctrl-C in foreground mode).
    std::atomic<bool> terminate{false};
    SignalHandle sig_hnd{ base, opts.pid_file, !opts.foreground, &terminate };
    event* sigterm_ev = evsignal_new(base, SIGTERM, on_shutdown_signal, &sig_hnd);
    event* sigint_ev  = evsignal_new(base, SIGINT,  on_shutdown_signal, &sig_hnd);
    event_add(sigterm_ev, nullptr);
    event_add(sigint_ev, nullptr);

    // Polls `terminate` 200ms adds negligible shutdown latency.
    event* terminate_poll_ev = event_new(base, -1, EV_PERSIST, on_terminate_poll, &sig_hnd);
    timeval terminate_poll_interval{0, 200000};
    event_add(terminate_poll_ev, &terminate_poll_interval);

    // <<< unique_ptr so we can reset() it before event_base_free(base) on every exit path —
    // its dtor frees an event tied to base, so it must not outlive base.
    auto backfill_manager = std::make_unique<pgcdc::BackfillManager>(base, opts.config_path);
    backfill_manager->set_worker_path(backfill_worker_path);

    auto dispatcher = std::make_unique<pgcdc::EventDispatcher>(
            cfg.settings.batch_size,
            std::chrono::milliseconds(cfg.settings.batch_timeout_ms));

    std::vector<pgcdc::TableMapping> backfill_table_mappings; // <<< union across all sinks' mappings, same scope live dispatch already uses
    for (const auto& sink_instance : cfg.sinks) {
        auto tms = sink_instance->mappings();
        backfill_table_mappings.insert(backfill_table_mappings.end(), tms.begin(), tms.end());
    }

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
    	src_config.backfill         = src.backfill;
    	src_config.tls              = src.tls;
    	if (src.backfill) {
    	    src_config.backfill_table_mappings = backfill_table_mappings;
    	    src_config.backfill_store_path     = cfg.settings.backfill_dir + "/" + src.slot_name + ".sqlite3";
    	}
    	sources.push_back(std::make_unique<pgcdc::PgReplicationSource>(next_source_id++, src_config));
    }

    std::unordered_map<SourceId, pgcdc::ReplicationSource*> source_by_id;
    for (auto& source : sources) {
        source_by_id[source->id()] = source.get();
    }    
 
    std::vector<pgcdc::SinkHandle> sinks;
    try {
        std::shared_ptr<pgcdc::EmbeddingProvider> shared_provider = any_sink_needs_provider(cfg)
            ? pgcdc::create_initialized_embedding_provider(cfg.embedding)
            : nullptr;

        for (const auto& sink_instance : cfg.sinks) {
            auto sink = sink_instance->create_sink(sink_instance->needs_embedding_provider() ? shared_provider : nullptr);
            bool required = sink_instance->required_override.value_or(sink->default_required());
            sinks.emplace_back(sink, required);
        }
    } catch (const std::exception& e) {
        spdlog::critical("Fatal: failed to initialize sink/embedding provider: {}", e.what());
        dispatcher.reset();
        backfill_manager.reset();
        event_free(sigterm_ev);
        event_free(sigint_ev);
        event_free(terminate_poll_ev);
        event_base_free(base);

        pgcdc::http_global_cleanup();
        if (!opts.foreground) {
            pgcdc::remove_pid_file(opts.pid_file);
        }
        spdlog::shutdown();
        return 1;
    }

    dispatcher->set_on_fatal([&terminate] {
        terminate.store(true, std::memory_order_release);
    });

    dispatcher->set_on_confirmed([&source_by_id](SourceId id, uint64_t lsn) {
        if (auto it = source_by_id.find(id); it != source_by_id.end()) {
            it->second->set_confirmed_lsn(lsn);
        }
    });

    auto dispatch_handle = [&](const pgcdc::ChangeEvent& event, SourceId source_id) {
	    pgcdc::EventJob job;
	    job.ev = event;
	    job.source_id = source_id;
	    job.sinks = sinks;
	    dispatcher->post_job(std::move(job));
    };
   
    for (auto& source : sources) {
        if (!source->connect()) {
            std::cerr << "Source connection failed: " << source->last_error() << "\n";
            spdlog::error("Source connection failed - {}", source->last_error());
            backfill_manager.reset();
            event_base_free(base);
            return 1;
        }

        if (!source->run_backfill_dump_if_required()) {
            std::cerr << "Source backfill dump failed: " << source->last_error() << "\n";
            spdlog::error("Source backfill dump failed - {}", source->last_error());
            backfill_manager.reset();
            event_base_free(base);
            return 1;
        }

        auto spawn_status = backfill_manager->spawn_if_required(*source);
        if (spawn_status != pgcdc::BackfillSpawnStatus::Spawned) {
            pgcdc::BackfillManager::log_spawn_status(spawn_status, *source, backfill_worker_path);
        }

        if (!source->start_streaming()) {
            std::cerr << "Source replication streaming failed: " << source->last_error() << "\n";
            spdlog::error("Source replication streaming failed - {}", source->last_error());
            backfill_manager.reset();
            event_base_free(base);
            return 1;
        }

        if (!source->register_event_loop(base, dispatch_handle))
        {
            std::cerr << "Source event loop failed: " << source->last_error() << "\n";
            spdlog::error("Source event loop failed - {}", source->last_error());
            backfill_manager.reset();
            event_base_free(base);
            return 1;
        }
    }

    spdlog::info("Walkrie system running — {} source(s), pid={}", sources.size(), getpid());
    if (opts.foreground) {
        std::cout << "Walkrie running in foreground (pid=" << getpid()
                  << "). Ctrl-C or SIGTERM to stop.\n";
    }
    
    event_base_dispatch(base);

    for (auto& source : sources) {
        spdlog::info("Finalizing source streaming - {}", source->last_error());
    }

    spdlog::info("Waiting for backfill workers to finish...");
    backfill_manager->wait_for_all_workers();
    backfill_manager.reset(); // frees reap_timer_ — must happen before event_base_free(base) below

    dispatcher.reset(); // joins the worker thread — drain_remaining() has already run and updated confirmed_lsn_ per source

    for (auto& source : sources) {
        source->flush_confirmed_lsn(); // periodic timer won't fire again post-loopbreak; send the final ack now
    }

    sources.clear();
    event_free(sigterm_ev);
    event_free(sigint_ev);
    event_free(terminate_poll_ev);
    event_base_free(base);

    pgcdc::http_global_cleanup();
    if (!opts.foreground) {
        pgcdc::remove_pid_file(opts.pid_file);
    }

    sinks.clear();

    spdlog::info("Walkrie shutdown complete.");
    spdlog::shutdown();
    
    return 0;
}
