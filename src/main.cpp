#include <iostream>
#include <memory>
#include <vector>
#include <csignal>
#include <getopt.h>

#include <event2/event.h>
#include <spdlog/spdlog.h>
#include <spdlog/async.h> // Required for async thread pool
#include <spdlog/sinks/rotating_file_sink.h>

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
};

void on_shutdown_signal(evutil_socket_t sig, short, void* arg) 
{
    auto* ctx = static_cast<SignalHandle*>(arg);
    spdlog::info("Walkrie received signal {} — shutting down", sig);
    event_base_loopbreak(ctx->base);
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
    SignalHandle sig_hnd{ base, opts.pid_file, !opts.foreground };
    event* sigterm_ev = evsignal_new(base, SIGTERM, on_shutdown_signal, &sig_hnd);
    event* sigint_ev  = evsignal_new(base, SIGINT,  on_shutdown_signal, &sig_hnd);
    event_add(sigterm_ev, nullptr);
    event_add(sigint_ev, nullptr);

    auto dispatcher = std::make_unique<pgcdc::EventDispatcher>();  
    
    std::vector<std::unique_ptr<pgcdc::PgReplicationSource>> sources;
    for (const auto& src : cfg.sources) {
        pgcdc::PgReplicationConfig src_config;
	    src_config.host             = src.host;
	    src_config.port             = src.port;
    	src_config.dbname           = src.dbname;
    	src_config.user             = src.user;
    	src_config.password         = src.password;
    	src_config.slot_name        = src.slot_name;
    	src_config.publication_name = src.publication;
    	sources.push_back(std::make_unique<pgcdc::PgReplicationSource>(src_config));
    }

    std::vector<std::shared_ptr<pgcdc::EventSink>> sinks;
    for (const auto& sink_instance : cfg.sinks) {
        auto sink = sink_instance->create_sink(cfg.embedding);
        sinks.push_back(sink);
    }

    auto dispatch_handle = [&](const pgcdc::ChangeEvent& event) {
	    pgcdc::EventJob job;
	    job.ev = event;
	    job.sinks = sinks;
	    dispatcher->post_job(std::move(job));
    };

    for (auto& source : sources) {
        if (!source->connect()) {
            std::cerr << "Source connection failed: " << source->last_error() << "\n";
            spdlog::error("Source connection failed - {}", source->last_error());
            event_base_free(base);
            return 1;
        }
        if (!source->start_streaming()) {
            std::cerr << "Source replication streaming failed: " << source->last_error() << "\n";
            spdlog::error("Source replication streaming failed - {}", source->last_error());
            event_base_free(base);
            return 1;
        }
        
        if (!source->register_event_loop(base, dispatch_handle)) 
        {
            std::cerr << "Source event loop failed: " << source->last_error() << "\n";
            spdlog::error("Source event loop failed - {}", source->last_error());
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

    sources.clear();
    event_free(sigterm_ev);
    event_free(sigint_ev);
    event_base_free(base);
    
    pgcdc::http_global_cleanup();
    if (!opts.foreground) {
        pgcdc::remove_pid_file(opts.pid_file);
    }

    dispatcher.reset();
    sinks.clear();

    spdlog::info("Walkrie shutdown complete.");
    spdlog::shutdown();
    
    return 0;
}
