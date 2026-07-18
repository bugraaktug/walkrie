#include <iostream>
#include <memory>
#include <vector>
#include <event2/event.h>
#include <spdlog/spdlog.h>
#include <spdlog/async.h> // Required for async thread pool
#include <spdlog/sinks/rotating_file_sink.h>

#include "config.hpp"
#include "event_dispatcher.hpp"
#include "http_client.hpp"
#include "pgembedding_sink.hpp"
#include "pgreplication_source.hpp"
#include "readerwriterqueue.hpp"

void init_logger(const pgcdc::AppSettings& settings) {
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
        std::cerr << "Walkrie log initialization failed %s\n: " << e.what() << "\n";
    }
}


int main(int argc, char** argv) 
{
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml>\n"
                  << "example: " << argv[0] << " /etc/walkrie/config.toml\n";
        return 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }

    init_logger(cfg.settings);
    auto errors = cfg.validate();
    if (!errors.empty()) {
        std::cerr << "[Configuration] config validation failed:\n";
        for (auto& e : errors) { 
	        spdlog::error(e);
            std::cerr << "  - " << e << "\n";
        }
        return 1;
    }

    pgcdc::http_global_init();
    event_base* base = event_base_new();
    if (!base) {
        spdlog::error("[Dispatcher] event_base_new() failed to initialize...");
        return 1;
    }
    
    pgcdc::EventDispatcher dispatcher; 
    
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
	    dispatcher.post_job(std::move(job));
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

    std::cout << "Walkrie system running..." << "\n";
    std::cerr << "Running event loop for " << sources.size() << " source(s)... (Ctrl-C to stop)\n";
    
    spdlog::info("Walkrie system running...");
    spdlog::info("Running event loop for - {} source(s)...(Ctrl-C to stop)", sources.size());
    
    event_base_dispatch(base);

    for (auto& source : sources) {
        spdlog::info("Finalizing source streaming - {}", source->last_error());
    }

    event_base_free(base);
    
    pgcdc::http_global_cleanup();
    spdlog::shutdown();
    
    spdlog::info("Walkrie system shutting down...");
    return 0;
}
