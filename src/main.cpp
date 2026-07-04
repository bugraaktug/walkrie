#include <iostream>
#include <memory>
#include <vector>
#include <event2/event.h>

#include "config.hpp"
#include "readerwriterqueue.hpp"
#include "event_dispatcher.hpp"
#include "pgembedding_sink.hpp"
#include "pgreplication_source.hpp"

std::shared_ptr<pgcdc::EventSink> create_json_print_sink()
{
    struct JsonPrintSink : pgcdc::EventSink {
        void call(const pgcdc::ChangeEvent& event) override {
	        auto j = ordered_json(event);
            std::cout << j.dump(2) << "\n";
	    }
    }; 
    return std::make_shared<JsonPrintSink>();
}


std::shared_ptr<pgcdc::EventSink> create_pgembedding_sink(const pgcdc::AppConfig& cfg)
{
    std::shared_ptr<pgcdc::EmbeddingProvider> provider;
    try {
        provider = pgcdc::make_embedding_provider(cfg.embedding);
        provider->init();
    } catch (const std::exception& e) {
        std::cerr << "embedding provider init failed: " << e.what() << "\n";
        throw std::runtime_error("EmbeddingProvider: failed to create context");
    }

    // --- PgEmbeddingSink config from AppConfig ---
    pgcdc::PgEmbeddingSinkConfig sink_cfg;
    {
        std::ostringstream conn;
        conn << "host="     << cfg.sink.host
             << " port="    << cfg.sink.port
             << " dbname="  << cfg.sink.dbname
             << " user="    << cfg.sink.user
             << " password=" << cfg.sink.password;
        sink_cfg.pg_conninfo  = conn.str();
        sink_cfg.sink_table   = cfg.sink.table;
        sink_cfg.embed_column = cfg.embedding.embed_column;
        sink_cfg.id_column    = cfg.embedding.id_column;
    }

    auto pg_sink = std::make_shared<pgcdc::PgEmbeddingSink>(sink_cfg, provider);
    pg_sink->init();

    return pg_sink;
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

    auto errors = cfg.validate();
    if (!errors.empty()) {
        std::cerr << "config validation failed:\n";
        for (auto& e : errors) 
	    std::cerr << "  - " << e << "\n";
        return 1;
    }

    event_base* base = event_base_new();
    if (!base) {
        std::cerr << "event_base_new() failed\n";
        return 1;
    }
  
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


    pgcdc::EventDispatcher dispatcher; 

    std::vector<std::shared_ptr<pgcdc::EventSink>> sinks;
    sinks.push_back(create_pgembedding_sink(cfg));
    sinks.push_back(create_json_print_sink());

    auto dispatch_handle = [&](const pgcdc::ChangeEvent& event) {
	    pgcdc::EventJob job;
	    job.ev = event;
	    job.sinks = sinks;
	    dispatcher.post_job(std::move(job));
    };

    for (auto& source : sources) {
        if (!source->connect()) {
            std::cerr << "connect failed: " << source->last_error() << "\n";
            event_base_free(base);
            return 1;
        }
        if (!source->start_streaming()) {
            std::cerr << "start_streaming failed: " << source->last_error() << "\n";
            event_base_free(base);
            return 1;
        }
        
        if (!source->register_event_loop(base, dispatch_handle)) 
        {
            std::cerr << "start_event_loop failed: " << source->last_error() << "\n";
            event_base_free(base);
            return 1;
        }
    }

    std::cerr << "running event loop for " << sources.size() << " source(s)... (Ctrl-C to stop)\n";
    event_base_dispatch(base);

    for (auto& source : sources) {
        std::cerr << "source ended: " << source->last_error() << "\n";
    }

    event_base_free(base);
    return 0;
}
