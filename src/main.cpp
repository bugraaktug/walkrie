#include <iostream>
#include <memory>
#include <vector>
#include <event2/event.h>

#include "readerwriterqueue.hpp"
#include "event_dispatcher.hpp"
#include "pgreplication_source.hpp"

int main(int argc, char** argv) 
{
    pgcdc::PgReplicationConfig config_test;
    config_test.dbname = "qdb";
    config_test.user = "quser";
    config_test.password = "quser1234";
    config_test.slot_name = "cdc_slot";
    config_test.publication_name = "test_pub";
    
    pgcdc::PgReplicationConfig config_pgcdc;
    config_pgcdc.dbname = "qdb";
    config_pgcdc.user = "quser";
    config_pgcdc.password = "quser1234";
    config_pgcdc.slot_name = "pgcdc_slot";
    config_pgcdc.publication_name = "pgcdc_pub";
    
    event_base* base = event_base_new();
    if (!base) {
        std::cerr << "event_base_new() failed\n";
        return 1;
    }
  
    std::vector<std::unique_ptr<pgcdc::PgReplicationSource>> sources;
    sources.push_back(std::make_unique<pgcdc::PgReplicationSource>(config_test));
    sources.push_back(std::make_unique<pgcdc::PgReplicationSource>(config_pgcdc));

    pgcdc::EventDispatcher dispatcher; 
    
    auto dispatch_handle = [&dispatcher](const pgcdc::ChangeEvent& event) {
	//auto j = ordered_json(event);
	//std::cout << j.dump(2) << "\n";
	dispatcher.post_job(event);
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
