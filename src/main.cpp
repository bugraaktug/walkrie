#include <iostream>
#include <memory>
#include <vector>
#include <event2/event.h>

#include "pgreplication_source.hpp"

int main(int argc, char** argv) 
{
    pgcdc::PgReplicationConfig config;
    config.dbname = "qdb";
    config.user = "quser";
    config.password = "quser1234";
    config.slot_name = "cdc_slot";
    config.publication_name = "test_pub";
    
    event_base* base = event_base_new();
    if (!base) {
        std::cerr << "event_base_new() failed\n";
        return 1;
    }
  
    std::vector<std::unique_ptr<pgcdc::PgReplicationSource>> sources;
    sources.push_back(std::make_unique<pgcdc::PgReplicationSource>(config));
 

    auto handle = [](const pgcdc::ChangeEvent& event) {
	auto j = ordered_json(event);
	std::cout << j.dump(2) << "\n";
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
        
	if (!source->register_event_loop(base, handle)) 
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
