#include "sink_configuration.hpp"

#include "pgsink_configuration.hpp"
#include "jsonsink_configuration.hpp"
#include "config.hpp"

#include <stdexcept>

namespace pgcdc
{

std::unique_ptr<SinkConfiguration> instantiate_sink(const std::string& type)
{
    if (type == "postgres-embedding") {	
        return std::make_unique<PgSinkConfiguration>();
    } else if (type == "json-output") {	
        return std::make_unique<JsonSinkConfiguration>();
    }

    throw std::runtime_error(
        "make_sink: unknown sink type '" + type + "' — "
        "valid values: 'postgres-embedding', 'json-output'");
}

}
