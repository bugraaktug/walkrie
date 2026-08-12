#include "sink_configuration.hpp"

#include "pgsink_configuration.hpp"
#include "jsonsink_configuration.hpp"
#include "qdrantsink_configuration.hpp"
#include "config.hpp"

#include <stdexcept>

namespace pgcdc
{

std::unique_ptr<SinkConfiguration> instantiate_sink(const std::string& type)
{
    if (type == "pgvector") {
        return std::make_unique<PgSinkConfiguration>();
    } else if (type == "json-output") {
        return std::make_unique<JsonSinkConfiguration>();
    } else if (type == "qdrant") {
        return std::make_unique<QdrantSinkConfiguration>();
    }

    throw std::runtime_error(
        "make_sink: unknown sink type '" + type + "' — "
        "valid values: 'pgvector', 'json-output', 'qdrant'");
}

}
