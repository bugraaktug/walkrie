#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <toml.hpp>
#include "event_sink.hpp"

namespace pgcdc
{

class EmbeddingProvider; // forward decl — avoids a circular include with embedding_provider.hpp
struct TableMapping;     // forward decl — avoids a circular include with config.hpp

class SinkConfiguration
{
public:
    virtual ~SinkConfiguration() = default;

    virtual std::string type() const = 0;
    virtual void load_from_config(const toml::table& t) = 0;
    virtual std::vector<std::string> validate() const = 0;
    // provider is already constructed+init()'d by the caller — one shared instance across every
    // sink that needs one, see needs_embedding_provider(). Null iff !needs_embedding_provider().
    virtual std::shared_ptr<EventSink> create_sink(std::shared_ptr<EmbeddingProvider> provider) const = 0;
    virtual bool needs_embedding_provider() const { return true; } // <<< JsonSinkConfiguration overrides to false
    virtual std::vector<TableMapping> mappings() const { return {}; } // <<< only pg-style sinks have table mappings; not backfill-specific, kept generic for future callers

    std::optional<bool> required_override; // <<< from `required =` in [[sink]]; unset falls back to EventSink::default_required()
};

std::unique_ptr<SinkConfiguration> instantiate_sink(const std::string& type);

} // namespace pgcdc
