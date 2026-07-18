#pragma once

#include <memory>
#include <string>
#include <vector>
#include <toml.hpp>
#include "event_sink.hpp"

namespace pgcdc
{

struct EmbeddingConfig; // forward decl — only pg-style embedding sinks need this

class SinkConfiguration
{
public:
    virtual ~SinkConfiguration() = default;

    virtual std::string type() const = 0;
    virtual void load_from_config(const toml::table& t) = 0;
    virtual std::vector<std::string> validate() const = 0;
    virtual std::shared_ptr<EventSink> create_sink(const EmbeddingConfig& embedding_cfg) const = 0;
};

std::unique_ptr<SinkConfiguration> instantiate_sink(const std::string& type);

} // namespace pgcdc
