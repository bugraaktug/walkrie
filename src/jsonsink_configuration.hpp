#pragma once

#include "sink_configuration.hpp"
#include "config.hpp"

namespace pgcdc
{

class JsonSinkConfiguration : public SinkConfiguration
{
public:
    std::string type() const override { return "json-output"; }
    void load_from_config(const toml::table& t) override;
    std::vector<std::string> validate() const override { return {}; }
    std::shared_ptr<EventSink> create_sink(std::shared_ptr<EmbeddingProvider> provider) const override;
    bool needs_embedding_provider() const override { return false; }

    std::string output_target = "stdout";

};

} // namespace pgcdc
