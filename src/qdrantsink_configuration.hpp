#pragma once
#include "sink_configuration.hpp"
#include "config.hpp"

namespace pgcdc
{

class QdrantSinkConfiguration : public SinkConfiguration
{
public:
    std::string type() const override { return "qdrant"; }
    void load_from_config(const toml::table& t) override;
    std::vector<std::string> validate() const override;
    std::shared_ptr<EventSink> create_sink(std::shared_ptr<EmbeddingProvider> provider) const override;
    std::vector<TableMapping> mappings() const override { return table_mappings; }

    std::string url = "http://localhost:6333";
    std::string api_key;
    std::string collection;
    std::vector<TableMapping> table_mappings;
};

} // namespace pgcdc
