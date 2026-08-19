#pragma once
#include "sink_configuration.hpp"
#include "config.hpp"

namespace pgcdc
{

class MilvusSinkConfiguration : public SinkConfiguration
{
public:
    std::string type() const override { return "milvus"; }
    void load_from_config(const toml::table& t) override;
    std::vector<std::string> validate() const override;
    std::shared_ptr<EventSink> create_sink(std::shared_ptr<EmbeddingProvider> provider) const override;
    std::vector<TableMapping> mappings() const override { return table_mappings; }

    std::string url = "http://localhost:19530";
    std::string token;
    std::string db_name;
    std::string collection;
    std::vector<TableMapping> table_mappings;
};

} // namespace pgcdc
