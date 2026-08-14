#pragma once
#include "sink_configuration.hpp"
#include "config.hpp"

namespace pgcdc
{

class PgSinkConfiguration : public SinkConfiguration
{
public:
    std::string type() const override { return "pgvector"; }
    void load_from_config(const toml::table& t) override;
    std::vector<std::string> validate() const override;
    std::shared_ptr<EventSink> create_sink(std::shared_ptr<EmbeddingProvider> provider) const override;
    std::vector<TableMapping> mappings() const override { return table_mappings; }
    
    std::string host     = "localhost";
    std::string port     = "5432";
    std::string dbname;
    std::string user;
    std::string password;
    std::string table;
    std::string embedding_column;
    std::vector<TableMapping> table_mappings;
    pgcdc::TlsConfig tls;

};

} // namespace pgcdc
