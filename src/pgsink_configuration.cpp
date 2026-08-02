#include "pgsink_configuration.hpp"
#include "pgembedding_sink.hpp"
#include "pg_sql_builder.hpp"
#include "embedding_provider.hpp"
#include <sstream>
#include <spdlog/spdlog.h>

namespace pgcdc
{

namespace
{

std::string str_or(const toml::table* t, const char* key, const std::string& def) 
{
    if (!t) return def;
    auto v = t->get_as<std::string>(key);
    return v ? **v : def;
}

}

void PgSinkConfiguration::load_from_config(const toml::table& t) 
{
    host     = str_or(&t, "host",     host);
    port     = str_or(&t, "port",     port);
    dbname   = str_or(&t, "dbname",   dbname);
    user     = str_or(&t, "user",     user);
    password = str_or(&t, "password", password);
    table    = str_or(&t, "table",    table);
    embedding_column = str_or(&t, "embed_column", embedding_column);

    if (auto req = t.get_as<bool>("required")) required_override = **req;

    if (auto* tm_arr = t["table_mapping"].as_array()) {
        for (auto& tm_elem : *tm_arr) {
            TableMapping tm;
            if (auto* tm_tbl = tm_elem.as_table()) {
                tm.source_table = str_or(tm_tbl, "source_table", "");
                std::string disc_col = str_or(tm_tbl, "discriminator_column", "");
                std::string disc_lbl = str_or(tm_tbl, "discriminator_label", "");
                if (!disc_col.empty()) {
                    tm.has_discriminator_   = true;
                    tm.discriminator_sink_  = disc_col;
                    tm.discriminator_label_ = disc_lbl;
                }
                if (auto* col_arr = (*tm_tbl)["columns"].as_array()) {
                    for (auto& col_elem : *col_arr) {
                        ColumnMapping m;
                        if (auto* c = col_elem.as_table()) {
                            m.source_column = str_or(c, "source_column", "");
                            m.sink_column   = str_or(c, "sink_column",   "");
                            m.role          = str_or(c, "role",          "");
                        }
                        if (!m.source_column.empty() && !m.role.empty())
                            tm.columns.push_back(m);
                    }
                }
                tm.resolve_columns();
            }
            if (!tm.source_table.empty())
                table_mappings.push_back(tm);
        }
    }
}

std::vector<std::string> PgSinkConfiguration::validate() const 
{
    std::vector<std::string> errors;
    if (dbname.empty()) {
        errors.push_back("[sink] dbname is required");
    }
    if (user.empty()) {
        errors.push_back("[sink] user is required");
    }
    if (table.empty()) { 
        errors.push_back("[sink] table is required");
    }
    if (embedding_column.empty()) {
        errors.push_back("[sink] embedding column is required");
    }
    if (table_mappings.empty())
        errors.push_back("[sink] at least one [[sink.table_mapping]] block is required");

    for (const auto& tm : table_mappings) {
        if (tm.source_table.empty()) {
            errors.push_back("[sink.table_mapping] source_table is required");
        }
        if (tm.id_source_.empty()) {
            errors.push_back("[sink.table_mapping:" + tm.source_table + "] needs a mapping with role='id'");
        }
        if (tm.embed_source_.empty()) {
            errors.push_back("[sink.table_mapping:" + tm.source_table + "] needs a mapping with role='embed'");
        }
        if (tm.has_discriminator_ && tm.discriminator_label_.empty()) {
            errors.push_back("[sink.table_mapping:" + tm.source_table + "] discriminator_column is set but discriminator_label is empty");
        }
    }
    return errors;
}

std::shared_ptr<EventSink> PgSinkConfiguration::create_sink(const EmbeddingConfig& embedding_cfg) const 
{
    std::shared_ptr<pgcdc::EmbeddingProvider> provider;
    try {
        provider = pgcdc::make_embedding_provider(embedding_cfg);
        provider->init();
    } catch (const std::exception& e) {
        spdlog::info("[EmbeddingProvider] failed to initialized — {}", e.what());
        throw std::runtime_error("EmbeddingProvider: failed to create context");
    }

    std::ostringstream conn;
    conn << "host=" << host 
         << " port=" << port 
         << " dbname=" << dbname
         << " user=" << user 
         << " password=" << password;

    PgEmbeddingSinkConfig sink_cfg;
    sink_cfg.pg_conninfo = conn.str();
    sink_cfg.sink_table  = table;
    sink_cfg.sink_column = embedding_column;
    sink_cfg.mappings    = table_mappings;

    auto sink = std::make_shared<PgEmbeddingSink>(sink_cfg, provider);
    sink->init();
    
    spdlog::info("[SinkConfiguration] initialized sink — {}", type());
    return sink;
}

} // namespace pgcdc
