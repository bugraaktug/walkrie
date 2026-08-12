#include "qdrantsink_configuration.hpp"
#include "qdrant_sink.hpp"
#include "embedding_provider.hpp"
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

void QdrantSinkConfiguration::load_from_config(const toml::table& t)
{
    url        = str_or(&t, "url",        url);
    api_key    = str_or(&t, "api_key",    api_key);
    collection = str_or(&t, "collection", collection);

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

std::vector<std::string> QdrantSinkConfiguration::validate() const
{
    std::vector<std::string> errors;
    if (url.empty()) {
        errors.push_back("[sink] url is required");
    }
    if (collection.empty()) {
        errors.push_back("[sink] collection is required");
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

std::shared_ptr<EventSink> QdrantSinkConfiguration::create_sink(std::shared_ptr<EmbeddingProvider> provider) const
{
    QdrantSinkConfig sink_cfg;
    sink_cfg.url        = url;
    sink_cfg.api_key    = api_key;
    sink_cfg.collection = collection;
    sink_cfg.mappings   = table_mappings;

    auto sink = std::make_shared<QdrantSink>(sink_cfg, provider);
    sink->init();

    spdlog::info("[SinkConfiguration] initialized sink — {}", type());
    return sink;
}

} // namespace pgcdc
