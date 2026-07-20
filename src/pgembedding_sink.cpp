#include "pgembedding_sink.hpp"
#include "pg_sql_builder.hpp"

#include <cstdio>
#include <cstring>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>

namespace pgcdc 
{

PgEmbeddingSink::PgEmbeddingSink(PgEmbeddingSinkConfig config,
                                 std::shared_ptr<EmbeddingProvider> provider)
    : config_(std::move(config))
    , provider_(std::move(provider)) {}

PgEmbeddingSink::~PgEmbeddingSink() 
{
    if (pg_) { 
        PQfinish(pg_); 
        pg_ = nullptr; 
    }
}

void PgEmbeddingSink::init() 
{
    sql_builder_ = std::make_unique<PgSqlBuilder>(config_.sink_table, config_.sink_column);

    for (const auto& tm : config_.mappings) {
        upsert_sql_list_[tm.source_table] = sql_builder_->build_upsert_sql(tm);
        spdlog::debug("[PgEmbeddingSink] upsert sql for mapping {} ready - {}",
                      tm.source_table, upsert_sql_list_[tm.source_table]);
        delete_sql_list_[tm.source_table] = sql_builder_->build_delete_sql(tm);
        spdlog::debug("[PgEmbeddingSink] delete sql for mapping {} ready - {}",
                      tm.source_table, delete_sql_list_[tm.source_table]);
    }

    // --- pgvector sink connection ---
    pg_ = PQconnectdb(config_.pg_conninfo.c_str());
    if (PQstatus(pg_) != CONNECTION_OK) {
        std::string err = PQerrorMessage(pg_);
        PQfinish(pg_);
        pg_ = nullptr;
        spdlog::error("[PgEmbeddingSink] pg connection failed - {} ", err); 
        throw std::runtime_error("EmbeddingSink: pg connection failed: " + err);
    }

    // Verify pgvector extension is installed — fail early rather than
    // getting a cryptic error on the first upsert.
    PGresult* res = PQexec(pg_, "SELECT 1 FROM pg_extension WHERE extname = 'vector'");
    bool has_vector = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0);
    PQclear(res);
    if (!has_vector) {
        spdlog::error("[PgEmbeddingSink] pgvector extension not installed in target db {}", config_.sink_table); 
        throw std::runtime_error("EmbeddingSink: pgvector extension not installed in target db. "
                                 "Run: CREATE EXTENSION vector;");
    }

    spdlog::info("[PgEmbeddingSink] pg connection ok, pgvector present for {} table", config_.sink_table);
}

void PgEmbeddingSink::call(const ChangeEvent& event) {
    const TableMapping* tm = nullptr;
    for (const auto& m : config_.mappings) {
        if (m.source_table == event.table_name) {
            tm = &m;
            break;
        }
    }
    if (!tm) {
        spdlog::warn("[PgEmbeddingSink] No mapping configured for - {}", event.table_name);
        return;
    }

    switch (event.op) {
	    case ChangeEvent::Op::Insert: {
            spdlog::debug("[PgEmbeddingSink] insert event received - {}", event.table_name);
            if (!event.new_row) { 
                spdlog::warn("[PgEmbeddingSink] insert event dropped, no new row received - {}", event.table_name);
                return;
            }
            const std::string id_val    = get_column(*event.new_row, tm->id_source_);
            const std::string embed_val = get_column(*event.new_row, tm->embed_source_);
            if (id_val.empty() || embed_val.empty())
            { 
                spdlog::warn("[PgEmbeddingSink] insert event dropped, no id source - {}", event.table_name);
                return;
            }
            // Collect metadata values in mapping order
            std::vector<std::string> meta_vals;
            for (const auto& m : tm->columns) {
                if (m.role == "metadata")
                    meta_vals.push_back(get_column(*event.new_row, m.source_column));
            }

            auto vec = provider_->embed(embed_val);
            if (vec.empty()) { 
                spdlog::warn("[PgEmbeddingSink] insert event dropped, no embedding value generated - {}", event.table_name);
                return;
            }
            upsert(*tm, id_val, embed_val, meta_vals, vec);
            break;
        }
        case ChangeEvent::Op::Update: {
            spdlog::debug("[PgEmbeddingSink] update event received - {}", event.table_name);
            if (!event.new_row)
            { 
                spdlog::warn("[PgEmbeddingSink] update event dropped, no new row received - {}", event.table_name);
                return;
            }
            // This is the core value proposition: skip the embedding API call
            // entirely if the embeddable column didn't change.
            //
            // Three cases where we skip:
            //   1. new value is unchanged_toast — Postgres didn't resend it
            //      because it didn't change (see ColumnValue::is_unchanged_toast)
            //   2. old and new text values are identical strings
            //   3. new text is empty/null — nothing to embed
            const std::string id_val    = get_column(*event.new_row, tm->id_source_);
            const std::string embed_val = get_column(*event.new_row, tm->embed_source_);

            // Core value prop: skip embedding call if the embed column didn't change
            if (is_toast(*event.new_row, tm->embed_source_)) {
                spdlog::warn("[PgEmbeddingSink] update event dropped, skip id={}: {} unchanged (toast) - {}", 
                             id_val.c_str(), 
                             tm->embed_source_.c_str(),
                             event.table_name);
                return;
            }
        
            if (event.old_row) {
                const std::string old_val = get_column(*event.old_row, tm->embed_source_);
                if (!old_val.empty() && old_val == embed_val) {
                    spdlog::warn("[PgEmbeddingSink] update event dropped, skip id={}: {} unchanged - {}",
                                 id_val.c_str(), 
                                 tm->embed_source_.c_str(),
                                 event.table_name);
                    return;
                }
            }

            if (id_val.empty() || embed_val.empty()) { 
                spdlog::warn("[PgEmbeddingSink] update event dropped, no id or embedding val  - {}", event.table_name);
                return;
            }

            std::vector<std::string> meta_vals;
            for (const auto& m : tm->columns) {
                if (m.role == "metadata")
                    meta_vals.push_back(get_column(*event.new_row, m.source_column));
            }

            auto vec = provider_->embed(embed_val);
            if (vec.empty()) { 
                spdlog::warn("[PgEmbeddingSink] update event dropped, no embedding value generated - {}", event.table_name);
                return;
            }
            upsert(*tm, id_val, embed_val, meta_vals, vec);    
            break;
        }
        case ChangeEvent::Op::Delete: {
            spdlog::debug("[PgEmbeddingSink] delete event received - {}", event.table_name);
            if (!event.old_row) { 
                spdlog::warn("[PgEmbeddingSink] delete event dropped, no old row received - {}", event.table_name);
                return;
            }
            const std::string id_val = get_column(*event.old_row, tm->id_source_);
            if (!id_val.empty()) { 
                remove(*tm, id_val);
            } else {
                spdlog::warn("[PgEmbeddingSink] delete event dropped, no id val  - {}", event.table_name);
            }
            break;
        }
    }
}

std::string PgEmbeddingSink::get_column(const DecodedRow& row, const std::string& col_name) 
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) {
            if (col.is_null || col.is_unchanged_toast) 
                return "";
            return col.text_value;
        }
    }
    return "";
}

bool PgEmbeddingSink::upsert(const TableMapping& tm,
                             const std::string& id_value,
                             const std::string& embed_text,
                             const std::vector<std::string>& metadata_values,
                             const std::vector<float>& embedding)
{
    const std::string& upsert_sql = upsert_sql_list_.at(tm.source_table);

    std::ostringstream vec_str;
    vec_str << "[";
    for (size_t i = 0; i < embedding.size(); ++i) {
        if (i > 0) vec_str << ",";
        vec_str << embedding[i];
    }
    vec_str << "]";
    const std::string vec_literal = vec_str.str();

    std::vector<const char*> params;
    params.push_back(id_value.c_str());
    if (tm.has_discriminator_) {
        params.push_back(tm.discriminator_label_.c_str());
    }
    params.push_back(embed_text.c_str());
    for (const auto& v : metadata_values) {
        params.push_back(v.c_str());
    }
    params.push_back(vec_literal.c_str());

    PGresult* res = PQexecParams(
        pg_, upsert_sql.c_str(), static_cast<int>(params.size()),
        nullptr, params.data(), nullptr, nullptr, 0);

    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) {
        spdlog::error("[PgEmbeddingSink] upsert failed id={} table={}: {}",
                      id_value, tm.source_table, PQerrorMessage(pg_));
    } else {
        spdlog::debug("[PgEmbeddingSink] upserted id={} table={} - {}",
                      id_value, tm.source_table, config_.sink_table);
    }
    PQclear(res);
    return ok;
}

bool PgEmbeddingSink::remove(const TableMapping& tm, const std::string& id_value)
{
    const std::string& sql = delete_sql_list_.at(tm.source_table);

    std::vector<const char*> params = { id_value.c_str() };
    if (tm.has_discriminator_) {
        params.push_back(tm.discriminator_label_.c_str());
    }

    PGresult* res = PQexecParams(pg_, sql.c_str(), static_cast<int>(params.size()),
                                  nullptr, params.data(), nullptr, nullptr, 0);
    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) {
        spdlog::error("[PgEmbeddingSink] delete failed id={} table={}: {}",
                      id_value, tm.source_table, PQerrorMessage(pg_));
    }
    PQclear(res);
    return ok;
}

bool PgEmbeddingSink::is_toast(const DecodedRow& row, const std::string& col_name) 
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) return col.is_unchanged_toast;
    }
    return false;
}

} // namespace pgcdc
