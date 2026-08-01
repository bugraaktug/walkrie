#include "pgembedding_sink.hpp"
#include "pg_sql_builder.hpp"

#include <cstdio>
#include <cstdlib>
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
        truncate_sql_list_[tm.source_table] = sql_builder_->build_truncate_sql(tm);
        spdlog::debug("[PgEmbeddingSink] truncate sql for mapping {} ready - {}",
                      tm.source_table, truncate_sql_list_[tm.source_table]);
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

    verify();

    spdlog::info("[PgEmbeddingSink] pg connection ok, pgvector present for {} table", config_.sink_table);
}

void PgEmbeddingSink::verify()
{
    const char* params[2] = { config_.sink_table.c_str(), config_.sink_column.c_str() };
    PGresult* res = PQexecParams(pg_,
        "SELECT atttypmod FROM pg_attribute "
        "WHERE attrelid = $1::regclass AND attname = $2 AND NOT attisdropped",
        2, nullptr, params, nullptr, nullptr, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::string err = PQerrorMessage(pg_);
        PQclear(res);
        throw std::runtime_error("EmbeddingSink: failed to look up sink column '" + config_.sink_column +
                                 "' on table '" + config_.sink_table + "': " + err);
    }
    if (PQntuples(res) == 0) {
        PQclear(res);
        throw std::runtime_error("EmbeddingSink: sink column '" + config_.sink_column +
                                 "' not found on table '" + config_.sink_table + "'");
    }

    // pgvector stores a vector(N) column's declared width directly as
    // atttypmod (no VARHDRSZ-style offset, unlike e.g. varchar(N)); -1 means
    // the column was declared as a bare `vector` with no dimension, which we
    // can't check anything against.
    int column_dims = std::atoi(PQgetvalue(res, 0, 0));
    PQclear(res);

    if (column_dims <= 0) {
        spdlog::debug("[PgEmbeddingSink] sink column '{}.{}' has no declared vector dimension — skipping dims check",
                      config_.sink_table, config_.sink_column);
        return;
    }

    int provider_dims = provider_->dimensions();
    if (column_dims != provider_dims) {
        throw std::runtime_error("EmbeddingSink: sink column '" + config_.sink_table + "." + config_.sink_column +
                                 "' is vector(" + std::to_string(column_dims) +
                                 ") but the embedding provider produces " + std::to_string(provider_dims) +
                                 "-dimensional vectors — fix [embedding] dimensions or the column definition");
    }
}

std::optional<BatchEvent> PgEmbeddingSink::prepare_upsert(const TableMapping& tm, 
                                                          const ChangeEvent& event) 
{
    if (!event.new_row) {
        spdlog::warn("[PgEmbeddingSink] {} event dropped, no new row received - {}",
                      event.op == ChangeEvent::Op::Insert ? "insert" : "update",
                      event.table_name);
        return std::nullopt;
    }

    const std::string id_val    = get_column(*event.new_row, tm.id_source_);
    const std::string embed_val = get_column(*event.new_row, tm.embed_source_);

    if (event.op == ChangeEvent::Op::Update) {
        // Core value prop: skip embedding call if the embed column didn't change
        if (is_toast(*event.new_row, tm.embed_source_)) {
            spdlog::warn("[PgEmbeddingSink] update event dropped, skip id={}: {} unchanged (toast) - {}",
                         id_val, tm.embed_source_, event.table_name);
            return std::nullopt;
        }
        if (event.old_row) {
            const std::string old_val = get_column(*event.old_row, tm.embed_source_);
            if (!old_val.empty() && old_val == embed_val) {
                spdlog::warn("[PgEmbeddingSink] update event dropped, skip id={}: {} unchanged - {}",
                             id_val, tm.embed_source_, event.table_name);
                return std::nullopt;
            }
        }
    }

    if (id_val.empty() || embed_val.empty()) {
        spdlog::warn("[PgEmbeddingSink] {} event dropped, no id{} - {}",
                      event.op == ChangeEvent::Op::Insert ? "insert" : "update",
                      event.op == ChangeEvent::Op::Insert ? " source" : " or embedding val",
                      event.table_name);
        return std::nullopt;
    }

    BatchEvent be;
    be.kind      = BatchEvent::Kind::Upsert;
    be.tm        = &tm;
    be.id_val    = id_val;
    be.embed_val = embed_val;
    for (const auto& m : tm.columns) {
        if (m.role == "metadata") {
            be.meta_vals.push_back(get_column(*event.new_row, m.source_column));
        }
    }
    return be;
}

void PgEmbeddingSink::call_batch(const std::vector<ChangeEvent>& events) {
    Batch batch;
    batch.reserve(events.size());

    // Pass 1: validate every event and record what needs to happen, in
    // ORIGINAL event order — no database writes here.
    for (const auto& event : events) {
        const TableMapping* tm = nullptr;
        for (const auto& m : config_.mappings) {
            if (m.source_table == event.table_name) { tm = &m; break; }
        }
        if (!tm) {
            spdlog::warn("[PgEmbeddingSink] No mapping configured for - {}", event.table_name);
            continue;
        }

        switch (event.op) {
            case ChangeEvent::Op::Insert:
            case ChangeEvent::Op::Update: {
                auto prepared = prepare_upsert(*tm, event);
                if (!prepared) continue;
                batch.push_back(std::move(*prepared));
                break;
            }
            case ChangeEvent::Op::Delete: {
                spdlog::debug("[PgEmbeddingSink] delete event received - {}", event.table_name);
                if (!event.old_row) {
                    spdlog::warn("[PgEmbeddingSink] delete event dropped, no old row received - {}", event.table_name);
                    break;
                }
                std::string id_val = get_column(*event.old_row, tm->id_source_);
                if (id_val.empty()) {
                    spdlog::warn("[PgEmbeddingSink] delete event dropped, no id val  - {}", event.table_name);
                    break;
                }
                BatchEvent be;
                be.kind   = BatchEvent::Kind::Delete;
                be.tm     = tm;
                be.id_val = id_val;
                batch.push_back(std::move(be));
                break;
            }
            case ChangeEvent::Op::Truncate: {
                spdlog::info("[PgEmbeddingSink] truncate event received - {}", event.table_name);
                BatchEvent be;
                be.kind = BatchEvent::Kind::Truncate;
                be.tm   = tm;
                batch.push_back(std::move(be));
                break;
            }
        }
    }

    if (batch.empty()) return;

    // Derived from the batch itself rather than accumulated in a separate
    // parallel vector during pass 1 — one source of truth
    std::vector<std::string> texts_to_embed;
    texts_to_embed.reserve(batch.size());
    for (const auto& be : batch) {
        if (be.kind == BatchEvent::Kind::Upsert) {
            texts_to_embed.push_back(be.embed_val);
        }
    }

    std::vector<std::vector<float>> vectors;
    if (!texts_to_embed.empty()) {
        vectors = provider_->embed_batch(texts_to_embed);
        if (vectors.size() != texts_to_embed.size()) {
            spdlog::error("[PgEmbeddingSink] embed_batch returned {} vectors for {} requested — dropping this batch",
                          vectors.size(), texts_to_embed.size());
            return;
        }
    }

    // Pass 2: apply every action in ORIGINAL event order — upserts use
    // their precomputed embedding, deletes run inline at their correct
    // position relative to same-batch upserts for the same id.
    size_t vec_idx = 0;
    for (auto& be : batch) {
        switch (be.kind) {
            case BatchEvent::Kind::Upsert: {
                const auto& vec = vectors[vec_idx++];
                if (vec.empty()) {
                    spdlog::warn("[PgEmbeddingSink] event dropped, no embedding value generated - id={} table={}",
                                  be.id_val, be.tm->source_table);
                    continue;
                }
                upsert(*be.tm, be.id_val, be.embed_val, be.meta_vals, vec);
                break;
            }
            case BatchEvent::Kind::Delete:
                remove(*be.tm, be.id_val);
                break;
            case BatchEvent::Kind::Truncate:
                truncate(*be.tm);
                break;
        }
    }
}

void PgEmbeddingSink::call(const ChangeEvent& event) 
{
    call_batch({event});
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

bool PgEmbeddingSink::truncate(const TableMapping& tm)
{
    const std::string& sql = truncate_sql_list_.at(tm.source_table);

    std::vector<const char*> params;
    if (tm.has_discriminator_) {
        params.push_back(tm.discriminator_label_.c_str());
    } else {
        spdlog::warn("[PgEmbeddingSink] truncate for table={} has no discriminator configured — "
                     "deleting every row in sink table {} (including rows from any other mapping "
                     "sharing this sink table)", tm.source_table, config_.sink_table);
    }

    PGresult* res = PQexecParams(pg_, sql.c_str(), static_cast<int>(params.size()),
                                  nullptr, params.data(), nullptr, nullptr, 0);
    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) {
        spdlog::error("[PgEmbeddingSink] truncate failed table={}: {}",
                      tm.source_table, PQerrorMessage(pg_));
    } else {
        spdlog::info("[PgEmbeddingSink] truncated sink rows for table={} - {}",
                     tm.source_table, config_.sink_table);
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
