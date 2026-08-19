#include "milvus_sink.hpp"

#include <cstring>
#include <uuid/uuid.h>
#include <spdlog/spdlog.h>

namespace pgcdc
{

namespace
{

// Fixed app namespace for id UUIDv5 — do not change, would remap every existing row.
constexpr unsigned char kIdNamespace[16] = {
    0x4a, 0xe0, 0x9f, 0x2c, 0x1b, 0x77, 0x4d, 0x63,
    0x9b, 0x2e, 0x6d, 0x0c, 0x5a, 0x8f, 0x31, 0x77
};

} // namespace

MilvusSink::MilvusSink(MilvusSinkConfig config, std::shared_ptr<EmbeddingProvider> provider)
    : config_(std::move(config))
    , provider_(std::move(provider))
    , http_(/*timeout_secs=*/30)
{
    if (!config_.token.empty()) {
        headers_.push_back("Authorization: Bearer " + config_.token);
    }
}

std::string MilvusSink::composite_key(const TableMapping& tm, const std::string& source_id) const
{
    return tm.has_discriminator_ ? (tm.discriminator_label_ + ":" + source_id) : source_id; // <<< mirrors QdrantSink
}

std::string MilvusSink::uuid5_for(const std::string& key)
{
    uuid_t out;
    uuid_generate_sha1(out, kIdNamespace, key.data(), key.size());
    char buf[37];
    uuid_unparse_lower(out, buf);
    return std::string(buf);
}

int64_t MilvusSink::int64_id_for(const std::string& key)
{
    uuid_t out;
    uuid_generate_sha1(out, kIdNamespace, key.data(), key.size());
    uint64_t v = 0;
    std::memcpy(&v, out, sizeof(v));
    return static_cast<int64_t>(v & 0x7FFFFFFFFFFFFFFFULL); // <<< clear sign bit, keep non-negative
}

std::string MilvusSink::get_column(const DecodedRow& row, const std::string& col_name)
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) {
            if (col.is_null || col.is_unchanged_toast) return "";
            return col.text_value;
        }
    }
    return "";
}

bool MilvusSink::is_toast(const DecodedRow& row, const std::string& col_name)
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) return col.is_unchanged_toast;
    }
    return false;
}

void MilvusSink::init()
{
    ordered_json req;
    req["collectionName"] = config_.collection;
    if (!config_.db_name.empty()) req["dbName"] = config_.db_name;

    auto resp = http_.post_json(config_.url + "/v2/vectordb/collections/describe", req.dump(), headers_);

    if (!resp.curl_ok) {
        throw std::runtime_error("MilvusSink: failed to reach " + config_.url + ": " + resp.curl_error);
    }
    if (resp.status_code != 200) {
        throw std::runtime_error("MilvusSink: unexpected response checking collection '" + config_.collection +
                                 "' (HTTP " + std::to_string(resp.status_code) + "): " + resp.body);
    }

    ordered_json j;
    try {
        j = ordered_json::parse(resp.body);
    } catch (const std::exception& e) {
        throw std::runtime_error("MilvusSink: couldn't parse describe-collection response: " + std::string(e.what()));
    }
    if (j.at("code").get<int>() != 0) {
        std::string msg = j.value("message", "");
        throw std::runtime_error("MilvusSink: collection '" + config_.collection + "' does not exist at " +
                                 config_.url + " (" + msg + ") — create it first (walkrie does not auto-create collections)");
    }

    int collection_dims = -1;
    for (const auto& f : j.at("data").at("fields")) {
        if (f.value("primaryKey", false)) {
            pk_field_ = f.at("name").get<std::string>();
            pk_is_varchar_ = f.at("type").get<std::string>() == "VarChar";
        }
        if (f.at("type").get<std::string>() == "FloatVector") {
            vector_field_ = f.at("name").get<std::string>();
            for (const auto& p : f.at("params")) {
                if (p.at("key").get<std::string>() == "dim") {
                    collection_dims = std::stoi(p.at("value").get<std::string>());
                }
            }
        }
    }
    if (pk_field_.empty()) {
        throw std::runtime_error("MilvusSink: couldn't find a primary key field on collection '" + config_.collection + "'");
    }
    if (vector_field_.empty() || collection_dims < 0) {
        throw std::runtime_error("MilvusSink: couldn't find a FloatVector field with a dim param on collection '" +
                                 config_.collection + "' — named/multi-vector collections aren't supported yet");
    }

    int provider_dims = provider_->dimensions();
    if (collection_dims != provider_dims) {
        throw std::runtime_error("MilvusSink: collection '" + config_.collection + "' vector field '" + vector_field_ +
                                 "' dim (" + std::to_string(collection_dims) + ") does not match the embedding provider's " +
                                 std::to_string(provider_dims) + " dimensions");
    }

    // pk_field_/vector_field_ are written automatically on every upsert (hashed id, embedding) —
    // a table_mapping column colliding with either would silently overwrite one or the other.
    for (const auto& tm : config_.mappings) {
        if (tm.id_sink_ == pk_field_ || tm.id_sink_ == vector_field_) {
            throw std::runtime_error("MilvusSink: table_mapping for '" + tm.source_table + "' has id role sink_column='" +
                                     tm.id_sink_ + "' which collides with collection '" + config_.collection +
                                     "'s auto-populated pk/vector field — pick a distinct scalar field to hold the readable source id");
        }
        if (tm.embed_sink_ == pk_field_ || tm.embed_sink_ == vector_field_) {
            throw std::runtime_error("MilvusSink: table_mapping for '" + tm.source_table + "' has embed role sink_column='" +
                                     tm.embed_sink_ + "' which collides with collection '" + config_.collection +
                                     "'s auto-populated pk/vector field — pick a distinct scalar field to hold the source text");
        }
    }

    spdlog::info("[MilvusSink] connection ok, collection '{}' verified (pk={} {}, vector={} {} dims)",
                 config_.collection, pk_field_, pk_is_varchar_ ? "VarChar" : "Int64", vector_field_, collection_dims);
}

void MilvusSink::call(const ChangeEvent& event)
{
    call_batch({event});
}

bool MilvusSink::upsert(const TableMapping& tm,
                         const std::string& id_value,
                         const std::string& embed_text,
                         const std::vector<std::string>& metadata_values,
                         const std::vector<float>& embedding)
{
    ordered_json entity;
    const std::string key = composite_key(tm, id_value);
    entity[pk_field_] = pk_is_varchar_ ? ordered_json(uuid5_for(key)) : ordered_json(int64_id_for(key));
    entity[vector_field_] = embedding;
    // <<< id_sink_/embed_sink_ hold the readable source id/text, same as QdrantSink's payload — the
    // true row identity is always pk_field_ above, hashed for filter-expression safety (see header).
    entity[tm.id_sink_] = id_value;
    entity[tm.embed_sink_] = embed_text;
    if (tm.has_discriminator_) {
        entity[tm.discriminator_sink_] = tm.discriminator_label_;
    }
    size_t mi = 0;
    for (const auto& m : tm.columns) {
        if (m.role == "metadata") {
            entity[m.sink_column] = metadata_values.at(mi++);
        }
    }

    ordered_json body;
    body["collectionName"] = config_.collection;
    if (!config_.db_name.empty()) body["dbName"] = config_.db_name;
    body["data"] = ordered_json::array({entity});

    auto resp = http_.post_json(config_.url + "/v2/vectordb/entities/upsert", body.dump(), headers_);

    bool ok = resp.curl_ok && resp.status_code == 200;
    if (ok) {
        try {
            ok = ordered_json::parse(resp.body).at("code").get<int>() == 0;
        } catch (const std::exception&) {
            ok = false;
        }
    }
    if (!ok) {
        spdlog::error("[MilvusSink] upsert failed id={} table={} status={} body={} curl_err={}",
                      id_value, tm.source_table, resp.status_code, resp.body, resp.curl_error);
    }
    return ok;
}

bool MilvusSink::remove(const TableMapping& tm, const std::string& id_value)
{
    const std::string key = composite_key(tm, id_value);
    std::string id_literal = pk_is_varchar_ ? ("\"" + uuid5_for(key) + "\"") : std::to_string(int64_id_for(key));

    ordered_json body;
    body["collectionName"] = config_.collection;
    if (!config_.db_name.empty()) body["dbName"] = config_.db_name;
    body["filter"] = pk_field_ + " == " + id_literal;

    auto resp = http_.post_json(config_.url + "/v2/vectordb/entities/delete", body.dump(), headers_);

    bool ok = resp.curl_ok && resp.status_code == 200;
    if (ok) {
        try {
            ok = ordered_json::parse(resp.body).at("code").get<int>() == 0;
        } catch (const std::exception&) {
            ok = false;
        }
    }
    if (!ok) {
        spdlog::error("[MilvusSink] delete failed id={} table={} status={} body={} curl_err={}",
                      id_value, tm.source_table, resp.status_code, resp.body, resp.curl_error);
    }
    return ok;
}

bool MilvusSink::truncate(const TableMapping& tm)
{
    std::string filter;
    if (tm.has_discriminator_) {
        filter = tm.discriminator_sink_ + " == \"" + tm.discriminator_label_ + "\"";
    } else {
        // <<< an empty filter (Milvus's documented "match everything" shorthand for delete) is
        // rejected outright by this server (code 1802, "Filter" required) — verified live against
        // Milvus 2.6.18, contradicts the REST API docs. A pk-type-generic always-true expression
        // works instead.
        filter = pk_is_varchar_ ? (pk_field_ + " like \"%\"") : (pk_field_ + " >= -9223372036854775808");
        spdlog::warn("[MilvusSink] truncate for table={} has no discriminator configured — "
                     "deleting every row in collection {} (including rows from any other mapping "
                     "sharing this collection)", tm.source_table, config_.collection);
    }

    ordered_json body;
    body["collectionName"] = config_.collection;
    if (!config_.db_name.empty()) body["dbName"] = config_.db_name;
    body["filter"] = filter;

    auto resp = http_.post_json(config_.url + "/v2/vectordb/entities/delete", body.dump(), headers_);

    bool ok = resp.curl_ok && resp.status_code == 200;
    if (ok) {
        try {
            ok = ordered_json::parse(resp.body).at("code").get<int>() == 0;
        } catch (const std::exception&) {
            ok = false;
        }
    }
    if (!ok) {
        spdlog::error("[MilvusSink] truncate failed table={} status={} body={} curl_err={}",
                      tm.source_table, resp.status_code, resp.body, resp.curl_error);
    } else {
        spdlog::info("[MilvusSink] truncated sink rows for table={} - {}", tm.source_table, config_.collection);
    }
    return ok;
}

std::optional<BatchEvent> MilvusSink::prepare_upsert(const TableMapping& tm, const ChangeEvent& event)
{
    if (!event.new_row) {
        spdlog::warn("[MilvusSink] {} event dropped, no new row received - {}",
                      event.op == ChangeEvent::Op::Insert ? "insert" : "update", event.table_name);
        return std::nullopt;
    }

    const std::string id_val    = get_column(*event.new_row, tm.id_source_);
    const std::string embed_val = get_column(*event.new_row, tm.embed_source_);

    if (event.op == ChangeEvent::Op::Update) {
        if (is_toast(*event.new_row, tm.embed_source_)) {
            spdlog::warn("[MilvusSink] update event dropped, skip id={}: {} unchanged (toast) - {}",
                         id_val, tm.embed_source_, event.table_name);
            return std::nullopt;
        }
        if (event.old_row) {
            const std::string old_val = get_column(*event.old_row, tm.embed_source_);
            if (!old_val.empty() && old_val == embed_val) {
                spdlog::warn("[MilvusSink] update event dropped, skip id={}: {} unchanged - {}",
                             id_val, tm.embed_source_, event.table_name);
                return std::nullopt;
            }
        }
    }

    if (id_val.empty() || embed_val.empty()) {
        spdlog::warn("[MilvusSink] {} event dropped, no id{} - {}",
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

void MilvusSink::call_batch(const std::vector<ChangeEvent>& events)
{
    Batch batch;
    batch.reserve(events.size());

    // Pass 1: validate every event and record what needs to happen, in
    // ORIGINAL event order — no Milvus writes here.
    for (const auto& event : events) {
        if (event.op == ChangeEvent::Op::Commit) continue;

        const TableMapping* tm = nullptr;
        for (const auto& m : config_.mappings) {
            if (m.source_table == event.table_name) { tm = &m; break; }
        }
        if (!tm) {
            spdlog::warn("[MilvusSink] No mapping configured for - {}", event.table_name);
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
                if (!event.old_row) {
                    spdlog::warn("[MilvusSink] delete event dropped, no old row received - {}", event.table_name);
                    break;
                }
                std::string id_val = get_column(*event.old_row, tm->id_source_);
                if (id_val.empty()) {
                    spdlog::warn("[MilvusSink] delete event dropped, no id val - {}", event.table_name);
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
                spdlog::info("[MilvusSink] truncate event received - {}", event.table_name);
                BatchEvent be;
                be.kind = BatchEvent::Kind::Truncate;
                be.tm   = tm;
                batch.push_back(std::move(be));
                break;
            }
            case ChangeEvent::Op::Commit:
                break; // <<< unreachable — skipped at the top of this loop
        }
    }

    if (batch.empty()) return;

    std::vector<std::string> texts_to_embed;
    texts_to_embed.reserve(batch.size());
    for (const auto& be : batch) {
        if (be.kind == BatchEvent::Kind::Upsert) texts_to_embed.push_back(be.embed_val);
    }

    std::vector<std::vector<float>> vectors;
    if (!texts_to_embed.empty()) {
        vectors = provider_->embed_batch(texts_to_embed);
        if (vectors.size() != texts_to_embed.size()) {
            spdlog::error("[MilvusSink] embed_batch returned {} vectors for {} requested — dropping this batch",
                          vectors.size(), texts_to_embed.size());
            return;
        }
    }

    // Pass 2: apply every action in ORIGINAL event order — upserts use
    // their precomputed embedding, deletes/truncates run inline at their
    // correct position relative to same-batch upserts.
    size_t vec_idx = 0;
    for (auto& be : batch) {
        switch (be.kind) {
            case BatchEvent::Kind::Upsert: {
                const auto& vec = vectors[vec_idx++];
                if (vec.empty()) {
                    spdlog::warn("[MilvusSink] event dropped, no embedding value generated - id={} table={}",
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

} // namespace pgcdc
