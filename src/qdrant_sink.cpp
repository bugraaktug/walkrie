#include "qdrant_sink.hpp"

#include <uuid/uuid.h>
#include <spdlog/spdlog.h>

namespace pgcdc
{

namespace
{

// Fixed app namespace for point-id UUIDv5 — do not change, would remap every existing point.
constexpr unsigned char kIdNamespace[16] = {
    0xbd, 0x18, 0x03, 0x45, 0xc3, 0xdb, 0x46, 0x86,
    0x86, 0x5f, 0xd7, 0x0b, 0x69, 0x49, 0xd8, 0xe1
};

} // namespace

QdrantSink::QdrantSink(QdrantSinkConfig config, std::shared_ptr<EmbeddingProvider> provider)
    : config_(std::move(config))
    , provider_(std::move(provider))
    , http_(/*timeout_secs=*/30)
{
    if (!config_.api_key.empty()) {
        headers_.push_back("api-key: " + config_.api_key);
    }
}

std::string QdrantSink::point_id_for(const TableMapping& tm, const std::string& source_id)
{
    const std::string key = tm.has_discriminator_ ? (tm.discriminator_label_ + ":" + source_id) : source_id; // <<< mirrors PgSqlBuilder's ON CONFLICT (id[, discriminator]) target
    uuid_t out;
    uuid_generate_sha1(out, kIdNamespace, key.data(), key.size());
    char buf[37];
    uuid_unparse_lower(out, buf);
    return std::string(buf);
}

std::string QdrantSink::get_column(const DecodedRow& row, const std::string& col_name)
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) {
            if (col.is_null || col.is_unchanged_toast) return "";
            return col.text_value;
        }
    }
    return "";
}

bool QdrantSink::is_toast(const DecodedRow& row, const std::string& col_name)
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) return col.is_unchanged_toast;
    }
    return false;
}

void QdrantSink::init()
{
    auto resp = http_.get_json(config_.url + "/collections/" + config_.collection, headers_);

    if (!resp.curl_ok) {
        throw std::runtime_error("QdrantSink: failed to reach " + config_.url + ": " + resp.curl_error);
    }
    if (resp.status_code == 404) {
        throw std::runtime_error("QdrantSink: collection '" + config_.collection + "' does not exist at " +
                                 config_.url + " — create it first (walkrie does not auto-create collections)");
    }
    if (resp.status_code != 200) {
        throw std::runtime_error("QdrantSink: unexpected response checking collection '" + config_.collection +
                                 "' (HTTP " + std::to_string(resp.status_code) + "): " + resp.body);
    }

    int collection_dims = 0;
    try {
        auto j = ordered_json::parse(resp.body);
        collection_dims = j.at("result").at("config").at("params").at("vectors").at("size").get<int>();
    } catch (const std::exception& e) {
        throw std::runtime_error("QdrantSink: couldn't read vector size from collection '" + config_.collection +
                                 "' — named/multi-vector collections aren't supported yet: " + e.what());
    }

    int provider_dims = provider_->dimensions();
    if (collection_dims != provider_dims) {
        throw std::runtime_error("QdrantSink: collection '" + config_.collection + "' vector size (" +
                                 std::to_string(collection_dims) + ") does not match the embedding provider's " +
                                 std::to_string(provider_dims) + " dimensions");
    }

    spdlog::info("[QdrantSink] connection ok, collection '{}' verified ({} dims)", config_.collection, collection_dims);
}

void QdrantSink::call(const ChangeEvent& event)
{
    call_batch({event});
}

bool QdrantSink::upsert(const TableMapping& tm,
                         const std::string& id_value,
                         const std::string& embed_text,
                         const std::vector<std::string>& metadata_values,
                         const std::vector<float>& embedding)
{
    ordered_json payload;
    payload[tm.id_sink_] = id_value;
    if (tm.has_discriminator_) {
        payload[tm.discriminator_sink_] = tm.discriminator_label_;
    }
    payload[tm.embed_sink_] = embed_text;
    size_t mi = 0;
    for (const auto& m : tm.columns) {
        if (m.role == "metadata") {
            payload[m.sink_column] = metadata_values.at(mi++);
        }
    }

    ordered_json point;
    point["id"] = point_id_for(tm, id_value);
    point["vector"] = embedding;
    point["payload"] = payload;

    ordered_json body;
    body["points"] = ordered_json::array({point});

    auto resp = http_.put_json(config_.url + "/collections/" + config_.collection + "/points?wait=true",
                                body.dump(), headers_);

    if (!resp.curl_ok || resp.status_code / 100 != 2) {
        spdlog::error("[QdrantSink] upsert failed id={} table={} status={} body={} curl_err={}",
                      id_value, tm.source_table, resp.status_code, resp.body, resp.curl_error);
        return false;
    }
    return true;
}

bool QdrantSink::remove(const TableMapping& tm, const std::string& id_value)
{
    ordered_json body;
    body["points"] = ordered_json::array({point_id_for(tm, id_value)});

    auto resp = http_.post_json(config_.url + "/collections/" + config_.collection + "/points/delete?wait=true",
                                 body.dump(), headers_);

    bool ok = resp.curl_ok && resp.status_code / 100 == 2;
    if (!ok) {
        spdlog::error("[QdrantSink] delete failed id={} table={} status={} body={} curl_err={}",
                      id_value, tm.source_table, resp.status_code, resp.body, resp.curl_error);
    }
    return ok;
}

bool QdrantSink::truncate(const TableMapping& tm)
{
    ordered_json filter;
    if (tm.has_discriminator_) {
        ordered_json cond;
        cond["key"] = tm.discriminator_sink_;
        cond["match"]["value"] = tm.discriminator_label_;
        filter["must"] = ordered_json::array({cond});
    } else {
        spdlog::warn("[QdrantSink] truncate for table={} has no discriminator configured — "
                     "deleting every point in collection {} (including points from any other mapping "
                     "sharing this collection)", tm.source_table, config_.collection);
    }

    ordered_json body;
    body["filter"] = filter;

    auto resp = http_.post_json(config_.url + "/collections/" + config_.collection + "/points/delete?wait=true",
                                 body.dump(), headers_);

    bool ok = resp.curl_ok && resp.status_code / 100 == 2;
    if (!ok) {
        spdlog::error("[QdrantSink] truncate failed table={} status={} body={} curl_err={}",
                      tm.source_table, resp.status_code, resp.body, resp.curl_error);
    } else {
        spdlog::info("[QdrantSink] truncated sink points for table={} - {}", tm.source_table, config_.collection);
    }
    return ok;
}

std::optional<BatchEvent> QdrantSink::prepare_upsert(const TableMapping& tm, const ChangeEvent& event)
{
    if (!event.new_row) {
        spdlog::warn("[QdrantSink] {} event dropped, no new row received - {}",
                      event.op == ChangeEvent::Op::Insert ? "insert" : "update", event.table_name);
        return std::nullopt;
    }

    const std::string id_val    = get_column(*event.new_row, tm.id_source_);
    const std::string embed_val = get_column(*event.new_row, tm.embed_source_);

    if (event.op == ChangeEvent::Op::Update) {
        if (is_toast(*event.new_row, tm.embed_source_)) {
            spdlog::warn("[QdrantSink] update event dropped, skip id={}: {} unchanged (toast) - {}",
                         id_val, tm.embed_source_, event.table_name);
            return std::nullopt;
        }
        if (event.old_row) {
            const std::string old_val = get_column(*event.old_row, tm.embed_source_);
            if (!old_val.empty() && old_val == embed_val) {
                spdlog::warn("[QdrantSink] update event dropped, skip id={}: {} unchanged - {}",
                             id_val, tm.embed_source_, event.table_name);
                return std::nullopt;
            }
        }
    }

    if (id_val.empty() || embed_val.empty()) {
        spdlog::warn("[QdrantSink] {} event dropped, no id{} - {}",
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

void QdrantSink::call_batch(const std::vector<ChangeEvent>& events)
{
    Batch batch;
    batch.reserve(events.size());

    // Pass 1: validate every event and record what needs to happen, in
    // ORIGINAL event order — no Qdrant writes here.
    for (const auto& event : events) {
        if (event.op == ChangeEvent::Op::Commit) continue;

        const TableMapping* tm = nullptr;
        for (const auto& m : config_.mappings) {
            if (m.source_table == event.table_name) { tm = &m; break; }
        }
        if (!tm) {
            spdlog::warn("[QdrantSink] No mapping configured for - {}", event.table_name);
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
                    spdlog::warn("[QdrantSink] delete event dropped, no old row received - {}", event.table_name);
                    break;
                }
                std::string id_val = get_column(*event.old_row, tm->id_source_);
                if (id_val.empty()) {
                    spdlog::warn("[QdrantSink] delete event dropped, no id val - {}", event.table_name);
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
                spdlog::info("[QdrantSink] truncate event received - {}", event.table_name);
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
            spdlog::error("[QdrantSink] embed_batch returned {} vectors for {} requested — dropping this batch",
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
                    spdlog::warn("[QdrantSink] event dropped, no embedding value generated - id={} table={}",
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
