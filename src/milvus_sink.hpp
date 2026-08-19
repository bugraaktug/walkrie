#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "batch_event.hpp"
#include "config.hpp"
#include "embedding_provider.hpp"
#include "event_sink.hpp"
#include "http_client.hpp"

namespace pgcdc
{

struct MilvusSinkConfig
{
    std::string url;        // e.g. http://localhost:19530, no trailing slash
    std::string token;      // optional; "user:pass" or an API key, sent as "Authorization: Bearer <token>"
    std::string db_name;    // optional; omitted from requests when empty (server's default db)
    std::string collection;
    std::vector<TableMapping> mappings;
};

// REST only — same transport approach as QdrantSink (HttpClient, no gRPC).
class MilvusSink : public EmbeddingSink
{
public:
    explicit MilvusSink(MilvusSinkConfig config, std::shared_ptr<EmbeddingProvider> provider);
    ~MilvusSink() override = default;

    void init() override;
    void call(const ChangeEvent& event) override;
    void call_batch(const std::vector<ChangeEvent>& events) override;
    std::string name() const override { return "milvus"; }

protected:
    virtual bool upsert(const TableMapping& tm,
                         const std::string& id_value,
                         const std::string& embed_text,
                         const std::vector<std::string>& metadata_values,
                         const std::vector<float>& embedding);
    virtual bool remove(const TableMapping& tm, const std::string& item_id);
    virtual bool truncate(const TableMapping& tm);

private:
    MilvusSinkConfig config_;
    std::shared_ptr<EmbeddingProvider> provider_;
    HttpClient http_;
    std::vector<std::string> headers_;

    std::string pk_field_;       // <<< discovered at init() from the collection schema, never user-configured
    bool pk_is_varchar_ = false; // <<< Int64 vs VarChar primary key — controls id encoding below
    std::string vector_field_;   // <<< discovered at init()

    std::optional<BatchEvent> prepare_upsert(const TableMapping& tm, const ChangeEvent& event);
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
    static bool is_toast(const DecodedRow& row, const std::string& col_name);

    // <<< Milvus delete only takes a `filter` expression string, not an id array — hash to
    // hex/hyphen UUID so ids never need escaping there. Mirrors QdrantSink::point_id_for();
    // namespace constant is permanent, changing it would remap every existing row.
    static std::string uuid5_for(const std::string& key);
    static int64_t int64_id_for(const std::string& key); // <<< same UUID's first 8 bytes, for Int64 pks

    std::string composite_key(const TableMapping& tm, const std::string& source_id) const;
};

} // namespace pgcdc
