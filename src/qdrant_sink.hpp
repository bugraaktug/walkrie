#pragma once

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

struct QdrantSinkConfig
{
    std::string url;        // e.g. http://localhost:6333, no trailing slash
    std::string api_key;    // optional; sent as "api-key" header when set
    std::string collection;
    std::vector<TableMapping> mappings;
};

// REST only for now; a gRPC transport is a planned follow-up.
class QdrantSink : public EmbeddingSink
{
public:
    explicit QdrantSink(QdrantSinkConfig config, std::shared_ptr<EmbeddingProvider> provider);
    ~QdrantSink() override = default;

    void init() override;
    void call(const ChangeEvent& event) override;
    void call_batch(const std::vector<ChangeEvent>& events) override;
    std::string name() const override { return "qdrant"; }

protected:
    virtual bool upsert(const TableMapping& tm,
                         const std::string& id_value,
                         const std::string& embed_text,
                         const std::vector<std::string>& metadata_values,
                         const std::vector<float>& embedding);
    virtual bool remove(const TableMapping& tm, const std::string& item_id);
    virtual bool truncate(const TableMapping& tm);

private:
    QdrantSinkConfig config_;
    std::shared_ptr<EmbeddingProvider> provider_;
    HttpClient http_;
    std::vector<std::string> headers_;

    std::optional<BatchEvent> prepare_upsert(const TableMapping& tm, const ChangeEvent& event);
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
    static bool is_toast(const DecodedRow& row, const std::string& col_name);
    // Qdrant point ids must be a u64 or UUID — deterministic UUIDv5(fixed namespace, key).
    // Key is "<discriminator_label>:<source_id>" when tm has a discriminator, else just
    // source_id — mirrors PgSqlBuilder's ON CONFLICT (id[, discriminator]) target so two
    // mappings sharing one collection with overlapping source ids don't collide.
    static std::string point_id_for(const TableMapping& tm, const std::string& source_id);
};

} // namespace pgcdc
