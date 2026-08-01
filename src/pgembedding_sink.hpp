#pragma once
 
#include <libpq-fe.h>
 
#include <string>
#include <vector>
 
#include "batch_event.hpp"
#include "config.hpp"
#include "embedding_provider.hpp"
#include "event_sink.hpp"
#include "sql_builder.hpp"

namespace pgcdc 
{

struct PgEmbeddingSinkConfig
{
    std::string pg_conninfo;
    std::string sink_table;
    std::string sink_column;
    std::vector<TableMapping> mappings;
};

class PgEmbeddingSink : public EmbeddingSink 
{
public:
    explicit PgEmbeddingSink(PgEmbeddingSinkConfig config, 
                             std::shared_ptr<EmbeddingProvider> provider);
    ~PgEmbeddingSink() override;
 
    PgEmbeddingSink(const PgEmbeddingSink&) = delete;
    PgEmbeddingSink& operator=(const PgEmbeddingSink&) = delete;
 
    void init() override;
    void call(const ChangeEvent& event) override;
    void call_batch(const std::vector<ChangeEvent>& events) override;

protected:
    virtual bool upsert(const TableMapping& tm,
		                const std::string& id_value,
                        const std::string& embed_text,
                        const std::vector<std::string>& metadata_values,
                        const std::vector<float>& embedding);
    virtual bool remove(const TableMapping& tm, const std::string& item_id);
    virtual bool truncate(const TableMapping& tm);

private:
    PgEmbeddingSinkConfig config_;
    std::shared_ptr<EmbeddingProvider> provider_;
    std::unique_ptr<ISqlBuilder> sql_builder_;
    std::unordered_map<std::string, std::string> upsert_sql_list_;
    std::unordered_map<std::string, std::string> delete_sql_list_;
    std::unordered_map<std::string, std::string> truncate_sql_list_;
    PGconn* pg_ = nullptr;

    std::optional<BatchEvent> prepare_upsert(const TableMapping& tm,
                                             const ChangeEvent& event);
    void verify();
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
    static bool is_toast(const DecodedRow& row, const std::string& col_name);
};
 
} // namespace pgcdc
