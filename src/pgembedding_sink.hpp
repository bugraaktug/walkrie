#pragma once
 
#include <libpq-fe.h>
#include <llama.h>
 
#include <string>
#include <vector>
 
#include "embedding_provider.hpp"
#include "event_sink.hpp"
#include "config.hpp"

namespace pgcdc 
{

struct PgEmbeddingSinkConfig
{
    std::string pg_conninfo;
    std::string sink_table;
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
 
private:
    PgEmbeddingSinkConfig config_;
    std::shared_ptr<EmbeddingProvider> provider_; 
    std::unordered_map<std::string, std::string> upsert_sql_list_;
    PGconn* pg_ = nullptr;

    std::string build_upsert_sql(const TableMapping& tm);

    bool upsert(const TableMapping& tm,
		const std::string& id_value,
                const std::string& embed_text,
                const std::vector<std::string>& metadata_values,
                const std::vector<float>& embedding);
    bool remove(const TableMapping& tm, const std::string& item_id);
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
    static bool is_toast(const DecodedRow& row, const std::string& col_name);
};
 
} // namespace pgcdc
