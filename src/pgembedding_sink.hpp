#pragma once
 
#include <libpq-fe.h>
#include <llama.h>
 
#include <string>
#include <vector>
 
#include "embedding_provider.hpp"
#include "event_sink.hpp"
 
namespace pgcdc 
{
struct PgEmbeddingSinkConfig
{
    std::string pg_conninfo;
    std::string sink_table   = "public.test_embeddings";
    std::string embed_column = "name";
    std::string id_column    = "id";
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
    PGconn* pg_ = nullptr;

    bool upsert(const std::string& item_id,
                const std::string& item_name,
                const std::vector<float>& embedding); 
    bool remove(const std::string& item_id);
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
    static bool  is_toast  (const DecodedRow& row, const std::string& col_name);
};
 
} // namespace pgcdc
