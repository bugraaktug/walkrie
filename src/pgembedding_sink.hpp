#pragma once
 
#include <libpq-fe.h>
#include <llama.h>
 
#include <string>
#include <vector>
 
#include "event_sink.hpp"
 
namespace pgcdc 
{
 
struct PgEmbeddingSinkConfig 
{
    std::string model_path;         // path to bge-m3-Q4_K_M.gguf
    int         n_threads   = 4;    // CPU threads for inference
    int         n_ctx       = 512;  // context window — 512 is enough for short text columns
 
    std::string embed_column = "name"; // test_table.name in your case
    std::string sink_table   = "public.test_embeddings";
    std::string pg_conninfo; // e.g. "host=localhost dbname=qdb user=quser password=..."
};

// Owns one llama_model + llama_context (loaded once at construction) and
// one PGconn to the sink database. Both are long-lived for the process
// lifetime — model loading is expensive (~1-2s), so it must not happen
// per-event.
class PgEmbeddingSink : public EmbeddingSink 
{
public:
    explicit PgEmbeddingSink(PgEmbeddingSinkConfig config);
    ~PgEmbeddingSink() override;
 
    PgEmbeddingSink(const PgEmbeddingSink&) = delete;
    PgEmbeddingSink& operator=(const PgEmbeddingSink&) = delete;
 
    void init() override;
    void call(const ChangeEvent& event) override;
 
private:
    PgEmbeddingSinkConfig config_;
 
    llama_model*   model_   = nullptr;
    llama_context* ctx_     = nullptr;
 
    PGconn* pg_ = nullptr;

    bool upsert(const std::string& item_id,
                const std::string& item_name,
                const std::vector<float>& embedding); 
    bool remove(const std::string& item_id);
    std::vector<float> embed(const std::string& text); // Produces a normalized 1024-float embedding for the given text.
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
};
 
} // namespace pgcdc
