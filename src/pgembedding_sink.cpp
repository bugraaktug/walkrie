#include "pgembedding_sink.hpp"

#include <cstdio>
#include <cstring>
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
    // --- pgvector sink connection ---
    pg_ = PQconnectdb(config_.pg_conninfo.c_str());
    if (PQstatus(pg_) != CONNECTION_OK) {
        std::string err = PQerrorMessage(pg_);
        PQfinish(pg_);
        pg_ = nullptr;
        throw std::runtime_error("EmbeddingSink: pg connection failed: " + err);
    }

    // Verify pgvector extension is installed — fail early rather than
    // getting a cryptic error on the first upsert.
    PGresult* res = PQexec(pg_, "SELECT 1 FROM pg_extension WHERE extname = 'vector'");
    bool has_vector = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0);
    PQclear(res);
    if (!has_vector) {
        throw std::runtime_error("EmbeddingSink: pgvector extension not installed in target db. "
                                 "Run: CREATE EXTENSION vector;");
    }

    std::fprintf(stderr, "[EmbeddingSink] pg connection ok, pgvector present\n");
}


void PgEmbeddingSink::call(const ChangeEvent& event) {
    switch (event.op) {
	case ChangeEvent::Op::Insert: {
	    std::fprintf(stderr, "[EmbeddingSink] insert\n");
	    if (!event.new_row) 
		return;
            const std::string text = get_column(*event.new_row, config_.embed_column);
	    if (text.empty()) 
		return;
	    const std::string id   = get_column(*event.new_row, "id");
	    auto vec = provider_->embed(text);
	    if (vec.empty()) 
		return;
        
	    upsert(id, text, vec);
	    break;
    	}
    	case ChangeEvent::Op::Update: {
    	    std::fprintf(stderr, "[EmbeddingSink] update\n");
            if (!event.new_row) 
                return;
            
            const std::string new_text = get_column(*event.new_row, config_.embed_column);
            const std::string id       = get_column(*event.new_row, "id");
 
            // This is the core value proposition: skip the embedding API call
            // entirely if the embeddable column didn't change.
            //
            // Three cases where we skip:
            //   1. new value is unchanged_toast — Postgres didn't resend it
            //      because it didn't change (see ColumnValue::is_unchanged_toast)
            //   2. old and new text values are identical strings
            //   3. new text is empty/null — nothing to embed
            if (event.old_row) {
                const std::string old_text = get_column(*event.old_row, config_.embed_column);
 
                // Check if the column was marked unchanged_toast in the new row
                // — this means it definitely didn't change, skip without comparing
                for (const auto& col : event.new_row->columns) {
                    if (col.name == config_.embed_column && col.is_unchanged_toast) {
                        std::fprintf(stderr, "[EmbeddingSink] skip update id=%s: %s unchanged (toast)\n",
                                    id.c_str(), config_.embed_column.c_str());
                        return;
                    }
                }
 
                // Full string compare: skip if text is identical
                if (!old_text.empty() && old_text == new_text) {
                    std::fprintf(stderr, "[EmbeddingSink] skip update id=%s: %s unchanged\n",
                                id.c_str(), config_.embed_column.c_str());
                    return;
                }
            }
 
            if (new_text.empty()) 
                return;
 
            auto vec = provider_->embed(new_text);
            if (vec.empty()) 
                return;
            upsert(id, new_text, vec);
            break;
    	}
    	case ChangeEvent::Op::Delete: {
    	    std::fprintf(stderr, "[EmbeddingSink] delete\n");
            if (!event.old_row) 
                return;
        
            const std::string id = get_column(*event.old_row, "id");
            if (!id.empty()) 
                remove(id);
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


bool PgEmbeddingSink::upsert(const std::string& item_id,
                             const std::string& item_name,
                             const std::vector<float>& embedding) 
{
    std::ostringstream vec_str;
    vec_str << "[";
    for (size_t i = 0; i < embedding.size(); ++i) {
        if (i > 0) 
            vec_str << ",";
        vec_str << embedding[i];
    }
    vec_str << "]";

    const std::string vec_literal = vec_str.str();

    // Parameterized query — $1=item_id, $2=item_name, $3=vector literal.
    // ON CONFLICT on item_id (unique constraint from our schema fix) means
    // INSERT on first sight, UPDATE on subsequent changes.
    const char* sql =
        "INSERT INTO public.test_embeddings (item_id, item_name, embedding) "
        "VALUES ($1, $2, $3::vector) "
        "ON CONFLICT (item_id) DO UPDATE SET "
        "    item_name = EXCLUDED.item_name, "
        "    embedding = EXCLUDED.embedding";

    const char* params[3] = {
        item_id.c_str(),
        item_name.c_str(),
        vec_literal.c_str()
    };

    PGresult* res = PQexecParams(pg_, sql, 3, nullptr, params, nullptr, nullptr, 0);
    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) {
        std::fprintf(stderr, "[EmbeddingSink] upsert failed for item_id=%s: %s\n",
                     item_id.c_str(), PQerrorMessage(pg_));
    } else {
        std::fprintf(stderr, "[EmbeddingSink] upserted item_id=%s\n", item_id.c_str());
    }
    PQclear(res);
    return ok;
}

bool PgEmbeddingSink::remove(const std::string& item_id) 
{
    const char* sql =
        "DELETE FROM public.test_embeddings WHERE item_id = $1";
    const char* params[1] = { item_id.c_str() };

    PGresult* res = PQexecParams(pg_, sql, 1, nullptr, params, nullptr, nullptr, 0);
    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) {
        std::fprintf(stderr, "[EmbeddingSink] delete failed for item_id=%s: %s\n",
                     item_id.c_str(), PQerrorMessage(pg_));
    } else {
        std::fprintf(stderr, "[EmbeddingSink] deleted item_id=%s\n", item_id.c_str());
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
