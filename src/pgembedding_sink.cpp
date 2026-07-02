#include "pgembedding_sink.hpp"

#include <cstdio>
#include <cstring>
#include <sstream>
#include <stdexcept>

namespace pgcdc {

PgEmbeddingSink::PgEmbeddingSink(PgEmbeddingSinkConfig config)
    : config_(std::move(config)) {}

PgEmbeddingSink::~PgEmbeddingSink() 
{
    if (ctx_) { 
	llama_free(ctx_);        
	ctx_ = nullptr; 
    }
    if (model_) { 
	llama_model_free(model_); 
	model_ = nullptr; 
    }
    if (pg_) { 
	PQfinish(pg_);            
	pg_ = nullptr; 
    }
}

void PgEmbeddingSink::init() 
{
    // --- llama.cpp model load ---
    llama_backend_init();

    llama_model_params mparams = llama_model_default_params();
    mparams.n_gpu_layers = 0; // CPU only; set > 0 if you add GPU support later

    model_ = llama_model_load_from_file(config_.model_path.c_str(), mparams);
    if (!model_) {
        throw std::runtime_error("EmbeddingSink: failed to load model from " + config_.model_path);
    }

    llama_context_params cparams = llama_context_default_params();
    cparams.n_ctx       = static_cast<uint32_t>(config_.n_ctx);
    cparams.n_threads   = static_cast<uint32_t>(config_.n_threads);
    cparams.embeddings  = true;  // required: tells llama.cpp to compute and
                                  // expose embedding vectors, not just logits

    ctx_ = llama_init_from_model(model_, cparams);
    if (!ctx_) {
        llama_model_free(model_);
        model_ = nullptr;
        throw std::runtime_error("EmbeddingSink: failed to create llama context");
    }

    std::fprintf(stderr, "[EmbeddingSink] model loaded: %s (%d dims)\n",
                 config_.model_path.c_str(),
                 llama_model_n_embd(model_));

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
	    auto vec = embed(text);
	    if (vec.empty()) 
		return;
        
	    upsert(id, text, vec);
	    break;
    	}
    	case ChangeEvent::Op::Update: {
    	    std::fprintf(stderr, "[EmbeddingSink] update\n");
            break;
    	}
    	case ChangeEvent::Op::Delete: {
    	    std::fprintf(stderr, "[EmbeddingSink] delete\n");
            break;
    	}
    }
}

std::vector<float> PgEmbeddingSink::embed(const std::string& text) 
{
    // Tokenize. llama_tokenize returns the number of tokens written, or a
    // negative number if the output buffer was too small.
    const int n_ctx = static_cast<int>(llama_n_ctx(ctx_));
    std::vector<llama_token> tokens(n_ctx);

    const llama_vocab* vocab = llama_model_get_vocab(model_);
    int n_tokens = llama_tokenize(
        vocab,
        text.c_str(),
        static_cast<int32_t>(text.size()),
        tokens.data(),
        static_cast<int32_t>(tokens.size()),
        /*add_special=*/true,
        /*parse_special=*/false
    );

    if (n_tokens < 0) {
        std::fprintf(stderr, "[EmbeddingSink] tokenize failed for text (too long?): %.80s...\n",
                     text.c_str());
        return {};
    }
    tokens.resize(static_cast<size_t>(n_tokens));

    llama_memory_clear(llama_get_memory(ctx_), true);
    // llama_batch_get_one: a convenience wrapper that sets up a batch for a
    // single sequence (seq_id=0) from a token array. All tokens get the
    // same seq_id so llama_get_embeddings_seq(ctx_, 0) retrieves the pooled
    // embedding for the whole sequence, not just the last token.
    llama_batch batch = llama_batch_get_one(tokens.data(), n_tokens);

    if (llama_decode(ctx_, batch) != 0) {
        std::fprintf(stderr, "[EmbeddingSink] llama_decode failed\n");
        return {};
    }

    // llama_get_embeddings_seq: returns a pointer to the pooled embedding
    // for sequence 0. The pointer is owned by the context and valid until
    // the next llama_decode or llama_free call — we must copy it out.
    const int n_embd = llama_model_n_embd(model_);
    const float* embd = llama_get_embeddings_seq(ctx_, 0);
    if (!embd) {
        // Fallback: some model/context configurations return null from seq
        // but work with ith. Try token 0 as a last resort before giving up.
        embd = llama_get_embeddings_ith(ctx_, 0);
    }
    if (!embd) {
        std::fprintf(stderr, "[EmbeddingSink] llama_get_embeddings returned null — "
                     "check that cparams.embeddings = true was set\n");
        return {};
    }

    return std::vector<float>(embd, embd + n_embd);
}

std::string PgEmbeddingSink::get_column(const DecodedRow& row, const std::string& col_name) 
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) {
            if (col.is_null || col.is_unchanged_toast) return "";
            return col.text_value;
        }
    }
    return "";
}


bool PgEmbeddingSink::upsert(const std::string& item_id,
			     const std::string& item_name,
                             const std::vector<float>& embedding) {
    
    std::ostringstream vec_str;
    vec_str << "[";
    for (size_t i = 0; i < embedding.size(); ++i) {
        if (i > 0) vec_str << ",";
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

} // namespace pgcdc
