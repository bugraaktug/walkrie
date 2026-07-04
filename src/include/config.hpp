#pragma once

#include <stdexcept>
#include <string>
#include <vector>

#include <toml.hpp>

namespace pgcdc 
{

struct SourceConfig 
{
    std::string host        = "localhost";
    std::string port        = "5432";
    std::string dbname;
    std::string user;
    std::string password;
    std::string slot_name   = "pgcdc_slot";
    std::string publication = "pgcdc_pub";
};

struct SinkConfig 
{
    std::string host     = "localhost";
    std::string port     = "5432";
    std::string dbname;
    std::string user;
    std::string password;
    std::string table    = "public.test_embeddings";
};

struct EmbeddingConfig 
{
    std::string provider    = "llama";   // "llama" or "openai" (openai = future stub)
    std::string model_path;              // required for llama; ignored for openai
    std::string api_key;                 // required for openai; ignored for llama
    std::string embed_column = "name";   // which source column to embed
    std::string id_column    = "id";     // primary key column name
    int         dimensions   = 1024;     // must match model output + pgvector column
    int         n_threads    = 4;        // llama.cpp CPU threads
    int         n_ctx        = 512;      // llama.cpp context window
};

struct AppConfig 
{
    std::vector<SourceConfig>    sources;
    SinkConfig      sink;
    EmbeddingConfig embedding;

    std::vector<std::string> validate() const {
        std::vector<std::string> errors;

	if (sources.empty())
            errors.push_back("[source] at least one [[source]] block is required");

        for (size_t i = 0; i < sources.size(); ++i) {
            const auto& s = sources[i];
            if (s.dbname.empty())
                errors.push_back("[source][" + std::to_string(i) + "] dbname is required");
            if (s.user.empty())
                errors.push_back("[source][" + std::to_string(i) + "] user is required");
        }

        if (sink.dbname.empty())
            errors.push_back("[sink] dbname is required");
        if (sink.user.empty())
            errors.push_back("[sink] user is required");
        if (sink.table.empty())
            errors.push_back("[sink] table is required");

        if (embedding.provider != "llama" && embedding.provider != "openai")
            errors.push_back("[embedding] provider must be 'llama' or 'openai'");

        if (embedding.provider == "llama" && embedding.model_path.empty())
            errors.push_back("[embedding] model_path is required when provider = 'llama'");

        if (embedding.provider == "openai" && embedding.api_key.empty())
            errors.push_back("[embedding] api_key is required when provider = 'openai'");

        if (embedding.embed_column.empty())
            errors.push_back("[embedding] embed_column is required");
        if (embedding.id_column.empty())
            errors.push_back("[embedding] id_column is required");
        if (embedding.dimensions <= 0)
            errors.push_back("[embedding] dimensions must be > 0");

        return errors;
    }
};

inline AppConfig load_config(const std::string& path) 
{
    toml::table tbl;
    try {
        tbl = toml::parse_file(path);
    } catch (const toml::parse_error& e) {
        throw std::runtime_error(
            std::string("config parse error in '") + path + "': " + e.what());
    }

    AppConfig cfg;

    // Helper: read a string key from a toml table, falling back to default
    auto str = [](const toml::table* t, const char* key, const std::string& def) -> std::string {
        if (!t) 
	    return def;
        auto v = t->get_as<std::string>(key);
        return v ? **v : def;
    };
    auto i32 = [](const toml::table* t, const char* key, int def) -> int {
        if (!t) 
	    return def;
        auto v = t->get_as<int64_t>(key);
        return v ? static_cast<int>(**v) : def;
    };


    if (auto* arr = tbl["source"].as_array()) {
        for (auto& elem : *arr) {
	    SourceConfig repl_cfg;
	    if (auto* s = elem.as_table()) {
		repl_cfg.host        = str(s, "host",        repl_cfg.host);
		repl_cfg.port        = str(s, "port",        repl_cfg.port);
		repl_cfg.dbname      = str(s, "dbname",      repl_cfg.dbname);
		repl_cfg.user        = str(s, "user",        repl_cfg.user);
            	repl_cfg.password    = str(s, "password",    repl_cfg.password);
            	repl_cfg.slot_name   = str(s, "slot_name",   repl_cfg.slot_name);
            	repl_cfg.publication = str(s, "publication", repl_cfg.publication);
	    }
	    cfg.sources.push_back(repl_cfg);
    	}
    }	    

    if (auto* s = tbl["sink"].as_table()) {
        cfg.sink.host     = str(s, "host",     cfg.sink.host);
        cfg.sink.port     = str(s, "port",     cfg.sink.port);
        cfg.sink.dbname   = str(s, "dbname",   cfg.sink.dbname);
        cfg.sink.user     = str(s, "user",     cfg.sink.user);
        cfg.sink.password = str(s, "password", cfg.sink.password);
        cfg.sink.table    = str(s, "table",    cfg.sink.table);
    }

    if (auto* e = tbl["embedding"].as_table()) {
        cfg.embedding.provider    = str(e, "provider",     cfg.embedding.provider);
        cfg.embedding.model_path  = str(e, "model_path",   cfg.embedding.model_path);
        cfg.embedding.api_key     = str(e, "api_key",      cfg.embedding.api_key);
        cfg.embedding.embed_column= str(e, "embed_column", cfg.embedding.embed_column);
        cfg.embedding.id_column   = str(e, "id_column",    cfg.embedding.id_column);
        cfg.embedding.dimensions  = i32(e, "dimensions",   cfg.embedding.dimensions);
        cfg.embedding.n_threads   = i32(e, "n_threads",    cfg.embedding.n_threads);
        cfg.embedding.n_ctx       = i32(e, "n_ctx",        cfg.embedding.n_ctx);
    }

    return cfg;
}

} // namespace pgcdc
