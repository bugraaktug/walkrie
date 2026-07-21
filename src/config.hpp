#pragma once

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unistd.h>  
#include <vector>

#include <toml.hpp>

#include "sink_configuration.hpp"

namespace pgcdc 
{

struct AppSettings 
{
    std::string log_level        = "info";
    std::string log_file         = "/var/log/walkrie/walkrie.log";
    int         log_max_size_mb  = 10;
    int         log_max_files    = 5;
};

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

//   "id"       — primary key used as the upsert key in the sink table
//   "embed"    — text passed to the embedding provider; result stored as vector
//   "metadata" — stored as-is in the sink table alongside the vector
struct ColumnMapping
{
    std::string source_column; // column name in the source (watched) table
    std::string sink_column;   // column name in the sink (embeddings) table
    std::string role;          // "id", "embed", or "metadata"
};

struct TableMapping
{
    std::string source_table; 
    std::vector<ColumnMapping> columns;

    // Derived at load time — no re-scanning per event
    std::string id_source_;
    std::string id_sink_;
    std::string embed_source_;
    std::string embed_sink_;

    bool has_discriminator_ = false;
    std::string discriminator_sink_;   // sink column name, e.g. "category"
    std::string discriminator_label_;  // fixed value for every row from this mapping


    void resolve_columns() {
        for (const auto& m : columns) {
            if (m.role == "id") {
                id_source_ = m.source_column;
                id_sink_   = m.sink_column;
            } else if (m.role == "embed") {
                embed_source_ = m.source_column;
                embed_sink_   = m.sink_column;
            }
        }
    }
};

struct EmbeddingConfig 
{
    std::string provider    = "llama";   // "llama" or "openai" (openai = future stub)
    std::string model;			         // required for openai; ignored for llama
    std::string model_path;              // required for llama; ignored for openai
    std::string api_key;                 // required for openai; ignored for llama
    int         dimensions   = 1024;     // must match model output + pgvector column
    int         n_threads    = 4;        // llama.cpp CPU threads
    int         n_ctx        = 512;      // llama.cpp context window
};

struct AppConfig 
{
    AppSettings                                     settings;
    std::vector<SourceConfig>                       sources;
    std::vector<std::unique_ptr<SinkConfiguration>> sinks;
    EmbeddingConfig                                 embedding;

    std::vector<std::string> validate() const {
        std::vector<std::string> errors;

        const std::vector<std::string> valid_levels = {
            "trace",
            "debug",
            "info",
            "warn",
            "error",
            "critical"
        };
        
        if (std::find(valid_levels.begin(), valid_levels.end(), settings.log_level) == valid_levels.end()) {
            errors.push_back("[app] log_level must be one of: trace, debug, info, warn, error, critical");
        }
	    if (sources.empty()) {
            errors.push_back("[source] at least one [[source]] block is required");
        }

        for (size_t i = 0; i < sources.size(); ++i) {
            const auto& s = sources[i];
            if (s.dbname.empty()) {
                errors.push_back("[source][" + std::to_string(i) + "] dbname is required");
            }
	        if (s.user.empty()) {
                errors.push_back("[source][" + std::to_string(i) + "] user is required");
            }
        }

        if (embedding.provider != "llama" && embedding.provider != "openai") {
            errors.push_back("[embedding] provider must be 'llama' or 'openai'");
        }
        if (embedding.provider == "llama") {
            if (embedding.model_path.empty()) {
                errors.push_back("[embedding] model_path is required when provider = 'llama'");
            } else {
                namespace fs = std::filesystem;
                std::error_code ec;

                if (!fs::exists(embedding.model_path, ec)) {
                    errors.push_back("[embedding] model_path does not exist: '" +
                                    embedding.model_path + "' — place a GGUF model file at this "
                                    "path before starting walkrie (see README.md for download "
                                    "instructions)");
                } else if (!fs::is_regular_file(embedding.model_path, ec)) {
                    errors.push_back("[embedding] model_path exists but is not a regular file: '" +
                                    embedding.model_path + "'");
                } else if (access(embedding.model_path.c_str(), R_OK) != 0) {
                    errors.push_back("[embedding] model_path exists but is not readable by the "
                                    "current user: '" + embedding.model_path +
                                    "' — check file ownership/permissions (the walkrie service "
                                    "runs as the 'walkrie' system user)");
                } else if (fs::file_size(embedding.model_path, ec) == 0) {
                    errors.push_back("[embedding] model_path points to an empty (0-byte) file: '" +
                                    embedding.model_path + "' — the download may have failed or "
                                    "been interrupted");
                }
            }
	        if (embedding.dimensions <= 0) {
                errors.push_back("[embedding] dimensions must be > 0");
            }
        }
        if (embedding.provider == "openai") {
            if (embedding.api_key.empty()) {
                errors.push_back("[embedding] api_key is required when provider = 'openai'");
            }
            if (embedding.model.empty()) {
                errors.push_back("[embedding] model is required when provider = 'openai'");
            }
            // text-embedding-3-* models support truncating to a smaller `dimensions`
            if (embedding.model == "text-embedding-3-small" && embedding.dimensions > 1536) {
                errors.push_back("[embedding] dimensions must be <= 1536 for text-embedding-3-small");
            }
            if (embedding.model == "text-embedding-3-large" && embedding.dimensions > 3072) {
                errors.push_back("[embedding] dimensions must be <= 3072 for text-embedding-3-large");
            }
            if (embedding.model == "text-embedding-ada-002" && embedding.dimensions != 1536) {
                errors.push_back("[embedding] text-embedding-ada-002 does not support dimension truncation — dimensions must be exactly 1536");
            }
        }

        for (size_t i = 0; i < sinks.size(); ++i) {
            const auto& sink = sinks[i];
            if (sink) {
                auto sink_errors = sink->validate();
                errors.insert(errors.end(), sink_errors.begin(), sink_errors.end());
            } else {
                errors.push_back("[sink] a valid sink type is required");
            }
        }

        return errors;
    }
};

inline AppConfig load_config(const std::string& path) 
{
    toml::table tbl;
    try {
        tbl = toml::parse_file(path);
    } catch (const toml::parse_error& e) {
        throw std::runtime_error(std::string("config parse error in '") + path + "': " + e.what());
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

    if (auto* a = tbl["app"].as_table()) {
        cfg.settings.log_level       = str(a, "log_level",       cfg.settings.log_level);
        cfg.settings.log_file        = str(a, "log_file",        cfg.settings.log_file);
        cfg.settings.log_max_size_mb = i32(a, "log_max_size_mb", cfg.settings.log_max_size_mb);
        cfg.settings.log_max_files   = i32(a, "log_max_files",   cfg.settings.log_max_files);
    }

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
    
    if (auto* arr = tbl["sink"].as_array()) {
        for (auto& elem : *arr) {
            if (auto* s = elem.as_table()) {
                std::string sink_type = str(s, "type", "postgres-embedding");
                auto sink = instantiate_sink(sink_type);
                sink->load_from_config(*s);
                cfg.sinks.push_back(std::move(sink));
            }
        }
    }

    if (auto* e = tbl["embedding"].as_table()) {
        cfg.embedding.provider    = str(e, "provider",     cfg.embedding.provider);
        cfg.embedding.model       = str(e, "model",        cfg.embedding.model);
        cfg.embedding.model_path  = str(e, "model_path",   cfg.embedding.model_path);
        cfg.embedding.api_key     = str(e, "api_key",      cfg.embedding.api_key);
        cfg.embedding.dimensions  = i32(e, "dimensions",   cfg.embedding.dimensions);
        cfg.embedding.n_threads   = i32(e, "n_threads",    cfg.embedding.n_threads);
        cfg.embedding.n_ctx       = i32(e, "n_ctx",        cfg.embedding.n_ctx);
    }

    return cfg;
}

} // namespace pgcdc
