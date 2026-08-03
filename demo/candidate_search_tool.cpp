// candidate_search.cpp
//
// Semantic + hybrid search CLI for the walkrie CV/HR demo. Walkrie itself
// only populates the vector sink table (source -> embedding -> upsert);
// it doesn't query it. This tool closes that loop for demo purposes:
// embed a free-text query with the *same* model/config walkrie used for
// ingestion, then find the nearest candidates by cosine distance —
// optionally narrowed first by a plain SQL WHERE filter on `location`.
//
// This is "hybrid search" in the sense that matters for a real deployment:
// exact structured filtering (location, and easy to extend to years of
// experience, department, etc.) combined with meaning-based ranking in a
// single query, rather than choosing one or the other.
//
// usage:
//   ./candidate_search <config.toml> <query text...> [--limit N] [--location <city>] [--conninfo <libpq conninfo>]
//
// examples:
//   ./candidate_search config_cv_demo.toml "senior backend engineer with postgres and docker experience"
//
//   ./candidate_search config_cv_demo.toml "senior backend engineer with postgres and docker experience" --location Berlin --limit 5
//
//   ./candidate_search config_cv_demo.toml "senior backend engineer with postgres and docker experience" --conninfo "host=localhost port=5432 dbname=hr_demo user=walkrie_demo password=walkrie"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <libpq-fe.h>
#include "config.hpp"
#include "embedding_provider.hpp"

int main(int argc, char** argv) 
{
    if (argc < 3) {
        std::cerr << "usage: " << argv[0]
                  << " <config.toml> <query text...> [--limit N] [--location <city>] [--conninfo <libpq conninfo>]\n";
        return 1;
    }

    std::string config_path = argv[1];
    int limit = 10;
    std::string location_filter; // empty = no filter, semantic search only
    std::string conninfo_override; // empty = use hardcoded default below
    std::vector<std::string> query_parts;

    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--limit" && i + 1 < argc) {
            limit = std::stoi(argv[++i]);
        } else if (arg == "--location" && i + 1 < argc) {
            location_filter = argv[++i];
        } else if (arg == "--conninfo" && i + 1 < argc) {
            conninfo_override = argv[++i];
        } else {
            query_parts.push_back(arg);
        }
    }
    if (query_parts.empty()) {
        std::cerr << "error: no query text provided\n";
        return 1;
    }

    std::ostringstream qss;
    for (size_t i = 0; i < query_parts.size(); ++i) {
        if (i > 0) qss << " ";
        qss << query_parts[i];
    }
    std::string query_text = qss.str();

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }

    std::cerr << "Loading embedding model (this can take a few seconds)...\n";
    auto provider = pgcdc::make_embedding_provider(cfg.embedding);
    try {
        provider->init();
    } catch (const std::exception& e) {
        std::cerr << "error initializing embedding provider: " << e.what() << "\n";
        return 1;
    }

    auto vecs = provider->embed_batch({query_text});
    if (vecs.size() != 1 || vecs[0].empty()) {
        std::cerr << "error: embedding failed for query text\n";
        return 1;
    }
    auto& vec = vecs[0];

    std::ostringstream vec_str;
    vec_str << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        if (i > 0) vec_str << ",";
        vec_str << vec[i];
    }
    vec_str << "]";
    std::string vec_literal = vec_str.str();

    // NOTE: this demo tool hardcodes its own connection string rather than
    // reading it back out of cfg.sinks, since SinkConfiguration doesn't
    // currently expose a public accessor for raw connection fields (it's
    // only consumed internally by create_sink()). Adjust to match your
    // real [[sink]] block, or pass --conninfo to override without editing
    // the binary.
    std::string conninfo = conninfo_override.empty()
        ? "host=localhost port=5432 dbname=hr_demo user=walkrie_demo password=walkrie"
        : conninfo_override;

    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "database connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }

    std::string limit_str = std::to_string(limit);
    std::string sql;
    std::vector<const char*> params;

    if (location_filter.empty()) {
        // Pure semantic search — no structured filter applied.
        sql =
            "SELECT candidate_id, full_name, years_experience, location, cv_text, "
            "embedding <=> $1::vector AS distance "
            "FROM candidate_search "
            "ORDER BY distance ASC "
            "LIMIT $2";
        params = { vec_literal.c_str(), limit_str.c_str() };
    } else {
        // Hybrid search: exact SQL filter on location narrows the
        // candidate set first, then vector distance ranks what's left.
        // This is a plain WHERE clause — nothing special about pgvector
        // here, which is exactly the point: structured and semantic
        // filtering compose naturally in ordinary SQL.
        sql =
            "SELECT candidate_id, full_name, years_experience, location, cv_text, "
            "embedding <=> $1::vector AS distance "
            "FROM candidate_search "
            "WHERE location = $2 "
            "ORDER BY distance ASC "
            "LIMIT $3";
        params = { vec_literal.c_str(), location_filter.c_str(), limit_str.c_str() };
    }

    PGresult* res = PQexecParams(pg, sql.c_str(), static_cast<int>(params.size()),
                                  nullptr, params.data(), nullptr, nullptr, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "query failed: " << PQerrorMessage(pg) << "\n";
        PQclear(res);
        PQfinish(pg);
        return 1;
    }

    int n = PQntuples(res);
    std::cout << "\nQuery: \"" << query_text << "\"";
    if (!location_filter.empty()) {
        std::cout << "  (filtered to location = \"" << location_filter << "\")";
    }
    std::cout << "\nTop " << n << " matches:\n\n";

    for (int i = 0; i < n; ++i) {
        std::cout << (i + 1) << ". " << PQgetvalue(res, i, 1)
                  << " (" << PQgetvalue(res, i, 2) << " yrs, " << PQgetvalue(res, i, 3) << ")"
                  << " — distance: " << PQgetvalue(res, i, 5) << "\n"
                  << "   " << PQgetvalue(res, i, 4) << "\n\n";
    }

    PQclear(res);
    PQfinish(pg);
    return 0;
}
