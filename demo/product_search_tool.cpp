// product_search_tool.cpp
//
// Semantic + hybrid search CLI for the walkrie multilingual e-commerce demo.
// Mirrors candidate_search_tool.cpp's pattern, with one difference that's the
// whole point of this demo: it must be pointed at config_product_query.toml
// (the retrieval.query LoRA adapter), not config_product_demo.toml (the
// retrieval.passage adapter walkrie itself uses to index products) — see
// config_product_query.toml for why those can't be the same file.
//
// usage:
//   ./product_search <config.toml> <query text...> [--limit N] [--category <name>] [--language ja|tr] [--conninfo <libpq conninfo>]
//
// examples:
//   ./product_search demo/config_product_query.toml "防水のバックパック"
//   ./product_search demo/config_product_query.toml "su geçirmez sırt çantası" --language tr --limit 5
//   ./product_search demo/config_product_query.toml "yüz bakım ürünleri" --category Beauty

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
                  << " <config.toml> <query text...> [--limit N] [--category <name>] [--language ja|tr] [--conninfo <libpq conninfo>]\n";
        return 1;
    }

    std::string config_path = argv[1];
    int limit = 10;
    std::string category_filter; // empty = no filter
    std::string language_filter; // empty = no filter
    std::string conninfo_override; // empty = use hardcoded default below
    std::vector<std::string> query_parts;

    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--limit" && i + 1 < argc) {
            limit = std::stoi(argv[++i]);
        } else if (arg == "--category" && i + 1 < argc) {
            category_filter = argv[++i];
        } else if (arg == "--language" && i + 1 < argc) {
            language_filter = argv[++i];
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

    // NOTE: hardcodes its own connection string for the same reason
    // candidate_search_tool.cpp does — pass --conninfo to override.
    std::string conninfo = conninfo_override.empty()
        ? "host=localhost port=5432 dbname=ecommerce_demo user=walkrie_demo password=walkrie"
        : conninfo_override;

    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "database connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }

    std::string limit_str = std::to_string(limit);

    std::string sql =
        "SELECT product_id, title, category, language, price, currency, description, "
        "embedding <=> $1::vector AS distance "
        "FROM product_search ";
    std::vector<const char*> params = { vec_literal.c_str() };
    std::vector<std::string> where_clauses;
    if (!category_filter.empty()) {
        where_clauses.push_back("category = $" + std::to_string(params.size() + 1));
        params.push_back(category_filter.c_str());
    }
    if (!language_filter.empty()) {
        where_clauses.push_back("language = $" + std::to_string(params.size() + 1));
        params.push_back(language_filter.c_str());
    }
    if (!where_clauses.empty()) {
        sql += "WHERE ";
        for (size_t i = 0; i < where_clauses.size(); ++i) {
            if (i > 0) sql += " AND ";
            sql += where_clauses[i];
        }
        sql += " ";
    }
    sql += "ORDER BY distance ASC LIMIT $" + std::to_string(params.size() + 1);
    params.push_back(limit_str.c_str());

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
    if (!category_filter.empty()) std::cout << "  (category = \"" << category_filter << "\")";
    if (!language_filter.empty()) std::cout << "  (language = \"" << language_filter << "\")";
    std::cout << "\nTop " << n << " matches:\n\n";

    for (int i = 0; i < n; ++i) {
        std::cout << (i + 1) << ". " << PQgetvalue(res, i, 1)
                  << " [" << PQgetvalue(res, i, 2) << ", " << PQgetvalue(res, i, 3) << "]"
                  << " — " << PQgetvalue(res, i, 4) << " " << PQgetvalue(res, i, 5)
                  << " — distance: " << PQgetvalue(res, i, 7) << "\n"
                  << "   " << PQgetvalue(res, i, 6) << "\n\n";
    }

    PQclear(res);
    PQfinish(pg);
    return 0;
}
