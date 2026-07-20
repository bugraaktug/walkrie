// src/bench/pg_upsert_bench.cpp
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <sstream>
#include <random>

#include <libpq-fe.h>
#include "config.hpp"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--calls N] [--vector-dims D]\n";
        return 1;
    }

    int num_calls = 100;
    int dims = 1024;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--calls" && i + 1 < argc) num_calls = std::stoi(argv[++i]);
        if (arg == "--vector-dims" && i + 1 < argc) dims = std::stoi(argv[++i]);
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }

    std::string conninfo = "host=localhost port=5432 dbname=qdb user=quser password=quser1234";
    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }

    // ADJUST: match this exactly to PgSqlBuilder's actual output for your
    // real sink table/columns — table name, column names, and conflict
    // target must match a real unique constraint or every call will error.
    std::string upsert_sql =
        "INSERT INTO test_embeddings (item_id, item_name, embedding) "
        "VALUES ($1, $2, $3::vector) "
        "ON CONFLICT (item_id) DO UPDATE SET "
        "item_name = EXCLUDED.item_name, embedding = EXCLUDED.embedding";

    // Build one fixed dummy vector once — this bench isolates DB cost only,
    // so embedding content/randomness doesn't matter, just realistic size.
    std::mt19937 rng(42);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::ostringstream vec_str;
    vec_str << "[";
    for (int i = 0; i < dims; ++i) {
        if (i > 0) vec_str << ",";
        vec_str << dist(rng);
    }
    vec_str << "]";
    std::string vec_literal = vec_str.str();

    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<size_t>(num_calls));

    std::cout << "running " << num_calls << " upsert calls against a "
              << dims << "-dim vector column...\n";

    for (int i = 0; i < num_calls; ++i) {
        std::string id_val = "bench-" + std::to_string(i);
        std::string body_val = "Bench upsert row " + std::to_string(i);

        const char* params[3] = { id_val.c_str(), body_val.c_str(), vec_literal.c_str() };

        auto start = std::chrono::steady_clock::now();
        PGresult* res = PQexecParams(pg, upsert_sql.c_str(), 3, nullptr, params, nullptr, nullptr, 0);
        auto end = std::chrono::steady_clock::now();

        bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
        if (!ok) {
            std::cerr << "call " << i << " failed: " << PQerrorMessage(pg) << "\n";
            PQclear(res);
            continue;
        }
        PQclear(res);

        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        latencies_ms.push_back(ms);
    }

    PQfinish(pg);

    if (latencies_ms.empty()) {
        std::cerr << "no successful calls — nothing to report\n";
        return 1;
    }

    std::sort(latencies_ms.begin(), latencies_ms.end());
    double sum = std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);
    double avg = sum / latencies_ms.size();
    double p50 = latencies_ms[latencies_ms.size() / 2];
    double p95 = latencies_ms[static_cast<size_t>(latencies_ms.size() * 0.95)];

    std::cout << "\n=== pg upsert latency (no embedding, dummy vector) ===\n";
    std::cout << "successful calls: " << latencies_ms.size() << " / " << num_calls << "\n";
    std::cout << "min:  " << latencies_ms.front() << " ms\n";
    std::cout << "avg:  " << avg << " ms\n";
    std::cout << "p50:  " << p50 << " ms\n";
    std::cout << "p95:  " << p95 << " ms\n";
    std::cout << "max:  " << latencies_ms.back() << " ms\n";

    return 0;
}
