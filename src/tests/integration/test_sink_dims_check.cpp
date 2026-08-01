// test_sink_dims_check.cpp
//
// Integration test: verifies PgEmbeddingSink::verify_sink_column_dimensions()
// (pgembedding_sink.cpp), called from init() right after the pgvector
// extension check. It compares the sink table's actual vector(N) column
// width (read from pg_attribute.atttypmod) against the embedding
// provider's real output size, and is meant to throw before any writes
// happen rather than let every upsert fail at runtime.
//
// This can't be covered by the doctest unit suite (src/tests/) — the
// check requires a real pg_attribute lookup against a live Postgres
// connection, which the unit suite deliberately avoids needing.
//
// Uses its own dedicated tables (walkrie_it_dims_*, dropped/recreated per
// case) and its own dedicated config file (config_sample_dims_check_test.toml)
// distinct from test_sink_batch_mode's (walkrie_it_single/walkrie_it_batch) —
// so this test, that one, and your own manually-managed tables/configs don't
// collide.
//
// usage:
//   ./test_sink_dims_check <config.toml> --conninfo "<pg conninfo>"
//
// example:
//   ./test_sink_dims_check ../config_samples/config_sample_dims_check_test.toml --conninfo "host=localhost port=5432 dbname=walkrie_test user=walkrie_demo password=changeme"

#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <libpq-fe.h>
#include "config.hpp"
#include "embedding_provider.hpp"
#include "pgembedding_sink.hpp"

namespace {

pgcdc::TableMapping make_mapping()
{
    pgcdc::TableMapping tm;
    tm.source_table  = "it_events";
    tm.id_source_    = "id";
    tm.id_sink_      = "item_id";
    tm.embed_source_ = "body";
    tm.embed_sink_   = "item_body";
    return tm;
}

void drop_table(PGconn* pg, const std::string& table)
{
    std::string sql = "DROP TABLE IF EXISTS " + table;
    PGresult* r = PQexec(pg, sql.c_str());
    PQclear(r);
}

// column_ddl == nullopt creates the table with NO embedding column at all
// (exercises the "sink column not found" path); otherwise it's the full
// column type, e.g. "vector(1024)" or bare "vector" (unconstrained).
bool create_table(PGconn* pg, const std::string& table, const std::optional<std::string>& column_ddl)
{
    std::ostringstream sql;
    sql << "CREATE TABLE " << table << " ("
        << "  row_id bigserial PRIMARY KEY,"
        << "  item_id text NOT NULL UNIQUE,"
        << "  item_body text NOT NULL";
    if (column_ddl) {
        sql << ", embedding " << *column_ddl;
    }
    sql << ")";

    PGresult* r = PQexec(pg, sql.str().c_str());
    bool ok = (PQresultStatus(r) == PGRES_COMMAND_OK);
    if (!ok) {
        std::cerr << "schema setup failed for " << table << ": " << PQerrorMessage(pg) << "\n";
    }
    PQclear(r);
    return ok;
}

struct Case
{
    std::string name;
    std::string table_name;
    std::optional<std::string> column_ddl;
    bool expect_throw;
    std::vector<std::string> expect_message_contains; // only checked when expect_throw
};

} // namespace

int main(int argc, char** argv)
{
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml> --conninfo \"<pg conninfo>\"\n";
        return 1;
    }

    std::string config_path = argv[1];
    std::string conninfo;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--conninfo" && i + 1 < argc) conninfo = argv[++i];
    }
    if (conninfo.empty()) {
        std::cerr << "error: --conninfo is required\n";
        return 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }

    std::cerr << "Loading embedding provider (" << cfg.embedding.provider << ")...\n";
    auto provider = pgcdc::make_embedding_provider(cfg.embedding);
    try {
        provider->init();
    } catch (const std::exception& e) {
        std::cerr << "error initializing embedding provider: " << e.what() << "\n";
        return 1;
    }
    const int provider_dims = provider->dimensions();
    std::cerr << "provider dims = " << provider_dims << "\n";

    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "database connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }
    PGresult* ext = PQexec(pg, "CREATE EXTENSION IF NOT EXISTS vector");
    PQclear(ext);

    const std::string mismatched_ddl = "vector(" + std::to_string(provider_dims + 7) + ")";

    std::vector<Case> cases;
    cases.push_back({
        "matching_dims_passes_init",
        "walkrie_it_dims_match",
        "vector(" + std::to_string(provider_dims) + ")",
        false,
        {}
    });
    cases.push_back({
        "mismatched_dims_throws_naming_both_values",
        "walkrie_it_dims_mismatch",
        mismatched_ddl,
        true,
        {mismatched_ddl, std::to_string(provider_dims) + "-dimensional"}
    });
    cases.push_back({
        "unconstrained_vector_column_skips_check",
        "walkrie_it_dims_unconstrained",
        std::string("vector"), // bare, no typmod — atttypmod = -1
        false,
        {}
    });
    cases.push_back({
        "missing_sink_column_throws",
        "walkrie_it_dims_no_column",
        std::nullopt, // embedding column never created
        true,
        {"not found"}
    });

    int total = 0;
    int passed = 0;

    for (auto& c : cases) {
        ++total;
        std::cout << "\n=== Case: " << c.name << " ===\n";

        drop_table(pg, c.table_name);
        if (!create_table(pg, c.table_name, c.column_ddl)) {
            std::cout << "FAIL (schema setup failed)\n";
            continue;
        }

        pgcdc::PgEmbeddingSinkConfig sink_cfg;
        sink_cfg.pg_conninfo = conninfo;
        sink_cfg.sink_table  = c.table_name;
        sink_cfg.sink_column = "embedding";
        sink_cfg.mappings.push_back(make_mapping());

        bool threw = false;
        std::string message;
        try {
            pgcdc::PgEmbeddingSink sink(sink_cfg, provider);
            sink.init();
        } catch (const std::exception& e) {
            threw = true;
            message = e.what();
        }

        bool ok = (threw == c.expect_throw);
        if (ok && threw) {
            for (auto& needle : c.expect_message_contains) {
                if (message.find(needle) == std::string::npos) {
                    ok = false;
                    std::cout << "  - expected message to contain \"" << needle << "\" but got: " << message << "\n";
                }
            }
        }

        if (ok) {
            std::cout << "PASS" << (threw ? (" (threw as expected: " + message + ")") : " (no throw, as expected)") << "\n";
            ++passed;
        } else {
            std::cout << "FAIL (expected " << (c.expect_throw ? "throw" : "no throw")
                      << ", got " << (threw ? "throw" : "no throw")
                      << (threw ? (": " + message) : "") << ")\n";
        }

        drop_table(pg, c.table_name);
    }

    std::cout << "\n=== " << passed << " / " << total << " cases passed ===\n";

    PQfinish(pg);
    return (passed == total) ? 0 : 1;
}
