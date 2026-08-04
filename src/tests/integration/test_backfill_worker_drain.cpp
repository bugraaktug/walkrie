// test_backfill_worker_drain.cpp
//
// Integration test for issue #1 / WLK-0001 slice 4: verifies BackfillWorker::run()
// end to end against a live Postgres + real embedding provider — the doctest suite
// (test_backfill_worker.cpp) only covers to_change_event()'s pure JSON rehydration and
// dispatch through a recording sink, since run() needs a real config-driven sink stack.
//
// usage:
//   ./test_backfill_worker_drain <config.toml> [--sink-table NAME] [--conninfo "<pg conninfo>"]
//
// example:
//   ./test_backfill_worker_drain ../config_samples/config_sample_backfill.toml

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string>

#include <libpq-fe.h>

#include "backfill_store.hpp"
#include "backfill_worker.hpp"
#include "config.hpp"

namespace {

struct Options
{
    std::string config_path;
    std::string sink_table = "public.test_embeddings"; // <<< must match config_sample_backfill.toml's [[sink]] table
    std::string conninfo; // <<< optional override for the admin/setup connection; falls back to the config's [[source]] connection
};

Options parse_args(int argc, char** argv)
{
    Options opts;
    if (argc > 1) opts.config_path = argv[1];
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--sink-table" && i + 1 < argc) opts.sink_table = argv[++i];
        else if (arg == "--conninfo" && i + 1 < argc) opts.conninfo = argv[++i];
    }
    return opts;
}

void exec_or_die(PGconn* pg, const std::string& sql)
{
    PGresult* res = PQexec(pg, sql.c_str());
    auto status = PQresultStatus(res);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        std::cerr << "SQL failed: " << sql << " -- " << PQerrorMessage(pg) << "\n";
        PQclear(res);
        std::exit(1);
    }
    PQclear(res);
}

void exec_ignore_errors(PGconn* pg, const std::string& sql)
{
    PGresult* res = PQexec(pg, sql.c_str());
    PQclear(res);
}

} // namespace

int main(int argc, char** argv)
{
    Options opts = parse_args(argc, argv);
    if (opts.config_path.empty()) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--sink-table NAME] [--conninfo \"<pg conninfo>\"]\n";
        return 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(opts.config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }
    if (cfg.sources.empty() || cfg.sinks.empty()) {
        std::cerr << "config needs at least one [[source]] and one [[sink]]\n";
        return 1;
    }
    const auto& src_toml = cfg.sources.front();

    // This test drives BackfillWorker directly against a store it seeds itself — it
    // doesn't run a real dump, so the source_table/sink columns it uses must match
    // what the config's [[sink.table_mapping]] actually maps, not opts.table/opts.sink_table.
    // Reusing the config's own mapping keeps this test honest about what the worker
    // will really see in production, at the cost of tying the test to the sample config's shape.
    auto sink_mappings = cfg.sinks.front()->mappings(); // <<< mappings() returns by value — keep it alive, don't bind a reference straight to .front()
    if (sink_mappings.empty()) {
        std::cerr << "config's [[sink]] has no [[sink.table_mapping]] blocks\n";
        return 1;
    }
    const std::string source_table = sink_mappings.front().source_table;

    std::string admin_conninfo = opts.conninfo;
    if (admin_conninfo.empty()) {
        std::ostringstream derived;
        derived << "host=" << src_toml.host << " port=" << src_toml.port
                << " dbname=" << src_toml.dbname << " user=" << src_toml.user;
        if (!src_toml.password.empty()) derived << " password=" << src_toml.password;
        admin_conninfo = derived.str();
    }

    PGconn* admin = PQconnectdb(admin_conninfo.c_str());
    if (PQstatus(admin) != CONNECTION_OK) {
        std::cerr << "admin connection failed: " << PQerrorMessage(admin) << "\n";
        return 1;
    }

    exec_or_die(admin, "CREATE EXTENSION IF NOT EXISTS vector");
    exec_ignore_errors(admin, "TRUNCATE TABLE " + opts.sink_table);
    exec_or_die(admin, "DROP TABLE IF EXISTS " + source_table);
    exec_or_die(admin, "CREATE TABLE " + source_table + " (id text, name text)");

    // Not exercising dump_all() here (that's slice 3's test) — seed the store directly
    // to isolate BackfillWorker::run()'s claim -> rehydrate -> dispatch -> mark_done loop.
    auto store_path = std::filesystem::temp_directory_path() / "walkrie_it_worker.sqlite3";
    std::filesystem::remove(store_path);
    {
        pgcdc::BackfillStore store(store_path.string());
        store.open();
        store.insert_row(source_table, "wkr-1", R"({"id":"wkr-1","name":"alpha body"})");
        store.insert_row(source_table, "wkr-2", R"({"id":"wkr-2","name":"beta body"})");
        store.mark_table_dumped(source_table);
    }

    bool ok = true;

    pgcdc::BackfillWorker worker(opts.config_path, store_path.string(), /*claim_batch_size=*/10);
    if (!worker.run()) {
        std::cerr << "worker.run() failed: " << worker.last_error() << "\n";
        return 1;
    }

    {
        pgcdc::BackfillStore inspect(store_path.string());
        inspect.open();
        if (inspect.get_row_data(source_table, "wkr-1").has_value() ||
            inspect.get_row_data(source_table, "wkr-2").has_value()) {
            std::cout << "FAIL: drained rows are still present in the backfill store\n";
            ok = false;
        }
    }

    PGresult* res = PQexec(admin, ("SELECT item_id, embedding IS NOT NULL FROM " + opts.sink_table +
                                    " ORDER BY item_id").c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cout << "FAIL: could not read sink table " << opts.sink_table << ": " << PQerrorMessage(admin) << "\n";
        ok = false;
    } else if (PQntuples(res) != 2) {
        std::cout << "FAIL: expected 2 rows upserted into " << opts.sink_table << ", got " << PQntuples(res) << "\n";
        ok = false;
    } else {
        std::string id0 = PQgetvalue(res, 0, 0);
        std::string id1 = PQgetvalue(res, 1, 0);
        std::string has_vec0 = PQgetvalue(res, 0, 1);
        std::string has_vec1 = PQgetvalue(res, 1, 1);
        if (id0 != "wkr-1" || id1 != "wkr-2") {
            std::cout << "FAIL: unexpected item_id values — got '" << id0 << "', '" << id1 << "'\n";
            ok = false;
        }
        if (has_vec0 != "t" || has_vec1 != "t") {
            std::cout << "FAIL: an upserted row has a null embedding\n";
            ok = false;
        }
    }
    PQclear(res);

    exec_ignore_errors(admin, "TRUNCATE TABLE " + opts.sink_table);
    exec_ignore_errors(admin, "DROP TABLE IF EXISTS " + source_table);
    PQfinish(admin);
    std::filesystem::remove(store_path);

    if (ok) {
        std::cout << "PASS: worker drained claimed rows, upserted them via the real sink, "
                     "and removed them from the store\n";
    }
    return ok ? 0 : 1;
}
