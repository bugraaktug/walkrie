// test_backfill_dump.cpp
//
// Integration test for issue #1 / WLK-0001 slice 3: verifies the actual
// dump phase + live-event reconciliation wired into PgReplicationSource
// against a live Postgres — the doctest suite only covers BackfillUtil's
// pure logic (test_backfill_util.cpp) since dump_all() needs a real PGconn.
//
// usage:
//   ./test_backfill_dump <config.toml> [--slot NAME] [--publication NAME] [--table NAME] [--conninfo "<pg conninfo>"]
//
// example:
//   ./test_backfill_dump ../config_samples/config_sample_backfill.toml

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <event2/event.h>
#include <libpq-fe.h>

#include "backfill_store.hpp"
#include "config.hpp"
#include "pgreplication_source.hpp"

namespace {

struct Options
{
    std::string config_path;
    std::string slot = "walkrie_it_backfill_slot";
    std::string publication = "walkrie_it_backfill_pub";
    std::string table = "walkrie_it_backfill_table";
    std::string conninfo; // <<< optional override for the admin/setup connection; falls back to the config's [[source]] connection
};

Options parse_args(int argc, char** argv)
{
    Options opts;
    if (argc > 1) opts.config_path = argv[1];
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--slot" && i + 1 < argc) opts.slot = argv[++i];
        else if (arg == "--publication" && i + 1 < argc) opts.publication = argv[++i];
        else if (arg == "--table" && i + 1 < argc) opts.table = argv[++i];
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

pgcdc::TableMapping make_mapping(const std::string& table)
{
    pgcdc::TableMapping tm;
    tm.source_table  = table;
    tm.id_source_    = "id";
    tm.embed_source_ = "body";

    pgcdc::ColumnMapping cm;
    cm.source_column = "category";
    cm.sink_column   = "category";
    cm.role          = "metadata";
    tm.columns.push_back(cm);

    return tm;
}

// Pumps the event loop (non-blocking) until `done` returns true or the
// timeout elapses — avoids a fixed "long enough" sleep.
bool pump_until(event_base* base, const std::function<bool()>& done, std::chrono::milliseconds timeout)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        event_base_loop(base, EVLOOP_NONBLOCK);
        if (done()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return done();
}

} // namespace

int main(int argc, char** argv)
{
    Options opts = parse_args(argc, argv);
    if (opts.config_path.empty()) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--slot NAME] [--publication NAME] [--table NAME]\n";
        return 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(opts.config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }
    if (cfg.sources.empty()) {
        std::cerr << "config has no [[source]] blocks\n";
        return 1;
    }
    const auto& src_toml = cfg.sources.front();

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

    // Dedicated table/publication/slot/store so this test doesn't collide
    // with a real walkrie instance's replication or backfill state.
    exec_or_die(admin, "DROP TABLE IF EXISTS " + opts.table);
    exec_or_die(admin, "CREATE TABLE " + opts.table + " (id int primary key, body text, category text)");
    exec_ignore_errors(admin, "DROP PUBLICATION IF EXISTS " + opts.publication);
    exec_or_die(admin, "CREATE PUBLICATION " + opts.publication + " FOR TABLE " + opts.table);
    exec_ignore_errors(admin, "SELECT pg_drop_replication_slot('" + opts.slot + "')");

    // Rows inserted BEFORE the slot exists — these are the "pre-existing
    // rows" the backfill is supposed to catch.
    exec_or_die(admin, "INSERT INTO " + opts.table + " (id, body, category) VALUES "
                        "(1, 'alpha body', 'cat-a'), (2, 'beta body', 'cat-b')");

    auto store_path = std::filesystem::temp_directory_path() / "walkrie_it_backfill.sqlite3";
    std::filesystem::remove(store_path);

    pgcdc::PgReplicationConfig src_cfg;
    src_cfg.host             = src_toml.host;
    src_cfg.port             = src_toml.port;
    src_cfg.dbname           = src_toml.dbname;
    src_cfg.user             = src_toml.user;
    src_cfg.password         = src_toml.password;
    src_cfg.slot_name        = opts.slot;
    src_cfg.publication_name = opts.publication;
    src_cfg.backfill                   = true;
    src_cfg.backfill_table_mappings    = {make_mapping(opts.table)};
    src_cfg.backfill_store_path        = store_path.string();

    bool ok = true;

    // --- Phase A: fresh-slot dump correctness ---
    auto source = std::make_unique<pgcdc::PgReplicationSource>(/*id=*/1, src_cfg);
    if (!source->connect()) {
        std::cerr << "source connect failed: " << source->last_error() << "\n";
        return 1;
    }
    if (!source->was_slot_freshly_created()) {
        std::cout << "FAIL: expected a fresh slot on first connect()\n";
        ok = false;
    }
    if (!source->run_backfill_dump_if_required()) {
        std::cerr << "run_backfill_dump_if_required failed: " << source->last_error() << "\n";
        return 1;
    }

    {
        pgcdc::BackfillStore inspect(store_path.string());
        inspect.open();

        if (!inspect.is_table_dumped(opts.table)) {
            std::cout << "FAIL: table not marked dump_complete after run_backfill_dump_if_required()\n";
            ok = false;
        }

        auto row1 = inspect.get_row_data(opts.table, "1");
        auto row2 = inspect.get_row_data(opts.table, "2");
        if (!row1 || row1->find("alpha body") == std::string::npos || row1->find("cat-a") == std::string::npos) {
            std::cout << "FAIL: row id=1 missing or wrong content in backfill store: "
                       << (row1 ? *row1 : "<absent>") << "\n";
            ok = false;
        }
        if (!row2 || row2->find("beta body") == std::string::npos || row2->find("cat-b") == std::string::npos) {
            std::cout << "FAIL: row id=2 missing or wrong content in backfill store: "
                       << (row2 ? *row2 : "<absent>") << "\n";
            ok = false;
        }
    }

    // --- Phase B: live Update on a still-pending row merges + suppresses dispatch ---
    if (!source->start_streaming()) {
        std::cerr << "source start_streaming failed: " << source->last_error() << "\n";
        return 1;
    }

    event_base* base = event_base_new();
    std::vector<pgcdc::ChangeEvent> received;
    auto handle = [&](const pgcdc::ChangeEvent& ev, SourceId) { received.push_back(ev); };
    if (!source->register_event_loop(base, handle)) {
        std::cerr << "register_event_loop failed: " << source->last_error() << "\n";
        return 1;
    }

    // Only touches `category` (metadata) — `body` (embed) stays literally
    // unchanged, so this also exercises prepare_upsert's "unchanged embed"
    // dedup path being irrelevant here since the event never reaches sinks.
    exec_or_die(admin, "UPDATE " + opts.table + " SET category = 'cat-a-updated' WHERE id = 1");

    bool got_commit = pump_until(base, [&] {
        for (auto& ev : received) if (ev.op == pgcdc::ChangeEvent::Op::Commit) return true;
        return false;
    }, std::chrono::seconds(10));

    if (!got_commit) {
        std::cout << "FAIL: never received an Op::Commit marker for the update transaction\n";
        ok = false;
    }

    bool saw_update_dispatched = false;
    for (auto& ev : received) {
        if (ev.op == pgcdc::ChangeEvent::Op::Update && ev.table_name == opts.table) saw_update_dispatched = true;
    }
    if (saw_update_dispatched) {
        std::cout << "FAIL: Update event for a still-pending backfill row was dispatched to sinks instead of being suppressed\n";
        ok = false;
    }

    {
        pgcdc::BackfillStore inspect(store_path.string());
        inspect.open();
        auto row1 = inspect.get_row_data(opts.table, "1");
        if (!row1 || row1->find("cat-a-updated") == std::string::npos) {
            std::cout << "FAIL: live update was not merged into the backfill store's row_data: "
                       << (row1 ? *row1 : "<absent>") << "\n";
            ok = false;
        }
        if (!row1 || row1->find("alpha body") == std::string::npos) {
            std::cout << "FAIL: merge lost the previously-known embed value: "
                       << (row1 ? *row1 : "<absent>") << "\n";
            ok = false;
        }
    }

    event_base_free(base);
    source.reset(); // release the slot's client connection before the resume phase

    // --- Phase C: resuming an existing slot must not re-dump ---
    exec_or_die(admin, "INSERT INTO " + opts.table + " (id, body, category) VALUES (3, 'gamma body', 'cat-c')");

    auto source2 = std::make_unique<pgcdc::PgReplicationSource>(/*id=*/2, src_cfg);
    if (!source2->connect()) {
        std::cerr << "source2 connect failed: " << source2->last_error() << "\n";
        return 1;
    }
    if (source2->was_slot_freshly_created()) {
        std::cout << "FAIL: expected the slot to already exist (resume), not be freshly created\n";
        ok = false;
    }
    if (!source2->run_backfill_dump_if_required()) {
        std::cerr << "run_backfill_dump_if_required (resume) failed: " << source2->last_error() << "\n";
        return 1;
    }

    {
        pgcdc::BackfillStore inspect(store_path.string());
        inspect.open();
        auto row3 = inspect.get_row_data(opts.table, "3");
        if (row3) {
            std::cout << "FAIL: resumed slot re-ran the dump — id=3 (inserted after the fresh dump) "
                       << "should never have been backfilled\n";
            ok = false;
        }
    }
    source2.reset();

    // Cleanup.
    exec_ignore_errors(admin, "SELECT pg_drop_replication_slot('" + opts.slot + "')");
    exec_ignore_errors(admin, "DROP PUBLICATION IF EXISTS " + opts.publication);
    exec_ignore_errors(admin, "DROP TABLE IF EXISTS " + opts.table);
    PQfinish(admin);
    std::filesystem::remove(store_path);

    if (ok) {
        std::cout << "PASS: dump captured pre-existing rows, live update merged+suppressed, "
                     "resumed slot did not re-dump\n";
    }
    return ok ? 0 : 1;
}
