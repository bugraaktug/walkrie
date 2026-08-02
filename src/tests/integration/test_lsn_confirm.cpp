// test_lsn_confirm.cpp
//
// Integration test: verifies the fix for the "last transaction before
// shutdown replays on restart" bug (see the walkrie-lsn-backpressure
// design notes). The bug was that a transaction's Commit message never
// produced a ChangeEvent, so confirmed_lsn_ could only ever reach the
// position of the transaction's last row change, never its real commit
// LSN — Postgres's replication slot never actually advanced past that
// boundary, so the last transaction before a shutdown always replayed
// on reconnect.
//
// This test drives the real PgReplicationSource class (the same one
// main.cpp uses) against a live Postgres: it performs one transaction,
// waits for the Op::Commit marker event, confirms it exactly like
// EventDispatcher would after a successful sink write, then calls
// flush_confirmed_lsn() (the same shutdown-time call main.cpp makes) and
// asserts the server's pg_replication_slots.confirmed_flush_lsn actually
// advances to (or past) that transaction's real commit LSN — the exact
// invariant that prevents the replay-on-restart bug.
//
// usage:
//   ./test_lsn_confirm <config.toml> [--slot NAME] [--publication NAME] [--table NAME]
//
// example:
//   ./test_lsn_confirm ../config_samples/config_sample.toml

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <event2/event.h>
#include <libpq-fe.h>

#include "config.hpp"
#include "pgreplication_source.hpp"

namespace {

struct Options
{
    std::string config_path;
    std::string slot = "walkrie_it_lsn_slot";
    std::string publication = "walkrie_it_lsn_pub";
    std::string table = "walkrie_it_lsn_table";
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
    }
    return opts;
}

uint64_t parse_lsn(const std::string& s)
{
    uint32_t hi = 0, lo = 0;
    std::sscanf(s.c_str(), "%X/%X", &hi, &lo);
    return (static_cast<uint64_t>(hi) << 32) | lo;
}

std::string format_lsn(uint64_t lsn)
{
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%X/%X",
                  static_cast<uint32_t>(lsn >> 32),
                  static_cast<uint32_t>(lsn & 0xFFFFFFFF));
    return buf;
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

// Empty string if the slot doesn't exist yet or has no confirmed position.
std::string confirmed_flush_lsn(PGconn* pg, const std::string& slot)
{
    std::string sql = "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" + slot + "'";
    PGresult* res = PQexec(pg, sql.c_str());
    std::string out;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0 && !PQgetisnull(res, 0, 0)) {
        out = PQgetvalue(res, 0, 0);
    }
    PQclear(res);
    return out;
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

    std::ostringstream admin_conninfo;
    admin_conninfo << "host=" << src_toml.host << " port=" << src_toml.port
                    << " dbname=" << src_toml.dbname << " user=" << src_toml.user;
    if (!src_toml.password.empty()) admin_conninfo << " password=" << src_toml.password;

    PGconn* admin = PQconnectdb(admin_conninfo.str().c_str());
    if (PQstatus(admin) != CONNECTION_OK) {
        std::cerr << "admin connection failed: " << PQerrorMessage(admin) << "\n";
        return 1;
    }

    // Dedicated table/publication/slot so this test doesn't collide with a
    // real walkrie instance's replication state — always start fresh.
    exec_or_die(admin, "DROP TABLE IF EXISTS " + opts.table);
    exec_or_die(admin, "CREATE TABLE " + opts.table + " (id int primary key, body text)");
    exec_ignore_errors(admin, "DROP PUBLICATION IF EXISTS " + opts.publication);
    exec_or_die(admin, "CREATE PUBLICATION " + opts.publication + " FOR TABLE " + opts.table);
    exec_ignore_errors(admin, "SELECT pg_drop_replication_slot('" + opts.slot + "')");

    pgcdc::PgReplicationConfig src_cfg;
    src_cfg.host = src_toml.host;
    src_cfg.port = src_toml.port;
    src_cfg.dbname = src_toml.dbname;
    src_cfg.user = src_toml.user;
    src_cfg.password = src_toml.password;
    src_cfg.slot_name = opts.slot;
    src_cfg.publication_name = opts.publication;

    pgcdc::PgReplicationSource source(/*id=*/1, src_cfg);
    if (!source.connect()) {
        std::cerr << "source connect failed: " << source.last_error() << "\n";
        return 1;
    }
    if (!source.start_streaming()) {
        std::cerr << "source start_streaming failed: " << source.last_error() << "\n";
        return 1;
    }

    event_base* base = event_base_new();
    std::vector<pgcdc::ChangeEvent> received;
    auto handle = [&](const pgcdc::ChangeEvent& ev, SourceId) {
        received.push_back(ev);
        if (ev.op == pgcdc::ChangeEvent::Op::Commit) {
            // Exactly what EventDispatcher::notify_confirmed does once a
            // batch's required sinks have durably succeeded.
            source.set_confirmed_lsn(ev.commit_lsn);
        }
    };
    if (!source.register_event_loop(base, handle)) {
        std::cerr << "register_event_loop failed: " << source.last_error() << "\n";
        return 1;
    }

    // One real transaction on a separate connection.
    exec_or_die(admin, "INSERT INTO " + opts.table + " (id, body) VALUES (1, 'hello')");

    bool got_commit = pump_until(base, [&] {
        for (auto& ev : received) if (ev.op == pgcdc::ChangeEvent::Op::Commit) return true;
        return false;
    }, std::chrono::seconds(10));

    if (!got_commit) {
        std::cout << "FAIL: never received an Op::Commit marker for the transaction\n";
        return 1;
    }

    uint64_t marker_lsn = 0;
    for (auto& ev : received) {
        if (ev.op == pgcdc::ChangeEvent::Op::Commit) marker_lsn = ev.commit_lsn;
    }

    bool ok = true;

    // Sanity check: before we've flushed anything, Postgres shouldn't
    // already have confirmed past this transaction on its own — proves
    // the upcoming assertion isn't vacuously true.
    std::string before = confirmed_flush_lsn(admin, opts.slot);
    if (!before.empty() && parse_lsn(before) >= marker_lsn) {
        std::cout << "FAIL: confirmed_flush_lsn already at/past the commit LSN before flush_confirmed_lsn() "
                   << "was called (before=" << before << ", marker=" << format_lsn(marker_lsn) << ") "
                   << "— test isn't isolated, or the slot was left in an unexpected state\n";
        ok = false;
    }

    source.flush_confirmed_lsn(); // <<< exactly the shutdown-time call main.cpp makes

    // The server's walsender must actually read and process our standby
    // status update before pg_replication_slots reflects it — poll rather
    // than assuming that round trip is instantaneous.
    std::string after;
    uint64_t after_lsn = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        after = confirmed_flush_lsn(admin, opts.slot);
        after_lsn = after.empty() ? 0 : parse_lsn(after);
        if (after_lsn >= marker_lsn) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    } while (std::chrono::steady_clock::now() < deadline);

    if (after_lsn < marker_lsn) {
        std::cout << "FAIL: confirmed_flush_lsn did not reach the transaction's commit LSN "
                   << "(after=" << after << ", marker=" << format_lsn(marker_lsn) << ") "
                   << "— this is the replay-on-restart bug\n";
        ok = false;
    }

    // Cleanup.
    event_base_free(base);
    exec_ignore_errors(admin, "SELECT pg_drop_replication_slot('" + opts.slot + "')");
    exec_ignore_errors(admin, "DROP PUBLICATION IF EXISTS " + opts.publication);
    exec_ignore_errors(admin, "DROP TABLE IF EXISTS " + opts.table);
    PQfinish(admin);

    if (ok) {
        std::cout << "PASS: confirmed_flush_lsn=" << after << " >= transaction commit LSN=" << format_lsn(marker_lsn) << "\n";
    }
    return ok ? 0 : 1;
}
