// src/bench/backfill_bench.cpp
//
// Benches the real backfill path end to end — no synthetic/hand-rolled batching
// anywhere, unlike embed_backfill_batched_bench (which seeds the SQLite store
// directly). This tool instead:
//   1. Drops/recreates the mapped table + publication + slot (mirrors
//      test_backfill_dump.cpp's integration-test setup) so every run starts
//      from a genuinely fresh slot, guaranteeing dump_all() actually runs.
//   2. INSERTs --rows real pre-existing rows into real Postgres before the
//      slot exists — these are what the real dump_all() SELECT picks up.
//   3. Drives the real PgReplicationSource: connect() -> run_backfill_dump_if_required()
//      -> spawns the real walkrie_worker binary (same fork/execl main.cpp uses)
//      -> start_streaming() -> register_event_loop(), same sequence main.cpp's
//      per-source setup loop uses. All embedding-call batching is owned entirely
//      by the real BackfillWorker/PgEmbeddingSink/EmbeddingProvider code paths.
//   4. --live-load optionally inserts more rows *after* the slot exists, on a
//      background thread overlapping the drain — exercises absorb_event()'s
//      live-reconciliation merge/suppress path for real, concurrently, via the
//      real EventDispatcher + sink (wrapped in BenchmarkingSink for lag stats).
//
// usage:
//   ./backfill_bench <config.toml> [--rows N] [--live-load]
//                     [--live-load-rows N] [--live-load-batch N]
//
// example (see also the walkrie-initial-backfill design notes on why a
// dedicated OpenAI backfill bench is worth having separately from Section 6's
// llama-based one — network-bound, doesn't inherit this VM's CPU variance):
//   ./backfill_bench ../config_samples/config_sample_backfill_openai_batched.toml --rows 1000
//   ./backfill_bench ../config_samples/config_sample_backfill_openai_batched.toml --rows 10000 --live-load

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include <event2/event.h>
#include <libpq-fe.h>
#include <spdlog/spdlog.h>

#include "config.hpp"
#include "embedding_provider.hpp"
#include "event_dispatcher.hpp"
#include "http_client.hpp"
#include "pgreplication_source.hpp"

#include "bench/bench_stats.hpp"
#include "bench/benchmarking_sink.hpp"

namespace {

std::atomic<bool> g_stop{false};
event_base* g_base = nullptr;

void handle_sigint(int)
{
    g_stop = true;
    if (g_base) event_base_loopbreak(g_base);
}

struct Options
{
    std::string config_path;
    int rows = 1000;
    bool live_load = false;
    int live_load_rows = 200;
    int live_load_batch = 10;
};

Options parse_args(int argc, char** argv)
{
    Options opts;
    if (argc > 1) opts.config_path = argv[1];
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--rows" && i + 1 < argc) opts.rows = std::stoi(argv[++i]);
        else if (arg == "--live-load") opts.live_load = true;
        else if (arg == "--live-load-rows" && i + 1 < argc) opts.live_load_rows = std::stoi(argv[++i]);
        else if (arg == "--live-load-batch" && i + 1 < argc) opts.live_load_batch = std::stoi(argv[++i]);
    }
    return opts;
}

std::string sample_text(int i)
{
    return "This is a representative sample sentence used to benchmark embedding latency for the walkrie CDC pipeline. " + std::to_string(i);
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

// SQL type for a mapped source column — "id" needs to be a real integer PK
// for the bench's own generated row ids; everything else is plain text.
std::string sql_type_for(const pgcdc::ColumnMapping& cm)
{
    return cm.role == "id" ? "int primary key" : "text";
}

std::string sql_value_for(const pgcdc::ColumnMapping& cm, int i)
{
    if (cm.role == "id") return std::to_string(i);
    return sample_text(i); // embed/metadata columns both get placeholder text — good enough for a throughput bench
}

// Inserts rows [start_id, start_id+count) in batches of `batch_size`, one commit
// per batch (mirrors generate_batched_load.sh's shape) — spread over real time
// so events arrive as a stream, not one giant transaction.
void run_live_load(std::string conninfo, const pgcdc::TableMapping& tm, int start_id, int count, int batch_size,
                    std::atomic<bool>& done)
{
    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "[live-load] connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        done = true;
        return;
    }

    for (int base = 0; base < count && !g_stop.load(); base += batch_size) {
        int this_batch = std::min(batch_size, count - base);
        std::ostringstream sql;
        sql << "INSERT INTO " << tm.source_table << " (";
        for (size_t c = 0; c < tm.columns.size(); ++c) {
            if (c > 0) sql << ", ";
            sql << tm.columns[c].source_column;
        }
        sql << ") VALUES ";
        for (int r = 0; r < this_batch; ++r) {
            if (r > 0) sql << ", ";
            sql << "(";
            int id = start_id + base + r;
            for (size_t c = 0; c < tm.columns.size(); ++c) {
                if (c > 0) sql << ", ";
                const auto& cm = tm.columns[c];
                if (cm.role == "id") sql << id;
                else sql << "'" << sql_value_for(cm, id) << "'";
            }
            sql << ")";
        }
        PGresult* res = PQexec(pg, sql.str().c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "[live-load] insert batch failed: " << PQerrorMessage(pg) << "\n";
        }
        PQclear(res);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    PQfinish(pg);
    done = true;
}

std::filesystem::path resolve_worker_path(const char* argv0)
{
    std::error_code ec;
    auto self_exe = std::filesystem::canonical("/proc/self/exe", ec);
    return (!ec ? self_exe.parent_path() : std::filesystem::path(argv0).parent_path()) / "walkrie_worker";
}

} // namespace

int main(int argc, char** argv)
{
    Options opts = parse_args(argc, argv);
    if (opts.config_path.empty()) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--rows N] [--live-load] "
                     "[--live-load-rows N] [--live-load-batch N]\n";
        return 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(opts.config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }
    if (cfg.sources.empty() || !cfg.sources.front().backfill) {
        std::cerr << "config's first [[source]] must have backfill = true\n";
        return 1;
    }
    if (cfg.sinks.empty()) {
        std::cerr << "config needs at least one [[sink]]\n";
        return 1;
    }
    auto sink_mappings = cfg.sinks.front()->mappings(); // <<< mappings() returns by value — must be held, not bound past .front()
    if (sink_mappings.empty()) {
        std::cerr << "config's [[sink]] has no [[sink.table_mapping]] blocks\n";
        return 1;
    }
    const auto& tm = sink_mappings.front();
    const auto& src = cfg.sources.front();

    std::ostringstream admin_conninfo;
    admin_conninfo << "host=" << src.host << " port=" << src.port
                    << " dbname=" << src.dbname << " user=" << src.user;
    if (!src.password.empty()) admin_conninfo << " password=" << src.password;

    // --- Startup: fresh table, publication, slot, store — every run starts from a clean epoch ---
    PGconn* admin = PQconnectdb(admin_conninfo.str().c_str());
    if (PQstatus(admin) != CONNECTION_OK) {
        std::cerr << "admin connection failed: " << PQerrorMessage(admin) << "\n";
        return 1;
    }

    std::cout << "startup: resetting table/slot for '" << tm.source_table << "'...\n";
    // TRUNCATE (not DROP+CREATE) when the table already exists — publication membership
    // doesn't survive DROP TABLE, and in this dev setup the publication is often owned by
    // a different role (e.g. postgres) than the one this tool connects as, so it can't
    // assume DROP/CREATE PUBLICATION privileges. TRUNCATE keeps the table's identity (and
    // therefore its publication membership) intact while still clearing prior run's rows.
    PGresult* exists_res = PQexec(admin, ("SELECT to_regclass('" + tm.source_table + "')").c_str());
    bool table_exists = PQresultStatus(exists_res) == PGRES_TUPLES_OK &&
                         PQntuples(exists_res) > 0 && !PQgetisnull(exists_res, 0, 0);
    PQclear(exists_res);

    if (table_exists) {
        exec_or_die(admin, "TRUNCATE TABLE " + tm.source_table);
    } else {
        std::ostringstream create_sql;
        create_sql << "CREATE TABLE " << tm.source_table << " (";
        for (size_t c = 0; c < tm.columns.size(); ++c) {
            if (c > 0) create_sql << ", ";
            create_sql << tm.columns[c].source_column << " " << sql_type_for(tm.columns[c]);
        }
        create_sql << ")";
        exec_or_die(admin, create_sql.str());
        exec_ignore_errors(admin, "CREATE PUBLICATION " + src.publication + " FOR TABLE " + tm.source_table);
        std::cout << "note: table '" << tm.source_table << "' didn't exist — created it and attempted to "
                     "create publication '" << src.publication << "'. If that publication already existed "
                     "under a different owner, add this table to it manually "
                     "(ALTER PUBLICATION ... ADD TABLE) before running again.\n";
    }
    exec_ignore_errors(admin, "SELECT pg_drop_replication_slot('" + src.slot_name + "')");

    auto store_path = cfg.settings.backfill_dir + "/" + src.slot_name + ".sqlite3";
    std::filesystem::remove(store_path);

    // --- Seed pre-existing rows: these are what the real dump_all() SELECT is supposed to catch ---
    std::cout << "seeding " << opts.rows << " pre-existing row(s)...\n";
    {
        std::ostringstream sql;
        sql << "INSERT INTO " << tm.source_table << " SELECT ";
        for (size_t c = 0; c < tm.columns.size(); ++c) {
            if (c > 0) sql << ", ";
            const auto& cm = tm.columns[c];
            if (cm.role == "id") sql << "i";
            else sql << "'" << sample_text(0) << "' || i";
        }
        sql << " FROM generate_series(1, " << opts.rows << ") AS i";
        exec_or_die(admin, sql.str());
    }

    pgcdc::http_global_init();
    event_base* base = event_base_new();
    if (!base) {
        std::cerr << "event_base_new() failed\n";
        return 1;
    }
    g_base = base;
    signal(SIGINT, handle_sigint);

    pgcdc::bench::LagStats lag_stats;
    std::shared_ptr<pgcdc::EventSink> real_sink;
    try {
        const auto& sink_cfg = cfg.sinks.front();
        auto provider = sink_cfg->needs_embedding_provider()
            ? pgcdc::create_initialized_embedding_provider(cfg.embedding)
            : nullptr;
        real_sink = sink_cfg->create_sink(provider);
    } catch (const std::exception& e) {
        std::cerr << "sink/embedding provider init failed: " << e.what() << "\n";
        event_base_free(base);
        pgcdc::http_global_cleanup();
        return 1;
    }
    auto bench_sink = std::make_shared<pgcdc::bench::BenchmarkingSink>(real_sink, lag_stats);
    std::vector<pgcdc::SinkHandle> sinks{ bench_sink };

    pgcdc::EventDispatcher dispatcher(cfg.settings.batch_size, std::chrono::milliseconds(cfg.settings.batch_timeout_ms));
    auto dispatch_handle = [&](const pgcdc::ChangeEvent& event, SourceId source_id) {
        pgcdc::EventJob job;
        job.ev = event;
        job.source_id = source_id;
        job.sinks = sinks;
        dispatcher.post_job(std::move(job));
    };

    pgcdc::PgReplicationConfig src_config;
    src_config.host             = src.host;
    src_config.port             = src.port;
    src_config.dbname           = src.dbname;
    src_config.user             = src.user;
    src_config.password         = src.password;
    src_config.slot_name        = src.slot_name;
    src_config.publication_name = src.publication;
    src_config.backfill         = true;
    src_config.backfill_table_mappings = sink_mappings;
    src_config.backfill_store_path     = store_path;

    pgcdc::PgReplicationSource source(1, src_config);

    if (!source.connect()) {
        std::cerr << "source connect failed: " << source.last_error() << "\n";
        return 1;
    }
    if (!source.was_slot_freshly_created()) {
        std::cerr << "warning: slot was not freshly created — dump may not run (startup's slot drop should have prevented this)\n";
    }

    auto bench_start = std::chrono::steady_clock::now();

    std::cout << "running backfill dump...\n";
    if (!source.run_backfill_dump_if_required()) {
        std::cerr << "run_backfill_dump_if_required failed: " << source.last_error() << "\n";
        return 1;
    }

    auto worker_path = resolve_worker_path(argv[0]);
    if (!std::filesystem::exists(worker_path)) {
        std::cerr << "walkrie_worker binary not found at " << worker_path << " — build it first\n";
        return 1;
    }

    pid_t worker_pid = -1;
    if (source.has_pending_backfill_work()) {
        worker_pid = fork();
        if (worker_pid == 0) {
            execl(worker_path.c_str(), worker_path.c_str(),
                  "-c", opts.config_path.c_str(),
                  "--store", store_path.c_str(),
                  "--slot", src.slot_name.c_str(),
                  static_cast<char*>(nullptr));
            _exit(127);
        }
        std::cout << "spawned walkrie_worker pid=" << worker_pid << "\n";
    } else {
        std::cout << "nothing to drain (dump found 0 rows?)\n";
    }

    std::atomic<bool> live_load_done{!opts.live_load};
    std::thread live_load_thread;
    if (opts.live_load) {
        std::cout << "starting live load: " << opts.live_load_rows << " row(s) in batches of "
                  << opts.live_load_batch << ", overlapping the drain...\n";
        live_load_thread = std::thread(run_live_load, admin_conninfo.str(), tm,
                                        opts.rows + 1, opts.live_load_rows, opts.live_load_batch,
                                        std::ref(live_load_done));
    }

    if (!source.start_streaming()) {
        std::cerr << "source start_streaming failed: " << source.last_error() << "\n";
        return 1;
    }
    if (!source.register_event_loop(base, dispatch_handle)) {
        std::cerr << "source register_event_loop failed: " << source.last_error() << "\n";
        return 1;
    }

    // Poll every 200ms: loopbreak once the worker has exited, the live-load
    // thread (if any) has finished inserting, and a short grace period has
    // passed for its last events to actually arrive and get processed.
    struct CompletionCtx
    {
        pid_t worker_pid;
        bool worker_done = false;
        std::atomic<bool>* live_load_done;
        event_base* base;
        std::chrono::steady_clock::time_point ready_since{};
    };
    CompletionCtx ctx{worker_pid, worker_pid < 0, &live_load_done, base, {}};

    event* completion_timer = event_new(base, -1, EV_PERSIST,
        [](evutil_socket_t, short, void* arg) {
            auto* c = static_cast<CompletionCtx*>(arg);
            if (!c->worker_done && c->worker_pid > 0) {
                int status = 0;
                if (waitpid(c->worker_pid, &status, WNOHANG) > 0) c->worker_done = true;
            }
            bool ready_now = c->worker_done && c->live_load_done->load();
            if (ready_now) {
                if (c->ready_since.time_since_epoch().count() == 0) {
                    c->ready_since = std::chrono::steady_clock::now();
                } else if (std::chrono::steady_clock::now() - c->ready_since > std::chrono::milliseconds(1000)) {
                    event_base_loopbreak(c->base);
                }
            } else {
                c->ready_since = {};
            }
        },
        &ctx);
    struct timeval interval{0, 200000};
    event_add(completion_timer, &interval);

    std::cout << "\nbackfill_bench running — Ctrl-C to stop early\n";
    event_base_dispatch(base);

    auto bench_end = std::chrono::steady_clock::now();
    if (live_load_thread.joinable()) live_load_thread.join();

    double wall_secs = std::chrono::duration<double>(bench_end - bench_start).count();
    size_t live_processed = bench_sink->processed_count();
    auto lag_summary = lag_stats.summarize();

    std::cout << "\n=== backfill_bench report ===\n";
    std::cout << "rows seeded (pre-existing): " << opts.rows << "\n";
    std::cout << "wall time (dump start -> drain+live done): " << std::fixed << std::setprecision(2) << wall_secs << " s\n";
    std::cout << "backfill throughput:  " << std::fixed << std::setprecision(1)
               << (opts.rows / std::max(wall_secs, 0.001)) << " rows/sec\n";

    if (opts.live_load) {
        std::cout << "\nlive load: " << opts.live_load_rows << " row(s) inserted concurrently\n";
        std::cout << "live events processed via sink: " << live_processed << "\n";
        if (lag_stats.count() > 0) {
            std::cout << "live event lag (commit -> sink processed):\n";
            std::cout << "  avg:  " << lag_summary.avg_us / 1000.0 << " ms\n";
            std::cout << "  p95:  " << lag_summary.p95_us / 1000.0 << " ms\n";
            std::cout << "  max:  " << lag_summary.max_us / 1000.0 << " ms\n";
        }
    }

    event_free(completion_timer);
    event_base_free(base);
    pgcdc::http_global_cleanup();
    PQfinish(admin);
    return 0;
}
