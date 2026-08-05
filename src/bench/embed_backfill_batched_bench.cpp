// src/bench/embed_backfill_batched_bench.cpp
//
// Benches the backfill drain pipeline in two phases against the same row count:
//   (a) a raw batched embed_batch() call — pure model compute, no store/pg involved.
//   (b) draining that many seeded rows through the real walkrie_worker binary,
//       end to end (SQLite claim -> rehydrate -> embed -> pg upsert -> mark_done).
// --workers N forks N walkrie_worker processes against the SAME store concurrently —
// a stress test of BackfillStore::claim_pending's cross-process concurrency guarantees
// (see the busy_timeout hardening in backfill_store.cpp and the walkrie-initial-backfill
// memory note on why this matters for a future multi-worker-per-source design).
//
// usage:
//   ./embed_backfill_batched_bench <config.toml> [--rows N] [--workers N] [--batch-size N]
//
// example:
//   ./embed_backfill_batched_bench ../config_samples/config_sample_backfill.toml --rows 200 --workers 4

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "backfill_store.hpp"
#include "config.hpp"
#include "embedding_provider.hpp"

namespace {

struct Options
{
    std::string config_path;
    int rows = 200;
    int workers = 1;
    int batch_size = 20; // <<< deliberately smaller than rows/1 so multi-worker runs make several claim_pending round trips and actually interleave
};

Options parse_args(int argc, char** argv)
{
    Options opts;
    if (argc > 1) opts.config_path = argv[1];
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--rows" && i + 1 < argc) opts.rows = std::stoi(argv[++i]);
        else if (arg == "--workers" && i + 1 < argc) opts.workers = std::stoi(argv[++i]);
        else if (arg == "--batch-size" && i + 1 < argc) opts.batch_size = std::stoi(argv[++i]);
    }
    return opts;
}

std::string sample_text(int i)
{
    return "This is a representative sample sentence used to benchmark embedding latency for the walkrie CDC pipeline. " + std::to_string(i);
}

std::string json_escape(const std::string& s)
{
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '"' || c == '\\') out += '\\';
        out += c;
    }
    return out;
}

} // namespace

int main(int argc, char** argv)
{
    Options opts = parse_args(argc, argv);
    if (opts.config_path.empty()) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--rows N] [--workers N] [--batch-size N]\n";
        return 1;
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(opts.config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
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

    // --- Phase A: raw batched embed_batch() baseline — pure model compute, no store/pg involved ---
    std::vector<std::string> texts;
    texts.reserve(static_cast<size_t>(opts.rows));
    for (int i = 0; i < opts.rows; ++i) texts.push_back(sample_text(i));

    std::cout << "loading embedding provider...\n";
    auto provider = pgcdc::make_embedding_provider(cfg.embedding);
    provider->init();

    auto baseline_start = std::chrono::steady_clock::now();
    auto vecs = provider->embed_batch(texts);
    auto baseline_end = std::chrono::steady_clock::now();
    if (vecs.size() != texts.size()) {
        std::cerr << "warning: embed_batch returned " << vecs.size() << " vectors for " << texts.size() << " requested\n";
    }
    double baseline_ms = std::chrono::duration<double, std::milli>(baseline_end - baseline_start).count();

    // --- Phase B: seed a fresh store, then drain it end-to-end via real walkrie_worker process(es) ---
    auto store_path = std::filesystem::temp_directory_path() / "walkrie_bench_backfill.sqlite3";
    std::filesystem::remove(store_path);
    {
        pgcdc::BackfillStore store(store_path.string());
        store.open();
        for (int i = 0; i < opts.rows; ++i) {
            std::string id = "bench-" + std::to_string(i);
            std::string row_json = "{\"" + tm.id_source_ + "\":\"" + json_escape(id) + "\",\"" +
                                    tm.embed_source_ + "\":\"" + json_escape(sample_text(i)) + "\"}";
            store.insert_row(tm.source_table, id, row_json);
        }
        store.mark_table_dumped(tm.source_table, opts.rows);
    }

    auto worker_path = std::filesystem::path(argv[0]).parent_path() / "walkrie_worker";
    if (!std::filesystem::exists(worker_path)) {
        std::cerr << "walkrie_worker binary not found at " << worker_path << " — build it first\n";
        return 1;
    }

    std::cout << "draining " << opts.rows << " row(s) with " << opts.workers
              << " worker process(es), batch-size=" << opts.batch_size << " each...\n";

    // All N children are forked up front so they race for the store's write lock
    // concurrently, rather than running one after another.
    std::vector<pid_t> pids;
    pids.reserve(static_cast<size_t>(opts.workers));

    auto drain_start = std::chrono::steady_clock::now();
    for (int w = 0; w < opts.workers; ++w) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "fork failed for worker " << w << "\n";
            continue;
        }
        if (pid == 0) {
            execl(worker_path.c_str(), worker_path.c_str(),
                  "-c", opts.config_path.c_str(),
                  "--store", store_path.string().c_str(),
                  "--batch-size", std::to_string(opts.batch_size).c_str(),
                  static_cast<char*>(nullptr));
            _exit(127); // exec failed
        }
        pids.push_back(pid);
    }

    int failures = 0;
    for (size_t w = 0; w < pids.size(); ++w) {
        int status = 0;
        waitpid(pids[w], &status, 0);
        int exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
        if (exit_code != 0) {
            std::cerr << "worker " << w << " (pid " << pids[w] << ") exited with code " << exit_code << "\n";
            ++failures;
        }
    }
    auto drain_end = std::chrono::steady_clock::now();
    double drain_ms = std::chrono::duration<double, std::milli>(drain_end - drain_start).count();

    std::filesystem::remove(store_path);

    std::cout << "\n=== embed_backfill_batched_bench report ===\n";
    std::cout << "rows:                " << opts.rows << "\n";
    std::cout << "workers:             " << opts.workers << " (batch-size=" << opts.batch_size << " each)\n";
    std::cout << "worker failures:     " << failures << " / " << opts.workers;
    if (failures > 0) std::cout << "  (drain incomplete — check the sink table)";
    std::cout << "\n";

    std::cout << "\nbaseline (raw embed_batch, no store/pg):\n";
    std::cout << "  wall time:   " << baseline_ms << " ms\n";
    std::cout << "  throughput:  " << (opts.rows / (baseline_ms / 1000.0)) << " rows/sec\n";

    std::cout << "\nend-to-end (" << opts.workers << " walkrie_worker process(es), real SQLite+PG):\n";
    std::cout << "  wall time:   " << drain_ms << " ms\n";
    std::cout << "  throughput:  " << (opts.rows / (drain_ms / 1000.0)) << " rows/sec\n";

    double overhead_ms = drain_ms - baseline_ms;
    std::cout << "\npipeline overhead over raw embedding:\n";
    std::cout << "  " << (overhead_ms >= 0 ? "+" : "") << overhead_ms << " ms"
               << "  (" << (overhead_ms / baseline_ms * 100.0) << "%)\n";

    return failures > 0 ? 1 : 0;
}
