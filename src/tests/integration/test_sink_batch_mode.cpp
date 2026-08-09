// test_sink_batch_mode.cpp
//
// Integration test: verifies that PgEmbeddingSink::call() (single-event
// path) and PgEmbeddingSink::call_batch() (batched path) produce
// IDENTICAL final database state for the same sequence of events,
// against two predefined, fixed-schema tables and a real embedding
// provider + real Postgres connection.
//
// This deliberately bypasses EventDispatcher (no threads, no timing
// windows) — it calls the sink's public API directly, so the test is
// fast, deterministic, and repeatable. EventDispatcher's own batching/
// timeout grouping behavior is covered separately by the doctest suite
// (test_event_dispatcher_batching.cpp).
//
// IMPORTANT — read before extending this test:
// LlamaProvider has a real embed_batch() override (multi-sequence
// llama_encode() via n_seq_max) — the "batched" and "single" paths do
// genuinely different compute (batched GEMM vs. per-row GEMV), so their
// resulting embeddings are NOT guaranteed to be bit-identical: floating-
// point summation order differs between the two, an expected property of
// batched matmul, not a bug.
//
// The embedding comparison is gated on COSINE DISTANCE (1 - cosine
// similarity), not raw per-dimension max_abs_diff — deliberately. Raw
// abs-diff bakes in whatever absolute scale a given provider happens to
// return: LlamaProvider returns raw, unnormalized vectors (norm ~25 for
// the model this test was developed against); OpenAI's API returns
// unit-normalized vectors (norm ~1) by default. The same numeric
// --epsilon is nowhere near equivalently strict against those two scales.
// Cosine distance is scale-invariant, so one --epsilon value means the
// same thing regardless of provider, quantization, or hardware.
// max_abs_diff is still computed and printed on a failure, purely as
// diagnostic context (e.g. to sanity-check the embedding's scale) — it
// does not gate pass/fail.
//
// The default --epsilon (1e-6, i.e. "must be near-bit-identical") only
// holds on some model/hardware/build combinations, not universally.
// Confirmed so far: passes at 1e-6 for bge-m3-Q8_0.gguf on one machine
// (a VirtualBox VM — see PERFORMANCE.md's Environment 1), fails at 1e-6
// for the SAME model file on a different machine (bare-metal, native-CPU
// build — PERFORMANCE.md's Environment 2), where cosine_similarity was
// ~0.9995-0.9997 (cosine distance ~0.0003-0.0005) — the embeddings are
// still ~99.95%+ similar, just not within a bit-identical tolerance.
// Root cause not established — how far apart batched vs. sequential land
// is hardware/build-dependent even though the fact that they differ at
// all is universal. See TECHNICAL.md's Known Limitations for the full
// writeup.
//
// Practical implication: pick --epsilon for the hardware you're actually
// running on rather than assuming 1e-6 is portable (e.g. --epsilon 0.001
// comfortably clears the drift measured above). A genuine bug looks
// different from this floating-point-drift effect: a LOW
// cosine_similarity (rows semantically diverging), a row count mismatch,
// an item_body mismatch, or an expect_present/expect_absent spot-check
// failure.
//
// usage:
//   ./test_sink_batch_mode <config.toml> --conninfo "<pg conninfo>" [--epsilon 1e-6]
//   (--epsilon is a max acceptable cosine DISTANCE: 1 - cosine_similarity)
//
// example:
//   ./test_sink_batch_mode ../config_samples/config_sample.toml --conninfo "host=localhost port=5432 dbname=walkrie_test user=walkrie_demo password=changeme"

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <cmath>
#include <algorithm>
#include <map>

#include <libpq-fe.h>
#include "config.hpp"
#include "embedding_provider.hpp"
#include "pgembedding_sink.hpp"
#include "event.hpp"

namespace {

constexpr const char* kSingleTable = "walkrie_it_single";
constexpr const char* kBatchTable  = "walkrie_it_batch";

// ---------------------------------------------------------------------
// Event builders
// ---------------------------------------------------------------------

pgcdc::ChangeEvent make_insert(const std::string& id, const std::string& body) 
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Insert;
    ev.table_name = "it_events";
    pgcdc::DecodedRow row;
    row.table_name = "it_events";
    row.kind = pgcdc::TupleKind::Insert;
    row.columns.push_back({"id", false, false, id});
    row.columns.push_back({"body", false, false, body});
    ev.new_row = row;
    return ev;
}

pgcdc::ChangeEvent make_update(const std::string& id, const std::string& body,
                                const std::string& old_body = "") 
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Update;
    ev.table_name = "it_events";
    pgcdc::DecodedRow new_row;
    new_row.table_name = "it_events";
    new_row.kind = pgcdc::TupleKind::UpdateNew;
    new_row.columns.push_back({"id", false, false, id});
    new_row.columns.push_back({"body", false, false, body});
    ev.new_row = new_row;

    if (!old_body.empty()) {
        pgcdc::DecodedRow old_row;
        old_row.table_name = "it_events";
        old_row.kind = pgcdc::TupleKind::UpdateOld;
        old_row.columns.push_back({"id", false, false, id});
        old_row.columns.push_back({"body", false, false, old_body});
        ev.old_row = old_row;
    }
    return ev;
}

pgcdc::ChangeEvent make_delete(const std::string& id) 
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Delete;
    ev.table_name = "it_events";
    pgcdc::DecodedRow row;
    row.table_name = "it_events";
    row.kind = pgcdc::TupleKind::Delete;
    row.columns.push_back({"id", false, false, id});
    ev.old_row = row;
    return ev;
}

// ---------------------------------------------------------------------
// Scenarios — each is a fixed, deterministic sequence of events.
// ---------------------------------------------------------------------

struct Scenario 
{
    std::string name;
    std::vector<pgcdc::ChangeEvent> events;
    // Expected-present/expected-absent spot checks, independent of the
    // single-vs-batch comparison — catches a case where BOTH paths agree
    // with each other but are both wrong.
    std::vector<std::string> expect_present_ids;
    std::vector<std::string> expect_absent_ids;
};

std::vector<Scenario> build_scenarios() 
{
    std::vector<Scenario> scenarios;

    scenarios.push_back({
        "simple_inserts",
        {
            make_insert("1", "alpha"),
            make_insert("2", "beta"),
            make_insert("3", "gamma"),
            make_insert("4", "delta"),
            make_insert("5", "epsilon"),
            make_insert("6", "zeta"),
        },
        {"1", "2", "3", "4", "5", "6"},
        {}
    });

    scenarios.push_back({
        "inserts_then_updates_including_unchanged_skip",
        {
            make_insert("10", "original ten"),
            make_insert("11", "original eleven"),
            make_insert("12", "original twelve"),
            make_insert("13", "original thirteen"),
            // id 10: real content change — should re-embed.
            make_update("10", "updated ten", "original ten"),
            // id 11: identical old/new text — should be SKIPPED (no
            // re-embed, row keeps its original embedding/content), per
            // PgEmbeddingSink's skip-unchanged logic. Exercising this in
            // both single and batch paths is exactly the kind of thing
            // that could silently diverge if the two-pass batch rewrite
            // had a bug.
            make_update("11", "original eleven", "original eleven"),
            make_update("12", "updated twelve", "original twelve"),
            make_update("13", "original thirteen", "original thirteen"), // also unchanged
        },
        {"10", "11", "12", "13"},
        {}
    });

    scenarios.push_back({
        "insert_then_delete_same_id",
        {
            make_insert("100", "will be deleted"),
            make_insert("101", "unrelated neighbor before"),
            make_delete("100"),
            make_insert("102", "unrelated neighbor after"),
        },
        {"101", "102"},
        {"100"} // THE regression case: must be absent, not resurrected
    });

    scenarios.push_back({
        "delete_then_insert_same_id",
        {
            make_insert("200", "padding before"),
            make_delete("201"), // no-op: row doesn't exist yet
            make_insert("201", "created after the no-op delete"),
            make_insert("202", "padding after"),
        },
        {"200", "201", "202"},
        {}
    });

    scenarios.push_back({
        "mixed_realistic_5insert_1delete_4update",
        {
            make_insert("301", "insert one"),
            make_insert("302", "insert two"),
            make_insert("303", "insert three"), // targeted by the delete below
            make_insert("304", "insert four"),
            make_insert("305", "insert five"),
            make_delete("303"),
            make_update("401", "update one v2", "update one v1-does-not-exist-yet"),
            make_update("402", "update two v2", "update two v1-does-not-exist-yet"),
            make_update("403", "update three v2", "update three v1-does-not-exist-yet"),
            make_update("404", "update four v2", "update four v1-does-not-exist-yet"),
        },
        {"301", "302", "304", "305", "401", "402", "403", "404"},
        {"303"}
    });

    return scenarios;
}

// ---------------------------------------------------------------------
// DB setup / row fetch / comparison
// ---------------------------------------------------------------------

void ensure_schema(PGconn* pg, int dimensions) 
{
    PGresult* res = PQexec(pg, "CREATE EXTENSION IF NOT EXISTS vector");
    PQclear(res);

    for (const char* table : {kSingleTable, kBatchTable}) {
        std::ostringstream sql;
        sql << "CREATE TABLE IF NOT EXISTS " << table << " ("
            << "  row_id bigserial PRIMARY KEY,"
            << "  item_id text NOT NULL UNIQUE,"
            << "  item_body text NOT NULL,"
            << "  embedding vector(" << dimensions << ")"
            << ")";
        PGresult* r = PQexec(pg, sql.str().c_str());
        if (PQresultStatus(r) != PGRES_COMMAND_OK) {
            std::cerr << "schema setup failed for " << table << ": " << PQerrorMessage(pg) << "\n";
        }
        PQclear(r);
    }
}

void truncate_tables(PGconn* pg) 
{
    std::ostringstream sql;
    sql << "TRUNCATE TABLE " << kSingleTable << ", " << kBatchTable << " RESTART IDENTITY";
    PGresult* r = PQexec(pg, sql.str().c_str());
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        std::cerr << "truncate failed: " << PQerrorMessage(pg) << "\n";
    }
    PQclear(r);
}

struct FetchedRow 
{
    std::string item_id;
    std::string item_body;
    std::vector<float> embedding;
};

std::vector<float> parse_vector_text(const std::string& text) 
{
    // pgvector's ::text cast format: "[0.1,0.2,0.3]"
    std::vector<float> out;
    std::string inner = text.substr(1, text.size() - 2); // strip [ ]
    std::stringstream ss(inner);
    std::string item;
    while (std::getline(ss, item, ',')) {
        out.push_back(std::stof(item));
    }
    return out;
}

std::map<std::string, FetchedRow> fetch_table(PGconn* pg, const std::string& table) 
{
    std::map<std::string, FetchedRow> rows;
    std::string sql = "SELECT item_id, item_body, embedding::text FROM " + table + " ORDER BY item_id";
    PGresult* res = PQexec(pg, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "fetch failed for " << table << ": " << PQerrorMessage(pg) << "\n";
        PQclear(res);
        return rows;
    }
    int n = PQntuples(res);
    for (int i = 0; i < n; ++i) {
        FetchedRow row;
        row.item_id   = PQgetvalue(res, i, 0);
        row.item_body = PQgetvalue(res, i, 1);
        row.embedding = parse_vector_text(PQgetvalue(res, i, 2));
        rows[row.item_id] = row;
    }
    PQclear(res);
    return rows;
}

double max_abs_diff(const std::vector<float>& a, const std::vector<float>& b)
{
    if (a.size() != b.size()) return std::numeric_limits<double>::infinity();
    double max_diff = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        max_diff = std::max(max_diff, static_cast<double>(std::fabs(a[i] - b[i])));
    }
    return max_diff;
}

// Reported alongside max_abs_diff on a mismatch so a raw per-dimension
// diff can be judged in context: these embeddings are raw/unnormalized
// (see LlamaProvider::extract_embeddings), so their component magnitude
// varies by model — a max_abs_diff that looks large in isolation can
// still mean the two vectors are ~99%+ similar. See the file header
// comment above for how to read this.
double cosine_similarity(const std::vector<float>& a, const std::vector<float>& b)
{
    if (a.size() != b.size() || a.empty()) return std::numeric_limits<double>::quiet_NaN();
    double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        dot    += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        norm_a += static_cast<double>(a[i]) * static_cast<double>(a[i]);
        norm_b += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }
    if (norm_a == 0.0 || norm_b == 0.0) return std::numeric_limits<double>::quiet_NaN();
    return dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
}

// ---------------------------------------------------------------------
// Sink construction
// ---------------------------------------------------------------------

std::unique_ptr<pgcdc::PgEmbeddingSink> make_sink(const std::string& conninfo,
                                                   const std::string& table_name,
                                                   std::shared_ptr<pgcdc::EmbeddingProvider> provider) 
{
    pgcdc::PgEmbeddingSinkConfig cfg;
    cfg.pg_conninfo = conninfo;
    cfg.sink_table  = table_name;
    cfg.sink_column = "embedding";

    pgcdc::TableMapping tm;
    tm.source_table  = "it_events";
    tm.id_source_    = "id";
    tm.id_sink_      = "item_id";
    tm.embed_source_ = "body";
    tm.embed_sink_   = "item_body";
    cfg.mappings.push_back(tm);

    auto sink = std::make_unique<pgcdc::PgEmbeddingSink>(cfg, provider);
    sink->init();
    return sink;
}

} // namespace

int main(int argc, char** argv) 
{
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml> --conninfo \"<pg conninfo>\" [--epsilon 1e-6]\n"
                   << "  --epsilon: max acceptable cosine distance (1 - cosine_similarity) between\n"
                   << "             the single-path and batch-path embedding for the same row.\n";
        return 1;
    }

    std::string config_path = argv[1];
    std::string conninfo;
    double epsilon = 1e-6;

    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--conninfo" && i + 1 < argc) conninfo = argv[++i];
        if (arg == "--epsilon" && i + 1 < argc) epsilon = std::stod(argv[++i]);
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

    PGconn* pg = PQconnectdb(conninfo.c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "database connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }

    ensure_schema(pg, cfg.embedding.dimensions);

    auto single_sink = make_sink(conninfo, kSingleTable, provider);
    auto batch_sink   = make_sink(conninfo, kBatchTable, provider);
/*
auto a  = provider->embed("hello world");
auto b1 = provider->embed_batch({"hello world"});          // batch capacity 4, but only 1 real sequence used
auto b2 = provider->embed_batch({"hello world", "hello world"}); // identical text in both slots
//auto b3 = provider->embed_batch({"hello world", "hello world", "hello world"}); // identical text in both slots

std::cout << "embed() vs embed_batch(1 item):  " << max_abs_diff(a, b1.at(0)) << "\n";
std::cout << "embed_batch b2 1.item vs b2 2.item (same text): " << max_abs_diff(b2.at(0), b2.at(1)) << "\n";
//std::cout << "embed_batch b3 1.item  vs a  (same text): " << max_abs_diff(b3.at(0), a) << "\n";
//std::cout << "embed_batch b3 3.item  vs a  (same text): " << max_abs_diff(b3.at(2), a) << "\n";
//std::cout << "embed_batch b3 3.item  vs b3 2.item  (same text): " << max_abs_diff(b3.at(2), b3.at(1)) << "\n";
//std::cout << "embed_batch b3 2.item vs b2 2.item (same text): " << max_abs_diff(b3.at(1), b2.at(1)) << "\n";

auto print_head = [](const std::string& label, const std::vector<float>& v) {
    std::cout << label << ": ";
    for (int i = 0; i < 25; ++i) std::cout << v[i] << " ";
    std::cout << "\n";
};

print_head("a       ", a);
//print_head("b1.at(0)", b1.at(0));
print_head("b2.at(0)", b2.at(0));
print_head("b2.at(1)", b2.at(1));
//print_head("b2.at(1)", b2.at(1));
//print_head("b3.at(0)", b3.at(0));
//print_head("b3.at(1)", b3.at(1));
//print_head("b3.at(2)", b3.at(2));
*/
    auto scenarios = build_scenarios();

    int total = 0;
    int passed = 0;

    for (auto& scenario : scenarios) {
        ++total;
        std::cout << "\n=== Scenario: " << scenario.name << " ===\n";
        truncate_tables(pg);

        // "Single" path: exactly what batch_size=1 does today — every
        // event goes through call() individually (which itself is just
        // call_batch({event}) internally, per PgEmbeddingSink's design).
        for (const auto& ev : scenario.events) {
            single_sink->call(ev);
        }

        // "Batch" path: the entire scenario delivered as one call_batch()
        // invocation — what a single dispatcher batch of this size would do.
        batch_sink->call_batch(scenario.events);

        auto single_rows = fetch_table(pg, kSingleTable);
        auto batch_rows   = fetch_table(pg, kBatchTable);

        bool scenario_ok = true;
        std::vector<std::string> failures;

        // Spot checks: does either path even produce the objectively
        // correct result, independent of whether they agree with each other?
        for (const auto& id : scenario.expect_present_ids) {
            if (single_rows.find(id) == single_rows.end()) {
                failures.push_back("id=" + id + " expected PRESENT but missing from single-path table");
                scenario_ok = false;
            }
            if (batch_rows.find(id) == batch_rows.end()) {
                failures.push_back("id=" + id + " expected PRESENT but missing from batch-path table");
                scenario_ok = false;
            }
        }
        for (const auto& id : scenario.expect_absent_ids) {
            if (single_rows.find(id) != single_rows.end()) {
                failures.push_back("id=" + id + " expected ABSENT but found in single-path table");
                scenario_ok = false;
            }
            if (batch_rows.find(id) != batch_rows.end()) {
                failures.push_back("id=" + id + " expected ABSENT but found in batch-path table");
                scenario_ok = false;
            }
        }

        // Cross-comparison: do single and batch paths agree with each other?
        if (single_rows.size() != batch_rows.size()) {
            failures.push_back("row count mismatch: single=" + std::to_string(single_rows.size()) +
                               " batch=" + std::to_string(batch_rows.size()));
            scenario_ok = false;
        }

        for (const auto& [id, single_row] : single_rows) {
            auto it = batch_rows.find(id);
            if (it == batch_rows.end()) {
                failures.push_back("id=" + id + " present in single-path table, missing from batch-path table");
                scenario_ok = false;
                continue;
            }
            const auto& batch_row = it->second;

            if (single_row.item_body != batch_row.item_body) {
                failures.push_back("id=" + id + " item_body mismatch: single=\"" + single_row.item_body +
                                   "\" batch=\"" + batch_row.item_body + "\"");
                scenario_ok = false;
            }

            double cos_sim = cosine_similarity(single_row.embedding, batch_row.embedding);
            // NaN (size mismatch or a zero vector) can never satisfy a distance
            // check — treat it as maximally divergent rather than silently
            // passing through a false comparison.
            double cos_dist = std::isnan(cos_sim) ? std::numeric_limits<double>::infinity() : (1.0 - cos_sim);
            if (cos_dist > epsilon) {
                double diff = max_abs_diff(single_row.embedding, batch_row.embedding);
                failures.push_back("id=" + id + " embedding mismatch: cosine_distance=" +
                                   std::to_string(cos_dist) + " (epsilon=" + std::to_string(epsilon) +
                                   ") cosine_similarity=" + std::to_string(cos_sim) +
                                   " max_abs_diff=" + std::to_string(diff) + " (diagnostic only)");
                scenario_ok = false;
            }
        }
        for (const auto& [id, batch_row] : batch_rows) {
            if (single_rows.find(id) == single_rows.end()) {
                failures.push_back("id=" + id + " present in batch-path table, missing from single-path table");
                scenario_ok = false;
            }
        }

        if (scenario_ok) {
            std::cout << "PASS (" << single_rows.size() << " rows, single/batch tables match)\n";
            ++passed;
        } else {
            std::cout << "FAIL\n";
            for (auto& f : failures) {
                std::cout << "  - " << f << "\n";
            }
        }
    }

    std::cout << "\n=== " << passed << " / " << total << " scenarios passed ===\n";

    PQfinish(pg);
    return (passed == total) ? 0 : 1;
}
