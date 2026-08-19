// test_milvus_sink.cpp
//
// Integration test: exercises MilvusSink directly (no EventDispatcher, no
// threads — same style as test_qdrant_sink.cpp) against a real running
// Milvus instance, with real embeddings from the configured provider.
//
// Milvus collections need an explicit schema + index before they can take
// writes, which the REST API alone doesn't make convenient to script here —
// so, same as MilvusSink::init() itself, this test never creates a
// collection, only verifies against ones that already exist. Create the
// three collections this test needs once, e.g. with pymilvus. "id"/"vector"
// are MilvusSink's auto-discovered pk/vector fields (hashed id + embedding);
// "item_id"/"item_name" hold the readable source id/text this test's
// TableMapping points its id/embed roles at — see make_it_events_mapping():
//
//   from pymilvus import MilvusClient, DataType
//   client = MilvusClient(uri="http://localhost:19530")
//   for name in ["walkrie_it_milvus_single", "walkrie_it_milvus_batch", "walkrie_it_milvus_discriminator"]:
//       schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
//       schema.add_field("id", DataType.INT64, is_primary=True)
//       schema.add_field("item_id", DataType.VARCHAR, max_length=128)
//       schema.add_field("item_name", DataType.VARCHAR, max_length=4096)
//       schema.add_field("category", DataType.VARCHAR, max_length=128)
//       schema.add_field("vector", DataType.FLOAT_VECTOR, dim=<provider dims, e.g. 1024>)
//       index_params = client.prepare_index_params()
//       index_params.add_index(field_name="vector", metric_type="COSINE")
//       client.create_collection(collection_name=name, schema=schema, index_params=index_params)
//
// usage:
//   ./test_milvus_sink <config.toml> [--epsilon 1e-6]
//   (config.toml only needs a valid [embedding] block; [source]/[sink] are
//   unused — this test builds its own MilvusSink instances directly, same
//   as test_qdrant_sink does for QdrantSink)
//
// example:
//   ./test_milvus_sink ../config_samples/config_sample_milvus.toml

#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <thread>
#include <sstream>
#include <string>
#include <vector>

#include <uuid/uuid.h>

#include "config.hpp"
#include "embedding_provider.hpp"
#include "event.hpp"
#include "http_client.hpp"
#include "milvus_sink.hpp"

namespace {

constexpr const char* kUrl = "http://localhost:19530";
constexpr const char* kSingleCollection        = "walkrie_it_milvus_single";
constexpr const char* kBatchCollection         = "walkrie_it_milvus_batch";
constexpr const char* kDiscriminatorCollection = "walkrie_it_milvus_discriminator";

// Mirrors the private namespace constant in milvus_sink.cpp — duplicated here
// (not exposed by MilvusSink) so this test can assert on specific ids the
// same way test_qdrant_sink.cpp does, instead of just row counts.
constexpr unsigned char kIdNamespace[16] = {
    0x4a, 0xe0, 0x9f, 0x2c, 0x1b, 0x77, 0x4d, 0x63,
    0x9b, 0x2e, 0x6d, 0x0c, 0x5a, 0x8f, 0x31, 0x77
};

std::string expected_int64_id(const std::string& source_id, bool has_discriminator = false, const std::string& label = "")
{
    std::string key = has_discriminator ? (label + ":" + source_id) : source_id;
    uuid_t out;
    uuid_generate_sha1(out, kIdNamespace, key.data(), key.size());
    uint64_t v = 0;
    std::memcpy(&v, out, sizeof(v));
    v &= 0x7FFFFFFFFFFFFFFFULL;
    return std::to_string(static_cast<int64_t>(v));
}

// ---------------------------------------------------------------------
// Event builders — mirrors test_qdrant_sink.cpp's helpers.
// ---------------------------------------------------------------------

pgcdc::ChangeEvent make_insert(const std::string& table, const std::string& id, const std::string& body)
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Insert;
    ev.table_name = table;
    pgcdc::DecodedRow row;
    row.table_name = table;
    row.kind = pgcdc::TupleKind::Insert;
    row.columns.push_back({"id", false, false, id});
    row.columns.push_back({"body", false, false, body});
    ev.new_row = row;
    return ev;
}

pgcdc::ChangeEvent make_update(const std::string& table, const std::string& id, const std::string& body,
                                const std::string& old_body = "")
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Update;
    ev.table_name = table;
    pgcdc::DecodedRow new_row;
    new_row.table_name = table;
    new_row.kind = pgcdc::TupleKind::UpdateNew;
    new_row.columns.push_back({"id", false, false, id});
    new_row.columns.push_back({"body", false, false, body});
    ev.new_row = new_row;

    if (!old_body.empty()) {
        pgcdc::DecodedRow old_row;
        old_row.table_name = table;
        old_row.kind = pgcdc::TupleKind::UpdateOld;
        old_row.columns.push_back({"id", false, false, id});
        old_row.columns.push_back({"body", false, false, old_body});
        ev.old_row = old_row;
    }
    return ev;
}

pgcdc::ChangeEvent make_delete(const std::string& table, const std::string& id)
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Delete;
    ev.table_name = table;
    pgcdc::DecodedRow row;
    row.table_name = table;
    row.kind = pgcdc::TupleKind::Delete;
    row.columns.push_back({"id", false, false, id});
    ev.old_row = row;
    return ev;
}

pgcdc::ChangeEvent make_truncate(const std::string& table)
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Truncate;
    ev.table_name = table;
    return ev;
}

// ---------------------------------------------------------------------
// Scenarios — same shape/intent as test_qdrant_sink.cpp's. Source ids are
// distinct across scenarios (not reused per-collection) since Milvus rows
// are looked up here by re-deriving MilvusSink's own hashed id, not by
// filtering on source id (which isn't stored anywhere in the schema).
// ---------------------------------------------------------------------

struct Scenario
{
    std::string name;
    std::vector<pgcdc::ChangeEvent> events;
    std::vector<std::string> expect_present_ids;
    std::vector<std::string> expect_absent_ids;
};

std::vector<Scenario> build_scenarios()
{
    std::vector<Scenario> scenarios;

    scenarios.push_back({
        "simple_inserts",
        {
            make_insert("it_events", "1", "alpha"),
            make_insert("it_events", "2", "beta"),
            make_insert("it_events", "3", "gamma"),
        },
        {"1", "2", "3"},
        {}
    });

    scenarios.push_back({
        "inserts_then_updates_including_unchanged_skip",
        {
            make_insert("it_events", "10", "original ten"),
            make_insert("it_events", "11", "original eleven"),
            make_update("it_events", "10", "updated ten", "original ten"),
            // identical old/new text — MilvusSink should skip (no re-upsert)
            make_update("it_events", "11", "original eleven", "original eleven"),
        },
        {"10", "11"},
        {}
    });

    scenarios.push_back({
        "insert_then_delete_same_id",
        {
            make_insert("it_events", "100", "will be deleted"),
            make_insert("it_events", "101", "unrelated neighbor before"),
            make_delete("it_events", "100"),
            make_insert("it_events", "102", "unrelated neighbor after"),
        },
        {"101", "102"},
        {"100"} // must not be resurrected by pass-2 event ordering
    });

    scenarios.push_back({
        "delete_then_insert_same_id",
        {
            make_insert("it_events", "200", "padding before"),
            make_delete("it_events", "201"), // no-op: doesn't exist yet
            make_insert("it_events", "201", "created after the no-op delete"),
        },
        {"200", "201"},
        {}
    });

    return scenarios;
}

// ---------------------------------------------------------------------
// Milvus HTTP helpers — independent of MilvusSink's own code, same spirit
// as test_qdrant_sink.cpp reading back state without going through the
// sink under test. Rows are keyed by the same hashed id MilvusSink
// computes internally (Int64 pk here, no discriminator except in the
// discriminator scenario).
// ---------------------------------------------------------------------

void delete_all(pgcdc::HttpClient& http, const std::string& name)
{
    ordered_json body;
    body["collectionName"] = name;
    // An empty filter (Milvus's documented "match everything" shorthand) is rejected by this
    // server (code 1802) — same finding as MilvusSink::truncate()'s no-discriminator path, see
    // its comment. All test collections use an Int64 "id" pk.
    body["filter"] = "id >= -9223372036854775808";
    http.post_json(std::string(kUrl) + "/v2/vectordb/entities/delete", body.dump(), {});
}

struct FetchedRow
{
    std::string item_name;
    std::string category;
    std::vector<float> embedding;
};

// Keyed by Milvus's int64 "id" field (as a decimal string) rather than the
// original Postgres source id — MilvusSink hashes the source id before it
// ever reaches the pk field; expected_int64_id() re-derives that hash so
// callers can still look a row up by its original source id.
std::map<std::string, FetchedRow> query_collection(pgcdc::HttpClient& http, const std::string& name)
{
    std::map<std::string, FetchedRow> rows;

    ordered_json body;
    body["collectionName"] = name;
    body["filter"] = "";
    body["outputFields"] = ordered_json::array({"id", "item_name", "category", "vector"});
    body["limit"] = 1000;

    auto resp = http.post_json(std::string(kUrl) + "/v2/vectordb/entities/query", body.dump(), {});
    if (!resp.curl_ok || resp.status_code != 200) {
        std::cerr << "query failed for " << name << ": " << resp.body << "\n";
        return rows;
    }

    auto j = ordered_json::parse(resp.body);
    if (j.at("code").get<int>() != 0) {
        std::cerr << "query error for " << name << ": " << j.dump() << "\n";
        return rows;
    }
    for (const auto& row : j.at("data")) {
        FetchedRow fr;
        std::string id = std::to_string(row.at("id").get<int64_t>());
        fr.item_name = row.value("item_name", "");
        fr.category  = row.value("category", "");
        for (const auto& v : row.at("vector")) fr.embedding.push_back(v.get<float>());
        rows[id] = fr;
    }
    return rows;
}

// Milvus's REST query API has no synchronous read-your-writes guarantee the
// way Qdrant's `?wait=true` gives us — a just-applied upsert/delete can take
// a bit to become visible to query(). Polls until the row count matches what
// the caller expects (or times out and returns whatever was last read, so a
// genuine bug still fails loudly instead of being masked).
std::map<std::string, FetchedRow> query_collection_until_count(pgcdc::HttpClient& http, const std::string& name,
                                                                 size_t expected_count, int timeout_ms = 8000)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::map<std::string, FetchedRow> rows;
    while (true) {
        rows = query_collection(http, name);
        if (rows.size() == expected_count || std::chrono::steady_clock::now() >= deadline) return rows;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void clear_collection(pgcdc::HttpClient& http, const std::string& name)
{
    delete_all(http, name);
    query_collection_until_count(http, name, 0);
}

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

pgcdc::TableMapping make_it_events_mapping()
{
    pgcdc::TableMapping tm;
    tm.source_table  = "it_events";
    tm.id_source_    = "id";
    tm.id_sink_      = "item_id";   // <<< readable source id — distinct from the collection's auto-discovered pk field
    tm.embed_source_ = "body";
    tm.embed_sink_   = "item_name"; // <<< readable source text — distinct from the collection's auto-discovered vector field
    return tm;
}

std::unique_ptr<pgcdc::MilvusSink> make_sink(const std::string& collection,
                                              const std::vector<pgcdc::TableMapping>& mappings,
                                              std::shared_ptr<pgcdc::EmbeddingProvider> provider)
{
    pgcdc::MilvusSinkConfig cfg;
    cfg.url        = kUrl;
    cfg.collection = collection;
    cfg.mappings   = mappings;

    auto sink = std::make_unique<pgcdc::MilvusSink>(cfg, provider);
    sink->init();
    return sink;
}

int run_main_scenarios(std::shared_ptr<pgcdc::EmbeddingProvider> provider, double epsilon)
{
    pgcdc::HttpClient http(/*timeout_secs=*/30);

    auto single_sink = make_sink(kSingleCollection, {make_it_events_mapping()}, provider);
    auto batch_sink   = make_sink(kBatchCollection, {make_it_events_mapping()}, provider);

    auto scenarios = build_scenarios();
    int total = 0, passed = 0;

    for (auto& scenario : scenarios) {
        ++total;
        std::cout << "\n=== Scenario: " << scenario.name << " ===\n";
        clear_collection(http, kSingleCollection);
        clear_collection(http, kBatchCollection);

        for (const auto& ev : scenario.events) single_sink->call(ev);
        batch_sink->call_batch(scenario.events);

        size_t expect_count = scenario.expect_present_ids.size();
        auto single_rows = query_collection_until_count(http, kSingleCollection, expect_count);
        auto batch_rows   = query_collection_until_count(http, kBatchCollection, expect_count);

        bool ok = true;
        std::vector<std::string> failures;

        for (const auto& id : scenario.expect_present_ids) {
            std::string hashed = expected_int64_id(id);
            if (!single_rows.count(hashed)) { failures.push_back("id=" + id + " expected PRESENT, missing from single-path collection"); ok = false; }
            if (!batch_rows.count(hashed))  { failures.push_back("id=" + id + " expected PRESENT, missing from batch-path collection"); ok = false; }
        }
        for (const auto& id : scenario.expect_absent_ids) {
            std::string hashed = expected_int64_id(id);
            if (single_rows.count(hashed)) { failures.push_back("id=" + id + " expected ABSENT, found in single-path collection"); ok = false; }
            if (batch_rows.count(hashed))  { failures.push_back("id=" + id + " expected ABSENT, found in batch-path collection"); ok = false; }
        }

        if (single_rows.size() != batch_rows.size()) {
            failures.push_back("row count mismatch: single=" + std::to_string(single_rows.size()) +
                               " batch=" + std::to_string(batch_rows.size()));
            ok = false;
        }

        for (const auto& [id, sr] : single_rows) {
            auto it = batch_rows.find(id);
            if (it == batch_rows.end()) {
                failures.push_back("id=" + id + " present in single-path, missing from batch-path");
                ok = false;
                continue;
            }
            if (sr.item_name != it->second.item_name) {
                failures.push_back("id=" + id + " item_name mismatch: single=\"" + sr.item_name +
                                   "\" batch=\"" + it->second.item_name + "\"");
                ok = false;
            }
            double cos_sim = cosine_similarity(sr.embedding, it->second.embedding);
            double cos_dist = std::isnan(cos_sim) ? std::numeric_limits<double>::infinity() : (1.0 - cos_sim);
            if (cos_dist > epsilon) {
                failures.push_back("id=" + id + " embedding mismatch: cosine_distance=" + std::to_string(cos_dist) +
                                   " (epsilon=" + std::to_string(epsilon) + ")");
                ok = false;
            }
        }

        if (ok) {
            std::cout << "PASS (" << single_rows.size() << " rows, single/batch collections match)\n";
            ++passed;
        } else {
            std::cout << "FAIL\n";
            for (auto& f : failures) std::cout << "  - " << f << "\n";
        }
    }

    std::cout << "\n=== " << passed << " / " << total << " main scenarios passed ===\n";
    return total - passed;
}

// Verifies the discriminator/id collision fix: two TableMappings sharing one
// collection, overlapping source ids ("1","2","3" in both), distinguished
// only by discriminator_label — mirrors test_qdrant_sink.cpp's analogous
// scenario for the same class of bug.
int run_discriminator_scenario(std::shared_ptr<pgcdc::EmbeddingProvider> provider)
{
    pgcdc::HttpClient http(/*timeout_secs=*/30);
    clear_collection(http, kDiscriminatorCollection);

    std::cout << "\n=== Scenario: discriminator_scoped_truncate_and_id_collision ===\n";

    pgcdc::TableMapping tm_a = make_it_events_mapping();
    tm_a.source_table          = "table_a";
    tm_a.has_discriminator_    = true;
    tm_a.discriminator_sink_   = "category";
    tm_a.discriminator_label_  = "table_a";

    pgcdc::TableMapping tm_b = make_it_events_mapping();
    tm_b.source_table          = "table_b";
    tm_b.has_discriminator_    = true;
    tm_b.discriminator_sink_   = "category";
    tm_b.discriminator_label_  = "table_b";

    auto sink = make_sink(kDiscriminatorCollection, {tm_a, tm_b}, provider);

    std::vector<pgcdc::ChangeEvent> events = {
        make_insert("table_a", "1", "a-one"),
        make_insert("table_a", "2", "a-two"),
        make_insert("table_a", "3", "a-three"),
        make_insert("table_b", "1", "b-one"),
        make_insert("table_b", "2", "b-two"),
        make_insert("table_b", "3", "b-three"),
    };
    sink->call_batch(events);

    bool ok = true;
    std::vector<std::string> failures;

    auto rows = query_collection_until_count(http, kDiscriminatorCollection, 6);
    if (rows.size() != 6) {
        failures.push_back("expected 6 distinct rows after inserting id 1/2/3 under two discriminator labels, found " +
                           std::to_string(rows.size()) + " — id collision regression");
        ok = false;
    }

    // Truncate table_a's scope only.
    sink->call_batch({make_truncate("table_a")});

    rows = query_collection_until_count(http, kDiscriminatorCollection, 3);
    if (rows.size() != 3) {
        failures.push_back("expected 3 rows remaining after table_a-scoped truncate, found " + std::to_string(rows.size()));
        ok = false;
    }
    for (const auto& [id, row] : rows) {
        if (row.category != "table_b") {
            failures.push_back("id=" + id + " survived truncate with unexpected category=" + row.category);
            ok = false;
        }
    }

    if (ok) {
        std::cout << "PASS (6 rows before truncate, 3 table_b rows survive a table_a-scoped truncate)\n";
        return 0;
    }
    std::cout << "FAIL\n";
    for (auto& f : failures) std::cout << "  - " << f << "\n";
    return 1;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <config.toml> [--epsilon 1e-6]\n";
        return 1;
    }

    std::string config_path = argv[1];
    double epsilon = 1e-6;
    for (int i = 2; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--epsilon" && i + 1 < argc) epsilon = std::stod(argv[++i]);
    }

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }

    std::cerr << "Loading embedding provider (" << cfg.embedding.provider << ")...\n";
    std::shared_ptr<pgcdc::EmbeddingProvider> provider;
    try {
        provider = pgcdc::create_initialized_embedding_provider(cfg.embedding);
    } catch (const std::exception& e) {
        std::cerr << "error initializing embedding provider: " << e.what() << "\n";
        return 1;
    }

    int failures = run_main_scenarios(provider, epsilon);
    failures += run_discriminator_scenario(provider);

    return failures == 0 ? 0 : 1;
}
