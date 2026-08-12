// test_qdrant_sink.cpp
//
// Integration test: exercises QdrantSink directly (no EventDispatcher, no
// threads — same style as test_sink_batch_mode.cpp) against a real running
// Qdrant instance, with real embeddings from the configured provider.
//
// Two things this specifically closes out (see the qdrant sink design memo):
// 1. Slice 2 (call_batch/delete/truncate/discriminator scoping) was unit-
//    tested and manually spot-checked, but never driven by an automated
//    integration test the way PgEmbeddingSink has test_sink_batch_mode.
// 2. The discriminator/point-id collision fix (two TableMappings sharing
//    one collection with overlapping source ids, distinguished only by
//    discriminator_label) was reasoned through and unit-tested, but never
//    verified against a real Qdrant instance.
//
// usage:
//   ./test_qdrant_sink <config.toml> [--epsilon 1e-6]
//   (config.toml only needs a valid [embedding] block; [source]/[sink] are
//   unused — this test builds its own QdrantSink instances directly, same
//   as test_sink_batch_mode does for PgEmbeddingSink)
//
// example:
//   ./test_qdrant_sink ../config_samples/config_sample_qdrant.toml

#include <cmath>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "config.hpp"
#include "embedding_provider.hpp"
#include "event.hpp"
#include "http_client.hpp"
#include "qdrant_sink.hpp"

namespace {

constexpr const char* kUrl = "http://localhost:6333";
constexpr const char* kSingleCollection        = "walkrie_it_qdrant_single";
constexpr const char* kBatchCollection         = "walkrie_it_qdrant_batch";
constexpr const char* kDiscriminatorCollection = "walkrie_it_qdrant_discriminator";

// ---------------------------------------------------------------------
// Event builders — mirrors test_sink_batch_mode.cpp's helpers.
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
// Scenarios — same shape/intent as test_sink_batch_mode.cpp's.
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
            // identical old/new text — QdrantSink should skip (no re-upsert)
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
// Qdrant HTTP helpers — independent of QdrantSink's own code, same
// spirit as test_sink_batch_mode.cpp issuing raw SQL rather than going
// through PgEmbeddingSink to read back state.
// ---------------------------------------------------------------------

void ensure_collection(pgcdc::HttpClient& http, const std::string& name, int dims)
{
    auto resp = http.get_json(std::string(kUrl) + "/collections/" + name, {});
    if (resp.status_code == 200) return;

    ordered_json body;
    body["vectors"]["size"]     = dims;
    body["vectors"]["distance"] = "Cosine";
    auto create = http.put_json(std::string(kUrl) + "/collections/" + name, body.dump(), {});
    if (!create.curl_ok || create.status_code / 100 != 2) {
        std::cerr << "failed to create collection " << name << ": " << create.body << "\n";
    }
}

void clear_collection(pgcdc::HttpClient& http, const std::string& name)
{
    ordered_json body;
    body["filter"] = ordered_json::object();
    http.post_json(std::string(kUrl) + "/collections/" + name + "/points/delete?wait=true", body.dump(), {});
}

struct FetchedPoint
{
    std::string item_body;
    std::vector<float> embedding;
};

std::map<std::string, FetchedPoint> scroll_collection(pgcdc::HttpClient& http, const std::string& name)
{
    std::map<std::string, FetchedPoint> points;

    ordered_json body;
    body["limit"]       = 1000;
    body["with_payload"] = true;
    body["with_vector"]  = true;

    auto resp = http.post_json(std::string(kUrl) + "/collections/" + name + "/points/scroll", body.dump(), {});
    if (!resp.curl_ok || resp.status_code / 100 != 2) {
        std::cerr << "scroll failed for " << name << ": " << resp.body << "\n";
        return points;
    }

    auto j = ordered_json::parse(resp.body);
    for (const auto& pt : j.at("result").at("points")) {
        FetchedPoint fp;
        std::string item_id = pt.at("payload").at("item_id").get<std::string>();
        fp.item_body = pt.at("payload").at("item_body").get<std::string>();
        for (const auto& v : pt.at("vector")) fp.embedding.push_back(v.get<float>());
        points[item_id] = fp;
    }
    return points;
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
    tm.id_sink_      = "item_id";
    tm.embed_source_ = "body";
    tm.embed_sink_   = "item_body";
    return tm;
}

std::unique_ptr<pgcdc::QdrantSink> make_sink(const std::string& collection,
                                              const std::vector<pgcdc::TableMapping>& mappings,
                                              std::shared_ptr<pgcdc::EmbeddingProvider> provider)
{
    pgcdc::QdrantSinkConfig cfg;
    cfg.url        = kUrl;
    cfg.collection = collection;
    cfg.mappings   = mappings;

    auto sink = std::make_unique<pgcdc::QdrantSink>(cfg, provider);
    sink->init();
    return sink;
}

int run_main_scenarios(std::shared_ptr<pgcdc::EmbeddingProvider> provider, double epsilon)
{
    pgcdc::HttpClient http(/*timeout_secs=*/30);
    ensure_collection(http, kSingleCollection, provider->dimensions());
    ensure_collection(http, kBatchCollection, provider->dimensions());

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

        auto single_points = scroll_collection(http, kSingleCollection);
        auto batch_points   = scroll_collection(http, kBatchCollection);

        bool ok = true;
        std::vector<std::string> failures;

        for (const auto& id : scenario.expect_present_ids) {
            if (!single_points.count(id)) { failures.push_back("id=" + id + " expected PRESENT, missing from single-path collection"); ok = false; }
            if (!batch_points.count(id))  { failures.push_back("id=" + id + " expected PRESENT, missing from batch-path collection"); ok = false; }
        }
        for (const auto& id : scenario.expect_absent_ids) {
            if (single_points.count(id)) { failures.push_back("id=" + id + " expected ABSENT, found in single-path collection"); ok = false; }
            if (batch_points.count(id))  { failures.push_back("id=" + id + " expected ABSENT, found in batch-path collection"); ok = false; }
        }

        if (single_points.size() != batch_points.size()) {
            failures.push_back("point count mismatch: single=" + std::to_string(single_points.size()) +
                               " batch=" + std::to_string(batch_points.size()));
            ok = false;
        }

        for (const auto& [id, sp] : single_points) {
            auto it = batch_points.find(id);
            if (it == batch_points.end()) {
                failures.push_back("id=" + id + " present in single-path, missing from batch-path");
                ok = false;
                continue;
            }
            if (sp.item_body != it->second.item_body) {
                failures.push_back("id=" + id + " item_body mismatch: single=\"" + sp.item_body +
                                   "\" batch=\"" + it->second.item_body + "\"");
                ok = false;
            }
            double cos_sim = cosine_similarity(sp.embedding, it->second.embedding);
            double cos_dist = std::isnan(cos_sim) ? std::numeric_limits<double>::infinity() : (1.0 - cos_sim);
            if (cos_dist > epsilon) {
                failures.push_back("id=" + id + " embedding mismatch: cosine_distance=" + std::to_string(cos_dist) +
                                   " (epsilon=" + std::to_string(epsilon) + ")");
                ok = false;
            }
        }

        if (ok) {
            std::cout << "PASS (" << single_points.size() << " points, single/batch collections match)\n";
            ++passed;
        } else {
            std::cout << "FAIL\n";
            for (auto& f : failures) std::cout << "  - " << f << "\n";
        }
    }

    std::cout << "\n=== " << passed << " / " << total << " main scenarios passed ===\n";
    return total - passed;
}

// Verifies the discriminator/point-id collision fix: two TableMappings
// sharing one collection, overlapping source ids ("1","2","3" in both),
// distinguished only by discriminator_label. A truncate scoped to one
// mapping's discriminator must not touch the other mapping's points, and
// the two mappings' same-id rows must land as distinct points (no
// silent overwrite) — the exact bug bugra caught during slice 2 design
// review, never exercised against a real Qdrant instance until now.
int run_discriminator_scenario(std::shared_ptr<pgcdc::EmbeddingProvider> provider)
{
    pgcdc::HttpClient http(/*timeout_secs=*/30);
    ensure_collection(http, kDiscriminatorCollection, provider->dimensions());
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

    // Both mappings use the same "item_id" payload key/sink column and the
    // same source ids, so scroll_collection()'s map (keyed by item_id) can
    // only hold ONE of the two colliding-id points — six real points must
    // exist even though this map can only report 3 distinct keys.
    ordered_json scroll_body;
    scroll_body["limit"] = 1000;
    scroll_body["with_payload"] = true;
    auto resp = http.post_json(std::string(kUrl) + "/collections/" + kDiscriminatorCollection + "/points/scroll",
                                scroll_body.dump(), {});
    auto j = ordered_json::parse(resp.body);
    size_t point_count = j.at("result").at("points").size();
    if (point_count != 6) {
        failures.push_back("expected 6 distinct points after inserting id 1/2/3 under two discriminator labels, found " +
                           std::to_string(point_count) + " — point-id collision regression");
        ok = false;
    }

    // Truncate table_a's scope only.
    sink->call_batch({make_truncate("table_a")});

    resp = http.post_json(std::string(kUrl) + "/collections/" + kDiscriminatorCollection + "/points/scroll",
                           scroll_body.dump(), {});
    j = ordered_json::parse(resp.body);
    std::map<std::string, std::string> remaining; // item_id -> category
    for (const auto& pt : j.at("result").at("points")) {
        remaining[pt.at("payload").at("item_id").get<std::string>()] = pt.at("payload").at("category").get<std::string>();
    }
    if (remaining.size() != 3) {
        failures.push_back("expected 3 points remaining after table_a-scoped truncate, found " + std::to_string(remaining.size()));
        ok = false;
    }
    for (const auto& [id, category] : remaining) {
        if (category != "table_b") {
            failures.push_back("id=" + id + " survived truncate with unexpected category=" + category);
            ok = false;
        }
    }

    if (ok) {
        std::cout << "PASS (6 points before truncate, 3 table_b points survive a table_a-scoped truncate)\n";
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
