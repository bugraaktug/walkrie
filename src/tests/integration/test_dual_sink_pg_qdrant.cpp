// test_dual_sink_pg_qdrant.cpp
//
// Integration test: loads config_sample_pg_and_qdrant.toml (real dual-write
// topology — one source, two sink types) through the SAME code path
// main.cpp uses (load_config() -> instantiate_sink()/load_from_config()
// dispatch -> one shared EmbeddingProvider -> SinkConfiguration::create_sink()
// for each configured sink), then drives both resulting EventSinks through
// call_batch() with the same batch_events, exactly like
// EventDispatcher::dispatch() does. Verifies PgEmbeddingSink and QdrantSink
// end up holding the SAME set of rows/points after live insert/update/
// delete/truncate — the actual dual-write correctness question, distinct
// from test_sink_batch_mode (single-vs-batch within one sink) and
// test_qdrant_sink (Qdrant single-vs-batch within one sink).
//
// usage:
//   ./test_dual_sink_pg_qdrant [config.toml]
//   (defaults to ../config_samples/config_sample_pg_and_qdrant.toml)
//
// Requires: the config's [[sink]] pgvector block's conninfo fields
// to be reachable, and a running Qdrant reachable at the config's qdrant
// sink's `url`. Both sinks' collection/table are created here if missing.

#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <libpq-fe.h>

#include "config.hpp"
#include "embedding_provider.hpp"
#include "event.hpp"
#include "event_sink.hpp"
#include "http_client.hpp"
#include "pgsink_configuration.hpp"
#include "qdrantsink_configuration.hpp"

namespace {

// ---------------------------------------------------------------------
// Event builders — table_name/source-column names driven by the config's
// two table_mapping blocks (test_table: id/name, documents: id/body; both
// sink to item_id/item_name).
// ---------------------------------------------------------------------

pgcdc::ChangeEvent make_insert(const std::string& table, const std::string& id_col, const std::string& embed_col,
                                const std::string& id, const std::string& body)
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Insert;
    ev.table_name = table;
    pgcdc::DecodedRow row;
    row.table_name = table;
    row.kind = pgcdc::TupleKind::Insert;
    row.columns.push_back({id_col, false, false, id});
    row.columns.push_back({embed_col, false, false, body});
    ev.new_row = row;
    return ev;
}

pgcdc::ChangeEvent make_update(const std::string& table, const std::string& id_col, const std::string& embed_col,
                                const std::string& id, const std::string& body, const std::string& old_body)
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Update;
    ev.table_name = table;
    pgcdc::DecodedRow new_row;
    new_row.table_name = table;
    new_row.kind = pgcdc::TupleKind::UpdateNew;
    new_row.columns.push_back({id_col, false, false, id});
    new_row.columns.push_back({embed_col, false, false, body});
    ev.new_row = new_row;

    pgcdc::DecodedRow old_row;
    old_row.table_name = table;
    old_row.kind = pgcdc::TupleKind::UpdateOld;
    old_row.columns.push_back({id_col, false, false, id});
    old_row.columns.push_back({embed_col, false, false, old_body});
    ev.old_row = old_row;
    return ev;
}

pgcdc::ChangeEvent make_delete(const std::string& table, const std::string& id_col, const std::string& id)
{
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Delete;
    ev.table_name = table;
    pgcdc::DecodedRow row;
    row.table_name = table;
    row.kind = pgcdc::TupleKind::Delete;
    row.columns.push_back({id_col, false, false, id});
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
// Postgres verification helpers — raw SQL, independent of PgEmbeddingSink.
// ---------------------------------------------------------------------

void ensure_pg_schema(PGconn* pg, const std::string& table, const std::string& embed_col, int dims)
{
    PGresult* r = PQexec(pg, "CREATE EXTENSION IF NOT EXISTS vector");
    PQclear(r);

    std::ostringstream sql;
    sql << "CREATE TABLE IF NOT EXISTS " << table << " ("
        << "  row_id bigserial PRIMARY KEY,"
        << "  item_id text NOT NULL,"
        << "  category text NOT NULL,"
        << "  " << embed_col << " text NOT NULL,"
        << "  embedding vector(" << dims << "),"
        << "  UNIQUE(item_id, category)"
        << ")";
    r = PQexec(pg, sql.str().c_str());
    if (PQresultStatus(r) != PGRES_COMMAND_OK) {
        std::cerr << "pg schema setup failed: " << PQerrorMessage(pg) << "\n";
    }
    PQclear(r);
}

void clear_pg_table(PGconn* pg, const std::string& table)
{
    std::string sql = "TRUNCATE TABLE " + table + " RESTART IDENTITY";
    PGresult* r = PQexec(pg, sql.c_str());
    PQclear(r);
}

std::map<std::string, std::string> fetch_pg_rows(PGconn* pg, const std::string& table, const std::string& embed_col,
                                                   const std::string& category)
{
    std::map<std::string, std::string> rows;
    std::string sql = "SELECT item_id, " + embed_col + " FROM " + table + " WHERE category = '" + category + "'";
    PGresult* res = PQexec(pg, sql.c_str());
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "pg fetch failed: " << PQerrorMessage(pg) << "\n";
        PQclear(res);
        return rows;
    }
    int n = PQntuples(res);
    for (int i = 0; i < n; ++i) rows[PQgetvalue(res, i, 0)] = PQgetvalue(res, i, 1);
    PQclear(res);
    return rows;
}

// ---------------------------------------------------------------------
// Qdrant verification helpers.
// ---------------------------------------------------------------------

void ensure_qdrant_collection(pgcdc::HttpClient& http, const std::string& url, const std::string& collection, int dims)
{
    auto resp = http.get_json(url + "/collections/" + collection, {});
    if (resp.status_code == 200) return;

    ordered_json body;
    body["vectors"]["size"]     = dims;
    body["vectors"]["distance"] = "Cosine";
    http.put_json(url + "/collections/" + collection, body.dump(), {});
}

void clear_qdrant_collection(pgcdc::HttpClient& http, const std::string& url, const std::string& collection)
{
    ordered_json body;
    body["filter"] = ordered_json::object();
    http.post_json(url + "/collections/" + collection + "/points/delete?wait=true", body.dump(), {});
}

std::map<std::string, std::string> fetch_qdrant_points(pgcdc::HttpClient& http, const std::string& url,
                                                          const std::string& collection, const std::string& embed_col,
                                                          const std::string& category)
{
    std::map<std::string, std::string> points;

    ordered_json cond;
    cond["key"] = "category";
    cond["match"]["value"] = category;
    ordered_json body;
    body["filter"]["must"] = ordered_json::array({cond});
    body["limit"] = 1000;
    body["with_payload"] = true;

    auto resp = http.post_json(url + "/collections/" + collection + "/points/scroll", body.dump(), {});
    if (!resp.curl_ok || resp.status_code / 100 != 2) {
        std::cerr << "qdrant scroll failed: " << resp.body << "\n";
        return points;
    }
    auto j = ordered_json::parse(resp.body);
    for (const auto& pt : j.at("result").at("points")) {
        points[pt.at("payload").at("item_id").get<std::string>()] = pt.at("payload").at(embed_col).get<std::string>();
    }
    return points;
}

// ---------------------------------------------------------------------
// Cross-backend comparison
// ---------------------------------------------------------------------

bool compare_backends(const std::string& label, const std::map<std::string, std::string>& pg_rows,
                       const std::map<std::string, std::string>& qdrant_points,
                       const std::vector<std::string>& expect_present,
                       const std::vector<std::string>& expect_absent,
                       std::vector<std::string>& failures)
{
    bool ok = true;
    for (const auto& id : expect_present) {
        if (!pg_rows.count(id))      { failures.push_back(label + ": id=" + id + " expected PRESENT, missing from Postgres"); ok = false; }
        if (!qdrant_points.count(id)) { failures.push_back(label + ": id=" + id + " expected PRESENT, missing from Qdrant"); ok = false; }
    }
    for (const auto& id : expect_absent) {
        if (pg_rows.count(id))      { failures.push_back(label + ": id=" + id + " expected ABSENT, found in Postgres"); ok = false; }
        if (qdrant_points.count(id)) { failures.push_back(label + ": id=" + id + " expected ABSENT, found in Qdrant"); ok = false; }
    }
    if (pg_rows.size() != qdrant_points.size()) {
        failures.push_back(label + ": row/point count mismatch: pg=" + std::to_string(pg_rows.size()) +
                           " qdrant=" + std::to_string(qdrant_points.size()));
        ok = false;
    }
    for (const auto& [id, text] : pg_rows) {
        auto it = qdrant_points.find(id);
        if (it == qdrant_points.end()) {
            failures.push_back(label + ": id=" + id + " present in Postgres, missing from Qdrant");
            ok = false;
        } else if (it->second != text) {
            failures.push_back(label + ": id=" + id + " text mismatch: pg=\"" + text + "\" qdrant=\"" + it->second + "\"");
            ok = false;
        }
    }
    for (const auto& [id, text] : qdrant_points) {
        if (!pg_rows.count(id)) {
            failures.push_back(label + ": id=" + id + " present in Qdrant, missing from Postgres");
            ok = false;
        }
    }
    return ok;
}

} // namespace

int main(int argc, char** argv)
{
    std::string config_path = (argc >= 2) ? argv[1] : "../config_samples/config_sample_pg_and_qdrant.toml";

    pgcdc::AppConfig cfg;
    try {
        cfg = pgcdc::load_config(config_path);
    } catch (const std::exception& e) {
        std::cerr << "error loading config: " << e.what() << "\n";
        return 1;
    }

    pgcdc::PgSinkConfiguration* pg_cfg = nullptr;
    pgcdc::QdrantSinkConfiguration* qdrant_cfg = nullptr;
    for (auto& s : cfg.sinks) {
        if (auto* p = dynamic_cast<pgcdc::PgSinkConfiguration*>(s.get())) pg_cfg = p;
        if (auto* q = dynamic_cast<pgcdc::QdrantSinkConfiguration*>(s.get())) qdrant_cfg = q;
    }
    if (!pg_cfg || !qdrant_cfg) {
        std::cerr << "config must contain exactly one pgvector sink and one qdrant sink\n";
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

    std::ostringstream conn;
    conn << "host=" << pg_cfg->host << " port=" << pg_cfg->port << " dbname=" << pg_cfg->dbname
         << " user=" << pg_cfg->user << " password=" << pg_cfg->password;
    PGconn* pg = PQconnectdb(conn.str().c_str());
    if (PQstatus(pg) != CONNECTION_OK) {
        std::cerr << "database connection failed: " << PQerrorMessage(pg) << "\n";
        PQfinish(pg);
        return 1;
    }

    // pg_cfg->embedding_column names the VECTOR column ("embedding"); the
    // embed-TEXT column is per-mapping (embed_sink_ = "item_name" for both
    // mappings in config_sample_pg_and_qdrant.toml) — hardcode "item_name"
    // to match that config rather than re-deriving it generically.
    const std::string embed_text_col = "item_name";
    ensure_pg_schema(pg, pg_cfg->table, embed_text_col, cfg.embedding.dimensions);

    pgcdc::HttpClient http(/*timeout_secs=*/30);
    ensure_qdrant_collection(http, qdrant_cfg->url, qdrant_cfg->collection, cfg.embedding.dimensions);

    // Build the ACTUAL production sinks via SinkConfiguration::create_sink(),
    // same call main.cpp makes, off the one shared provider both need.
    std::vector<std::shared_ptr<pgcdc::EventSink>> sinks;
    for (auto& s : cfg.sinks) {
        sinks.push_back(s->create_sink(s->needs_embedding_provider() ? provider : nullptr));
    }

    auto dispatch = [&](const std::vector<pgcdc::ChangeEvent>& events) {
        for (auto& sink : sinks) sink->call_batch(events);
    };

    auto clear_all = [&]() {
        clear_pg_table(pg, pg_cfg->table);
        clear_qdrant_collection(http, qdrant_cfg->url, qdrant_cfg->collection);
    };

    int total = 0, passed = 0;
    auto run_scenario = [&](const std::string& name, const std::vector<pgcdc::ChangeEvent>& events,
                             const std::vector<std::string>& expect_present, const std::vector<std::string>& expect_absent) {
        ++total;
        std::cout << "\n=== Scenario: " << name << " ===\n";
        clear_all();
        dispatch(events);

        auto pg_rows      = fetch_pg_rows(pg, pg_cfg->table, embed_text_col, "test");
        auto qdrant_points = fetch_qdrant_points(http, qdrant_cfg->url, qdrant_cfg->collection, embed_text_col, "test");

        std::vector<std::string> failures;
        bool ok = compare_backends(name, pg_rows, qdrant_points, expect_present, expect_absent, failures);
        if (ok) {
            std::cout << "PASS (" << pg_rows.size() << " rows, Postgres/Qdrant match)\n";
            ++passed;
        } else {
            std::cout << "FAIL\n";
            for (auto& f : failures) std::cout << "  - " << f << "\n";
        }
    };

    run_scenario("simple_inserts",
        {
            make_insert("test_table", "id", "name", "1", "alpha"),
            make_insert("test_table", "id", "name", "2", "beta"),
            make_insert("test_table", "id", "name", "3", "gamma"),
        },
        {"1", "2", "3"}, {});

    run_scenario("update_including_unchanged_skip",
        {
            make_insert("test_table", "id", "name", "10", "original ten"),
            make_insert("test_table", "id", "name", "11", "original eleven"),
            make_update("test_table", "id", "name", "10", "updated ten", "original ten"),
            make_update("test_table", "id", "name", "11", "original eleven", "original eleven"),
        },
        {"10", "11"}, {});

    run_scenario("insert_then_delete_same_id",
        {
            make_insert("test_table", "id", "name", "100", "will be deleted"),
            make_insert("test_table", "id", "name", "101", "unrelated neighbor"),
            make_delete("test_table", "id", "100"),
        },
        {"101"}, {"100"});

    run_scenario("delete_then_insert_same_id",
        {
            make_delete("test_table", "id", "200"), // no-op
            make_insert("test_table", "id", "name", "200", "created after no-op delete"),
        },
        {"200"}, {});

    // Discriminator-scoped truncate: two source tables sharing one sink
    // table/collection. Truncating test_table must not touch documents.
    {
        ++total;
        std::cout << "\n=== Scenario: discriminator_scoped_truncate ===\n";
        clear_all();
        dispatch({
            make_insert("test_table", "id", "name", "1", "test row one"),
            make_insert("test_table", "id", "name", "2", "test row two"),
            make_insert("documents",  "id", "body", "1", "doc row one"),
            make_insert("documents",  "id", "body", "2", "doc row two"),
        });
        dispatch({ make_truncate("test_table") });

        auto pg_test  = fetch_pg_rows(pg, pg_cfg->table, embed_text_col, "test");
        auto pg_docs  = fetch_pg_rows(pg, pg_cfg->table, embed_text_col, "documents");
        auto qd_test  = fetch_qdrant_points(http, qdrant_cfg->url, qdrant_cfg->collection, embed_text_col, "test");
        auto qd_docs  = fetch_qdrant_points(http, qdrant_cfg->url, qdrant_cfg->collection, embed_text_col, "documents");

        std::vector<std::string> failures;
        bool ok = true;
        if (!pg_test.empty())  { failures.push_back("test_table rows survived truncate in Postgres"); ok = false; }
        if (!qd_test.empty())  { failures.push_back("test_table points survived truncate in Qdrant"); ok = false; }
        if (pg_docs.size() != 2)  { failures.push_back("documents rows lost in Postgres after test_table-scoped truncate"); ok = false; }
        if (qd_docs.size() != 2)  { failures.push_back("documents points lost in Qdrant after test_table-scoped truncate"); ok = false; }
        if (!compare_backends("discriminator_scoped_truncate/documents", pg_docs, qd_docs, {"1", "2"}, {}, failures)) ok = false;

        if (ok) {
            std::cout << "PASS (test_table truncated in both backends, documents untouched in both)\n";
            ++passed;
        } else {
            std::cout << "FAIL\n";
            for (auto& f : failures) std::cout << "  - " << f << "\n";
        }
    }

    std::cout << "\n=== " << passed << " / " << total << " scenarios passed ===\n";

    PQfinish(pg);
    return (passed == total) ? 0 : 1;
}
