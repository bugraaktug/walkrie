#include <doctest.h>

#include <memory>
#include <string>
#include <vector>

#include "backfill_worker.hpp"
#include "embedding_provider.hpp"
#include "pgembedding_sink.hpp"

namespace
{

pgcdc::BackfillStore::ClaimedRow make_claimed(const std::string& table, const std::string& id, const std::string& row_data)
{
    pgcdc::BackfillStore::ClaimedRow row;
    row.source_table = table;
    row.row_id = id;
    row.row_data = row_data;
    return row;
}

class TestEmbedProvider : public pgcdc::EmbeddingProvider
{
public:
    void init() override {}
    std::vector<float> embed(const std::string&) override { return {0.1f, 0.2f}; }
    int dimensions() const override { return 2; }
    std::string name() const override { return "test_embed_provider"; }
};

// <<< records upsert() calls instead of hitting a live Postgres — same pattern as test_pgembedding_sink_batch_ordering.cpp
class RecordingPgEmbeddingSink : public pgcdc::PgEmbeddingSink
{
public:
    using pgcdc::PgEmbeddingSink::PgEmbeddingSink;

    struct RecordedUpsert
    {
        std::string id_value;
        std::string embed_text;
        std::vector<std::string> metadata_values;
    };
    std::vector<RecordedUpsert> upserts;

protected:
    bool upsert(const pgcdc::TableMapping&, const std::string& id_value, const std::string& embed_text,
                const std::vector<std::string>& metadata_values, const std::vector<float>&) override
    {
        upserts.push_back({id_value, embed_text, metadata_values});
        return true;
    }
};

pgcdc::TableMapping make_mapping()
{
    pgcdc::TableMapping tm;
    tm.source_table  = "users";
    tm.id_source_    = "id";
    tm.embed_source_ = "body";

    pgcdc::ColumnMapping cm;
    cm.source_column = "category";
    cm.sink_column   = "category";
    cm.role          = "metadata";
    tm.columns.push_back(cm);
    return tm;
}

std::unique_ptr<RecordingPgEmbeddingSink> make_sink()
{
    pgcdc::PgEmbeddingSinkConfig cfg;
    cfg.sink_table  = "user_embeddings";
    cfg.sink_column = "embedding";
    cfg.mappings.push_back(make_mapping());
    return std::make_unique<RecordingPgEmbeddingSink>(cfg, std::make_shared<TestEmbedProvider>());
}

} // namespace

TEST_SUITE("BackfillWorker::to_change_event")
{

    TEST_CASE("rehydrates known columns as an Insert-shaped event")
    {
        auto row = make_claimed("users", "1", R"({"id":"1","body":"hello","category":"x"})");
        auto ev = pgcdc::BackfillWorker::to_change_event(row);

        CHECK(ev.op == pgcdc::ChangeEvent::Op::Insert);
        CHECK(ev.table_name == "users");
        REQUIRE(ev.new_row.has_value());
        REQUIRE(ev.new_row->columns.size() == 3);
        for (const auto& col : ev.new_row->columns) {
            CHECK_FALSE(col.is_null);
            CHECK_FALSE(col.is_unchanged_toast);
        }
    }

    TEST_CASE("rehydrates a JSON null column as is_null, not empty text")
    {
        auto row = make_claimed("users", "1", R"({"id":"1","body":"hello","category":null})");
        auto ev = pgcdc::BackfillWorker::to_change_event(row);

        bool found = false;
        for (const auto& col : ev.new_row->columns) {
            if (col.name == "category") {
                found = true;
                CHECK(col.is_null);
                CHECK(col.text_value.empty());
            }
        }
        CHECK(found);
    }
}

TEST_SUITE("BackfillWorker::to_change_event -> PgEmbeddingSink::call_batch")
{

    TEST_CASE("a rehydrated row upserts with its id/embed/metadata values, unaffected by Update-only toast checks")
    {
        auto sink = make_sink();
        auto row = make_claimed("users", "1", R"({"id":"1","body":"hello","category":"x"})");
        auto ev = pgcdc::BackfillWorker::to_change_event(row);

        sink->call_batch({ev});

        REQUIRE(sink->upserts.size() == 1);
        CHECK(sink->upserts[0].id_value == "1");
        CHECK(sink->upserts[0].embed_text == "hello");
        REQUIRE(sink->upserts[0].metadata_values.size() == 1);
        CHECK(sink->upserts[0].metadata_values[0] == "x");
    }

    TEST_CASE("a row for an unmapped table is dropped, matching the live-dispatch path")
    {
        auto sink = make_sink();
        auto row = make_claimed("orders", "1", R"({"id":"1","body":"hello"})");
        auto ev = pgcdc::BackfillWorker::to_change_event(row);

        sink->call_batch({ev});

        CHECK(sink->upserts.empty());
    }
}
