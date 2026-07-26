#include <doctest.h>
#include "pgembedding_sink.hpp"
#include "embedding_provider.hpp"
#include "event.hpp"
#include <memory>
#include <vector>
#include <string>

namespace {

struct DummyEmbeddingConfig {}; // placeholder — adjust if EmbeddingConfig needs real fields

class TestEmbedProvider : public pgcdc::EmbeddingProvider {
public:
    void init() override {}
    std::vector<float> embed(const std::string& /*text*/) override {
        return {0.1f, 0.2f, 0.3f, 0.4f}; // fixed non-empty vector — content is irrelevant here
    }
    int dimensions() const override { return 4; }
    std::string name() const override { return "test_embed_provider"; }
};

// Records upsert()/remove() calls instead of executing them against a
// live Postgres connection — this test is purely about the ORDER
// call_batch() applies actions in, which doesn't require a real DB at
// all. init() is deliberately never called on this sink, so pg_ stays
// nullptr — safe, since the overrides below never touch it.
class RecordingPgEmbeddingSink : public pgcdc::PgEmbeddingSink {
public:
    using pgcdc::PgEmbeddingSink::PgEmbeddingSink;

    struct RecordedAction {
        enum class Kind { Upsert, Delete } kind;
        std::string id_value;
    };
    std::vector<RecordedAction> actions;

protected:
    bool upsert(const pgcdc::TableMapping& /*tm*/,
                const std::string& id_value,
                const std::string& /*embed_text*/,
                const std::vector<std::string>& /*metadata_values*/,
                const std::vector<float>& /*embedding*/) override {
        actions.push_back({RecordedAction::Kind::Upsert, id_value});
        return true;
    }

    bool remove(const pgcdc::TableMapping& /*tm*/, const std::string& item_id) override {
        actions.push_back({RecordedAction::Kind::Delete, item_id});
        return true;
    }
};

pgcdc::TableMapping make_mapping() {
    pgcdc::TableMapping tm;
    tm.source_table  = "test_table";
    tm.id_source_    = "id";
    tm.embed_source_ = "body";
    return tm;
}

pgcdc::ChangeEvent make_insert(const std::string& id, const std::string& body) {
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Insert;
    ev.table_name = "test_table";
    pgcdc::DecodedRow row;
    row.table_name = "test_table";
    row.kind = pgcdc::TupleKind::Insert;
    row.columns.push_back({"id", false, false, id});
    row.columns.push_back({"body", false, false, body});
    ev.new_row = row;
    return ev;
}

pgcdc::ChangeEvent make_update(const std::string& id, const std::string& body) {
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Update;
    ev.table_name = "test_table";
    pgcdc::DecodedRow row;
    row.table_name = "test_table";
    row.kind = pgcdc::TupleKind::UpdateNew;
    row.columns.push_back({"id", false, false, id});
    row.columns.push_back({"body", false, false, body});
    ev.new_row = row;
    // no old_row set — skip-unchanged check that depends on old_row is
    // simply bypassed, which is fine: this test is about ordering, not
    // exercising the skip-unchanged logic (already covered elsewhere).
    return ev;
}

pgcdc::ChangeEvent make_delete(const std::string& id) {
    pgcdc::ChangeEvent ev;
    ev.op = pgcdc::ChangeEvent::Op::Delete;
    ev.table_name = "test_table";
    pgcdc::DecodedRow row;
    row.table_name = "test_table";
    row.kind = pgcdc::TupleKind::Delete;
    row.columns.push_back({"id", false, false, id});
    ev.old_row = row;
    return ev;
}

std::unique_ptr<RecordingPgEmbeddingSink> make_sink() {
    pgcdc::PgEmbeddingSinkConfig cfg;
    cfg.sink_table  = "test_embeddings";
    cfg.sink_column = "embedding";
    cfg.mappings.push_back(make_mapping());

    auto provider = std::make_shared<TestEmbedProvider>();
    // init() intentionally not called — no live pg_ connection needed,
    // since upsert()/remove() are overridden and never touch it.
    return std::make_unique<RecordingPgEmbeddingSink>(cfg, provider);
}

} // namespace

TEST_SUITE("PgEmbeddingSink::call_batch — insert/delete ordering") {

    TEST_CASE("insert then delete for the SAME id in one batch: delete must be applied after the insert's upsert") {
        // This is the exact bug the two-pass call_batch rewrite fixes:
        // deletes used to execute immediately, before same-batch
        // inserts/updates (which were deferred until after the batched
        // embed call) had been upserted — silently resurrecting a row
        // that should have ended up deleted. Asserting the recorded
        // action order here is a direct regression guard against that.
        auto sink = make_sink();

        std::vector<pgcdc::ChangeEvent> batch = {
            make_insert("42", "hello world"),
            make_delete("42"),
        };

        sink->call_batch(batch);

        REQUIRE(sink->actions.size() == 2);
        CHECK(sink->actions[0].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Upsert);
        CHECK(sink->actions[0].id_value == "42");
        CHECK(sink->actions[1].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Delete);
        CHECK(sink->actions[1].id_value == "42");
    }

    TEST_CASE("delete then insert for the SAME id in one batch: insert's upsert must be applied after the delete") {
        // Mirror case — delete arrives first in the batch, insert second.
        // Original event order must be preserved either way; the fix is
        // about respecting whatever order events actually arrived in,
        // not about always putting deletes last.
        auto sink = make_sink();

        std::vector<pgcdc::ChangeEvent> batch = {
            make_delete("99"),
            make_insert("99", "recreated row"),
        };

        sink->call_batch(batch);

        REQUIRE(sink->actions.size() == 2);
        CHECK(sink->actions[0].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Delete);
        CHECK(sink->actions[0].id_value == "99");
        CHECK(sink->actions[1].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Upsert);
        CHECK(sink->actions[1].id_value == "99");
    }

    TEST_CASE("mixed batch: 5 inserts, 1 delete, 4 updates — original relative order preserved throughout") {
        // Reproduces the exact scenario from the design discussion:
        // batch_size=10, five inserts, then a delete targeting the same
        // id as the third insert, then four updates.
        auto sink = make_sink();

        std::vector<pgcdc::ChangeEvent> batch = {
            make_insert("101", "insert one"),
            make_insert("102", "insert two"),
            make_insert("103", "insert three"), // delete below targets this id
            make_insert("104", "insert four"),
            make_insert("105", "insert five"),
            make_delete("103"),
            make_update("201", "update one"),
            make_update("202", "update two"),
            make_update("203", "update three"),
            make_update("204", "update four"),
        };

        sink->call_batch(batch);

        REQUIRE(sink->actions.size() == 10);

        // First five: upserts for 101-105, in order.
        for (int i = 0; i < 5; ++i) {
            CHECK(sink->actions[i].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Upsert);
        }
        CHECK(sink->actions[2].id_value == "103"); // insert #3

        // Sixth action: the delete for id 103 — must come AFTER insert
        // #3's upsert (index 2), preserving the row's correct final
        // state (deleted), not resurrected by a later-applied upsert.
        CHECK(sink->actions[5].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Delete);
        CHECK(sink->actions[5].id_value == "103");
        CHECK(5 > 2); // explicit: delete's position strictly after insert #3's position

        // Final four: upserts for the updates, in order.
        for (int i = 6; i < 10; ++i) {
            CHECK(sink->actions[i].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Upsert);
        }
        CHECK(sink->actions[6].id_value == "201");
        CHECK(sink->actions[9].id_value == "204");
    }

    TEST_CASE("call() (single-event path) still produces exactly one action, unaffected by batching changes") {
        auto sink = make_sink();
        sink->call(make_insert("1", "solo event"));

        REQUIRE(sink->actions.size() == 1);
        CHECK(sink->actions[0].kind == RecordingPgEmbeddingSink::RecordedAction::Kind::Upsert);
        CHECK(sink->actions[0].id_value == "1");
    }
}
