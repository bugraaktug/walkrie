// test_parser.cpp
#include "doctest.h"
#include "pgoutput_parser.hpp"   // adjust filename if different
#include "event.hpp"
#include <vector>

using pgcdc::PgOutputParser;
using pgcdc::ChangeEvent;
using pgcdc::TupleKind;

namespace 
{

// --- Insert transaction fixtures ---
const std::vector<uint8_t> insert_begin = {
    0x42, 0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xc0, 0x40, 0x00,
    0x02, 0xf9, 0x67, 0xa8, 0xb6, 0x00, 0xd6, 0x00, 0x00, 0x03, 0xad
};
const std::vector<uint8_t> relation_msg = {
    0x52, 0x00, 0x00, 0x40, 0x11, 0x70, 0x75, 0x62, 0x6c, 0x69,
    0x63, 0x00, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x74, 0x61, 0x62,
    0x6c, 0x65, 0x00, 0x64, 0x00, 0x03, 0x01, 0x69, 0x64, 0x00,
    0x00, 0x00, 0x00, 0x17, 0xff, 0xff, 0xff, 0xff, 0x00, 0x6e,
    0x61, 0x6d, 0x65, 0x00, 0x00, 0x00, 0x04, 0x13, 0x00, 0x00,
    0x00, 0x68, 0x00, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64,
    0x5f, 0x61, 0x74, 0x00, 0x00, 0x00, 0x04, 0x5a, 0xff, 0xff,
    0xff, 0xff
};
const std::vector<uint8_t> insert_msg = {
    0x49, 0x00, 0x00, 0x40, 0x11, 0x4e, 0x00, 0x03, 0x74, 0x00,
    0x00, 0x00, 0x02, 0x39, 0x32, 0x74, 0x00, 0x00, 0x00, 0x0c,
    0x54, 0x65, 0x73, 0x74, 0x20, 0x45, 0x6e, 0x74, 0x72, 0x79,
    0x39, 0x32, 0x74, 0x00, 0x00, 0x00, 0x1a, 0x32, 0x30, 0x32,
    0x36, 0x2d, 0x30, 0x37, 0x2d, 0x31, 0x32, 0x20, 0x31, 0x35,
    0x3a, 0x31, 0x32, 0x3a, 0x34, 0x30, 0x2e, 0x38, 0x36, 0x38,
    0x34, 0x34, 0x35
};
const std::vector<uint8_t> insert_commit = {
    0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xc0, 0x40,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xc0, 0x70, 0x00, 0x02,
    0xf9, 0x67, 0xa8, 0xb6, 0x00, 0xd6
};

// --- Update transaction fixtures (relation already cached, so no Relation msg) ---
const std::vector<uint8_t> update_begin = {
    0x42, 0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xf4, 0xa8, 0x00,
    0x02, 0xf9, 0x67, 0xa9, 0xa7, 0xed, 0x71, 0x00, 0x00, 0x03, 0xaf
};
const std::vector<uint8_t> update_msg = {
    0x55, 0x00, 0x00, 0x40, 0x11, 0x4e, 0x00, 0x03, 0x74, 0x00,
    0x00, 0x00, 0x02, 0x39, 0x32, 0x74, 0x00, 0x00, 0x00, 0x1a,
    0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x20, 0x54, 0x69,
    0x74, 0x6c, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x65, 0x6e,
    0x74, 0x72, 0x79, 0x20, 0x39, 0x32, 0x74, 0x00, 0x00, 0x00,
    0x1a, 0x32, 0x30, 0x32, 0x36, 0x2d, 0x30, 0x37, 0x2d, 0x31,
    0x32, 0x20, 0x31, 0x35, 0x3a, 0x31, 0x32, 0x3a, 0x34, 0x30,
    0x2e, 0x38, 0x36, 0x38, 0x34, 0x34, 0x35
};
const std::vector<uint8_t> update_commit = {
    0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xf4, 0xa8,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x44, 0xf4, 0xd8, 0x00, 0x02,
    0xf9, 0x67, 0xa9, 0xa7, 0xed, 0x71
};

// --- REPLICA IDENTITY FULL update transaction fixtures (documents table) ---
const std::vector<uint8_t> full_relation_msg = {
    0x52, 0x00, 0x00, 0xa1, 0x67, 0x70, 0x75, 0x62, 0x6c, 0x69,
    0x63, 0x00, 0x64, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74,
    0x73, 0x00, 0x66, 0x00, 0x04, 0x01, 0x69, 0x64, 0x00, 0x00,
    0x00, 0x00, 0x14, 0xff, 0xff, 0xff, 0xff, 0x01, 0x74, 0x69,
    0x74, 0x6c, 0x65, 0x00, 0x00, 0x00, 0x00, 0x19, 0xff, 0xff,
    0xff, 0xff, 0x01, 0x62, 0x6f, 0x64, 0x79, 0x00, 0x00, 0x00,
    0x00, 0x19, 0xff, 0xff, 0xff, 0xff, 0x01, 0x75, 0x70, 0x64,
    0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x00, 0x00, 0x00,
    0x04, 0xa0, 0xff, 0xff, 0xff, 0xff
};
const std::vector<uint8_t> full_update_begin = {
    0x42, 0x00, 0x00, 0x00, 0x00, 0x02, 0x45, 0x65, 0x98, 0x00,
    0x02, 0xf9, 0x68, 0x12, 0xfa, 0x09, 0x1a, 0x00, 0x00, 0x03, 0xb5
};
const std::vector<uint8_t> full_update_msg = {
    0x55, 0x00, 0x00, 0xa1, 0x67, 0x4f, 0x00, 0x04, 0x74, 0x00,
    0x00, 0x00, 0x01, 0x37, 0x74, 0x00, 0x00, 0x00, 0x09, 0x46,
    0x69, 0x72, 0x73, 0x74, 0x20, 0x64, 0x6f, 0x63, 0x74, 0x00,
    0x00, 0x00, 0x0e, 0x53, 0x6f, 0x6d, 0x65, 0x20, 0x62, 0x6f,
    0x64, 0x79, 0x20, 0x74, 0x65, 0x78, 0x74, 0x74, 0x00, 0x00,
    0x00, 0x1d, 0x32, 0x30, 0x32, 0x36, 0x2d, 0x30, 0x37, 0x2d,
    0x31, 0x32, 0x20, 0x31, 0x35, 0x3a, 0x34, 0x32, 0x3a, 0x30,
    0x34, 0x2e, 0x38, 0x30, 0x33, 0x34, 0x36, 0x37, 0x2b, 0x30,
    0x33, 0x4e, 0x00, 0x04, 0x74, 0x00, 0x00, 0x00, 0x01, 0x37,
    0x74, 0x00, 0x00, 0x00, 0x0d, 0x55, 0x70, 0x64, 0x61, 0x74,
    0x65, 0x64, 0x20, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x74, 0x00,
    0x00, 0x00, 0x13, 0x41, 0x20, 0x6e, 0x65, 0x77, 0x20, 0x62,
    0x6f, 0x64, 0x79, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x69, 0x64,
    0x20, 0x37, 0x74, 0x00, 0x00, 0x00, 0x1d, 0x32, 0x30, 0x32,
    0x36, 0x2d, 0x30, 0x37, 0x2d, 0x31, 0x32, 0x20, 0x31, 0x35,
    0x3a, 0x34, 0x32, 0x3a, 0x30, 0x34, 0x2e, 0x38, 0x30, 0x33,
    0x34, 0x36, 0x37, 0x2b, 0x30, 0x33
};

// --- Delete transaction fixtures (fresh session — Relation re-sent) ---
const std::vector<uint8_t> delete_begin = {
    0x42, 0x00, 0x00, 0x00, 0x00, 0x02, 0x45, 0x20, 0xd0, 0x00,
    0x02, 0xf9, 0x67, 0xc1, 0x74, 0xa5, 0xb5, 0x00, 0x00, 0x03, 0xb1
};
const std::vector<uint8_t> delete_msg = {
    0x44, 0x00, 0x00, 0x40, 0x11, 0x4b, 0x00, 0x03, 0x74, 0x00,
    0x00, 0x00, 0x02, 0x39, 0x32, 0x6e, 0x6e
};
const std::vector<uint8_t> delete_commit = {
    0x43, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x45, 0x20, 0xd0,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x45, 0x21, 0x00, 0x00, 0x02,
    0xf9, 0x67, 0xc1, 0x74, 0xa5, 0xb5
};

// --- Truncate fixtures ---
const std::vector<uint8_t> truncate_single_msg = {
    0x54, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x40, 0x11
};
const std::vector<uint8_t> truncate_multi_msg = {
    0x54, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x40, 0x11,
    0x00, 0x00, 0xa1, 0x67
};
const std::vector<uint8_t> truncate_unknown_msg = {
    0x54, 0x00, 0x00, 0x00, 0x01, 0x00, 0xde, 0xad, 0xbe, 0xef
};

const pgcdc::ColumnValue* find_col(const pgcdc::DecodedRow& row, const std::string& name)
{
    for (auto& c : row.columns) {
        if (c.name == name) return &c;
    }
    return nullptr;
}

} // namespace

TEST_SUITE("PgOutputParser") 
{

    TEST_CASE("Begin/Relation/Commit messages return nullopt") 
    {
        PgOutputParser parser;
        CHECK(!parser.parse(insert_begin.data(), insert_begin.size()).has_value());
        CHECK(!parser.parse(relation_msg.data(), relation_msg.size()).has_value());
        CHECK(!parser.parse(insert_commit.data(), insert_commit.size()).has_value());
    }

    TEST_CASE("Insert produces ChangeEvent with new_row populated") 
    {
        PgOutputParser parser;
        parser.parse(insert_begin.data(), insert_begin.size());
        parser.parse(relation_msg.data(), relation_msg.size());

        auto result = parser.parse(insert_msg.data(), insert_msg.size());

        REQUIRE(result.has_value());
        REQUIRE(result->size() == 1);
        auto& event = (*result)[0];
        CHECK(event.op == ChangeEvent::Op::Insert);
        CHECK(event.schema_name == "public");
        CHECK(event.table_name == "test_table");
        REQUIRE(event.new_row.has_value());
        CHECK(!event.old_row.has_value());
        CHECK(event.commit_timestamp > 0);

        auto* id = find_col(*event.new_row, "id");
        REQUIRE(id != nullptr);
        CHECK(id->text_value == "92");
        CHECK(!id->is_null);

        auto* name = find_col(*event.new_row, "name");
        REQUIRE(name != nullptr);
        CHECK(name->text_value == "Test Entry92");

        auto* created_at = find_col(*event.new_row, "created_at");
        REQUIRE(created_at != nullptr);
        CHECK(created_at->text_value == "2026-07-12 15:12:40.868445");
    }

    TEST_CASE("Update (REPLICA IDENTITY DEFAULT, no key change) produces new_row only") 
    {
        PgOutputParser parser;
        // Prime the relation cache — this transaction's dump had no Relation
        // message because it was already cached from a prior Insert in the
        // same session.
        parser.parse(insert_begin.data(), insert_begin.size());
        parser.parse(relation_msg.data(), relation_msg.size());
        parser.parse(insert_msg.data(), insert_msg.size());
        parser.parse(insert_commit.data(), insert_commit.size());

        parser.parse(update_begin.data(), update_begin.size());
        auto result = parser.parse(update_msg.data(), update_msg.size());

        REQUIRE(result.has_value());
        REQUIRE(result->size() == 1);
        auto& event = (*result)[0];
        CHECK(event.op == ChangeEvent::Op::Update);
        REQUIRE(event.new_row.has_value());
        CHECK(!event.old_row.has_value());  // no old tuple sent — REPLICA IDENTITY DEFAULT, PK unchanged

        auto* name = find_col(*event.new_row, "name");
        REQUIRE(name != nullptr);
        CHECK(name->text_value == "Updated Title for entry 92");
    }

    TEST_CASE("Delete (REPLICA IDENTITY DEFAULT) produces old_row with only key column populated") 
    {
        PgOutputParser parser;
        parser.parse(delete_begin.data(), delete_begin.size());
        parser.parse(relation_msg.data(), relation_msg.size());  // fresh session — Relation re-sent

        auto result = parser.parse(delete_msg.data(), delete_msg.size());

        REQUIRE(result.has_value());
        REQUIRE(result->size() == 1);
        auto& event = (*result)[0];
        CHECK(event.op == ChangeEvent::Op::Delete);
        REQUIRE(event.old_row.has_value());
        CHECK(!event.new_row.has_value());

        auto* id = find_col(*event.old_row, "id");
        REQUIRE(id != nullptr);
        CHECK(!id->is_null);
        CHECK(id->text_value == "92");

        auto* name = find_col(*event.old_row, "name");
        REQUIRE(name != nullptr);
        CHECK(name->is_null);  // not part of replica identity key — sent as null

        auto* created_at = find_col(*event.old_row, "created_at");
        REQUIRE(created_at != nullptr);
        CHECK(created_at->is_null);
    }

    TEST_CASE("full transaction sequence produces exactly one event per DML message") 
    {
        PgOutputParser parser;
        int events = 0;
        for (auto& msg : {insert_begin, relation_msg, insert_msg, insert_commit}) {
            if (parser.parse(msg.data(), msg.size()).has_value()) ++events;
        }
        CHECK(events == 1);  // only the Insert message should yield a ChangeEvent
    }

    TEST_CASE("Update with REPLICA IDENTITY FULL produces both old_row and new_row") 
    {
        PgOutputParser parser;
        parser.parse(full_update_begin.data(), full_update_begin.size());
        parser.parse(full_relation_msg.data(), full_relation_msg.size());

        auto result = parser.parse(full_update_msg.data(), full_update_msg.size());

        REQUIRE(result.has_value());
        REQUIRE(result->size() == 1);
        auto& event = (*result)[0];
        CHECK(event.op == pgcdc::ChangeEvent::Op::Update);
        CHECK(event.schema_name == "public");
        CHECK(event.table_name == "documents");
        REQUIRE(event.old_row.has_value());
        REQUIRE(event.new_row.has_value());

        auto* old_title = find_col(*event.old_row, "title");
        REQUIRE(old_title != nullptr);
        CHECK(old_title->text_value == "First doc");

        auto* new_title = find_col(*event.new_row, "title");
        REQUIRE(new_title != nullptr);
        CHECK(new_title->text_value == "Updated title");

        auto* old_body = find_col(*event.old_row, "body");
        REQUIRE(old_body != nullptr);
        CHECK(old_body->text_value == "Some body text");

        auto* new_body = find_col(*event.new_row, "body");
        REQUIRE(new_body != nullptr);
        CHECK(new_body->text_value == "A new body for id 7");

        // id and updated_at unchanged between old/new tuples in this capture
        auto* old_id = find_col(*event.old_row, "id");
        auto* new_id = find_col(*event.new_row, "id");
        REQUIRE(old_id != nullptr);
        REQUIRE(new_id != nullptr);
        CHECK(old_id->text_value == "7");
        CHECK(new_id->text_value == "7");

        auto* old_updated = find_col(*event.old_row, "updated_at");
        auto* new_updated = find_col(*event.new_row, "updated_at");
        REQUIRE(old_updated != nullptr);
        REQUIRE(new_updated != nullptr);
        CHECK(old_updated->text_value == new_updated->text_value);
    }

    TEST_CASE("Truncate naming one known relation produces a single Truncate ChangeEvent")
    {
        PgOutputParser parser;
        parser.parse(insert_begin.data(), insert_begin.size());
        parser.parse(relation_msg.data(), relation_msg.size());

        auto result = parser.parse(truncate_single_msg.data(), truncate_single_msg.size());

        REQUIRE(result.has_value());
        REQUIRE(result->size() == 1);
        auto& event = (*result)[0];
        CHECK(event.op == ChangeEvent::Op::Truncate);
        CHECK(event.schema_name == "public");
        CHECK(event.table_name == "test_table");
        CHECK(!event.old_row.has_value());
        CHECK(!event.new_row.has_value());
        CHECK(event.commit_timestamp > 0);
    }

    TEST_CASE("Truncate naming several relations (e.g. CASCADE) produces one event per relation")
    {
        PgOutputParser parser;
        parser.parse(insert_begin.data(), insert_begin.size());
        parser.parse(relation_msg.data(), relation_msg.size());
        parser.parse(full_relation_msg.data(), full_relation_msg.size());

        auto result = parser.parse(truncate_multi_msg.data(), truncate_multi_msg.size());

        REQUIRE(result.has_value());
        REQUIRE(result->size() == 2);
        CHECK((*result)[0].op == ChangeEvent::Op::Truncate);
        CHECK((*result)[0].table_name == "test_table");
        CHECK((*result)[1].op == ChangeEvent::Op::Truncate);
        CHECK((*result)[1].table_name == "documents");
    }

    TEST_CASE("Truncate naming an unknown relation is skipped, not thrown, and yields an empty (but present) vector")
    {
        PgOutputParser parser;
        parser.parse(insert_begin.data(), insert_begin.size());

        auto result = parser.parse(truncate_unknown_msg.data(), truncate_unknown_msg.size());

        REQUIRE(result.has_value());  // Truncate is an event-producing message type...
        CHECK(result->empty());      // ...it just didn't resolve any relation this time
    }
}
