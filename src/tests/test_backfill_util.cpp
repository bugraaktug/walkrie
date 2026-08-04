#include <doctest.h>

#include <filesystem>
#include <string>

#include "backfill_store.hpp"
#include "backfill_util.hpp"

namespace
{

std::string temp_db_path()
{
    static int counter = 0;
    auto path = std::filesystem::temp_directory_path() /
                ("backfill_util_test_" + std::to_string(counter++) + ".sqlite3");
    std::filesystem::remove(path);
    return path.string();
}

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

pgcdc::ColumnValue col(const std::string& name, const std::string& value)
{
    pgcdc::ColumnValue c;
    c.name = name;
    c.text_value = value;
    return c;
}

pgcdc::ColumnValue unchanged_toast_col(const std::string& name)
{
    pgcdc::ColumnValue c;
    c.name = name;
    c.is_unchanged_toast = true;
    return c;
}

void seed_row(const std::string& path, const std::string& table, const std::string& id, const std::string& row_data)
{
    pgcdc::BackfillStore store(path);
    store.open();
    store.insert_row(table, id, row_data);
}

std::optional<std::string> row_data(const std::string& path, const std::string& table, const std::string& id)
{
    pgcdc::BackfillStore store(path);
    store.open();
    return store.get_row_data(table, id);
}

} // namespace

TEST_SUITE("BackfillUtil")
{

    TEST_CASE("absorb_event ignores Insert — dump only ever holds pre-existing rows")
    {
        auto path = temp_db_path();
        seed_row(path, "users", "1", R"({"id":"1","body":"hello","category":"x"})");
        pgcdc::BackfillUtil util("", {make_mapping()}, path);
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Insert;
        ev.table_name = "users";
        ev.new_row = pgcdc::DecodedRow{"", "users", pgcdc::TupleKind::Insert,
            {col("id", "1"), col("body", "hello"), col("category", "x")}};

        CHECK_FALSE(util.absorb_event(ev));
        CHECK(row_data(path, "users", "1") == R"({"id":"1","body":"hello","category":"x"})");
    }

    TEST_CASE("absorb_event removes the row on Delete but still lets it reach sinks")
    {
        auto path = temp_db_path();
        seed_row(path, "users", "1", R"({"id":"1","body":"hello","category":"x"})");
        pgcdc::BackfillUtil util("", {make_mapping()}, path);
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Delete;
        ev.table_name = "users";
        ev.old_row = pgcdc::DecodedRow{"", "users", pgcdc::TupleKind::Delete, {col("id", "1")}};

        CHECK_FALSE(util.absorb_event(ev)); // <<< Delete always dispatches too
        CHECK_FALSE(row_data(path, "users", "1").has_value());
    }

    TEST_CASE("absorb_event merges a complete Update into the stored row and suppresses dispatch")
    {
        auto path = temp_db_path();
        seed_row(path, "users", "1", R"({"id":"1","body":"hello","category":"x"})");
        pgcdc::BackfillUtil util("", {make_mapping()}, path);
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Update;
        ev.table_name = "users";
        ev.new_row = pgcdc::DecodedRow{"", "users", pgcdc::TupleKind::UpdateNew,
            {col("id", "1"), col("body", "updated"), col("category", "y")}};

        CHECK(util.absorb_event(ev));
        CHECK(row_data(path, "users", "1") == R"({"id":"1","body":"updated","category":"y"})");
    }

    TEST_CASE("absorb_event preserves the previous embed value when Update has an unchanged-toast embed column")
    {
        auto path = temp_db_path();
        seed_row(path, "users", "1", R"({"id":"1","body":"hello","category":"x"})");
        pgcdc::BackfillUtil util("", {make_mapping()}, path);
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Update;
        ev.table_name = "users";
        ev.new_row = pgcdc::DecodedRow{"", "users", pgcdc::TupleKind::UpdateNew,
            {col("id", "1"), unchanged_toast_col("body"), col("category", "y")}};

        CHECK(util.absorb_event(ev)); // <<< still suppress — backfill owns writing this row
        CHECK(row_data(path, "users", "1") == R"({"id":"1","body":"hello","category":"y"})");
    }

    TEST_CASE("absorb_event preserves the previous metadata value when Update has an unchanged-toast metadata column")
    {
        auto path = temp_db_path();
        seed_row(path, "users", "1", R"({"id":"1","body":"hello","category":"x"})");
        pgcdc::BackfillUtil util("", {make_mapping()}, path);
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Update;
        ev.table_name = "users";
        ev.new_row = pgcdc::DecodedRow{"", "users", pgcdc::TupleKind::UpdateNew,
            {col("id", "1"), col("body", "updated"), unchanged_toast_col("category")}};

        CHECK(util.absorb_event(ev));
        CHECK(row_data(path, "users", "1") == R"({"id":"1","body":"updated","category":"x"})");
    }

    TEST_CASE("absorb_event on Update is a no-op when the row isn't in the store")
    {
        auto path = temp_db_path();
        pgcdc::BackfillUtil util("", {make_mapping()}, path); // nothing seeded — never dumped, or already drained
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Update;
        ev.table_name = "users";
        ev.new_row = pgcdc::DecodedRow{"", "users", pgcdc::TupleKind::UpdateNew,
            {col("id", "1"), col("body", "hello"), col("category", "x")}};

        CHECK_FALSE(util.absorb_event(ev));
        CHECK_FALSE(row_data(path, "users", "1").has_value());
    }

    TEST_CASE("absorb_event is a no-op for an unmapped table")
    {
        auto path = temp_db_path();
        seed_row(path, "orders", "1", "{}");
        pgcdc::BackfillUtil util("", {make_mapping()}, path); // only maps "users"
        util.open();

        pgcdc::ChangeEvent ev;
        ev.op = pgcdc::ChangeEvent::Op::Delete;
        ev.table_name = "orders";
        ev.old_row = pgcdc::DecodedRow{"", "orders", pgcdc::TupleKind::Delete, {col("id", "1")}};

        CHECK_FALSE(util.absorb_event(ev));
        CHECK(row_data(path, "orders", "1").has_value());
    }

    TEST_CASE("absorb_event is a no-op for Commit/Truncate")
    {
        auto path = temp_db_path();
        seed_row(path, "users", "1", "{}");
        pgcdc::BackfillUtil util("", {make_mapping()}, path);
        util.open();

        pgcdc::ChangeEvent commit_ev;
        commit_ev.op = pgcdc::ChangeEvent::Op::Commit;
        commit_ev.table_name = "users";
        CHECK_FALSE(util.absorb_event(commit_ev));

        pgcdc::ChangeEvent truncate_ev;
        truncate_ev.op = pgcdc::ChangeEvent::Op::Truncate;
        truncate_ev.table_name = "users";
        CHECK_FALSE(util.absorb_event(truncate_ev));

        CHECK(row_data(path, "users", "1").has_value());
    }

}
