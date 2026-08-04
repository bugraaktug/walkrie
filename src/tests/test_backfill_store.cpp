#include <doctest.h>

#include <algorithm>
#include <filesystem>
#include <string>

#include "backfill_store.hpp"

namespace
{

// Fresh, unique on-disk path per test — real files (not ":memory:") since
// durability across process restarts is the whole point of this store.
std::string temp_db_path()
{
    static int counter = 0;
    auto path = std::filesystem::temp_directory_path() /
                ("backfill_store_test_" + std::to_string(counter++) + ".sqlite3");
    std::filesystem::remove(path);
    return path.string();
}

bool contains_row(const std::vector<pgcdc::BackfillStore::ClaimedRow>& rows,
                   const std::string& source_table, const std::string& row_id)
{
    return std::any_of(rows.begin(), rows.end(), [&](const auto& r) {
        return r.source_table == source_table && r.row_id == row_id;
    });
}

} // namespace

TEST_SUITE("BackfillStore")
{

    TEST_CASE("open creates the db file")
    {
        auto path = temp_db_path();
        CHECK_FALSE(std::filesystem::exists(path));

        pgcdc::BackfillStore store(path);
        store.open();

        CHECK(std::filesystem::exists(path));
    }

    TEST_CASE("insert_row + claim_pending round-trips row_data")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        store.insert_row("users", "1", R"({"id":"1","name":"alice"})");

        auto claimed = store.claim_pending(10);
        REQUIRE(claimed.size() == 1);
        CHECK(claimed[0].source_table == "users");
        CHECK(claimed[0].row_id == "1");
        CHECK(claimed[0].row_data == R"({"id":"1","name":"alice"})");
    }

    TEST_CASE("insert_row is idempotent for the same (source_table, row_id)")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        store.insert_row("users", "1", R"({"id":"1","name":"alice"})");
        store.insert_row("users", "1", R"({"id":"1","name":"alice-should-be-ignored"})");

        auto claimed = store.claim_pending(10);
        REQUIRE(claimed.size() == 1);
        CHECK(claimed[0].row_data == R"({"id":"1","name":"alice"})");
    }

    TEST_CASE("same row_id is distinct across different source_table")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        store.insert_row("users", "1", R"({"id":"1"})");
        store.insert_row("orders", "1", R"({"id":"1"})");

        auto claimed = store.claim_pending(10);
        REQUIRE(claimed.size() == 2);
        CHECK(contains_row(claimed, "users", "1"));
        CHECK(contains_row(claimed, "orders", "1"));
    }

    TEST_CASE("claim_pending respects the limit and does not re-claim already-claimed rows")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        store.insert_row("users", "1", "{}");
        store.insert_row("users", "2", "{}");
        store.insert_row("users", "3", "{}");

        auto first = store.claim_pending(2);
        CHECK(first.size() == 2);

        auto second = store.claim_pending(10);
        REQUIRE(second.size() == 1); // only the row not claimed in the first batch
        CHECK(second[0].row_id == "3");
    }

    TEST_CASE("mark_done removes the row so it is not claimed again")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        store.insert_row("users", "1", "{}");
        auto claimed = store.claim_pending(10);
        REQUIRE(claimed.size() == 1);

        store.mark_done("users", "1");

        CHECK(store.claim_pending(10).empty());
    }

    TEST_CASE("is_table_dumped is false until mark_table_dumped is called")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        CHECK_FALSE(store.is_table_dumped("users"));

        store.mark_table_dumped("users");

        CHECK(store.is_table_dumped("users"));
        CHECK_FALSE(store.is_table_dumped("orders")); // unrelated table unaffected
    }

    TEST_CASE("mark_table_dumped is idempotent")
    {
        pgcdc::BackfillStore store(temp_db_path());
        store.open();

        store.mark_table_dumped("users");
        store.mark_table_dumped("users");

        CHECK(store.is_table_dumped("users"));
    }

    TEST_CASE("data persists across separate BackfillStore instances on the same file")
    {
        auto path = temp_db_path();

        {
            pgcdc::BackfillStore store(path);
            store.open();
            store.insert_row("users", "1", R"({"id":"1"})");
            store.mark_table_dumped("users");
        }

        {
            pgcdc::BackfillStore store(path);
            store.open();

            CHECK(store.is_table_dumped("users"));
            auto claimed = store.claim_pending(10);
            REQUIRE(claimed.size() == 1);
            CHECK(claimed[0].row_id == "1");
        }
    }

}
