#include <doctest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <set>
#include <string>
#include <thread>

#include <sqlite3.h>

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

    TEST_CASE("claim_pending blocks on a concurrent writer instead of throwing SQLITE_BUSY immediately")
    {
        // Regression test for the busy_timeout gap found while designing multi-worker-per-source
        // backfill: without it, a losing writer failed instantly instead of waiting its turn.
        auto path = temp_db_path();
        pgcdc::BackfillStore store(path);
        store.open();
        store.insert_row("users", "1", "{}");

        // A second raw connection to the same file holds the write lock via BEGIN IMMEDIATE,
        // standing in for a second walkrie_worker process mid-write.
        sqlite3* blocker = nullptr;
        REQUIRE(sqlite3_open(path.c_str(), &blocker) == SQLITE_OK);
        REQUIRE(sqlite3_exec(blocker, "BEGIN IMMEDIATE", nullptr, nullptr, nullptr) == SQLITE_OK);

        std::atomic<bool> claim_returned{false};
        std::vector<pgcdc::BackfillStore::ClaimedRow> claimed;
        std::thread t([&] {
            claimed = store.claim_pending(10); // should block here, not throw, until the lock below releases
            claim_returned = true;
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        CHECK_FALSE(claim_returned.load()); // still waiting on the blocker's lock

        sqlite3_exec(blocker, "COMMIT", nullptr, nullptr, nullptr);
        sqlite3_close(blocker);

        t.join();
        CHECK(claim_returned.load());
        REQUIRE(claimed.size() == 1);
        CHECK(claimed[0].row_id == "1");
    }

    TEST_CASE("concurrent claim_pending from two separate connections never double-claims a row")
    {
        auto path = temp_db_path();
        {
            pgcdc::BackfillStore seed(path);
            seed.open();
            for (int i = 0; i < 20; ++i) seed.insert_row("users", std::to_string(i), "{}");
        }

        pgcdc::BackfillStore store_a(path);
        store_a.open();
        pgcdc::BackfillStore store_b(path);
        store_b.open();

        std::vector<pgcdc::BackfillStore::ClaimedRow> claimed_a, claimed_b;
        std::thread ta([&] { claimed_a = store_a.claim_pending(20); });
        std::thread tb([&] { claimed_b = store_b.claim_pending(20); });
        ta.join();
        tb.join();

        CHECK(claimed_a.size() + claimed_b.size() == 20); // every row claimed exactly once, by exactly one side

        std::set<std::string> ids_a, ids_b;
        for (const auto& r : claimed_a) ids_a.insert(r.row_id);
        for (const auto& r : claimed_b) ids_b.insert(r.row_id);
        std::vector<std::string> overlap;
        std::set_intersection(ids_a.begin(), ids_a.end(), ids_b.begin(), ids_b.end(), std::back_inserter(overlap));
        CHECK(overlap.empty());
    }

}
