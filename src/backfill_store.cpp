#include "backfill_store.hpp"

#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace pgcdc
{

namespace
{

void exec_or_throw(sqlite3* db, const char* sql)
{
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "unknown sqlite error";
        sqlite3_free(err);
        throw std::runtime_error("BackfillStore: " + msg);
    }
}

// RAII wrapper around sqlite3_stmt — every method below prepares, binds, steps,
// then lets this finalize on scope exit (including the throw paths).
struct StmtGuard
{
    sqlite3_stmt* stmt = nullptr;
    ~StmtGuard() { if (stmt) sqlite3_finalize(stmt); }
};

sqlite3_stmt* prepare_or_throw(sqlite3* db, StmtGuard& g, const char* sql, const char* who)
{
    if (sqlite3_prepare_v2(db, sql, -1, &g.stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error(std::string("BackfillStore: ") + who + " prepare failed: " + sqlite3_errmsg(db));
    }
    return g.stmt;
}

} // namespace

BackfillStore::BackfillStore(std::string db_path)
    : db_path_(std::move(db_path)) {}

BackfillStore::~BackfillStore()
{
    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

void BackfillStore::open()
{
    if (sqlite3_open(db_path_.c_str(), &db_) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        spdlog::error("[BackfillStore] failed to open '{}' - {}", db_path_, err);
        throw std::runtime_error("BackfillStore: failed to open '" + db_path_ + "': " + err);
    }

    exec_or_throw(db_,
        "CREATE TABLE IF NOT EXISTS backfill_rows ("
        "  source_table TEXT NOT NULL,"
        "  row_id       TEXT NOT NULL,"
        "  row_data     TEXT NOT NULL,"
        "  status       TEXT NOT NULL DEFAULT 'pending',"
        "  PRIMARY KEY (source_table, row_id)"
        ")");

    exec_or_throw(db_,
        "CREATE TABLE IF NOT EXISTS backfill_tables ("
        "  source_table  TEXT PRIMARY KEY,"
        "  dump_complete INTEGER NOT NULL DEFAULT 0"
        ")");

    spdlog::debug("[BackfillStore] opened '{}'", db_path_);
}

void BackfillStore::insert_row(const std::string& source_table, const std::string& row_id,
                                const std::string& row_data_json)
{
    StmtGuard g;
    auto* stmt = prepare_or_throw(db_, g,
        "INSERT OR IGNORE INTO backfill_rows (source_table, row_id, row_data) VALUES (?, ?, ?)",
        "insert_row");
    sqlite3_bind_text(stmt, 1, source_table.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, row_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, row_data_json.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw std::runtime_error("BackfillStore: insert_row failed: " + std::string(sqlite3_errmsg(db_)));
    }
}

void BackfillStore::mark_table_dumped(const std::string& source_table)
{
    StmtGuard g;
    auto* stmt = prepare_or_throw(db_, g,
        "INSERT INTO backfill_tables (source_table, dump_complete) VALUES (?, 1) "
        "ON CONFLICT(source_table) DO UPDATE SET dump_complete = 1",
        "mark_table_dumped");
    sqlite3_bind_text(stmt, 1, source_table.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw std::runtime_error("BackfillStore: mark_table_dumped failed: " + std::string(sqlite3_errmsg(db_)));
    }
}

bool BackfillStore::is_table_dumped(const std::string& source_table) const
{
    StmtGuard g;
    auto* stmt = prepare_or_throw(db_, g,
        "SELECT dump_complete FROM backfill_tables WHERE source_table = ?",
        "is_table_dumped");
    sqlite3_bind_text(stmt, 1, source_table.c_str(), -1, SQLITE_TRANSIENT);

    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        return sqlite3_column_int(stmt, 0) != 0;
    }
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("BackfillStore: is_table_dumped failed: " + std::string(sqlite3_errmsg(db_)));
    }
    return false; // no row yet == not dumped
}

std::vector<BackfillStore::ClaimedRow> BackfillStore::claim_pending(size_t limit)
{
    StmtGuard g;
    auto* stmt = prepare_or_throw(db_, g,
        "UPDATE backfill_rows SET status = 'claimed' "
        "WHERE rowid IN (SELECT rowid FROM backfill_rows WHERE status = 'pending' LIMIT ?) "
        "RETURNING source_table, row_id, row_data",
        "claim_pending");
    sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(limit));

    std::vector<ClaimedRow> claimed;
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        ClaimedRow row;
        row.source_table = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        row.row_id       = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        row.row_data     = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        claimed.push_back(std::move(row));
    }
    if (rc != SQLITE_DONE) {
        throw std::runtime_error("BackfillStore: claim_pending failed: " + std::string(sqlite3_errmsg(db_)));
    }
    return claimed;
}

void BackfillStore::mark_done(const std::string& source_table, const std::string& row_id)
{
    StmtGuard g;
    auto* stmt = prepare_or_throw(db_, g,
        "DELETE FROM backfill_rows WHERE source_table = ? AND row_id = ?",
        "mark_done");
    sqlite3_bind_text(stmt, 1, source_table.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, row_id.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        throw std::runtime_error("BackfillStore: mark_done failed: " + std::string(sqlite3_errmsg(db_)));
    }
}

} // namespace pgcdc
