#pragma once

#include <optional>
#include <string>
#include <vector>

struct sqlite3;

namespace pgcdc
{
// Local durable staging store for the initial backfill scan (one SQLite file
// per source). Deliberately config/TableMapping-agnostic: callers extract required data
class BackfillStore
{
public:
    explicit BackfillStore(std::string db_path);
    ~BackfillStore();

    BackfillStore(const BackfillStore&) = delete;
    BackfillStore& operator=(const BackfillStore&) = delete;

    void open();
    void reset(); // <<< clears all rows + dump_complete markers — call when the underlying slot is freshly (re)created, discarding any prior epoch's state

    void insert_row(const std::string& source_table, const std::string& row_id,
                    const std::string& row_data_json); // INSERT OR IGNORE — idempotent re-dump
    void mark_table_dumped(const std::string& source_table);
    bool is_table_dumped(const std::string& source_table) const;

    bool has_pending() const; // <<< true if any row is still 'pending' — used to decide whether to spawn a drain worker at all

    std::optional<std::string> get_row_data(const std::string& source_table, const std::string& row_id) const; // <<< nullopt if not present
    bool update_row_data(const std::string& source_table, const std::string& row_id, const std::string& row_data_json); // <<< no-op, returns false if not present

    struct ClaimedRow
    {
        std::string source_table;
        std::string row_id;
        std::string row_data;
    };
    std::vector<ClaimedRow> claim_pending(size_t limit); // atomic: pending -> claimed
    void mark_done(const std::string& source_table, const std::string& row_id); // removes the row

private:
    std::string db_path_;
    sqlite3* db_ = nullptr;
};

} // namespace pgcdc
