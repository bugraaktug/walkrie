#pragma once

#include <string>
#include <vector>

#include "backfill_store.hpp"
#include "config.hpp"
#include "event.hpp"

namespace pgcdc
{

// Owns backfill dump + live-event reconciliation for one source, kept out of PgReplicationSource itself
class BackfillUtil
{
public:
    BackfillUtil(std::string conninfo, std::vector<TableMapping> table_mappings, std::string store_path);

    void open(); // <<< creates parent dir + store schema if missing; throws on failure

    bool dump_all(); // <<< SELECT-per-mapped-table dump via a separate plain PGconn; skips already-dumped tables; false + last_error() on failure

    bool absorb_event(const ChangeEvent& event); // <<< Delete removes the row and always returns false (still forwarded to sinks); Update merges known columns into the stored row and returns true (suppress dispatch) iff the row was still pending; Insert/Commit/Truncate always return false

    std::string last_error() const { return last_error_; }

private:
    std::string conninfo_;
    std::vector<TableMapping> table_mappings_;
    std::string store_path_;
    BackfillStore store_;
    std::string last_error_;

    const TableMapping* find_mapping(const std::string& source_table) const;
    static std::string get_column(const DecodedRow& row, const std::string& col_name);
};

} // namespace pgcdc
