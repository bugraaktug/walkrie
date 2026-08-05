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

    void open();
    void set_table_mappings(std::vector<TableMapping> table_mappings); // <<< replaces the mapping scope, e.g. after the caller narrows it to this source's own publication
    void reset();
    void reset_stale_claims(); // <<< call on every resumed connect() — see BackfillStore::reset_stale_claims()
    bool has_prior_dump_state() const; // <<< true if dump_all() was ever invoked before for this store — lets a resumed slot finish an interrupted dump without re-scanning a slot that just had backfill flipped on
    bool dump_all();

    bool absorb_event(const ChangeEvent& event); // <<< Delete removes the row and always returns false (still forwarded to sinks); 
                                                 // Update merges known columns into the stored row and returns true (suppress dispatch) iff the row was still pending; Insert/Commit/Truncate always return false
    bool has_pending_work() const;              // <<< true if the store still has rows to drain — caller uses this to decide whether to spawn a walkrie_worker at all
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
