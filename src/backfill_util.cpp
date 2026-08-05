#include "backfill_util.hpp"

#include <filesystem>
#include <sstream>

#include <libpq-fe.h>
#include <spdlog/spdlog.h>

namespace pgcdc
{

BackfillUtil::BackfillUtil(std::string conninfo, std::vector<TableMapping> table_mappings, std::string store_path)
    : conninfo_(std::move(conninfo))
    , table_mappings_(std::move(table_mappings))
    , store_path_(std::move(store_path))
    , store_(store_path_) {}

void BackfillUtil::open()
{
    std::filesystem::path p(store_path_);
    if (p.has_parent_path()) {
        std::filesystem::create_directories(p.parent_path());
    }
    store_.open();
}

void BackfillUtil::set_table_mappings(std::vector<TableMapping> table_mappings)
{
    table_mappings_ = std::move(table_mappings);
}

void BackfillUtil::reset()
{
    store_.reset();
}

void BackfillUtil::reset_stale_claims()
{
    store_.reset_stale_claims();
}

bool BackfillUtil::has_prior_dump_state() const
{
    return store_.has_any_dump_state();
}

const TableMapping* BackfillUtil::find_mapping(const std::string& source_table) const
{
    for (const auto& tm : table_mappings_) {
        if (tm.source_table == source_table) return &tm;
    }
    return nullptr;
}

std::string BackfillUtil::get_column(const DecodedRow& row, const std::string& col_name)
{
    for (const auto& col : row.columns) {
        if (col.name == col_name) {
            return (col.is_null || col.is_unchanged_toast) ? "" : col.text_value;
        }
    }
    return "";
}

namespace
{
// Overwrites j[col_name] from row if the column is known (present, not unchanged-toast); leaves j untouched otherwise, so a prior merge's value survives
void set_json_column(ordered_json& j, const DecodedRow& row, const std::string& col_name)
{
    for (const auto& col : row.columns) {
        if (col.name != col_name) continue;
        if (col.is_unchanged_toast) return;
        j[col_name] = col.is_null ? ordered_json(nullptr) : ordered_json(col.text_value);
        return;
    }
}

void merge_known_columns(ordered_json& merged, const DecodedRow& row, const TableMapping& tm)
{
    set_json_column(merged, row, tm.id_source_);
    set_json_column(merged, row, tm.embed_source_);
    for (const auto& cm : tm.columns) {
        if (cm.role == "metadata") set_json_column(merged, row, cm.source_column);
    }
}
} // namespace

bool BackfillUtil::dump_all()
{
    PGconn* conn = PQconnectdb(conninfo_.c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        last_error_ = PQerrorMessage(conn);
        PQfinish(conn);
        return false;
    }

    bool ok = true;
    for (const auto& tm : table_mappings_) { // <<< caller narrows this to the source's own publication via set_table_mappings() before calling
        if (store_.is_table_dumped(tm.source_table)) {
            spdlog::trace("[BackfillUtil] '{}' already dumped, skipping", tm.source_table);
            continue; // <<< already dumped in a prior (possibly crashed) run
        }

        int staged = store_.count_rows_for_table(tm.source_table);
        if (staged > 0) {
            spdlog::info("[BackfillUtil] resuming interrupted dump for '{}' — {} row(s) already staged from a prior attempt", tm.source_table, staged);
        }
        store_.mark_table_dump_started(tm.source_table); // <<< marker exists even if we crash before finishing this table's SELECT

        std::vector<std::string> select_cols = {tm.id_source_, tm.embed_source_};
        for (const auto& cm : tm.columns) {
            if (cm.role == "metadata") select_cols.push_back(cm.source_column);
        }

        std::ostringstream sql;
        sql << "SELECT ";
        for (size_t i = 0; i < select_cols.size(); ++i) {
            if (i > 0) sql << ", ";
            sql << select_cols[i];
        }
        sql << " FROM " << tm.source_table;

        PGresult* res = PQexec(conn, sql.str().c_str());
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            last_error_ = "backfill dump failed for '" + tm.source_table + "': " + PQerrorMessage(conn);
            PQclear(res);
            ok = false;
            break;
        }

        int nrows = PQntuples(res);
        for (int r = 0; r < nrows; ++r) {
            ordered_json row_json;
            std::string row_id;
            for (int c = 0; c < PQnfields(res); ++c) {
                const std::string& col = select_cols[static_cast<size_t>(c)];
                if (PQgetisnull(res, r, c)) {
                    row_json[col] = nullptr;
                } else {
                    std::string value = PQgetvalue(res, r, c);
                    row_json[col] = value;
                    if (col == tm.id_source_) row_id = value;
                }
            }
            if (row_id.empty()) {
                spdlog::warn("[BackfillUtil] row with empty/null id skipped - {}", tm.source_table);
                continue;
            }
            store_.insert_row(tm.source_table, row_id, row_json.dump());
        }
        PQclear(res);

        store_.mark_table_dumped(tm.source_table, nrows);
        spdlog::info("[BackfillUtil] dumped {} row(s) for '{}'", nrows, tm.source_table);
    }

    PQfinish(conn);
    return ok;
}

bool BackfillUtil::has_pending_work() const
{
    return store_.has_pending();
}

bool BackfillUtil::absorb_event(const ChangeEvent& event)
{
    if (event.op != ChangeEvent::Op::Update && event.op != ChangeEvent::Op::Delete) return false; // <<< Insert can't match a dumped row — dump only captures pre-existing rows

    const TableMapping* tm = find_mapping(event.table_name);
    if (!tm) return false; // <<< not a backfill-mapped table — no trace, would fire on every unrelated live event

    if (event.op == ChangeEvent::Op::Delete) {
        if (event.old_row) {
            std::string row_id = get_column(*event.old_row, tm->id_source_); // <<< id is always present under REPLICA IDENTITY DEFAULT
            if (!row_id.empty()) {
                spdlog::trace("[BackfillUtil] Delete for '{}' id={} — removing from store if present", tm->source_table, row_id);
                store_.mark_done(tm->source_table, row_id);
            }
        }
        return false; // <<< remove() is a safe no-op on a row never upserted — always let Delete reach sinks too
    }

    if (!event.new_row) return false;
    std::string row_id = get_column(*event.new_row, tm->id_source_);
    if (row_id.empty()) return false;

    auto existing = store_.get_row_data(tm->source_table, row_id);
    if (!existing) {
        spdlog::trace("[BackfillUtil] Update for '{}' id={} — not in store, dispatching normally", tm->source_table, row_id);
        return false; // <<< not a backfill row (or already drained) — dispatch normally
    }

    ordered_json merged = ordered_json::parse(*existing);
    merge_known_columns(merged, *event.new_row, *tm);
    store_.update_row_data(tm->source_table, row_id, merged.dump());
    spdlog::trace("[BackfillUtil] Update for '{}' id={} — merged into pending row, suppressing dispatch", tm->source_table, row_id);
    return true; // <<< backfill now owns writing this row; suppress live dispatch
}

} // namespace pgcdc
