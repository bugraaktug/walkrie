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
    for (const auto& tm : table_mappings_) {
        if (store_.is_table_dumped(tm.source_table)) continue; // <<< already dumped in a prior (possibly crashed) run

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

        store_.mark_table_dumped(tm.source_table);
        spdlog::info("[BackfillUtil] dumped {} row(s) for '{}'", nrows, tm.source_table);
    }

    PQfinish(conn);
    return ok;
}

bool BackfillUtil::absorb_event(const ChangeEvent& event)
{
    if (event.op != ChangeEvent::Op::Update && event.op != ChangeEvent::Op::Delete) return false; // <<< Insert can't match a dumped row — dump only captures pre-existing rows

    const TableMapping* tm = find_mapping(event.table_name);
    if (!tm) return false;

    if (event.op == ChangeEvent::Op::Delete) {
        if (event.old_row) {
            std::string row_id = get_column(*event.old_row, tm->id_source_); // <<< id is always present under REPLICA IDENTITY DEFAULT
            if (!row_id.empty()) store_.mark_done(tm->source_table, row_id);
        }
        return false; // <<< remove() is a safe no-op on a row never upserted — always let Delete reach sinks too
    }

    if (!event.new_row) return false;
    std::string row_id = get_column(*event.new_row, tm->id_source_);
    if (row_id.empty()) return false;

    auto existing = store_.get_row_data(tm->source_table, row_id);
    if (!existing) return false; // <<< not a backfill row (or already drained) — dispatch normally

    ordered_json merged = ordered_json::parse(*existing);
    merge_known_columns(merged, *event.new_row, *tm);
    store_.update_row_data(tm->source_table, row_id, merged.dump());
    return true; // <<< backfill now owns writing this row; suppress live dispatch
}

} // namespace pgcdc
