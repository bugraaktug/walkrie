#include "pg_sql_builder.hpp"
#include <sstream>

namespace pgcdc
{

PgSqlBuilder::PgSqlBuilder(std::string sink_table, std::string sink_column)
    : sink_table_(std::move(sink_table))
    , sink_column_(std::move(sink_column)) {}

std::string PgSqlBuilder::build_upsert_sql(const TableMapping& tm) const
{
    std::vector<std::string> sink_cols;
    std::vector<std::string> update_cols;

    // $1 = id
    sink_cols.push_back(tm.id_sink_);

    // $2 = discriminator, only if configured — part of the conflict key,
    // never updated (same treatment as id).
    if (tm.has_discriminator_) {
        sink_cols.push_back(tm.discriminator_sink_);
    }

    // embed text
    sink_cols.push_back(tm.embed_sink_);
    update_cols.push_back(tm.embed_sink_);

    // metadata columns
    for (const auto& m : tm.columns) {
        if (m.role == "metadata") {
            sink_cols.push_back(m.sink_column);
            update_cols.push_back(m.sink_column);
        }
    }

    // vector column, last param
    sink_cols.push_back(sink_column_);
    update_cols.push_back(sink_column_);

    std::ostringstream col_list, val_list, update_list;
    for (size_t i = 0; i < sink_cols.size(); ++i) {
        if (i > 0) { col_list << ", "; val_list << ", "; }
        col_list << sink_cols[i];
        if (sink_cols[i] == sink_column_) {
            val_list << "$" << (i + 1) << "::vector";
        } else {
            val_list << "$" << (i + 1);
        }
    }
    for (size_t i = 0; i < update_cols.size(); ++i) {
        if (i > 0) update_list << ", ";
        update_list << update_cols[i] << " = EXCLUDED." << update_cols[i];
    }

    std::ostringstream sql;
    sql << "INSERT INTO " << sink_table_
        << " (" << col_list.str() << ")"
        << " VALUES (" << val_list.str() << ")"
        << " ON CONFLICT (" << tm.id_sink_;
    if (tm.has_discriminator_) {
        sql << ", " << tm.discriminator_sink_;
    }
    sql << ") DO UPDATE SET " << update_list.str();

    return sql.str();
}

std::string PgSqlBuilder::build_delete_sql(const TableMapping& tm) const
{
    std::ostringstream sql;
    sql << "DELETE FROM " << sink_table_ << " WHERE " << tm.id_sink_ << " = $1";
    if (tm.has_discriminator_) {
        sql << " AND " << tm.discriminator_sink_ << " = $2";
    }
    return sql.str();
}

} // namespace pgcdc
