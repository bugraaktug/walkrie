#pragma once
#include "sql_builder.hpp"

namespace pgcdc
{

class PgSqlBuilder : public ISqlBuilder
{
public:
    PgSqlBuilder(std::string sink_table, std::string sink_column);

    std::string build_upsert_sql(const TableMapping& tm) const override;
    std::string build_delete_sql(const TableMapping& tm) const override;

private:
    std::string sink_table_;
    std::string sink_column_;
};

} // namespace pgcdc
