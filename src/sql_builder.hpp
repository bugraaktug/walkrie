#pragma once
#include <string>
#include "config.hpp"

namespace pgcdc
{

// Builds the parameterized SQL statements PgEmbeddingSink executes.
// Parameter order the caller must bind for each:
//   build_upsert_sql:   id, [discriminator], embed_text, metadata..., vector
//   build_delete_sql:   id, [discriminator]
//   build_truncate_sql: [discriminator] only — no id, since TRUNCATE carries
//                       no rows; with no discriminator configured, the
//                       statement has no WHERE clause and deletes every row
//                       in the sink table.
class ISqlBuilder
{
public:
    virtual ~ISqlBuilder() = default;

    virtual std::string build_upsert_sql(const TableMapping& tm) const = 0;
    virtual std::string build_delete_sql(const TableMapping& tm) const = 0;
    virtual std::string build_truncate_sql(const TableMapping& tm) const = 0;
};

} // namespace pgcdc
