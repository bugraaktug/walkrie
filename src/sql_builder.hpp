#pragma once
#include <string>
#include "config.hpp"

namespace pgcdc
{

class ISqlBuilder
{
public:
    virtual ~ISqlBuilder() = default;

    // Parameterized INSERT ... ON CONFLICT ... DO UPDATE statement.
    // Parameter order the caller must bind: id, [discriminator], embed_text,
    // metadata..., vector.
    virtual std::string build_upsert_sql(const TableMapping& tm) const = 0;

    // Parameterized DELETE statement.
    // Parameter order the caller must bind: id, [discriminator].
    virtual std::string build_delete_sql(const TableMapping& tm) const = 0;
};

} // namespace pgcdc
