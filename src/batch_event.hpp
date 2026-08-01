#pragma once
#include <string>
#include <vector>

#include "config.hpp"

namespace pgcdc
{

struct BatchEvent
{
    enum class Kind { Upsert, Delete, Truncate };

    Kind kind = Kind::Upsert;
    const TableMapping* tm = nullptr;
    std::string id_val;
    std::string embed_val;              // only meaningful for Kind::Upsert
    std::vector<std::string> meta_vals; // only meaningful for Kind::Upsert
};

using Batch = std::vector<BatchEvent>;

} // namespace pgcdc
