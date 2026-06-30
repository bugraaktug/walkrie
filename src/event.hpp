#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

using ordered_json = nlohmann::ordered_json;

namespace pgcdc
{

struct ColumnValue
{
    std::string name;
    bool is_null = false;
    bool is_unchanged_toast = false; //
    std::string text_value; // empty if is_null or is_unchanged_toast
};

enum class TupleKind
{
    Insert,
    UpdateOld,
    UpdateNew,
    Delete
};

struct DecodedRow
{
    std::string schema_name;
    std::string table_name;
    TupleKind kind;
    std::vector<ColumnValue> columns;

    friend void to_json(ordered_json& j, const DecodedRow& row) {
        for (size_t i = 0; i < row.columns.size(); ++i) {
	        const auto& col = row.columns[i];
	        if (col.is_null) {
		        j[col.name] = "null";
	        } else if (col.is_unchanged_toast) {
		        j[col.name] = "__unchanged_toast__";
	        } else {
		        j[col.name] = col.text_value;
	        }
	    }
    }
};

// One decoded transaction-level event. We emit Insert/Delete as a single row,
// and Update as a pair (old + new) so the caller (week 2) can diff them.
struct ChangeEvent
{
    enum class Op { Insert, Update, Delete } op;
    std::string schema_name;
    std::string table_name;
    std::optional<DecodedRow> old_row; // present for Update (if REPLICA IDENTITY FULL) and Delete
    std::optional<DecodedRow> new_row; // present for Insert and Update
    uint64_t commit_lsn = 0; // the LSN to acknowledge once this event is durably handled

    static std::string to_string(const Op& k)
    {
        switch (k) {
            case Op::Insert: return "insert";
            case Op::Update: return "update";
            case Op::Delete: return "delete";
        }
	    return "unhandled";
    }

    friend void to_json(ordered_json& j, const ChangeEvent& e) {
        j = ordered_json{
            {"op", to_string(e.op)},
	        {"schema", e.schema_name},
	        {"table", e.table_name},
	        {"lsn", e.commit_lsn}
        };
        
        if (e.old_row) {
            j["old"] = *e.old_row;
	    }
        
        if (e.new_row) {
	        j["new"] = *e.new_row;
        }

    }
};

// Relation (table schema) metadata, sent by Postgres before the first change
// for a table, and cached by relation OID for decoding subsequent
// Insert/Update/Delete messages, which only carry column *values*, not names.
struct RelationInfo 
{
    uint32_t relation_id = 0;
    std::string schema_name;
    std::string table_name;
    std::vector<std::string> column_names;
};

} //namespace
