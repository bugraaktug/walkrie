#include "pgoutput_parser.hpp"
#include "byte_utils.hpp"

#include <cstring>
#include <stdexcept>
#include <spdlog/spdlog.h>

using namespace bytes;

namespace pgcdc 
{

namespace 
{

constexpr uint64_t kPgEpochOffsetUs = 946684800ULL * 1'000'000ULL; // 2000-01-01 vs Unix epoch, in microseconds

uint64_t read_u64(const uint8_t* p) 
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(p[i]);
    return v;
}

} // namespace

void PgOutputParser::handle_relation(const uint8_t* data, size_t len) 
{
    const uint8_t* p = data;
    const uint8_t* end = data + len;
    (void)end; // bounds-checking omitted in this sketch; see README "hardening" note

    RelationInfo rel;
    rel.relation_id = read_u32(p);
    rel.schema_name = read_cstr(p);
    rel.table_name = read_cstr(p);

    uint8_t replica_identity = read_u8(p); // 'd'/'n'/'f'/'i' — unused for now
    (void)replica_identity;

    uint16_t num_columns = read_u16(p);
    rel.column_names.reserve(num_columns);

    for (uint16_t i = 0; i < num_columns; ++i) {
        uint8_t flags = read_u8(p); // bit 1 = part of key; unused for now
        (void)flags;
        std::string col_name = read_cstr(p);
        /* uint32_t type_oid = */ read_u32(p);
        /* int32_t type_modifier = */ read_u32(p);
        rel.column_names.push_back(std::move(col_name));
    }

    relations_[rel.relation_id] = std::move(rel);
}

void PgOutputParser::handle_begin(const uint8_t* data, size_t len)
{
    // type(1) + final_lsn(8) + commit_timestamp(8) + xid(4) = 21 bytes
    if (len < 20) {
        spdlog::warn("[PgOutputParser] truncated Begin message ({} bytes)", len);
        return;
    }
    uint64_t pg_epoch_us = read_u64(data + 8); // skip type(1) + final_lsn(8)
    pending_commit_timestamp_unix_us_ = pg_epoch_us + kPgEpochOffsetUs;
}

std::vector<ChangeEvent> PgOutputParser::handle_truncate(const uint8_t* data, size_t len)
{
    const uint8_t* p = data;
    const uint8_t* end = data + len;
    (void)end; // bounds-checking omitted in this sketch; see README "hardening" note

    uint32_t num_relations = read_u32(p);
    uint8_t flags = read_u8(p); // bit 1 = CASCADE, bit 2 = RESTART IDENTITY — unused for now
    (void)flags;

    std::vector<ChangeEvent> events;
    events.reserve(num_relations);

    for (uint32_t i = 0; i < num_relations; ++i) {
        uint32_t rel_id = read_u32(p);
        auto it = relations_.find(rel_id);
        if (it == relations_.end()) {
            // Unlike Insert/Update/Delete, a missing relation here doesn't
            // invalidate the rest of the message — other relation_ids in
            // the same Truncate can still be resolved and emitted.
            spdlog::warn("[PgOutputParser] Truncate for unknown relation id {} (missing Relation message) — skipping", rel_id);
            continue;
        }

        ChangeEvent ev;
        ev.op = ChangeEvent::Op::Truncate;
        ev.schema_name = it->second.schema_name;
        ev.table_name = it->second.table_name;
        ev.commit_timestamp = pending_commit_timestamp_unix_us_;
        events.push_back(std::move(ev));
    }

    return events;
}

DecodedRow PgOutputParser::decode_tuple(const RelationInfo& rel, const uint8_t*& p, const uint8_t* end, TupleKind kind)
{
    (void)end;
    DecodedRow row;
    row.schema_name = rel.schema_name;
    row.table_name = rel.table_name;
    row.kind = kind;

    uint16_t num_columns = read_u16(p); // matches rel.column_names.size()

    for (uint16_t i = 0; i < num_columns; ++i) {
        ColumnValue cv;
        cv.name = (i < rel.column_names.size()) ? rel.column_names[i] : "";

        uint8_t kind_byte = read_u8(p); // 'n' = null, 'u' = unchanged toast, 't' = text data follows
        if (kind_byte == 'n') {
            cv.is_null = true;
        } else if (kind_byte == 'u') {
            cv.is_unchanged_toast = true;
        } else if (kind_byte == 't') {
            uint32_t value_len = read_u32(p);
            cv.text_value.assign(reinterpret_cast<const char*>(p), value_len);
            p += value_len;
        } else {
            throw std::runtime_error("pgoutput: unexpected tuple column kind byte");
        }

        row.columns.push_back(std::move(cv));
    }

    return row;
}

std::optional<std::vector<ChangeEvent>> PgOutputParser::parse(const uint8_t* data, size_t len)
{
    if (len == 0) return std::nullopt;

    const uint8_t* p = data;
    const uint8_t* end = data + len;
    uint8_t msg_type = read_u8(p);

    switch (msg_type) {
        case 'B': // Begin — carries final LSN + commit timestamp + xid.
            handle_begin(p, static_cast<size_t>(end -p));
            return std::nullopt;

        case 'C': { // Commit
            // <<< emitted so its LSN (the transaction's real commit boundary,
            // stamped in by the caller from the XLogData header, same as every
            // other event) reaches EventDispatcher's confirm tracking; sinks
            // that don't care about transaction boundaries should skip Op::Commit
            ChangeEvent ev;
            ev.op = ChangeEvent::Op::Commit;
            ev.commit_timestamp = pending_commit_timestamp_unix_us_;
            return std::vector<ChangeEvent>{std::move(ev)};
        }

        case 'R': // Relation
            handle_relation(p, static_cast<size_t>(end - p));
            return std::nullopt;

        case 'Y': // Type — type OID -> name mapping for composite/enum/etc.
            return std::nullopt; // not needed for week 1 (we treat everything as text)

        case 'O': // Origin
            return std::nullopt;

        case 'T': // Truncate
            return handle_truncate(p, static_cast<size_t>(end - p));

        case 'I': { // Insert
            uint32_t rel_id = read_u32(p);
            auto it = relations_.find(rel_id);
            if (it == relations_.end()) {
                throw std::runtime_error("pgoutput: Insert for unknown relation (missing Relation message)");
            }
            uint8_t tuple_tag = read_u8(p); // 'N' = new tuple follows
            (void)tuple_tag;
            DecodedRow new_row = decode_tuple(it->second, p, end, TupleKind::Insert);

            ChangeEvent ev;
            ev.op = ChangeEvent::Op::Insert;
            ev.schema_name = it->second.schema_name;
            ev.table_name = it->second.table_name;
            ev.new_row = std::move(new_row);
            ev.commit_timestamp = pending_commit_timestamp_unix_us_;
            return std::vector<ChangeEvent>{std::move(ev)};
        }

        case 'U': { // Update
            uint32_t rel_id = read_u32(p);
            auto it = relations_.find(rel_id);
            if (it == relations_.end()) {
                throw std::runtime_error("pgoutput: Update for unknown relation (missing Relation message)");
            }

            ChangeEvent ev;
            ev.op = ChangeEvent::Op::Update;
            ev.schema_name = it->second.schema_name;
            ev.table_name = it->second.table_name;
            ev.commit_timestamp = pending_commit_timestamp_unix_us_;

            uint8_t tag = read_u8(p);
            // 'K' = old tuple is key-only (REPLICA IDENTITY DEFAULT, no real change in non-key cols sent)
            // 'O' = old tuple is full old row (REPLICA IDENTITY FULL — what setup.sql configures)
            // 'N' = new tuple (always present)
            if (tag == 'K' || tag == 'O') {
                ev.old_row = decode_tuple(it->second, p, end, TupleKind::UpdateOld);
                tag = read_u8(p); // consume the following 'N' tag
            }
            if (tag == 'N') {
                ev.new_row = decode_tuple(it->second, p, end, TupleKind::UpdateNew);
            }
            return std::vector<ChangeEvent>{std::move(ev)};
        }

        case 'D': { // Delete
            uint32_t rel_id = read_u32(p);
            auto it = relations_.find(rel_id);
            if (it == relations_.end()) {
                throw std::runtime_error("pgoutput: Delete for unknown relation (missing Relation message)");
            }

            ChangeEvent ev;
            ev.op = ChangeEvent::Op::Delete;
            ev.schema_name = it->second.schema_name;
            ev.table_name = it->second.table_name;
            ev.commit_timestamp = pending_commit_timestamp_unix_us_;

            uint8_t tag = read_u8(p); // 'K' (key-only) or 'O' (full old row, our setup)
            ev.old_row = decode_tuple(it->second, p, end, TupleKind::Delete);
            (void)tag;
            return std::vector<ChangeEvent>{std::move(ev)};
        }

        default:
            // Unknown/unhandled message type. Don't crash the daemon over a
            // message kind we haven't modeled yet — log and skip.
            return std::nullopt;
    }
}

} // namespace pgcdc
