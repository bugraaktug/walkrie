#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>
#include "event.hpp"


namespace pgcdc 
{
// Parses a single pgoutput logical-replication message (the payload of a
// XLogData message, with the leading 'w' and LSN header already stripped by
// the caller). Maintains relation cache internally across calls.
//
// Returns std::nullopt for message types we don't need to surface as a
// ChangeEvent yet (Begin, Commit, Relation, Type, Origin, Truncate) — the
// caller should just continue reading. Relation messages still update
// internal state even though they return nullopt.
class PgOutputParser 
{
public:
    std::optional<ChangeEvent> parse(const uint8_t* data, size_t len);

private:
    std::unordered_map<uint32_t, RelationInfo> relations_;
    uint64_t pending_commit_timestamp_unix_us_ = 0;

    void handle_relation(const uint8_t* data, size_t len);
    void handle_begin(const uint8_t* data, size_t len);
    DecodedRow decode_tuple(const RelationInfo& rel, const uint8_t*& p, const uint8_t* end, TupleKind kind);
};

} // namespace pgcdc
