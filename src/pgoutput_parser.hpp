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
// Returns std::nullopt for message types that never surface a ChangeEvent
// (Begin, Commit, Relation, Type, Origin) — the caller should just continue
// reading. Relation messages still update internal state even though they
// return nullopt.
//
// For message types that do surface events, the result is a (possibly
// empty) vector rather than a single event: Insert/Update/Delete always
// yield exactly one, but a Truncate message can name several relations at
// once (e.g. via CASCADE), and may yield zero if none of them match a
// relation we've cached (relation unknown => skipped with a warning, not
// treated as "no event produced at all").
class PgOutputParser
{
public:
    std::optional<std::vector<ChangeEvent>> parse(const uint8_t* data, size_t len);

private:
    std::unordered_map<uint32_t, RelationInfo> relations_;
    uint64_t pending_commit_timestamp_unix_us_ = 0;

    void handle_relation(const uint8_t* data, size_t len);
    void handle_begin(const uint8_t* data, size_t len);
    std::vector<ChangeEvent> handle_truncate(const uint8_t* data, size_t len);
    DecodedRow decode_tuple(const RelationInfo& rel, const uint8_t*& p, const uint8_t* end, TupleKind kind);
};

} // namespace pgcdc
