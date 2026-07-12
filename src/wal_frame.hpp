#pragma once

#include <cstdint>
#include <cstddef>
#include <optional>

namespace pgcdc {

// Parsed contents of an XLogData ('w') CopyData message.
// Layout on the wire: type(1) + start_lsn(8) + end_lsn(8) + send_time(8) + payload
struct XLogDataHeader {
    uint64_t wal_start;
    uint64_t wal_end;
    uint64_t send_time;
    const uint8_t* payload;
    size_t payload_len;
};

// Parsed contents of a Primary keepalive ('k') CopyData message.
// Layout on the wire: type(1) + walsender_lsn(8) + timestamp(8) + reply_requested(1)
struct KeepaliveMessage {
    uint64_t wal_end;
    uint64_t send_time;
    bool reply_requested;
};

// Parses an XLogData message
std::optional<XLogDataHeader> parse_xlogdata_header(const char* buf, size_t len);
std::optional<KeepaliveMessage> parse_keepalive_message(const char* buf, size_t len);

} // namespace pgcdc
