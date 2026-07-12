#include "wal_frame.hpp"

namespace pgcdc 
{

namespace 
{

uint64_t read_u64(const uint8_t* p) 
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<uint8_t>(p[i]);
    }
    return v;
}

} // namespace

std::optional<XLogDataHeader> parse_xlogdata_header(const char* buf, size_t len) 
{
    // type(1) + start_lsn(8) + end_lsn(8) + send_time(8) = 25 bytes minimum,
    // payload may legitimately be empty (len == 25).
    constexpr size_t headerLen = 1 + 8 + 8 + 8;
    if (buf == nullptr || len < headerLen) {
        return std::nullopt;
    }

    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf) + 1;

    XLogDataHeader header;
    header.wal_start  = read_u64(p);
    header.wal_end    = read_u64(p + 8);
    header.send_time  = read_u64(p + 16);
    header.payload    = reinterpret_cast<const uint8_t*>(buf) + headerLen;
    header.payload_len = len - headerLen;

    return header;
}

std::optional<KeepaliveMessage> parse_keepalive_message(const char* buf, size_t len) 
{
    // type(1) + walsender_lsn(8) + timestamp(8) + reply_requested(1) = 18 bytes.
    constexpr size_t keepaliveLen = 1 + 8 + 8 + 1;
    if (buf == nullptr || len < keepaliveLen) {
        return std::nullopt;
    }

    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf) + 1;

    KeepaliveMessage msg;
    msg.wal_end           = read_u64(p);
    msg.send_time         = read_u64(p + 8);
    msg.reply_requested   = static_cast<uint8_t>(buf[17]) != 0;

    return msg;
}

} // namespace pgcdc
