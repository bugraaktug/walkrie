#include "doctest.h"
#include "wal_frame.hpp"

#include <vector>
#include <cstring>

namespace 
{

void push_u64(std::vector<uint8_t>& buf, uint64_t v) 
{
    for (int i = 7; i >= 0; --i) {
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    }
}

} // namespace

TEST_SUITE("parse_wal_xlogdata_header") 
{

    TEST_CASE("extracts lsn fields and payload correctly") 
    {
        std::vector<uint8_t> buf;
        buf.push_back('w');

        uint64_t wal_start = 0x0000000123456789ULL;
        uint64_t wal_end    = 0x00000001234567FFULL;
        uint64_t send_time  = 0x000000AABBCCDDEEULL;
        push_u64(buf, wal_start);
        push_u64(buf, wal_end);
        push_u64(buf, send_time);

        std::vector<uint8_t> payload = {'I', 0x01, 0x02, 0x03};
        buf.insert(buf.end(), payload.begin(), payload.end());

        auto header = pgcdc::parse_xlogdata_header(reinterpret_cast<const char*>(buf.data()), buf.size());

        REQUIRE(header.has_value());
        CHECK(header->wal_start == wal_start);
        CHECK(header->wal_end == wal_end);
        CHECK(header->send_time == send_time);
        CHECK(header->payload_len == payload.size());
        CHECK(std::memcmp(header->payload, payload.data(), payload.size()) == 0);
    }

    TEST_CASE("accepts empty payload (25-byte message)") 
    {
        std::vector<uint8_t> buf;
        buf.push_back('w');
        push_u64(buf, 1);
        push_u64(buf, 2);
        push_u64(buf, 3);

        auto header = pgcdc::parse_xlogdata_header(reinterpret_cast<const char*>(buf.data()), buf.size());

        REQUIRE(header.has_value());
        CHECK(header->payload_len == 0);
    }

    TEST_CASE("rejects truncated buffer") 
    {
        std::vector<uint8_t> buf = {'w', 0x01, 0x02};
        auto header = pgcdc::parse_xlogdata_header(reinterpret_cast<const char*>(buf.data()), buf.size());

        CHECK(!header.has_value());
    }

    TEST_CASE("rejects null buffer") 
    {
        auto header = pgcdc::parse_xlogdata_header(nullptr, 0);
        CHECK(!header.has_value());
    }
}

TEST_SUITE("parse_wal_keepalive_message") 
{

    TEST_CASE("parses reply_requested = true") 
    {
        std::vector<uint8_t> buf;
        buf.push_back('k');
        push_u64(buf, 0x1122334455667788ULL);
        push_u64(buf, 0x99AABBCCDDEEFF00ULL);
        buf.push_back(1);

        auto msg = pgcdc::parse_keepalive_message(reinterpret_cast<const char*>(buf.data()), buf.size());

        REQUIRE(msg.has_value());
        CHECK(msg->wal_end == 0x1122334455667788ULL);
        CHECK(msg->send_time == 0x99AABBCCDDEEFF00ULL);
        CHECK(msg->reply_requested == true);
    }

    TEST_CASE("parses reply_requested = false") 
    {
        std::vector<uint8_t> buf;
        buf.push_back('k');
        push_u64(buf, 1);
        push_u64(buf, 2);
        buf.push_back(0);

        auto msg = pgcdc::parse_keepalive_message(reinterpret_cast<const char*>(buf.data()), buf.size());

        REQUIRE(msg.has_value());
        CHECK(msg->reply_requested == false);
    }

    TEST_CASE("rejects truncated buffer") 
    {
        std::vector<uint8_t> buf = {'k', 0x01, 0x02};
        auto msg = pgcdc::parse_keepalive_message(reinterpret_cast<const char*>(buf.data()), buf.size());

        CHECK(!msg.has_value());
    }
    
    TEST_CASE("regression: reply_requested is not confused with low byte of timestamp") 
    {
        std::vector<uint8_t> buf;
        buf.push_back('k');
        push_u64(buf, 1);
        push_u64(buf, 0x99AABBCCDDEEFF00ULL);  // low byte is 0x00 — would look "false" if misread
        buf.push_back(1);  // reply_requested = true, at the correct offset (17)

        auto msg = pgcdc::parse_keepalive_message(reinterpret_cast<const char*>(buf.data()), buf.size());

        REQUIRE(msg.has_value());
        CHECK(msg->reply_requested == true);
    }
}
