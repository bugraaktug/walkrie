#include "byte_utils.hpp"

#include <cstring>
#include <stdexcept>

namespace bytes
{
// pgoutput integers are big-endian (network byte order). These helpers read
// and advance a cursor pointer, so call sites stay readable as a sequence of
// "read the next field" statements instead of manual offset arithmetic.

uint8_t read_u8(const uint8_t*& p) 
{
    return *p++;
}

uint16_t read_u16(const uint8_t*& p) 
{
    uint16_t v;
    std::memcpy(&v, p, 2);
    p += 2;
    return (static_cast<uint16_t>(v & 0xFF) << 8) | (v >> 8);
}

uint32_t read_u32(const uint8_t*& p) 
{
    uint32_t v = (static_cast<uint32_t>(p[0]) << 24) |
                 (static_cast<uint32_t>(p[1]) << 16) |
                 (static_cast<uint32_t>(p[2]) << 8) |
                  static_cast<uint32_t>(p[3]);
    p += 4;
    return v;
}

uint64_t read_u64(const uint8_t*& p) 
{
    uint64_t hi = read_u32(p);
    uint64_t lo = read_u32(p);
    return (hi << 32) | lo;
}

// Reads a null-terminated C string (used for relation/column names).
std::string read_cstr(const uint8_t*& p) 
{
    const char* start = reinterpret_cast<const char*>(p);
    std::string s(start);
    p += s.size() + 1;
    return s;
}

}

