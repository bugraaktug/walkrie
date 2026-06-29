#pragma once

#include <string>

namespace bytes
{
    uint8_t read_u8(const uint8_t*& p);
    uint16_t read_u16(const uint8_t*& p);
    uint32_t read_u32(const uint8_t*& p);
    uint64_t read_u64(const uint8_t*& p); 
    std::string read_cstr(const uint8_t*& p);
}
