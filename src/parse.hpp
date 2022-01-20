#pragma once

#include <charconv>
#include <cstdint>

#include "constants.hpp"

constexpr inline unsigned char parse_hex_char(char c) {
    return (static_cast<int>('a' <= c && c <= 'z') * (c - 'a' + 10) +
            static_cast<int>('A' <= c && c <= 'Z') * (c - 'A' + 10) +
            static_cast<int>('0' <= c && c <= '9') * (c - '0'));
}

constexpr inline const char* parse_uint_str(const char* str, uint64_t* result) {
    if constexpr (use_std_from_chars) {
        return std::from_chars(str, str + 1024, *result).ptr;
    } else {
        *result = 0;
        while ('0' <= *str && *str <= '9') {
            *result = (*result * 10) + *str - '0';
            str++;
        }
        return str;
    }
}
