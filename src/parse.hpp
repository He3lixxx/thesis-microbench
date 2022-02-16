#pragma once

#include <charconv>
#include <cstdint>

#include "constants.hpp"

constexpr inline bool is_hex_char(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9');
}

constexpr inline unsigned char parse_hex_char(char c) {
    return (static_cast<int>('a' <= c && c <= 'z') * (c - 'a' + 10) +
            static_cast<int>('A' <= c && c <= 'Z') * (c - 'A' + 10) +
            static_cast<int>('0' <= c && c <= '9') * (c - '0'));
}

constexpr inline std::from_chars_result parse_uint_str(const char* str,
                                                       const char* str_end,
                                                       uint64_t* result) {
    if constexpr (use_std_from_chars) {
        return std::from_chars(str, str_end, *result);
    } else {
        if (*str < '0' || *str > '9') {
            return {nullptr, std::errc::invalid_argument};
        }

        *result = 0;
        while (str != str_end && '0' <= *str && *str <= '9') {
            *result = (*result * 10) + *str - '0';
            str++;
        }
        return {str, std::errc()};
    }
}
