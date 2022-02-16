#pragma once
#include <cstdint>

#define FIND_IS_CONSTEXPR false

#if 1
constexpr uint64_t generate_chunk_size = 1024;
constexpr bool debug_output = false;
#else
constexpr uint64_t generate_chunk_size = 1;
constexpr bool debug_output = true;
#endif

constexpr bool use_std_from_chars = false;
