#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "bench.hpp"

// clang-format off
IMPL_VISIBILITY void serialize_flatbuffer(const NativeTuple& tup, std::vector<std::byte>* buf);
IMPL_VISIBILITY bool parse_flatbuffer(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup);

extern template void generate_tuples<serialize_flatbuffer>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
extern template void parse_tuples<parse_flatbuffer>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
