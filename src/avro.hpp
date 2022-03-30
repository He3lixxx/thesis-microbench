#pragma once

#include <cstddef>
#include <vector>
#include "bench.hpp"

// clang-format off
IMPL_VISIBILITY void serialize_avro(const NativeTuple& tup, std::vector<std::byte>* buf);
IMPL_VISIBILITY bool parse_avro(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;

extern template void generate_tuples<serialize_avro>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
extern template void parse_tuples<parse_avro>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
