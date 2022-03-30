#pragma once

#include <cstddef>
#include <vector>
#include "bench.hpp"

// clang-format off
IMPL_VISIBILITY void serialize_csv(const NativeTuple& tup, std::vector<std::byte>* buf);
IMPL_VISIBILITY bool parse_csv_fast_float(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_csv_fast_float_custom(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_csv_std(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_csv_benstrasser(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;

extern template void generate_tuples<serialize_csv>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
extern template void parse_tuples<parse_csv_fast_float>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
extern template void parse_tuples<parse_csv_fast_float_custom>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
extern template void parse_tuples<parse_csv_std>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
extern template void parse_tuples<parse_csv_benstrasser>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
