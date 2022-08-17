#pragma once

#include <cstddef>
#include <vector>
#include "bench.hpp"

// clang-format off
IMPL_VISIBILITY void serialize_json(const NativeTuple& tup, std::vector<std::byte>* buf);

IMPL_VISIBILITY bool parse_rapidjson(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_rapidjson_insitu(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_rapidjson_sax(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;

IMPL_VISIBILITY bool parse_simdjson(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup);
IMPL_VISIBILITY bool parse_simdjson_out_of_order(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup);
IMPL_VISIBILITY bool parse_simdjson_error_codes(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_simdjson_error_codes_early(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;
IMPL_VISIBILITY bool parse_simdjson_unescaped(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup);

extern template void generate_tuples<serialize_json>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
