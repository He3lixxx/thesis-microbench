#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "bench.hpp"

IMPL_VISIBILITY void serialize_json(const NativeTuple& tup, fmt::memory_buffer* buf);
IMPL_VISIBILITY size_t parse_rapidjson(const std::byte* __restrict__ read_ptr, NativeTuple* tup);
IMPL_VISIBILITY size_t parse_rapidjson_sax(const std::byte* __restrict__ read_ptr, NativeTuple* tup);
IMPL_VISIBILITY size_t parse_simdjson(const std::byte* __restrict__ read_ptr, NativeTuple* tup);

extern template void fill_memory<serialize_json>(std::atomic<std::byte*>*,
                                                 const std::byte* const,
                                                 uint64_t*);
extern template void thread_func<parse_rapidjson_sax>(ThreadResult*, const std::vector<std::byte>&);
extern template void thread_func<parse_rapidjson>(ThreadResult*, const std::vector<std::byte>&);
extern template void thread_func<parse_simdjson>(ThreadResult*, const std::vector<std::byte>&);
