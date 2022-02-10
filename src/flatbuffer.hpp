#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "bench.hpp"

IMPL_VISIBILITY void serialize_flatbuffer(const NativeTuple& tup, fmt::memory_buffer* buf);
IMPL_VISIBILITY size_t parse_flatbuffer(const std::byte* __restrict__ read_ptr, NativeTuple* tup);

extern template void fill_memory<serialize_flatbuffer>(std::atomic<std::byte*>*,
                                                       const std::byte* const,
                                                       uint64_t*);
extern template void thread_func<parse_flatbuffer>(ThreadResult*, const std::vector<std::byte>&);