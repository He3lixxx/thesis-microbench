#pragma once

#include <cstddef>
#include <vector>
#include "bench.hpp"

// clang-format off
IMPL_VISIBILITY void serialize_avro(const NativeTuple& tup, MemoryBufferT* buf);
IMPL_VISIBILITY bool parse_avro(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) noexcept;

extern template void generate_tuples<serialize_avro>(MemoryBufferT* memory, size_t target_memory_size, TupleSizeBufferT* tuple_sizes, std::mutex* mutex);
extern template void parse_tuples<parse_avro>(ThreadResult* result, const MemoryBufferT& memory, const TupleSizeBufferT& tuple_sizes, const std::atomic<bool>& stop_flag);
