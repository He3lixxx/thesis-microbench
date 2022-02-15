#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "bench.hpp"

IMPL_VISIBILITY void serialize_csv(const NativeTuple& tup, fmt::memory_buffer* buf);
IMPL_VISIBILITY size_t parse_csv_fast_float(const std::byte* __restrict__ read_ptr,
                                            NativeTuple* tup);
IMPL_VISIBILITY size_t parse_csv_std(const std::byte* __restrict__ read_ptr, NativeTuple* tup);
IMPL_VISIBILITY size_t parse_csv_benstrasser(const std::byte* __restrict__ read_ptr, NativeTuple* tup);

extern template void fill_memory<serialize_csv>(std::atomic<std::byte*>*,
                                                const std::byte* const,
                                                uint64_t*);
extern template void thread_func<parse_csv_fast_float>(ThreadResult*,
                                                       const std::vector<std::byte>&);
extern template void thread_func<parse_csv_std>(ThreadResult*, const std::vector<std::byte>&);
extern template void thread_func<parse_csv_benstrasser>(ThreadResult*, const std::vector<std::byte>&);
