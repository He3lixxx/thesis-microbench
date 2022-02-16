#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include "bench.hpp"

IMPL_VISIBILITY void serialize_native(const NativeTuple&, std::vector<std::byte>*);
IMPL_VISIBILITY bool parse_native(const std::byte* __restrict__, tuple_size_t, NativeTuple*);

extern template void fill_memory<serialize_native>(std::atomic<std::byte*>*,
                                            const std::byte* const,
                                            uint64_t*);
extern template void thread_func<parse_native>(ThreadResult*, const std::vector<std::byte>&);
