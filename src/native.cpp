#include <fast_float/fast_float.h>
#include <fmt/format.h>
#include <algorithm>
#include <cstdint>

#include "bench.hpp"
#include "native.hpp"

IMPL_VISIBILITY void serialize_native(const NativeTuple& tup, MemoryBufferT* buf) {
    const size_t write_index = buf->size();
    buf->resize(buf->size() + sizeof(NativeTuple));
    auto* const write_ptr = buf->data() + write_index;
    std::copy_n(reinterpret_cast<const std::byte*>(&tup), sizeof(NativeTuple), write_ptr);
}

IMPL_VISIBILITY bool parse_native(const std::byte* __restrict__ read_ptr,
                                  tuple_size_t tup_size,
                                  NativeTuple* tup) noexcept {
    if (unlikely((tup_size != sizeof(NativeTuple)))) {
        return false;
    }

    *tup = *reinterpret_cast<const NativeTuple*>(read_ptr);
    return true;
}

template void generate_tuples<serialize_native>(MemoryBufferT* memory,
                                                size_t target_memory_size,
                                                TupleSizeBufferT* tuple_sizes,
                                                std::mutex* mutex);
template void parse_tuples<parse_native>(ThreadResult* result,
                                         const MemoryBufferT& memory,
                                         const TupleSizeBufferT& tuple_sizes,
                                         const std::atomic<bool>& stop_flag);
