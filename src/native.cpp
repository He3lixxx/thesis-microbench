#include <fast_float/fast_float.h>
#include <fmt/format.h>
#include <algorithm>
#include <cstdint>

#include "bench.hpp"
#include "native.hpp"

IMPL_VISIBILITY void serialize_native(const NativeTuple& tup, std::vector<std::byte>* buf) {
    const size_t write_index = buf->size();
    buf->resize(buf->size() + sizeof(NativeTuple));
    auto* const write_ptr = buf->data() + write_index;
    std::copy_n(reinterpret_cast<const std::byte*>(&tup), sizeof(NativeTuple), write_ptr);
}

IMPL_VISIBILITY bool parse_native(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    if(tup_size != sizeof(NativeTuple)) {
        return false;
    }

    *tup = *reinterpret_cast<const NativeTuple*>(read_ptr);
    return true;
}

template void fill_memory<serialize_native>(std::atomic<std::byte*>*,
                                            const std::byte* const,
                                            uint64_t*);
template void thread_func<parse_native>(ThreadResult*, const std::vector<std::byte>&);
