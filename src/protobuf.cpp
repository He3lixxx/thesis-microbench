#include <cstdint>

#include "./tuple.pb.h"
#include "bench.hpp"
#include "protobuf.hpp"

IMPL_VISIBILITY void serialize_protobuf(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local Tuple t;
    t.set_container_id(reinterpret_cast<const char*>(tup.container_id.data()),
                       tup.container_id.size());

    const auto tup_size = t.ByteSizeLong();
    const auto old_buffer_size = buf->size();
    buf->resize(old_buffer_size + tup_size);
    auto* write_ptr = reinterpret_cast<std::byte*>(buf->data() + old_buffer_size);

    t.SerializeToArray(write_ptr, static_cast<int>(tup_size));
}

IMPL_VISIBILITY bool parse_protobuf(const std::byte* __restrict__ read_ptr,
                                    tuple_size_t tup_size,
                                    NativeTuple* tup) noexcept {
    thread_local Tuple t;
    if (unlikely(!t.ParseFromArray(read_ptr, static_cast<int>(tup_size)))) {
        return false;
    }

    std::copy_n(t.container_id().data(), HASH_BYTES,
                reinterpret_cast<char*>(tup->container_id.data()));

    return true;
}

// clang-format off
template void generate_tuples<serialize_protobuf>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_protobuf>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
