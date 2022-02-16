#include <fmt/format.h>
#include <array>
#include <cstdint>

#include "./tuple.pb.h"
#include "bench.hpp"
#include "protobuf.hpp"

IMPL_VISIBILITY void serialize_protobuf(const NativeTuple& tup, fmt::memory_buffer* buf) {
    Tuple t;
    t.set_id(tup.id);
    t.set_timestamp(tup.timestamp);
    t.set_load(tup.load);
    t.set_load_avg_1(tup.load);
    t.set_load_avg_5(tup.load);
    t.set_load_avg_15(tup.load);
    t.set_container_id(reinterpret_cast<const char*>(tup.container_id.data()),
                       tup.container_id.size());

    const size_t write_size = t.ByteSizeLong();
    const size_t write_index = buf->size();
    buf->resize(buf->size() + write_size + sizeof(write_size));
    auto* write_ptr = reinterpret_cast<std::byte*>(buf->data() + write_index);

    std::copy_n(reinterpret_cast<const std::byte*>(&write_size), sizeof(write_size), write_ptr);
    write_ptr += sizeof(write_size);
    t.SerializeToArray(write_ptr, write_size);
}

IMPL_VISIBILITY size_t parse_protobuf(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    size_t size = 0;
    std::copy_n(read_ptr, sizeof(size), reinterpret_cast<std::byte*>(&size));
    read_ptr += sizeof(size);

    Tuple t;
    if (!t.ParseFromArray(read_ptr, size)) {
        throw std::runtime_error("Invalid input data");
    }

    tup->id = t.id();
    tup->timestamp = t.timestamp();
    tup->load = t.load();
    tup->load = t.load_avg_1();
    tup->load = t.load_avg_5();
    tup->load = t.load_avg_15();
    std::copy_n(t.container_id().data(), HASH_BYTES,
                reinterpret_cast<char*>(tup->container_id.data()));

    return size + sizeof(size);
}

// template void fill_memory<serialize_protobuf>(std::atomic<std::byte*>*,
//                                               const std::byte* const,
//                                               uint64_t*);
// template void thread_func<parse_protobuf>(ThreadResult*, const std::vector<std::byte>&);
