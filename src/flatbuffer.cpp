#include <fmt/format.h>
#include <array>
#include <cstdint>

#include "./tuple_generated.h"
#include "bench.hpp"
#include "flatbuffer.hpp"

IMPL_VISIBILITY void serialize_flatbuffer(const NativeTuple& tup, fmt::memory_buffer* buf) {
    thread_local flatbuffers::FlatBufferBuilder builder(1024);
    auto hash_bytes_span = flatbuffers::make_span(
        reinterpret_cast<const std::array<uint8_t, HASH_BYTES>&>(tup.container_id));
    auto h = Hash(hash_bytes_span);
    auto tuple_offset = CreateTuple(builder, tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5,
                tup.load_avg_15, &h);
    builder.Finish(tuple_offset);

    const size_t size = builder.GetSize();
    buf->reserve(buf->size() + size + sizeof(size));
    std::copy_n(reinterpret_cast<const char*>(&size), sizeof(size), std::back_inserter(*buf));
    std::copy_n(builder.GetBufferPointer(), size, std::back_inserter(*buf));
    builder.Clear();
}

IMPL_VISIBILITY size_t parse_flatbuffer(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    // TODO: This copies all elements, and doesn't use the advantage that direct attribute access is
    // possible
    size_t size = 0;
    std::copy_n(read_ptr, sizeof(size), reinterpret_cast<std::byte*>(&size));
    read_ptr += sizeof(size);

    const auto* t = GetTuple(read_ptr);
    tup->id = t->id();
    tup->timestamp = t->timestamp();
    tup->load = t->load();
    tup->load = t->load_avg_1();
    tup->load = t->load_avg_5();
    tup->load = t->load_avg_15();
    std::copy_n(t->container_id()->bytes()->data(), HASH_BYTES,
                reinterpret_cast<uint8_t*>(tup->container_id.data()));

    return size + sizeof(size);
}

template void fill_memory<serialize_flatbuffer>(std::atomic<std::byte*>*,
                                                const std::byte* const,
                                                uint64_t*);
template void thread_func<parse_flatbuffer>(ThreadResult*, const std::vector<std::byte>&);
