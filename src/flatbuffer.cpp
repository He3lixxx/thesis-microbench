#include <fmt/format.h>
#include <array>
#include <cstdint>

#include "./tuple.fb.h"
#include "bench.hpp"
#include "flatbuffer.hpp"

IMPL_VISIBILITY void serialize_flatbuffer(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local flatbuffers::FlatBufferBuilder builder(1024);
    auto tuple_offset = CreateTuple(builder, tup.id, tup.timestamp);
    builder.Finish(tuple_offset);

    const size_t size = builder.GetSize();
    buf->reserve(buf->size() + size);
    std::copy_n(reinterpret_cast<std::byte*>(builder.GetBufferPointer()), size,
                std::back_inserter(*buf));
    builder.Clear();
}

IMPL_VISIBILITY bool parse_flatbuffer(const std::byte* __restrict__ read_ptr,
                                      tuple_size_t tup_size,
                                      NativeTuple* tup) noexcept {
    // While this looks as if it makes a copy of the tuple, the compiler (tested: clang13) optimizes
    // this heavily: this whole function is inlined. Values of the tuple are loaded from memory into
    // registers exactly once and then used.

    auto verifyer = flatbuffers::Verifier(reinterpret_cast<const uint8_t*>(read_ptr), tup_size);
    if (unlikely(!VerifyTupleBuffer(verifyer))) {
        return false;
    }

    const auto* t = GetTuple(read_ptr);
    tup->id = t->id();
    tup->timestamp = t->timestamp();

    return true;
}

// clang-format off
template void generate_tuples<serialize_flatbuffer>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_flatbuffer>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
