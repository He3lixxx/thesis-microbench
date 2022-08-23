#include <cstdint>

#include "./tuple.pb.h"
#include "bench.hpp"
#include "protobuf.hpp"

IMPL_VISIBILITY void serialize_protobuf(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local Tuple t;
    t.set_id(tup.id);
    t.set_timestamp(tup.timestamp);
    t.set_timestamp2(tup.timestamp2);
    t.set_timestamp3(tup.timestamp3);
    t.set_timestamp4(tup.timestamp4);
    t.set_timestamp5(tup.timestamp5);
    t.set_timestamp6(tup.timestamp6);
    t.set_timestamp7(tup.timestamp7);
    t.set_timestamp8(tup.timestamp8);
    t.set_timestamp9(tup.timestamp9);
    t.set_timestamp10(tup.timestamp10);
    t.set_timestamp11(tup.timestamp11);
    t.set_timestamp12(tup.timestamp12);
    t.set_timestamp13(tup.timestamp13);
    t.set_timestamp14(tup.timestamp14);
    t.set_timestamp15(tup.timestamp15);

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

    tup->id = t.id();
    tup->timestamp = t.timestamp();
    tup->timestamp2 = t.timestamp2();
    tup->timestamp3 = t.timestamp3();
    tup->timestamp4 = t.timestamp4();
    tup->timestamp5 = t.timestamp5();
    tup->timestamp6 = t.timestamp6();
    tup->timestamp7 = t.timestamp7();
    tup->timestamp8 = t.timestamp8();
    tup->timestamp9 = t.timestamp9();
    tup->timestamp10 = t.timestamp10();
    tup->timestamp11 = t.timestamp11();
    tup->timestamp12 = t.timestamp12();
    tup->timestamp13 = t.timestamp13();
    tup->timestamp14 = t.timestamp14();
    tup->timestamp15 = t.timestamp15();
    return true;
}

// clang-format off
template void generate_tuples<serialize_protobuf>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_protobuf>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
