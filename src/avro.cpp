#include <cstdint>
#include <memory>

#include "avro/Decoder.hh"
#include "avro/Encoder.hh"

#include "./tuple.avro.h"
#include "avro.hpp"
#include "bench.hpp"

IMPL_VISIBILITY void serialize_avro(const NativeTuple& tup, std::vector<std::byte>* buf) {
    bench_avro::Tuple t;

    // unsigned to signed here was implementation defined up to c++17.
    // With c++20, we're guaranteed that uint64_t -> int64_t -> uint64_t gives the same result.
    // (https://en.cppreference.com/w/cpp/language/implicit_conversion#Integral_conversions)
    t.load = tup.load;
    t.load_avg_1 = tup.load_avg_1;
    t.load_avg_5 = tup.load_avg_5;
    t.load_avg_15 = tup.load_avg_15;

    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr e = avro::binaryEncoder();
    e->init(*out);
    avro::encode(*e, t);

    // hacky, but works for now -- see: https://stackoverflow.com/a/71354759/12345551
    // avro's api really is terrible.
    e->init(*out);

    const auto tup_size = out->byteCount();
    const auto old_buffer_size = buf->size();
    buf->resize(old_buffer_size + tup_size);
    auto* write_ptr = reinterpret_cast<std::byte*>(buf->data() + old_buffer_size);

    // avro does it right: You want to get the data written to a memoryOutputStream? You can't!
    auto in = avro::memoryInputStream(*out);
    const uint8_t* read_ptr = nullptr;
    size_t read_len = 0;
    in->next(&read_ptr, &read_len);

    // Not generally correct -- but our tuples are small, chunks for avro are 4*1024 by default, we
    // should be fine.
    if (read_len != tup_size) {
        throw std::runtime_error("Turns out avro really allocated more than one chunk");
    }

    std::copy_n(reinterpret_cast<const std::byte*>(read_ptr), read_len, write_ptr);
}

IMPL_VISIBILITY bool parse_avro(const std::byte* __restrict__ read_ptr,
                                tuple_size_t tup_size,
                                NativeTuple* tup) noexcept {
    // We can not do the usual tuple size verification because avro doesn't have fixed-size entries
    // the decoding will throw if there are too few bytes.

    auto in = avro::memoryInputStream(reinterpret_cast<const uint8_t*>(read_ptr), tup_size);

    // thread_local to prevent repeated costs -- compiler should be able to make it a stack vairable
    // (performance measured on t460: 4,8 vs 5,7 * 10^6 tps)
    thread_local avro::DecoderPtr d = avro::binaryDecoder();

    d->init(*in);
    bench_avro::Tuple t;
    avro::decode(*d, t);

    tup->load = t.load;
    tup->load_avg_1 = t.load_avg_1;
    tup->load_avg_5 = t.load_avg_5;
    tup->load_avg_15 = t.load_avg_15;

    return true;
}

// clang-format off
template void generate_tuples<serialize_avro>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_avro>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
