#pragma once

#include <fmt/format.h>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include "constants.hpp"
#include "parse.hpp"

// should be multiple of 8
constexpr size_t HASH_BYTES = 32;  // 256bit = 32 byte

struct NativeTuple {
    uint64_t id;
    uint64_t timestamp;
    float load;
    float load_avg_1;
    float load_avg_5;
    float load_avg_15;
    std::array<std::byte, HASH_BYTES> container_id;
    // std::string command_line;

    void set_container_id_from_hex_string(const char* str, size_t /*length*/) {
        for (size_t i = 0; i < HASH_BYTES; ++i) {
            if constexpr (use_std_from_chars) {
                std::from_chars(str + 2 * i, str + 2 * i + 2,
                                reinterpret_cast<unsigned char&>(container_id[i]), 16);
            } else {
                reinterpret_cast<unsigned char&>(container_id[i]) =
                    parse_hex_char(*(str + 2 * i)) * 16 + parse_hex_char(*(str + 2 * i + 1));
            }
        }
    }

    [[nodiscard]] std::byte read_all_values() {
        return (
            *reinterpret_cast<std::byte*>(&id)
            ^ *reinterpret_cast<std::byte*>(&timestamp)
            ^ *reinterpret_cast<std::byte*>(&load)
            ^ *reinterpret_cast<std::byte*>(&load_avg_1)
            ^ *reinterpret_cast<std::byte*>(&load_avg_5)
            ^ *reinterpret_cast<std::byte*>(&load_avg_15)
            ^ container_id[0]
        );
    }
};

template <>
struct fmt::formatter<NativeTuple> {
    [[nodiscard]] static constexpr auto parse(const format_parse_context& ctx)
        -> decltype(ctx.begin()) {
#if 0
        return std::find(ctx.begin(), ctx.end(), '}');
#else
        auto it = ctx.begin();
        while(*it != '}') ++it;
        return it;
#endif
    }

    template <typename FormatContext>
    auto format(const NativeTuple& tup, FormatContext& ctx) const  // NOLINT(runtime/references)
        -> decltype(ctx.out()) {
        return format_to(ctx.out(), R"(NativeTuple(
    id={},
    timestamp={},
    load={:f},
    load_avg_1={:f},
    load_avg_5={:f},
    load_avg_15={:f},
    container_id={:02x}
))",
                         tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5,
                         tup.load_avg_15, fmt::join(tup.container_id, ""));
    }
};

// https://github.com/google/benchmark/blob/main/include/benchmark/benchmark.h#L412
template <class Tp>
inline void DoNotOptimize(Tp const& value) {
    asm volatile("" : : "r,m"(value) : "memory");  // NOLINT(hicpp-no-assembler)
}

constexpr size_t cacheline_size = 64;
struct ThreadResult {
    alignas(cacheline_size) std::atomic<size_t> tuples_read = 0;
    alignas(cacheline_size) std::atomic<size_t> bytes_read = 0;
};

using SerializerFunc = void (*)(const NativeTuple&, fmt::memory_buffer*);
template <SerializerFunc serialize>
void fill_memory(std::atomic<std::byte*>* memory_ptr,
                 const std::byte* const memory_end,
                 uint64_t* tuple_count) {
    std::random_device dev;
    std::mt19937_64 gen(dev());
    std::uniform_real_distribution<float> load_distribution(0, 1);

    *tuple_count = 0;

    fmt::memory_buffer buf;
    buf.reserve(512);

    while (true) {
        NativeTuple tup;  // NOLINT(cppcoreguidelines-pro-type-member-init)
        tup.id = gen();
        tup.timestamp = gen();
        tup.load = load_distribution(gen);
        tup.load_avg_1 = load_distribution(gen);
        tup.load_avg_5 = load_distribution(gen);
        tup.load_avg_15 = load_distribution(gen);
        static_assert(HASH_BYTES % 8 == 0);
        std::generate_n(reinterpret_cast<uint64_t*>(tup.container_id.data()),
                        sizeof(tup.container_id) / sizeof(tup.container_id[0]) / 8, gen);

        buf.clear();
        serialize(tup, &buf);
        if constexpr (debug_output) {
            fmt::print("Serialized {}\n", tup);
        }


        auto buf_size = static_cast<std::ptrdiff_t>(buf.size());
        std::byte* const write_to = memory_ptr->fetch_add(buf_size);
        if (write_to > memory_end) {
            break;
        }
        if (memory_end - write_to < buf_size) {
            memory_ptr->store(write_to);
            break;
        }

        std::copy_n(buf.data(), buf.size(), reinterpret_cast<unsigned char*>(write_to));
        (*tuple_count)++;
    }
}

constexpr size_t RUN_SIZE = 1024ULL * 16;

using ParseFunc = size_t (*)(const std::byte*, NativeTuple*);
template <ParseFunc parse>
void thread_func(ThreadResult* result, const std::vector<std::byte>& memory) {
    const std::byte* const start_ptr = memory.data();
    const std::byte* read_ptr = start_ptr;
    const std::byte* const end_ptr = start_ptr + memory.size();

    while (true) {
        std::byte read_assurer{0b0};
        size_t total_bytes_read = 0;

        for (size_t i = 0; i < RUN_SIZE; ++i) {
            if (read_ptr >= end_ptr) {
                if constexpr (debug_output) {
                    return;
                }
                read_ptr = start_ptr;
            }

            NativeTuple tup{};
            const auto read_bytes = parse(read_ptr, &tup);

            if constexpr (debug_output) {
                fmt::print("Thread read tuple {}\n", tup);
            }
            read_assurer ^= tup.read_all_values();

            read_ptr += read_bytes;
            total_bytes_read += read_bytes;
        }

        DoNotOptimize(read_assurer);
        result->tuples_read += RUN_SIZE;
        result->bytes_read += total_bytes_read;
    }
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define IMPL_VISIBILITY __attribute__((visibility("hidden")))
