#pragma once

#include <fmt/format.h>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "constants.hpp"
#include "parse.hpp"

// should be multiple of 8
constexpr size_t HASH_BYTES = 32;  // 256bit = 32 byte

using tuple_size_t = uint_fast16_t;

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
        return (*reinterpret_cast<std::byte*>(&id) ^ *reinterpret_cast<std::byte*>(&timestamp) ^
                *reinterpret_cast<std::byte*>(&load) ^ *reinterpret_cast<std::byte*>(&load_avg_1) ^
                *reinterpret_cast<std::byte*>(&load_avg_5) ^
                *reinterpret_cast<std::byte*>(&load_avg_15) ^ container_id[0]);
    }
};

template <>
struct fmt::formatter<NativeTuple> {
    [[nodiscard]] static constexpr auto parse(const format_parse_context& ctx)
        -> decltype(ctx.begin()) {
        // std::find is not constexpr for some old compiler on lab machines.
        auto it = ctx.begin();
        auto end_it = ctx.end();
        while (it != end_it && *it != '}')
            ++it;
        return it;
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

using SerializerFunc = void (*)(const NativeTuple&, std::vector<std::byte>*);
template <SerializerFunc serialize>
void generate_tuples(std::vector<std::byte>* memory,
                     size_t target_memory_size,
                     std::vector<tuple_size_t>* tuple_sizes,
                     std::mutex* mutex) {
    std::random_device dev;
    std::mt19937_64 gen(dev());
    auto load_distribution = [](std::mt19937_64& generator) {
        return static_cast<double>(generator()) / static_cast<double>(std::mt19937_64::max());
    };

    std::vector<std::byte> local_buffer;
    std::vector<tuple_size_t> local_tuple_sizes;
    local_buffer.reserve(256 * generate_chunk_size);
    local_tuple_sizes.reserve(generate_chunk_size);

    while (true) {
        local_buffer.clear();
        local_tuple_sizes.clear();

        for (uint64_t i = 0; i < generate_chunk_size; ++i) {
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

            size_t old_size = local_buffer.size();
            serialize(tup, &local_buffer);
            size_t tup_size = local_buffer.size() - old_size;
            local_tuple_sizes.push_back(tup_size);

            if constexpr (debug_output) {
                fmt::print("Serialized {}\n", tup);
            }
        }

        {
            std::scoped_lock lock(*mutex);
            if (memory->size() + local_buffer.size() <= target_memory_size) {
                auto old_memory_size = memory->size();
                auto old_tuple_sizes_size = tuple_sizes->size();
                memory->resize(old_memory_size + local_buffer.size());
                tuple_sizes->resize(old_tuple_sizes_size + local_tuple_sizes.size());

                std::copy(begin(local_buffer), end(local_buffer), begin(*memory) + old_memory_size);
                std::copy(begin(local_tuple_sizes), end(local_tuple_sizes),
                          begin(*tuple_sizes) + old_tuple_sizes_size);
            } else {
                auto read_it = begin(local_buffer);
                for (const auto& tup_size : local_tuple_sizes) {
                    if (memory->size() + tup_size > target_memory_size) {
                        return;
                    }
                    std::copy_n(read_it, tup_size, std::back_inserter(*memory));
                    tuple_sizes->push_back(tup_size);
                    read_it += tup_size;
                }
            }
        }
    }
}

constexpr size_t RUN_SIZE = 1024ULL * 16;

using ParseFunc = bool (*)(const std::byte*, tuple_size_t, NativeTuple*);
template <ParseFunc parse>
void parse_tuples(ThreadResult* result,
                  const std::vector<std::byte>& memory,
                  const std::vector<tuple_size_t>& tuple_sizes) {
    const std::byte* const start_ptr = memory.data();
    const std::byte* const end_ptr = start_ptr + memory.size();

    const std::byte* read_ptr = start_ptr;
    size_t tuple_index = 0;
    while (true) {
        std::byte read_assurer{0b0};
        size_t total_bytes_read = 0;

        for (size_t i = 0; i < RUN_SIZE; ++i) {
            if (read_ptr >= end_ptr) {
                if constexpr (debug_output) {
                    return;
                }
                read_ptr = start_ptr;
                tuple_index = 0;
            }

            const tuple_size_t tup_size = tuple_sizes[tuple_index];

            NativeTuple tup{};
            if (!parse(read_ptr, tup_size, &tup)) {
                throw std::runtime_error("Invalid input tuple dropped");
            }

            read_ptr += tup_size;
            ++tuple_index;

            total_bytes_read += tup_size;

            if constexpr (debug_output) {
                fmt::print("Thread read tuple {}\n", tup);
            }
            read_assurer ^= tup.read_all_values();
        }

        DoNotOptimize(read_assurer);
        result->tuples_read += RUN_SIZE;
        result->bytes_read += total_bytes_read;
    }
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define IMPL_VISIBILITY __attribute__((visibility("hidden")))
