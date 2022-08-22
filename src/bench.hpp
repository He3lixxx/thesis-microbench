#pragma once

#include <fmt/format.h>
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "constants.hpp"
#include "parse.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define likely(x) __builtin_expect(!!(x), 1)
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define unlikely(x) __builtin_expect(!!(x), 0)

// should be multiple of 8
constexpr size_t HASH_BYTES = 32;  // 256bit = 32 byte

using tuple_size_t = uint_fast16_t;

template <class InputIt, class Pred>
bool vectorizable_any_of(InputIt first, InputIt last, Pred pred) {
    // supposed to replace:
    // std::any_of(std::execution::unseq, first, last, pred);
    // on environments where std::execution is not yet available (node-01: gcc09, no <execution>)
    // 18% faster than std::any_of on t460 with clang13, O3, measuring simdjson (0,81 vs 0,96)

    bool val = false;
    for (; first != last; ++first) {
        val |= pred(*first);
    }
    return val;
}

struct NativeTuple {
    uint64_t id;
    uint64_t timestamp;
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
    std::mt19937_64 gen(std::random_device{}());
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

            auto old_size = static_cast<int64_t>(local_buffer.size());
            serialize(tup, &local_buffer);
            tuple_size_t tup_size = local_buffer.size() - old_size;
            local_tuple_sizes.push_back(tup_size);
        }

        {
            std::scoped_lock lock(*mutex);
            if (memory->size() + local_buffer.size() <= target_memory_size) {
                auto old_memory_size = static_cast<int64_t>(memory->size());
                auto old_tuple_sizes_size = static_cast<int64_t>(tuple_sizes->size());
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
                    read_it += static_cast<int64_t>(tup_size);
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
                  const std::vector<tuple_size_t>& tuple_sizes,
                  const std::atomic<bool>& stop_flag) {
    const std::byte* const start_ptr = memory.data();
    const std::byte* read_ptr = start_ptr;
    size_t tuple_index = 0;
    const size_t tuple_count = tuple_sizes.size();

    while (!stop_flag.load(std::memory_order_relaxed)) {
        size_t total_bytes_read = 0;

        for (size_t i = 0; i < RUN_SIZE; ++i) {
            if (tuple_index == tuple_count) {
                if constexpr (debug_output) {
                    return;
                }
                read_ptr = start_ptr;
                tuple_index = 0;
            }

            const tuple_size_t tup_size = tuple_sizes[tuple_index];

            NativeTuple tup{};
            bool success = false;
            try {
                success = parse(read_ptr, tup_size, &tup);
            } catch (...) {
                success = false;
            }
            if (unlikely(!success)) {
                fmt::print("Invalid input tuple dropped\n");
                exit(1);  // NOLINT(concurrency-mt-unsafe)
            }
            DoNotOptimize(tup);

            read_ptr += tup_size;
            ++tuple_index;

            total_bytes_read += tup_size;

        }

        result->tuples_read += RUN_SIZE;
        result->bytes_read += total_bytes_read;
    }
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define IMPL_VISIBILITY __attribute__((visibility("hidden")))
