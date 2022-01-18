#include <fmt/compile.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <cxxopts.hpp>

#include <rapidjson/document.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using std::string_literals::operator""s;

constexpr size_t cacheline_size = 64;
struct ThreadResult {
    alignas(cacheline_size) std::atomic<size_t> tuples_read = 0;
    alignas(cacheline_size) std::atomic<size_t> bytes_read = 0;
};

constexpr size_t RUN_SIZE = 1024ULL * 16;

// should be multiple of 8
constexpr size_t HASH_BYTES = 32;  // 256bit = 32 byte

constexpr bool debug_output = false;

struct NativeTuple {
    uint64_t id;
    uint64_t timestamp;
    float load;
    float load_avg_1;
    float load_avg_5;
    float load_avg_15;
    std::byte container_id[HASH_BYTES];  // NOLINT(cppcoreguidelines-avoid-c-arrays)
    // std::string command_line;

    void set_container_id_from_hex_string(const char* str, size_t length) {
        while (std::isspace(static_cast<unsigned char>(*str))) {
            str++;
            length--;
        }

        if (length != 2 * HASH_BYTES) {
            throw std::runtime_error("Unexpected input for container_id");
        }

        for (size_t i = 0; i < HASH_BYTES; ++i) {
            std::from_chars(str + 2 * i, str + 2 * i + 2,
                            reinterpret_cast<unsigned char&>(container_id[i]), 16);
        }
    }
};

template <>
struct fmt::formatter<NativeTuple> {
    [[nodiscard]] static constexpr auto parse(const format_parse_context& ctx)
        -> decltype(ctx.begin()) {
        return std::find(ctx.begin(), ctx.end(), '}');
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
)
)",
                         tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5,
                         tup.load_avg_15, fmt::join(tup.container_id, ""));
    }
};

// https://github.com/google/benchmark/blob/main/include/benchmark/benchmark.h#L412
template <class Tp>
inline void DoNotOptimize(Tp const& value) {
    asm volatile("" : : "r,m"(value) : "memory");  // NOLINT(hicpp-no-assembler)
}

/*
 * native format
 */

inline void serialize_native(const NativeTuple& tup, fmt::memory_buffer* buf) {
    size_t write_index = buf->size();
    buf->resize(buf->size() + sizeof(NativeTuple));
    std::byte* write_ptr = reinterpret_cast<std::byte*>(buf->data() + write_index);
    std::copy_n(reinterpret_cast<const std::byte*>(&tup), sizeof(NativeTuple), write_ptr);
}

inline size_t parse_native(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    std::copy_n(read_ptr, sizeof(NativeTuple), reinterpret_cast<std::byte*>(tup));
    return sizeof(NativeTuple);
}

/*
 * CSV format
 */
inline void serialize_csv(const NativeTuple& tup, fmt::memory_buffer* buf) {
    fmt::format_to(std::back_inserter(*buf), FMT_COMPILE("{},{},{:f},{:f},{:f},{:f},{:02x}\0"),
                   tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5, tup.load_avg_15,
                   fmt::join(tup.container_id, ""));
}


inline size_t parse_csv_std(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    auto str_ptr = reinterpret_cast<const char*>(read_ptr);
    auto str_len = strlen(str_ptr);
    auto str_end = str_ptr + str_len;

    auto result = std::from_chars(str_ptr, str_end, tup->id);
    result = std::from_chars(result.ptr + 1, str_end, tup->timestamp);
    result = std::from_chars(result.ptr + 1, str_end, tup->load);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_1);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_5);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_15);
    tup->set_container_id_from_hex_string(result.ptr + 1, str_end - result.ptr - 1);

    return str_len + 1;
}

inline size_t parse_csv_fast_float(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    auto str_ptr = reinterpret_cast<const char*>(read_ptr);
    auto str_len = strlen(str_ptr);
    auto str_end = str_ptr + str_len;

    auto result_ptr = std::from_chars(str_ptr, str_end, tup->id).ptr;
    result_ptr = std::from_chars(result_ptr + 1, str_end, tup->timestamp).ptr;

    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_1).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_5).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_15).ptr;
    tup->set_container_id_from_hex_string(result_ptr + 1, str_end - result_ptr - 1);

    return str_len + 1;
}

/*
 * json format
 */

inline void serialize_json(const NativeTuple& tup, fmt::memory_buffer* buf) {
    // TODO: What happens if the tuples have different layout? Does any kind of prediction get
    // worse?
    // clang-format off
    fmt::format_to(std::back_inserter(*buf), FMT_COMPILE(R"({{
"id": {},
"timestamp": {},
"load": {:f},
"load_avg_1": {:f},
"load_avg_5": {:f},
"load_avg_15": {:f},
"container_id": "{:02x}"
}}
)"),
        tup.id,
        tup.timestamp,
        tup.load,
        tup.load_avg_1,
        tup.load_avg_5,
        tup.load_avg_15,
        fmt::join(tup.container_id, "")
    );
    // clang-format on

    buf->push_back('\0');
}

inline size_t parse_rapidjson(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    // TODO: Would make sense to re-use this, but that will leak memory(?)
    rapidjson::Document d;

    // TODO: Insitu-Parsing?
    d.Parse(reinterpret_cast<const char*>(read_ptr));

    if (!d["id"].IsUint64() || !d["timestamp"].IsUint64() || !d["load"].IsFloat() ||
        !d["load_avg_1"].IsFloat() || !d["load_avg_5"].IsFloat() || !d["load_avg_15"].IsFloat() ||
        !d["container_id"].IsString())
        throw std::runtime_error("Invalid input tuple");

    tup->id = d["id"].GetUint64();
    tup->timestamp = d["timestamp"].GetUint64();
    tup->load = d["load"].GetFloat();
    tup->load_avg_1 = d["load_avg_1"].GetFloat();
    tup->load_avg_5 = d["load_avg_5"].GetFloat();
    tup->load_avg_15 = d["load_avg_15"].GetFloat();
    tup->set_container_id_from_hex_string(d["container_id"].GetString(),
                                          d["container_id"].GetStringLength());

    // tup->load_avg_5 = *reinterpret_cast<const float*>(read_ptr);

    auto tuple_bytes = std::strlen(reinterpret_cast<const char*>(read_ptr)) + 1;
    return tuple_bytes;
}

/*
 * framework
 */

using SerializerFunc = void (*)(const NativeTuple&, fmt::memory_buffer*);
template <SerializerFunc serialize>
inline void fill_memory(std::atomic<std::byte*>* memory_ptr,
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
        std::generate_n(reinterpret_cast<uint64_t*>(tup.container_id),
                        sizeof(tup.container_id) / sizeof(tup.container_id[0]) / 8, gen);

        buf.clear();
        serialize(tup, &buf);

        std::byte* write_to = memory_ptr->fetch_add(static_cast<std::ptrdiff_t>(buf.size()));
        if (write_to >= memory_end) {
            break;
        }
        if (memory_end - write_to < static_cast<int64_t>(buf.size())) {
            memory_ptr->store(write_to);
            break;
        }

        if constexpr (debug_output) {
            fmt::print("{}", buf.data());
        }
        std::copy_n(buf.data(), buf.size(), reinterpret_cast<unsigned char*>(write_to));
        (*tuple_count)++;
    }
}

using ParseFunc = size_t (*)(const std::byte*, NativeTuple*);
template <ParseFunc parse>
inline void thread_func(ThreadResult* result, const std::vector<std::byte>& memory) {
    const std::byte* const start_ptr = memory.data();
    const std::byte* read_ptr = start_ptr;
    const std::byte* const end_ptr = start_ptr + memory.size();

    while (true) {
        uint64_t read_assurer = 0;
        size_t total_bytes_read = 0;

        for (size_t i = 0; i < RUN_SIZE; ++i) {
            if (read_ptr >= end_ptr) {
                if constexpr (debug_output) {
                    return;
                }
                read_ptr = start_ptr;
            }

            NativeTuple tup;
            auto read_bytes = parse(read_ptr, &tup);

            if constexpr (debug_output) {
                fmt::print("Thread read tuple {}\n", tup);
            }
            read_assurer += tup.load_avg_5 >= 0.99;

            read_ptr += read_bytes;
            total_bytes_read += read_bytes;
        }

        DoNotOptimize(read_assurer);
        result->tuples_read += RUN_SIZE;
        result->bytes_read += total_bytes_read;
    }
}

int main(int argc, char** argv) {
    /*
     * Command Line Arguments
     */

    cxxopts::Options options("Parser Benchmark",
                             "Benchmark parsing performance of different data formats and parsers");
    // clang-format off
    options.add_options()
        ("m,memory", "How much memory to use for input tuples. Supported suffixed: k, m, g, t", cxxopts::value<std::string>())
        ("t,threads", "How many threads to use for parsing tuples.", cxxopts::value<size_t>())
        ("p,parser", "Parser to use", cxxopts::value<std::string>())
        ("h,help", "Print usage");
    // clang-format on

    auto arguments = options.parse(argc, argv);

    if (arguments.count("help") != 0) {
        fmt::print("{}\n", options.help());
        exit(0);  // NOLINT(concurrency-mt-unsafe)
    }

    size_t suffix_position{};
    auto memory_bytes_string = arguments["memory"].as<std::string>();
    uint64_t memory_bytes = std::stoll(memory_bytes_string, &suffix_position, 0);
    if (suffix_position == memory_bytes_string.length() - 1) {
        char suffix = static_cast<char>(
            std::tolower(static_cast<unsigned char>(memory_bytes_string.at(suffix_position))));
        std::map multiplicators{
            std::make_pair('k', 1000ULL),
            std::make_pair('m', 1000ULL * 1000),
            std::make_pair('g', 1000ULL * 1000 * 1000),
            std::make_pair('t', 1000ULL * 1000 * 1000 * 1000),
        };
        auto it = multiplicators.find(suffix);
        if (it == multiplicators.end()) {
            fmt::print(stderr, "Invalid argument for memory: {}.\n", memory_bytes_string);
            exit(1);  // NOLINT(concurrency-mt-unsafe)
        }
        memory_bytes *= it->second;
    } else if (suffix_position < memory_bytes_string.length() - 1) {
        fmt::print(stderr, "Invalid argument for memory: {}.\nAllowed suffixes: k, m, g, t.",
                   memory_bytes_string);
        exit(1);  // NOLINT(concurrency-mt-unsafe)
    }

    size_t thread_count = arguments["threads"].as<size_t>();

    std::map generator_parser_map{
        std::make_pair("rapidjson"s,
                       std::make_tuple(fill_memory<serialize_json>, thread_func<parse_rapidjson>)),
        std::make_pair("native"s,
                       std::make_tuple(fill_memory<serialize_native>, thread_func<parse_native>)),
        std::make_pair("csvstd"s, std::make_tuple(fill_memory<serialize_csv>, thread_func<parse_csv_std>)),
        std::make_pair("csvfastfloat"s, std::make_tuple(fill_memory<serialize_csv>, thread_func<parse_csv_fast_float>)),
    };
    auto it = generator_parser_map.find(arguments["parser"].as<std::string>());
    if (it == generator_parser_map.end()) {
        fmt::print(stderr, "Invalid argument for parser: {}.\n",
                   arguments["parser"].as<std::string>());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
    }

    auto [generator, thread_func] = it->second;

    /*
     * Input Data Generation
     */
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    std::vector<std::byte> memory(memory_bytes);
    uint64_t tuple_count{};
    {
        fmt::print("Generating tuples for {}B of memory.\n", memory.size());
        auto timestamp = std::chrono::high_resolution_clock::now();

        std::atomic<std::byte*> write_ptr = &memory[0];
        std::byte* write_end = write_ptr + memory.size();

        for (size_t i = 0; i < thread_count; ++i) {
            threads.emplace_back(generator, &write_ptr, write_end, &tuple_count);
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();

        memory.resize(write_ptr.load() - &memory[0]);
        fmt::print("Memory resized to {}B.\n", memory.size());

        std::chrono::duration<double> elapsed_seconds =
            std::chrono::high_resolution_clock::now() - timestamp;
        fmt::print("Generated {} tuples in {}s.\n", tuple_count, elapsed_seconds.count());
    }

    /*
     * Actual Benchmark
     */

    std::vector<ThreadResult> thread_results(thread_count);

    auto timestamp = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back(thread_func, &thread_results[i], memory);
    }

    while (true) {
        size_t tuples_sum = 0;
        size_t bytes_sum = 0;
        for (auto& result : thread_results) {
            tuples_sum += result.tuples_read.exchange(0);
            bytes_sum += result.bytes_read.exchange(0);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - timestamp;
        timestamp = end;

        auto tuples_per_second = static_cast<double>(tuples_sum) / diff.count();
        auto bytes_per_second = static_cast<double>(bytes_sum) / diff.count();

        fmt::print(stderr, "{:11.6g} t/s,   {:11.6g} B/s = {:9.4g} GB/s\n", tuples_per_second,
                   bytes_per_second, bytes_per_second / 1e9);

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
