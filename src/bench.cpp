#include <fmt/compile.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <cxxopts.hpp>

#include <rapidjson/document.h>

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
struct AlignedAtomicSizeT {
    alignas(cacheline_size) std::atomic<size_t> value = 0;
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
    unsigned char container_id[HASH_BYTES];  // NOLINT(cppcoreguidelines-avoid-c-arrays)
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
            std::from_chars(str + 2 * i, str + 2 * i + 2, container_id[i], 16);
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
        return format_to(ctx.out(), FMT_COMPILE(R"(NativeTuple(
    id={},
    timestamp={},
    load={:f},
    load_avg_1={:f},
    load_avg_5={:f},
    load_avg_15={:f},
    container_id={:02x}
)
)"),
                         tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5,
                         tup.load_avg_15, fmt::join(tup.container_id, ""));
    }
};

// https://github.com/google/benchmark/blob/main/include/benchmark/benchmark.h#L412
template <class Tp>
inline void DoNotOptimize(Tp const& value) {
    asm volatile("" : : "r,m"(value) : "memory");  // NOLINT(hicpp-no-assembler)
}

inline void serialize_json(const NativeTuple& tuple, fmt::memory_buffer* buf) {
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
        tuple.id,
        tuple.timestamp,
        tuple.load,
        tuple.load_avg_1,
        tuple.load_avg_5,
        tuple.load_avg_15,
        fmt::join(tuple.container_id, "")
    );
    // clang-format on

    buf->push_back('\0');
}

inline size_t parse_rapidjson(const unsigned char* read_ptr, NativeTuple* tup) {
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

    auto tuple_bytes = std::strlen(reinterpret_cast<const char*>(read_ptr)) + 1;
    return tuple_bytes;
}

using SerializerFunc = void (*)(const NativeTuple&, fmt::memory_buffer*);

template <SerializerFunc serialize>
inline void fill_memory(std::atomic<unsigned char*>* memory_ptr,
                        const unsigned char* const memory_end,
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

        unsigned char* write_to = memory_ptr->fetch_add(static_cast<std::ptrdiff_t>(buf.size()));
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
        std::copy_n(buf.data(), buf.size(), write_to);
        (*tuple_count)++;
    }
}

using ParseFunc = size_t (*)(const unsigned char*, NativeTuple*);
template <ParseFunc parse>
inline void thread_func(AlignedAtomicSizeT* counter, const std::vector<unsigned char>& memory) {
    const unsigned char* const start_ptr = memory.data();
    const unsigned char* read_ptr = start_ptr;
    const unsigned char* const end_ptr = start_ptr + memory.size();

    while (true) {
        uint64_t read_assurer = 0;

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
        }

        DoNotOptimize(read_assurer);
        counter->value += RUN_SIZE;
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

    size_t suffix_position = 0;
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
            std::cerr << "Invalid argument for memory." << std::endl;
            exit(1);  // NOLINT(concurrency-mt-unsafe)
        }
        memory_bytes *= it->second;
    } else if (suffix_position < memory_bytes_string.length() - 1) {
        std::cerr
            << "Invalid argument for memory -- only single letter suffixes allowed (k, m, g, t): "
            << memory_bytes_string << std::endl;
        exit(1);  // NOLINT(concurrency-mt-unsafe)
    }

    size_t thread_count = arguments["threads"].as<size_t>();

    std::map generator_parser_map{
        std::make_pair("rapidjson"s,
                       std::make_tuple(fill_memory<serialize_json>, thread_func<parse_rapidjson>)),
    };
    auto it = generator_parser_map.find(arguments["parser"].as<std::string>());
    if (it == generator_parser_map.end()) {
        std::cerr << "Invalid argument for parser." << std::endl;
        exit(1);  // NOLINT(concurrency-mt-unsafe)
    }

    auto [generator, thread_func] = it->second;

    /*
     * Input Data Generation
     */
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    std::vector<unsigned char> memory(memory_bytes);
    uint64_t tuple_count = 0;
    {
        fmt::print("Generating tuples for {}B of memory.\n", memory.size());
        auto timestamp = std::chrono::high_resolution_clock::now();

        std::atomic<unsigned char*> write_ptr = &memory[0];
        unsigned char* write_end = write_ptr + memory.size();

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

    std::vector<AlignedAtomicSizeT> counters(thread_count);

    auto timestamp = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back(thread_func, &counters[i], memory);
    }

    while (true) {
        size_t sum = 0;
        for (auto& counter : counters) {
            sum += counter.value.exchange(0);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - timestamp;
        timestamp = end;

        auto iterations_per_second = static_cast<double>(sum) / diff.count();

        std::cerr << iterations_per_second << " tuples per second" << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
