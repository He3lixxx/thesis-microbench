#include <fmt/compile.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <cxxopts.hpp>

#include <rapidjson/document.h>

#include <atomic>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>

using std::string_literals::operator""s;

constexpr size_t cacheline_size = 64;
struct AlignedAtomicSizeT {
    alignas(cacheline_size) std::atomic<size_t> value;
};

constexpr size_t RUN_SIZE = 1024ULL * 16;

// should be multiple of 8
constexpr size_t HASH_BYTES = 32;  // 256bit = 32 byte

struct NativeTuple {
    uint64_t id;
    uint64_t timestamp;
    float load;
    float load_avg_1;
    float load_avg_5;
    float load_avg_15;
    unsigned char container_id[HASH_BYTES];  // NOLINT(cppcoreguidelines-avoid-c-arrays)
    // std::string command_line;
};

// https://github.com/google/benchmark/blob/main/include/benchmark/benchmark.h#L412
template <class Tp>
inline void DoNotOptimize(Tp const& value) {
    asm volatile("" : : "r,m"(value) : "memory");
}

inline void generate_json(std::atomic<unsigned char*>* memory_ptr,
                          unsigned char* const memory_end,
                          uint64_t* tuple_count) {
    // TODO: What happens if the tuples have different layout? Does any kind of prediction get
    // worse?

    std::random_device dev;
    std::mt19937_64 gen(dev());
    std::uniform_real_distribution<> load_distribution(0, 1);

    *tuple_count = 0;

    fmt::memory_buffer buf;
    buf.reserve(512);

    while (true) {
        buf.clear();

        static_assert(HASH_BYTES == 32);
        // clang-format off
        fmt::format_to(std::back_inserter(buf), FMT_COMPILE(R"({{
"id": {},
"timestamp": {},
"load": {:f},
"load_avg_1": {:f},
"load_avg_5": {:f},
"load_avg_15": {:f},
"container_id": "{:016x}{:016x}{:016x}{:016x}"
}}
)"),
             gen(),
             gen(),
             load_distribution(gen),
             load_distribution(gen),
             load_distribution(gen),
             load_distribution(gen),
             gen(), gen(), gen(), gen()
        );
        // clang-format on

        buf.push_back('\0');

        // std::cout << buf.data() << std::endl;

        unsigned char* write_to = memory_ptr->fetch_add(buf.size());
        if (write_to >= memory_end) {
            break;
        }
        if (memory_end - write_to < static_cast<int64_t>(buf.size())) {
            std::fill(write_to, memory_end, '\0');
            break;
        }

        std::copy_n(buf.data(), buf.size(), write_to);
        (*tuple_count)++;
    }
}

inline void thread_func_rapidjson(AlignedAtomicSizeT* counter,
                                  const std::vector<unsigned char>& memory) {
    const char* const start_ptr = reinterpret_cast<const char*>(memory.data());
    const char* read_ptr = start_ptr;
    const char* const end_ptr = start_ptr + memory.size();

    // TODO: This leaks memory
    // TODO: Insitu-Parsing?

    rapidjson::Document d;

    while (true) {
        uint64_t read_assurer = 0;

        for (size_t i = 0; i < RUN_SIZE; ++i) {
            if (read_ptr >= end_ptr) {
                read_ptr = start_ptr;
            }

            d.Parse(read_ptr);

            read_assurer += d["load_avg_15"].GetFloat() >= 0.99;

            auto tuple_end = std::find(read_ptr, end_ptr, '\0');
            read_ptr = tuple_end + 1;
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
        std::cout << options.help() << std::endl;
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
        std::make_pair("rapidjson"s, std::make_tuple(generate_json, thread_func_rapidjson)),
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
        std::cout << "Generating tuples for " << memory.size() << "B of memory" << std::endl;
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

        std::chrono::duration<double> elapsed_seconds =
            std::chrono::high_resolution_clock::now() - timestamp;
        std::cout << "Generated " << tuple_count << " tuples in " << elapsed_seconds.count() << "s."
                  << std::endl;
    }

    /*
     * Actual Benchmark
     */

    std::vector<AlignedAtomicSizeT> counters(thread_count);

    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back(thread_func, &counters[i], memory);
    }

    auto timestamp = std::chrono::high_resolution_clock::now();
    while (true) {
        size_t sum = 0;
        for (auto& counter : counters) {
            sum += counter.value.exchange(0);
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - timestamp;
        timestamp = end;

        auto iterations_per_second = static_cast<double>(sum) / diff.count();

        std::cerr << iterations_per_second << " tuples per second   = " << 0
                  << " B/s   = " << 0 / 1e9 << " GB/s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
