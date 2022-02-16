#include <fmt/core.h>
#include <cxxopts.hpp>

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bench.hpp"
#include "csv.hpp"
#include "flatbuffer.hpp"
#include "protobuf.hpp"
#include "json.hpp"
#include "native.hpp"

using std::string_literals::operator""s;

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

    const auto arguments = options.parse(argc, argv);

    if (arguments.count("help") != 0) {
        fmt::print("{}\n", options.help());
        exit(0);  // NOLINT(concurrency-mt-unsafe)
    }

    size_t suffix_position{};
    const auto memory_bytes_string = arguments["memory"].as<std::string>();
    uint64_t memory_bytes = std::stoll(memory_bytes_string, &suffix_position, 0);
    if (suffix_position == memory_bytes_string.length() - 1) {
        const char suffix = static_cast<char>(
            std::tolower(static_cast<unsigned char>(memory_bytes_string.at(suffix_position))));
        const std::map multiplicators{
            std::make_pair('k', 1000ULL),
            std::make_pair('m', 1000ULL * 1000),
            std::make_pair('g', 1000ULL * 1000 * 1000),
            std::make_pair('t', 1000ULL * 1000 * 1000 * 1000),
        };
        const auto it = multiplicators.find(suffix);
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

    const size_t thread_count = arguments["threads"].as<size_t>();

    const std::map generator_parser_map{
        std::make_pair("native"s,
                       std::make_tuple(fill_memory<serialize_native>, thread_func<parse_native>)),
        // std::make_pair("rapidjson"s,
        //                std::make_tuple(fill_memory<serialize_json>, thread_func<parse_rapidjson>)),
        // std::make_pair("rapidjsonsax"s,
        //                std::make_tuple(fill_memory<serialize_json>, thread_func<parse_rapidjson_sax>)),
        // std::make_pair("simdjson"s,
        //                std::make_tuple(fill_memory<serialize_json>, thread_func<parse_simdjson>)),
        // std::make_pair("flatbuf"s, std::make_tuple(fill_memory<serialize_flatbuffer>,
        //                                            thread_func<parse_flatbuffer>)),
        // std::make_pair("protobuf"s, std::make_tuple(fill_memory<serialize_protobuf>,
        //                                            thread_func<parse_protobuf>)),
        // std::make_pair("csvstd"s,
        //                std::make_tuple(fill_memory<serialize_csv>, thread_func<parse_csv_std>)),
        // std::make_pair("csvfastfloat"s, std::make_tuple(fill_memory<serialize_csv>,
        //                                                 thread_func<parse_csv_fast_float>)),
        // std::make_pair("csvbenstrasser"s, std::make_tuple(fill_memory<serialize_csv>,
        //                                                 thread_func<parse_csv_benstrasser>)),
    };

    const auto it = generator_parser_map.find(arguments["parser"].as<std::string>());
    if (it == generator_parser_map.end()) {
        fmt::print(stderr, "Invalid argument for parser: {}.\n",
                   arguments["parser"].as<std::string>());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
    }

    const auto [generator, thread_func] = it->second;

    /*
     * Input Data Generation
     */
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    std::vector<std::byte> memory(memory_bytes);
    uint64_t tuple_count{};
    {
        fmt::print("Generating tuples for {}B of memory.\n", memory.size());
        const auto timestamp = std::chrono::high_resolution_clock::now();

        std::atomic<std::byte*> write_ptr = &memory[0];
        const std::byte* write_end = write_ptr + memory.size();

        for (size_t i = 0; i < thread_count; ++i) {
            threads.emplace_back(generator, &write_ptr, write_end, &tuple_count);
        }
        for (auto& thread : threads) {
            thread.join();
        }
        threads.clear();

        memory.resize(write_ptr.load() - &memory[0]);
        fmt::print("Memory resized to {}B.\n", memory.size());

        const std::chrono::duration<double> elapsed_seconds =
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
        const auto end = std::chrono::high_resolution_clock::now();
        const std::chrono::duration<double> diff = end - timestamp;
        timestamp = end;

        const auto tuples_per_second = static_cast<double>(tuples_sum) / diff.count();
        const auto bytes_per_second = static_cast<double>(bytes_sum) / diff.count();

        fmt::print(stderr, "{:11.6g} t/s,   {:11.6g} B/s = {:9.4g} GB/s\n", tuples_per_second,
                   bytes_per_second, bytes_per_second / 1e9);

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}
