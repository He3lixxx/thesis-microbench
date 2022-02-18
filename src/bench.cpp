#include <fmt/core.h>
#include <simdjson.h>
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
#include "json.hpp"
#include "native.hpp"
#include "protobuf.hpp"

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

    // clang-format off
    const std::map generator_parser_map{
        std::make_pair("native"s, std::make_tuple(generate_tuples<serialize_native>, parse_tuples<parse_native>)),

        std::make_pair("rapidjson"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_rapidjson>)),
        std::make_pair("rapidjsoninsitu"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_rapidjson_insitu>)),
        std::make_pair("rapidjsonsax"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_rapidjson_sax>)),

        std::make_pair("simdjson"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_simdjson>)),
        std::make_pair("simdjsonec"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_simdjson_error_codes>)),
        std::make_pair("simdjsonece"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_simdjson_error_codes_early>)),
        std::make_pair("simdjsonu"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_simdjson_unescaped>)),
        std::make_pair("simdjsonooo"s, std::make_tuple(generate_tuples<serialize_json>, parse_tuples<parse_simdjson_out_of_order>)),

        std::make_pair("flatbuf"s, std::make_tuple(generate_tuples<serialize_flatbuffer>, parse_tuples<parse_flatbuffer>)),
        std::make_pair("protobuf"s, std::make_tuple(generate_tuples<serialize_protobuf>, parse_tuples<parse_protobuf>)),

        std::make_pair("csvstd"s, std::make_tuple(generate_tuples<serialize_csv>, parse_tuples<parse_csv_std>)),
        std::make_pair("csvfastfloat"s, std::make_tuple(generate_tuples<serialize_csv>, parse_tuples<parse_csv_fast_float>)),
        std::make_pair("csvbenstrasser"s, std::make_tuple(generate_tuples<serialize_csv>, parse_tuples<parse_csv_benstrasser>)),
    };
    // clang-format on

    const auto it = generator_parser_map.find(arguments["parser"].as<std::string>());
    if (it == generator_parser_map.end()) {
        fmt::print(stderr, "Invalid argument for parser: {}.\n",
                   arguments["parser"].as<std::string>());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
    }

    if (simdjson::builtin_implementation()->name() != "haswell") {
        fmt::print("\nWARNING\nsimdjson implementation: {} (should be haswell)\n\n",
                   simdjson::builtin_implementation()->name());
    }

    const auto [generator_func, parser_func] = it->second;

    /*
     * Input Data Generation
     */
    std::vector<std::byte> memory;
    std::vector<tuple_size_t> tuple_sizes;
    memory.reserve(memory_bytes + 1024);
    tuple_sizes.reserve(memory_bytes / 64);

    {
        auto gen_thread_count = std::max(1u, std::thread::hardware_concurrency() - 1);

        fmt::print("Generating tuples for {} B of memory using {} threads.\n", memory_bytes,
                   gen_thread_count);
        const auto timestamp = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        threads.reserve(gen_thread_count);
        std::mutex mutex;
        for (size_t i = 0; i < gen_thread_count; ++i) {
            threads.emplace_back(generator_func, &memory, memory_bytes, &tuple_sizes, &mutex);
        }
        for (auto& thread : threads) {
            thread.join();
        }

        const std::chrono::duration<double> elapsed_seconds =
            std::chrono::high_resolution_clock::now() - timestamp;
        fmt::print("Generated {} tuples ({} B) in {}s.\n", tuple_sizes.size(), memory.size(),
                   elapsed_seconds.count());
        // fmt::print("Memory contents:\n{}\n", (char*)(memory.data()));
        // fmt::print("Tuple sizes: {}\n", fmt::join(tuple_sizes, ", "));
    }

    /*
     * Actual Benchmark
     */

    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    std::vector<ThreadResult> thread_results(thread_count);

    auto timestamp = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back(parser_func, &thread_results[i], std::ref(memory), std::ref(tuple_sizes));
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
