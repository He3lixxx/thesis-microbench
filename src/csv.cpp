#include <fast_float/fast_float.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <algorithm>
#include <cstdint>
#include <csv.h>

#include "bench.hpp"
#include "csv.hpp"


IMPL_VISIBILITY void serialize_csv(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local auto local_buffer = fmt::memory_buffer();
    local_buffer.clear();

    fmt::format_to(std::back_inserter(local_buffer), FMT_COMPILE("{},{},{:f},{:f},{:f},{:f},{:02x}\0"),
                   tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5, tup.load_avg_15,
                   fmt::join(tup.container_id, ""));

    const auto old_size = buf->size();
    buf->resize(old_size + local_buffer.size());
    std::copy(begin(local_buffer), end(local_buffer), reinterpret_cast<char*>(buf->data() + old_size));
}

IMPL_VISIBILITY bool parse_csv_std(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    // TODO: Input validation
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
#if __cpp_lib_to_chars >= 201611
    const auto* const str_end = str_ptr + tup_size;

    auto result = std::from_chars(str_ptr, str_end, tup->id);
    result = std::from_chars(result.ptr + 1, str_end, tup->timestamp);
    result = std::from_chars(result.ptr + 1, str_end, tup->load);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_1);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_5);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_15);
    tup->set_container_id_from_hex_string(result.ptr + 1, str_end - result.ptr - 1);
#else
#warning "std::from_chars for float not supported. parser 'csvstd' will do nothing!"
#endif

    return true;
}

IMPL_VISIBILITY bool parse_csv_fast_float(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    // TODO: Input validation
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto* const str_end = str_ptr + tup_size;

    const auto* result_ptr = parse_uint_str(str_ptr, &tup->id);
    result_ptr = parse_uint_str(result_ptr + 1, &tup->timestamp);

    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_1).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_5).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_15).ptr;

    tup->set_container_id_from_hex_string(result_ptr + 1, str_end - result_ptr - 1);

    return true;
}

IMPL_VISIBILITY bool parse_csv_benstrasser(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    // TODO: Input validation
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto* const str_end = str_ptr + tup_size;

    io::CSVReader<7> in("filename.csv", str_ptr, str_end);
    char* container_id;
    in.read_row(tup->id, tup->timestamp, tup->load, tup->load_avg_1, tup->load_avg_5, tup->load_avg_15, container_id);
    tup->set_container_id_from_hex_string(container_id, str_end - container_id);

    return true;
}

// clang-format off
template void generate_tuples<serialize_csv>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_csv_fast_float>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
template void parse_tuples<parse_csv_std>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
template void parse_tuples<parse_csv_benstrasser>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
