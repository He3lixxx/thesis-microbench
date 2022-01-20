#include <fast_float/fast_float.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <algorithm>
#include <cstdint>

#include "bench.hpp"
#include "csv.hpp"

void serialize_csv(const NativeTuple& tup, fmt::memory_buffer* buf) {
    fmt::format_to(std::back_inserter(*buf), FMT_COMPILE("{},{},{:f},{:f},{:f},{:f},{:02x}\0"),
                   tup.id, tup.timestamp, tup.load, tup.load_avg_1, tup.load_avg_5, tup.load_avg_15,
                   fmt::join(tup.container_id, ""));
}

size_t parse_csv_std(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto str_len = strlen(str_ptr);
    const auto* const str_end = str_ptr + str_len;

    auto result = std::from_chars(str_ptr, str_end, tup->id);
    result = std::from_chars(result.ptr + 1, str_end, tup->timestamp);
    result = std::from_chars(result.ptr + 1, str_end, tup->load);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_1);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_5);
    result = std::from_chars(result.ptr + 1, str_end, tup->load_avg_15);
    tup->set_container_id_from_hex_string(result.ptr + 1, str_end - result.ptr - 1);

    return str_len + 1;
}

size_t parse_csv_fast_float(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto str_len = strlen(str_ptr);
    const auto* const str_end = str_ptr + str_len;

    const auto* result_ptr = parse_uint_str(str_ptr, &tup->id);
    result_ptr = parse_uint_str(result_ptr + 1, &tup->timestamp);

    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_1).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_5).ptr;
    result_ptr = fast_float::from_chars(result_ptr + 1, str_end, tup->load_avg_15).ptr;

    tup->set_container_id_from_hex_string(result_ptr + 1, str_end - result_ptr - 1);

    return str_len + 1;
}

template void fill_memory<serialize_csv>(std::atomic<std::byte*>*,
                                         const std::byte* const,
                                         uint64_t*);
template void thread_func<parse_csv_fast_float>(ThreadResult*, const std::vector<std::byte>&);
template void thread_func<parse_csv_std>(ThreadResult*, const std::vector<std::byte>&);
