#include <csv.h>
#include <fast_float/fast_float.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <algorithm>
#include <cstdint>

#include "bench.hpp"
#include "csv.hpp"

IMPL_VISIBILITY void serialize_csv(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local auto local_buffer = fmt::memory_buffer();
    local_buffer.clear();

    fmt::format_to(std::back_inserter(local_buffer), FMT_COMPILE("{},{}\0"), tup.id, tup.timestamp);

    const auto old_size = buf->size();
    buf->resize(old_size + local_buffer.size());
    std::copy(begin(local_buffer), end(local_buffer),
              reinterpret_cast<char*>(buf->data() + old_size));
}

IMPL_VISIBILITY bool parse_csv_std(const std::byte* __restrict__ read_ptr,
                                   tuple_size_t tup_size,
                                   NativeTuple* tup) noexcept {
#if __cpp_lib_to_chars >= 201611
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto* const str_end = str_ptr + tup_size;

    auto result = std::from_chars(str_ptr, str_end, tup->id);
    if (unlikely(result.ec != std::errc() || result.ptr >= str_end - 1 || *result.ptr != ',')) {
        return false;
    }

    result = std::from_chars(result.ptr + 1, str_end, tup->timestamp);

    return likely(result.ec == std::errc() && result.ptr == str_end - 1 && *result.ptr == '\0');

#else
#warning "std::from_chars for float not supported. parser 'csvstd' will do nothing!"
    return true;
#endif
}

IMPL_VISIBILITY bool parse_csv_fast_float(const std::byte* __restrict__ read_ptr,
                                          tuple_size_t tup_size,
                                          NativeTuple* tup) noexcept {
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto* const str_end = str_ptr + tup_size;

    auto result = std::from_chars(str_ptr, str_end, tup->id);
    if (unlikely(result.ec != std::errc() || result.ptr >= str_end - 1 || *result.ptr != ',')) {
        return false;
    }

    result = std::from_chars(result.ptr + 1, str_end, tup->timestamp);

    return likely(result.ec == std::errc() && result.ptr == str_end - 1 && *result.ptr == '\0');
}

IMPL_VISIBILITY bool parse_csv_fast_float_custom(const std::byte* __restrict__ read_ptr,
                                                 tuple_size_t tup_size,
                                                 NativeTuple* tup) noexcept {
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto* const str_end = str_ptr + tup_size;

    auto result = parse_uint_str(str_ptr, str_end, tup->id);
    if (unlikely(result.ec != std::errc() || result.ptr >= str_end - 1 || *result.ptr != ',')) {
        return false;
    }

    result = parse_uint_str(result.ptr + 1, str_end, tup->timestamp);

    return likely(result.ec == std::errc() && result.ptr == str_end - 1 && *result.ptr == '\0');
}

IMPL_VISIBILITY bool parse_csv_benstrasser(const std::byte* __restrict__ read_ptr,
                                           tuple_size_t tup_size,
                                           NativeTuple* tup) noexcept {
    const auto* const str_ptr = reinterpret_cast<const char*>(read_ptr);
    const auto* const str_end = str_ptr + tup_size;

    io::CSVReader<2> in("", str_ptr, str_end);
    if (unlikely(!in.read_row(tup->id, tup->timestamp))) {
        return false;
    }

    return true;
}

// clang-format off
template void generate_tuples<serialize_csv>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_csv_fast_float>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_csv_fast_float_custom>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_csv_std>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_csv_benstrasser>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
