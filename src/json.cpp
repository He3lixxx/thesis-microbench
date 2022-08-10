#include <fmt/compile.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <simdjson.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <string_view>
#include <utility>

#include "bench.hpp"
#include "json.hpp"

using namespace std::literals::string_view_literals;

// adapted from
// https://github.com/Tencent/rapidjson/blob/master/example/messagereader/messagereader.cpp
struct NativeTupleHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, NativeTupleHandler> {
    explicit NativeTupleHandler(NativeTuple* tup) : tup_(tup) {}

    bool StartObject() {
        if (unlikely(state_ != kExpectObjectStart)) {
            return false;
        }
        state_ = kExpectAttrNameOrObjectEnd;
        return true;
    }

    bool Key(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        if (unlikely(state_ != kExpectAttrNameOrObjectEnd)) {
            return false;
        }

        std::string_view key(str, length);

        // clang-format off
        if (key == "container_id"sv) { state_ = kExpectContainerId;
        } else { return false; }
        // clang-format on

        return true;
    }

    bool Double(double value) {
        return false;
    }

    bool Uint64(uint64_t val) {
        return false;
    }

    bool String(const Ch* str, rapidjson::SizeType length, bool /*copy*/) {
        if (unlikely(state_ != kExpectContainerId)) {
            return false;
        }
        state_ = kExpectAttrNameOrObjectEnd;
        auto result = tup_->set_container_id_from_hex_string(str, str + length);
        return likely(result.ec == std::errc() && result.ptr == str + length);
    }

    [[nodiscard]] bool EndObject(rapidjson::SizeType /*unused*/) const {
        return state_ == kExpectAttrNameOrObjectEnd;
    }

    static bool Default() { return false; }

    NativeTuple* tup_;

    enum State {
        kExpectObjectStart,
        kExpectAttrNameOrObjectEnd,
        kExpectId,
        kExpectTimestamp,
        kExpectLoad,
        kExpectLoadAvg1,
        kExpectLoadAvg5,
        kExpectLoadAvg15,
        kExpectContainerId,
        kInvalid,
    };

    State state_{kExpectObjectStart};
};

IMPL_VISIBILITY void serialize_json(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local fmt::memory_buffer local_buffer;
    local_buffer.clear();

    // clang-format off
    fmt::format_to(std::back_inserter(local_buffer), FMT_COMPILE(R"({{
"container_id": "{:02x}"
}}
)"),
        fmt::join(tup.container_id, "")
    );
    // clang-format on

    local_buffer.push_back('\0');

    const auto old_size = buf->size();
    buf->resize(old_size + local_buffer.size());
    std::copy(begin(local_buffer), end(local_buffer),
              reinterpret_cast<char*>(buf->data() + old_size));
}

IMPL_VISIBILITY bool parse_rapidjson(const std::byte* __restrict__ read_ptr,
                                     tuple_size_t tup_size,
                                     NativeTuple* tup) noexcept {
    rapidjson::Document d;

    if (unlikely(read_ptr[tup_size - 1] != std::byte{0b0})) {
        return false;
    }

    d.Parse(reinterpret_cast<const char*>(read_ptr));

    if (unlikely(d.HasParseError() || !d["id"].IsUint64() || !d["timestamp"].IsUint64() ||
                 !d["load"].IsFloat() || !d["load_avg_1"].IsFloat() || !d["load_avg_5"].IsFloat() ||
                 !d["load_avg_15"].IsFloat() || !d["container_id"].IsString())) {
        return false;
    }

    const char* container_id_begin = d["container_id"].GetString();
    const char* container_id_end = container_id_begin + d["container_id"].GetStringLength();
    auto result = tup->set_container_id_from_hex_string(container_id_begin, container_id_end);
    return likely(result.ec == std::errc() && result.ptr == container_id_end);
}

IMPL_VISIBILITY bool parse_rapidjson_insitu(const std::byte* __restrict__ read_ptr,
                                            tuple_size_t tup_size,
                                            NativeTuple* tup) noexcept {
    rapidjson::Document d;

    if (unlikely(read_ptr[tup_size - 1] != std::byte{0b0})) {
        return false;
    }

    std::array<std::byte, 256 + 64> local_buffer{};
    assert(tup_size <= local_buffer.size());

    std::copy_n(read_ptr, local_buffer.size(), local_buffer.data());

    d.ParseInsitu(reinterpret_cast<char*>(local_buffer.data()));

    if (unlikely(d.HasParseError() || !d["id"].IsUint64() || !d["timestamp"].IsUint64() ||
                 !d["load"].IsFloat() || !d["load_avg_1"].IsFloat() || !d["load_avg_5"].IsFloat() ||
                 !d["load_avg_15"].IsFloat() || !d["container_id"].IsString())) {
        return false;
    }

    const char* container_id_begin = d["container_id"].GetString();
    const char* container_id_end = container_id_begin + d["container_id"].GetStringLength();
    auto result = tup->set_container_id_from_hex_string(container_id_begin, container_id_end);
    return likely(result.ec == std::errc() && result.ptr == container_id_end);
}

IMPL_VISIBILITY bool parse_rapidjson_sax(const std::byte* __restrict__ read_ptr,
                                         tuple_size_t tup_size,
                                         NativeTuple* tup) noexcept {
    rapidjson::Reader reader;
    NativeTupleHandler handler{tup};

    if (unlikely(read_ptr[tup_size - 1] != std::byte{0b0})) {
        return false;
    }
    rapidjson::StringStream ss(reinterpret_cast<const char*>(read_ptr));

    return likely(reader.Parse(ss, handler) != nullptr);
}

IMPL_VISIBILITY bool parse_simdjson(const std::byte* __restrict__ read_ptr,
                                    tuple_size_t tup_size,
                                    NativeTuple* tup) {
    static thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string_view s(reinterpret_cast<const char*>(read_ptr), tup_size - 2,
                                         tup_size + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document d = parser.iterate(s);

    std::string_view container_id_view = d["container_id"].get_string();
    auto result = tup->set_container_id_from_hex_string(
        container_id_view.data(), container_id_view.data() + container_id_view.size());

    if (unlikely(result.ec != std::errc() ||
                 result.ptr != container_id_view.data() + container_id_view.size())) {
        throw std::invalid_argument("container_id");
    }

    return true;
}

IMPL_VISIBILITY bool parse_simdjson_out_of_order(const std::byte* __restrict__ read_ptr,
                                                 tuple_size_t tup_size,
                                                 NativeTuple* tup) {
    static thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string_view s(reinterpret_cast<const char*>(read_ptr), tup_size - 2,
                                         tup_size + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document d = parser.iterate(s);

    std::string_view container_id_view = d["container_id"].get_string();
    auto result = tup->set_container_id_from_hex_string(
        container_id_view.data(), container_id_view.data() + container_id_view.size());

    if (unlikely(result.ec != std::errc() ||
                 result.ptr != container_id_view.data() + container_id_view.size())) {
        throw std::invalid_argument("container_id");
    }

    return true;
}

IMPL_VISIBILITY bool parse_simdjson_error_codes(const std::byte* __restrict__ read_ptr,
                                                tuple_size_t tup_size,
                                                NativeTuple* tup) noexcept {
    static thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string_view s(reinterpret_cast<const char*>(read_ptr), tup_size - 2,
                                         tup_size + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document d;
    if (unlikely(parser.iterate(s).get(d) != 0U)) {
        return false;
    }

    std::string_view container_id_view;
    bool error = d["container_id"].get_string().get(container_id_view);

    auto result = tup->set_container_id_from_hex_string(
        container_id_view.data(), container_id_view.data() + container_id_view.size());

    error |= result.ec != std::errc() ||
             result.ptr != container_id_view.data() + container_id_view.size();
    return likely(!error);
}

IMPL_VISIBILITY bool parse_simdjson_error_codes_early(const std::byte* __restrict__ read_ptr,
                                                      tuple_size_t tup_size,
                                                      NativeTuple* tup) noexcept {
    static thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string_view s(reinterpret_cast<const char*>(read_ptr), tup_size - 2,
                                         tup_size + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document d;
    if (unlikely(parser.iterate(s).get(d) != 0U)) {
        return false;
    }

    std::string_view container_id_view;
    // clang-format off

    if (unlikely(d["container_id"].get_string().get(container_id_view) != 0U)) { return false; }
    // clang-format on

    auto result = tup->set_container_id_from_hex_string(
        container_id_view.data(), container_id_view.data() + container_id_view.size());

    return likely(result.ec == std::errc() &&
                  result.ptr == container_id_view.data() + container_id_view.size());
}

IMPL_VISIBILITY bool parse_simdjson_unescaped(const std::byte* __restrict__ read_ptr,
                                              tuple_size_t tup_size,
                                              NativeTuple* tup) {
    static thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string_view s(reinterpret_cast<const char*>(read_ptr), tup_size - 2,
                                         tup_size + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document d = parser.iterate(s);

    for (auto field : d.get_object()) {
        std::string_view key = field.unescaped_key();
        if (key == "container_id"sv) {
            std::string_view container_id_view = field.value();
            auto result = tup->set_container_id_from_hex_string(
                container_id_view.data(), container_id_view.data() + container_id_view.size());
            if (unlikely(result.ec != std::errc() ||
                         result.ptr != container_id_view.data() + container_id_view.size())) {
                throw std::invalid_argument("container_id");
            }
        }
    }

    return true;
}

// clang-format off
template void generate_tuples<serialize_json>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);

template void parse_tuples<parse_rapidjson>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_rapidjson_insitu>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_rapidjson_sax>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);

template void parse_tuples<parse_simdjson>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_simdjson_out_of_order>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_simdjson_error_codes>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_simdjson_error_codes_early>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
template void parse_tuples<parse_simdjson_unescaped>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes, const std::atomic<bool>& stop_flag);
