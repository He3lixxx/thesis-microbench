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
        if (key == "id"sv) { state_ = kExpectId;
        } else if (key == "timestamp"sv) { state_ = kExpectTimestamp;
        } else { return false; }
        // clang-format on

        return true;
    }

    bool Double(double value) {
        return false;
    }

    bool Uint64(uint64_t val) {
        switch (state_) {
            case kExpectId:
                tup_->id = val;
                state_ = kExpectAttrNameOrObjectEnd;
                return true;
            case kExpectTimestamp:
                tup_->timestamp = val;
                state_ = kExpectAttrNameOrObjectEnd;
                return true;
            default:
                return false;
        }
    }

    bool String(const Ch* str, rapidjson::SizeType length, bool /*copy*/) {
        return false;
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
        kInvalid,
    };

    State state_{kExpectObjectStart};
};

IMPL_VISIBILITY void serialize_json(const NativeTuple& tup, std::vector<std::byte>* buf) {
    thread_local fmt::memory_buffer local_buffer;
    local_buffer.clear();

    // clang-format off
    fmt::format_to(std::back_inserter(local_buffer), FMT_COMPILE(R"({{
"id": {},
"timestamp": {}
}}
)"),
        tup.id,
        tup.timestamp
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

    if (unlikely(d.HasParseError() || !d["id"].IsUint64() || !d["timestamp"].IsUint64())) {
        return false;
    }

    tup->id = d["id"].GetUint64();
    tup->timestamp = d["timestamp"].GetUint64();

    return true;
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

    if (unlikely(d.HasParseError() || !d["id"].IsUint64() || !d["timestamp"].IsUint64())) {
        return false;
    }

    tup->id = d["id"].GetUint64();
    tup->timestamp = d["timestamp"].GetUint64();

    return true;
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
    tup->id = d["id"].get_uint64();
    tup->timestamp = d["timestamp"].get_uint64();

    return true;
}

IMPL_VISIBILITY bool parse_simdjson_out_of_order(const std::byte* __restrict__ read_ptr,
                                                 tuple_size_t tup_size,
                                                 NativeTuple* tup) {
    static thread_local simdjson::ondemand::parser parser;
    const simdjson::padded_string_view s(reinterpret_cast<const char*>(read_ptr), tup_size - 2,
                                         tup_size + simdjson::SIMDJSON_PADDING);
    simdjson::ondemand::document d = parser.iterate(s);

    tup->id = d["id"].get_uint64();
    tup->timestamp = d["timestamp"].get_uint64();

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

    bool error = d["id"].get_uint64().get(tup->id) != 0U;
    error |= d["timestamp"].get_uint64().get(tup->timestamp);

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

    // clang-format off
    if (unlikely(d["id"].get_uint64().get(tup->id) != 0U)) { return false; }
    if (unlikely(d["timestamp"].get_uint64().get(tup->timestamp) != 0U)) { return false; }
    // clang-format on

    return true;
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
        if (key == "id"sv) {
            tup->id = field.value();
        } else if (key == "timestamp"sv) {
            tup->timestamp = field.value();
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
