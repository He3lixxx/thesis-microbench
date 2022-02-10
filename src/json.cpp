#include <fmt/compile.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <simdjson.h>
#include <algorithm>
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
    explicit NativeTupleHandler(NativeTuple* tup) : tup_(tup), state_(kExpectObjectStart) {}

    bool StartObject() {
        if (state_ != kExpectObjectStart) {
            return false;
        }
        state_ = kExpectAttrNameOrObjectEnd;
        return true;
    }

    bool Key(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        static constexpr KeyStateMap map;
        switch (state_) {
            case kExpectAttrNameOrObjectEnd:
                state_ = map.get(std::string_view(str, length));
                return state_ != kInvalid;
            default:
                return false;
        }
    }

    bool Double(double value) {
        switch (state_) {
            case kExpectLoad:
                tup_->load = value;
                state_ = kExpectAttrNameOrObjectEnd;
                return true;
            case kExpectLoadAvg1:
                tup_->load_avg_1 = value;
                state_ = kExpectAttrNameOrObjectEnd;
                return true;
            case kExpectLoadAvg5:
                tup_->load_avg_5 = value;
                state_ = kExpectAttrNameOrObjectEnd;
                return true;
            case kExpectLoadAvg15:
                tup_->load_avg_15 = value;
                state_ = kExpectAttrNameOrObjectEnd;
                return true;
            default:
                return false;
        }
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
        if (state_ != kExpectContainerId) {
            return false;
        }
        tup_->set_container_id_from_hex_string(str, length);
        state_ = kExpectAttrNameOrObjectEnd;
        return true;
    }

    bool EndObject(rapidjson::SizeType) { return state_ == kExpectAttrNameOrObjectEnd; }

    bool Default() { return false; }

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

    State state_;

    // https://www.youtube.com/watch?v=INn3xa4pMfg
    struct KeyStateMap {
        std::array<std::pair<std::string_view, State>, 7> data = {{
            {"id"sv, kExpectId},
            {"timestamp"sv, kExpectTimestamp},
            {"load"sv, kExpectLoad},
            {"load_avg_1"sv, kExpectLoadAvg1},
            {"load_avg_5"sv, kExpectLoadAvg5},
            {"load_avg_15"sv, kExpectLoadAvg15},
            {"container_id"sv, kExpectContainerId},
        }};

        [[nodiscard]] constexpr State get(const std::string_view& key) const noexcept {
#if FIND_IS_CONSTEXPR
            const auto it = std::find_if(begin(data), end(data),
                                          [&key](const auto& el) { return el.first == key; });
#else
            auto it = begin(data);
            auto end_it = end(data);
            while(it != end_it && it->first != key)
                ++it;
#endif
            if (it == end(data)) {
                return kInvalid;
            }
            return it->second;
        }
    };
};


void serialize_json(const NativeTuple& tup, fmt::memory_buffer* buf) {
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

size_t parse_rapidjson(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    // TODO: Would make sense to re-use this, but that will leak memory(?)
    rapidjson::Document d;

    // TODO: Insitu-Parsing?
    d.Parse(reinterpret_cast<const char*>(read_ptr));

    if (!d["id"].IsUint64() || !d["timestamp"].IsUint64() || !d["load"].IsFloat() ||
        !d["load_avg_1"].IsFloat() || !d["load_avg_5"].IsFloat() || !d["load_avg_15"].IsFloat() ||
        !d["container_id"].IsString()) {
        throw std::runtime_error("Invalid input tuple");
    }

    tup->id = d["id"].GetUint64();
    tup->timestamp = d["timestamp"].GetUint64();
    tup->load = d["load"].GetFloat();
    tup->load_avg_1 = d["load_avg_1"].GetFloat();
    tup->load_avg_5 = d["load_avg_5"].GetFloat();
    tup->load_avg_15 = d["load_avg_15"].GetFloat();
    tup->set_container_id_from_hex_string(d["container_id"].GetString(),
                                          d["container_id"].GetStringLength());

    const auto tuple_bytes = std::strlen(reinterpret_cast<const char*>(read_ptr)) + 1;
    return tuple_bytes;
}

size_t parse_rapidjson_sax(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    const auto tuple_bytes = std::strlen(reinterpret_cast<const char*>(read_ptr)) + 1;

    rapidjson::Reader reader;
    NativeTupleHandler handler{tup};
    rapidjson::StringStream ss(reinterpret_cast<const char*>(read_ptr));
    reader.Parse(ss, handler);
    // TODO: Error handling here and for other handlers (real system would need that, too?)
    // if(!reader.Parse(ss, handler)){
    //     rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
    //     size_t o = reader.GetErrorOffset();
    //     fmt::print("Error: {}\n", rapidjson::GetParseError_En(e));
    //     fmt::print("At offset {} near {}...\n", o, std::string_view(reinterpret_cast<const char*>(read_ptr)).substr(o, 10));
    // }

    return tuple_bytes;
}

size_t parse_simdjson(const std::byte* __restrict__ read_ptr, NativeTuple* tup) {
    static thread_local simdjson::ondemand::parser parser;
    const auto length = std::strlen(reinterpret_cast<const char*>(read_ptr));

    const simdjson::padded_string s(reinterpret_cast<const char*>(read_ptr), length);
    simdjson::ondemand::document d = parser.iterate(s);

    tup->id = d["id"].get_uint64();
    tup->timestamp = d["timestamp"].get_uint64();
    tup->load = static_cast<float>(d["load"].get_double());
    tup->load_avg_1 = static_cast<float>(d["load_avg_1"].get_double());
    tup->load_avg_5 = static_cast<float>(d["load_avg_5"].get_double());
    tup->load_avg_15 = static_cast<float>(d["load_avg_15"].get_double());

    const std::string_view container_id_view = d["container_id"].get_string();
    tup->set_container_id_from_hex_string(container_id_view.data(), container_id_view.length());

    return length + 1;
}

template void fill_memory<serialize_json>(std::atomic<std::byte*>*,
                                          const std::byte* const,
                                          uint64_t*);
template void thread_func<parse_rapidjson>(ThreadResult*, const std::vector<std::byte>&);
template void thread_func<parse_rapidjson_sax>(ThreadResult*, const std::vector<std::byte>&);
template void thread_func<parse_simdjson>(ThreadResult*, const std::vector<std::byte>&);
