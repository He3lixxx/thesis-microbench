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
#warning "Using custom std::find_if implementation for rapidjson sax"
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


IMPL_VISIBILITY void serialize_json(const NativeTuple& tup, std::vector<std::byte>* buf) {
    // TODO: What happens if the tuples have different layout? Does any kind of prediction get
    // worse?

    thread_local fmt::memory_buffer local_buffer;
    local_buffer.clear();

    // clang-format off
    fmt::format_to(std::back_inserter(local_buffer), FMT_COMPILE(R"({{
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

    local_buffer.push_back('\0');

    const auto old_size = buf->size();
    buf->resize(old_size + local_buffer.size());
    std::copy(begin(local_buffer), end(local_buffer), reinterpret_cast<char*>(buf->data() + old_size));
}

IMPL_VISIBILITY bool parse_rapidjson(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    // TODO: Would make sense to re-use this, but that will leak memory(?)
    rapidjson::Document d;

    if(read_ptr[tup_size - 1] != std::byte{0b0}) {
        return false;
    }

    // TODO: Insitu-Parsing?
    d.Parse(reinterpret_cast<const char*>(read_ptr));

    if (d.HasParseError() || !d["id"].IsUint64() || !d["timestamp"].IsUint64() || !d["load"].IsFloat() ||
        !d["load_avg_1"].IsFloat() || !d["load_avg_5"].IsFloat() || !d["load_avg_15"].IsFloat() ||
        !d["container_id"].IsString()) {
        return false;
    }

    tup->id = d["id"].GetUint64();
    tup->timestamp = d["timestamp"].GetUint64();
    tup->load = d["load"].GetFloat();
    tup->load_avg_1 = d["load_avg_1"].GetFloat();
    tup->load_avg_5 = d["load_avg_5"].GetFloat();
    tup->load_avg_15 = d["load_avg_15"].GetFloat();
    tup->set_container_id_from_hex_string(d["container_id"].GetString(),
                                          d["container_id"].GetStringLength());

    return true;
}

IMPL_VISIBILITY bool parse_rapidjson_sax(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    rapidjson::Reader reader;
    NativeTupleHandler handler{tup};

    if(read_ptr[tup_size - 1] != std::byte{0b0}) {
        return false;
    }
    rapidjson::StringStream ss(reinterpret_cast<const char*>(read_ptr));

    if(!reader.Parse(ss, handler)){
        return false;
    }

    return true;
}

IMPL_VISIBILITY bool parse_simdjson(const std::byte* __restrict__ read_ptr, tuple_size_t tup_size, NativeTuple* tup) {
    static thread_local simdjson::ondemand::parser parser;

    if(read_ptr[tup_size - 1] != std::byte{0b0}) {
        return false;
    }
    // TODO: Padding als gegeben ansehen?
    const simdjson::padded_string s(reinterpret_cast<const char*>(read_ptr), tup_size - 2);
    simdjson::ondemand::document d;
    auto error = parser.iterate(s).get(d);
    if(error) {
        return false;
    }

    tup->id = d["id"].get_uint64();
    tup->timestamp = d["timestamp"].get_uint64();
    tup->load = static_cast<float>(d["load"].get_double());
    tup->load_avg_1 = static_cast<float>(d["load_avg_1"].get_double());
    tup->load_avg_5 = static_cast<float>(d["load_avg_5"].get_double());
    tup->load_avg_15 = static_cast<float>(d["load_avg_15"].get_double());

    const std::string_view container_id_view = d["container_id"].get_string();
    tup->set_container_id_from_hex_string(container_id_view.data(), container_id_view.length());

    return true;
}

// clang-format off
template void generate_tuples<serialize_json>(std::vector<std::byte>* memory, size_t target_memory_size, std::vector<tuple_size_t>* tuple_sizes, std::mutex* mutex);
template void parse_tuples<parse_rapidjson>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
template void parse_tuples<parse_rapidjson_sax>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
template void parse_tuples<parse_simdjson>(ThreadResult* result, const std::vector<std::byte>& memory, const std::vector<tuple_size_t>& tuple_sizes);
