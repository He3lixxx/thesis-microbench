#include <fmt/compile.h>
#include <fmt/format.h>
#include <rapidjson/document.h>
#include <simdjson.h>
#include <algorithm>
#include <cstdint>

#include "bench.hpp"
#include "json.hpp"

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
template void thread_func<parse_simdjson>(ThreadResult*, const std::vector<std::byte>&);
