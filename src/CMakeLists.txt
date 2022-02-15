find_package(RapidJSON CONFIG REQUIRED)

find_package(Protobuf REQUIRED)
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HEADERS tuple.proto)
message("PROTO SRCS " ${PROTO_SRCS})
message("PROTO HEADERS " ${PROTO_HEADERS})
SET_SOURCE_FILES_PROPERTIES(${PROTO_SRC} ${PROTO_INCL} PROPERTIES GENERATED TRUE)

add_executable(bench bench.cpp native.cpp csv.cpp json.cpp flatbuffer.cpp protobuf.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(bench PRIVATE cxxopts::cxxopts fmt::fmt rapidjson fast_float simdjson flatbuffers protobuf::libprotobuf-lite fast-cpp-csv-parser)
target_include_directories(bench PRIVATE ${Protobuf_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})

set_target_properties(bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
