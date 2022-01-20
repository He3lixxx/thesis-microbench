add_executable(bench bench.cpp native.cpp csv.cpp json.cpp flatbuffer.cpp)

find_package(RapidJSON CONFIG REQUIRED)
target_link_libraries(bench PRIVATE cxxopts::cxxopts fmt::fmt rapidjson fast_float simdjson flatbuffers)

set_target_properties(bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
