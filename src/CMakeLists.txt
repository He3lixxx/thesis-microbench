add_executable(bench bench.cpp)

find_package(RapidJSON CONFIG REQUIRED)
target_link_libraries(bench cxxopts::cxxopts fmt::fmt rapidjson fast_float simdjson)

set_target_properties(bench PROPERTIES RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
