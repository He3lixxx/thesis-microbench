set(RAPIDJSON_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(RAPIDJSON_BUILD_DOC OFF CACHE BOOL "" FORCE)
set(RAPIDJSON_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(RAPIDJSON_BUILD_CXX11 OFF CACHE BOOL "" FORCE)
set(RAPIDJSON_BUILD_CXX17 ON CACHE BOOL "" FORCE)
add_subdirectory(rapidjson)

add_subdirectory(cxxopts)

add_subdirectory(fmt)

add_subdirectory(fast_float)

add_subdirectory(simdjson)

add_subdirectory(flatbuffers ${CMAKE_BINARY_DIR}/flatbuffers-build EXCLUDE_FROM_ALL)
