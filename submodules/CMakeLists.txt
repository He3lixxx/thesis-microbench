set(RAPIDJSON_BUILD_TESTS OFF)
set(RAPIDJSON_BUILD_DOC OFF)
set(RAPIDJSON_BUILD_EXAMPLES OFF)
set(RAPIDJSON_BUILD_CXX11 OFF)
set(RAPIDJSON_BUILD_CXX17 ON)
add_subdirectory(rapidjson)

add_subdirectory(cxxopts)

add_subdirectory(fmt)
