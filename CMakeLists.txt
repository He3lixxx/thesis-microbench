cmake_minimum_required(VERSION 3.13)
project(ThesisMicroBenchmarks CXX)

# https://stackoverflow.com/a/64294837/12345551
get_property(isMultiConfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)

if(isMultiConfig)
    if(NOT "Asan" IN_LIST CMAKE_CONFIGURATION_TYPES)
        list(APPEND CMAKE_CONFIGURATION_TYPES Asan)
    endif()
    if(NOT "Tsan" IN_LIST CMAKE_CONFIGURATION_TYPES)
        list(APPEND CMAKE_CONFIGURATION_TYPES Tsan)
    endif()
else()
    set(CONFIGURATIONS Release RelWithDebInfo Asan Tsan Debug)

    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "${CONFIGURATIONS}")

    if(CMAKE_BUILD_TYPE AND NOT CMAKE_BUILD_TYPE IN_LIST CONFIGURATIONS)
        message(FATAL_ERROR "Invalid build type: ${CMAKE_BUILD_TYPE}")
    endif()
endif()

# Set Optimization Flags
set(CMAKE_CXX_FLAGS_DEBUG "-Og -g -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -fno-omit-frame-pointer -DNDEBUG")

set(CMAKE_CXX_FLAGS_ASAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler for Asan build type or configuration." FORCE)

set(CMAKE_EXE_LINKER_FLAGS_ASAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined" CACHE STRING
    "Linker flags to be used to create executables for Asan build type." FORCE)

set(CMAKE_SHARED_LINKER_FLAGS_ASAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address -fsanitize=undefined" CACHE STRING
    "Linker lags to be used to create shared libraries for Asan build type." FORCE)

set(CMAKE_CXX_FLAGS_TSAN
    "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=thread -fsanitize=undefined -fno-omit-frame-pointer" CACHE STRING
    "Flags used by the C++ compiler for Tsan build type or configuration." FORCE)

set(CMAKE_EXE_LINKER_FLAGS_TSAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=thread -fsanitize=undefined" CACHE STRING
    "Linker flags to be used to create executables for Tsan build type." FORCE)

set(CMAKE_SHARED_LINKER_FLAGS_TSAN
    "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=thread -fsanitize=undefined" CACHE STRING
    "Linker lags to be used to create shared libraries for Tsan build type." FORCE)

# Check if build type is set
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING
            "Choose build type: Debug or Release."
            FORCE
            )
endif (NOT CMAKE_BUILD_TYPE)

function (exclude_items aItems aRegEx)
    foreach (item ${${aItems}})
        if ("${item}" MATCHES ${aRegEx})
            list (REMOVE_ITEM ${aItems} ${item})
        endif ("${item}" MATCHES ${aRegEx})
    endforeach(item)
    set(${aItems} ${${aItems}} PARENT_SCOPE)
endfunction (exclude_items)

file(GLOB_RECURSE ALL_SOURCE_FILES src/*.cpp src/*.hpp)
exclude_items(ALL_SOURCE_FILES ".*submodules/.*")

file(GLOB_RECURSE ALL_CPP_FILES src/*.cpp)
exclude_items(ALL_CPP_FILES ".*submodules/.*")

add_custom_target(clang-format COMMAND clang-format -style=file -i${INCLUDE_DIRECTORIES} ${ALL_SOURCE_FILES})
add_custom_target(clang-tidy COMMAND run-clang-tidy-13 -quiet ${ALL_CPP_FILES})
add_custom_target(cppcheck COMMAND cppcheck --enable=all --quiet --project=compile_commands.json --platform=unix64
    -DINTPTR_MAX=9223372036854775807LL --max-configs=32 --suppress="*:*submodules/*"
    --suppress=unmatchedSuppression --suppress=missingIncludeSystem --suppress=shadowFunction --inline-suppr)
add_custom_target(cpplint COMMAND cpplint --linelength=120 --quiet ${ALL_SOURCE_FILES})

add_custom_target(format DEPENDS clang-format)
add_custom_target(tidy DEPENDS clang-tidy)

add_custom_target(lint DEPENDS cppcheck cpplint clang-tidy)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_options("-Wall")
add_compile_options("-Wextra")
add_compile_options("-march=native")
add_compile_options("-mtune=native")

add_subdirectory(submodules)

add_subdirectory(src)
