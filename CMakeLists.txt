cmake_minimum_required(VERSION 3.16)
project(asyncmux LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(ENABLE_SANITIZERS "Enable Address/Undefined sanitizers" OFF)

set(CPPCORO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/cppcoro" CACHE PATH "Path to cppcoro source")
if(NOT EXISTS "${CPPCORO_ROOT}/include/cppcoro/task.hpp")
    message(FATAL_ERROR "cppcoro headers not found. Initialize submodule: git submodule update --init --recursive")
endif()

add_library(cppcoro_static_thread_pool STATIC
    ${CPPCORO_ROOT}/lib/auto_reset_event.cpp
    ${CPPCORO_ROOT}/lib/lightweight_manual_reset_event.cpp
    ${CPPCORO_ROOT}/lib/spin_mutex.cpp
    ${CPPCORO_ROOT}/lib/spin_wait.cpp
    ${CPPCORO_ROOT}/lib/static_thread_pool.cpp
)

target_include_directories(cppcoro_static_thread_pool
    PUBLIC
        ${CPPCORO_ROOT}/include
)

find_package(Threads REQUIRED)
target_link_libraries(cppcoro_static_thread_pool PUBLIC Threads::Threads)

function(add_asyncmux_test target source_file)
    add_executable(${target}
        ${source_file}
        amux/metadata_store.cc
        amux/placement_policy.cc
        amux/tier.cc
        amux/asyncmux.cc
        amux/tiers/filesystem_tier.cc
    )

    target_include_directories(${target}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
    )

    if(ENABLE_SANITIZERS)
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endif()

    target_link_libraries(${target} PRIVATE cppcoro_static_thread_pool)
endfunction()

file(GLOB TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cc)

foreach(test_source ${TEST_SOURCES})
    get_filename_component(test_target ${test_source} NAME_WE)
    add_asyncmux_test(${test_target} ${test_source})
endforeach()
