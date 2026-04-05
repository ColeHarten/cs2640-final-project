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

add_executable(asyncmux_tests
    test.cc
    amux/asyncmux.cc
    amux/tiers/memory_tier.cc
    amux/tiers/disk_tier.cc
)

target_include_directories(asyncmux_tests
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
)

if(ENABLE_SANITIZERS)
    target_compile_options(asyncmux_tests PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(asyncmux_tests PRIVATE -fsanitize=address,undefined)
endif()

target_link_libraries(asyncmux_tests PRIVATE cppcoro_static_thread_pool)
