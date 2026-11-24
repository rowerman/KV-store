cmake_minimum_required(VERSION 3.16)

if(NOT DEFINED VCPKG_INSTALLED_DIR)
  if(DEFINED ENV{VCPKG_ROOT})
    set(VCPKG_INSTALLED_DIR "$ENV{VCPKG_ROOT}/installed" CACHE PATH
      "The directory which contains vcpkg installations"
    )
  endif()
endif()

set(VCPKG_MANIFEST_INSTALL OFF CACHE BOOL
  "Dependencies are provisioned outside normal configure runs"
)

project(RaftKV VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_ASAN "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(ENABLE_ASAN AND ENABLE_TSAN)
  message(FATAL_ERROR "ENABLE_ASAN and ENABLE_TSAN cannot be enabled together")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo)
endif()

find_package(Protobuf REQUIRED)
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

add_library(raftkv_build_options INTERFACE)
target_compile_options(raftkv_build_options INTERFACE
  -Wall
  -Wextra
  -Wpedantic
)

if(ENABLE_ASAN)
  target_compile_options(raftkv_build_options INTERFACE
    -fsanitize=address,undefined
    -fno-omit-frame-pointer
  )
  target_link_options(raftkv_build_options INTERFACE
    -fsanitize=address,undefined
  )
endif()

if(ENABLE_TSAN)
  target_compile_options(raftkv_build_options INTERFACE
    -fsanitize=thread
    -fno-omit-frame-pointer
  )
  target_link_options(raftkv_build_options INTERFACE
    -fsanitize=thread
  )

  find_program(RAFTKV_SETARCH_EXECUTABLE setarch REQUIRED)
  set(RAFTKV_TSAN_TEST_LAUNCHER
    "${RAFTKV_SETARCH_EXECUTABLE};${CMAKE_SYSTEM_PROCESSOR};-R"
  )
endif()

enable_testing()

add_library(raftkv_common STATIC
  src/common/coding.cc
  src/common/crc32.cc
  src/common/logger.cc
)
target_include_directories(raftkv_common PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(raftkv_common PUBLIC
  raftkv_build_options
  Threads::Threads
)

add_library(raftkv_net STATIC
  src/net/buffer.cc
  src/net/event_loop.cc
  src/net/poller.cc
  src/net/timer_queue.cc
)
target_link_libraries(raftkv_net PUBLIC raftkv_common)

add_executable(smoke_test tests/smoke_test.cc)
target_link_libraries(smoke_test PRIVATE
  raftkv_build_options
  GTest::gtest_main
  protobuf::libprotobuf
  Threads::Threads
)

add_executable(common_test tests/common_test.cc)
target_link_libraries(common_test PRIVATE
  raftkv_common
  GTest::gtest_main
)

add_executable(common_coding_test tests/common_coding_test.cc)
target_link_libraries(common_coding_test PRIVATE
  raftkv_common
  GTest::gtest_main
)

add_executable(common_logger_test tests/common_logger_test.cc)
target_link_libraries(common_logger_test PRIVATE
  raftkv_common
  GTest::gtest_main
)

add_executable(common_mpsc_queue_test tests/common_mpsc_queue_test.cc)
target_link_libraries(common_mpsc_queue_test PRIVATE
  raftkv_common
  GTest::gtest_main
)

add_executable(buffer_test tests/buffer_test.cc)
target_link_libraries(buffer_test PRIVATE
  raftkv_net
  GTest::gtest_main
)

add_executable(poller_test tests/poller_test.cc)
target_link_libraries(poller_test PRIVATE
  raftkv_net
  GTest::gtest_main
)

add_executable(event_loop_test tests/event_loop_test.cc)
target_link_libraries(event_loop_test PRIVATE
  raftkv_net
  GTest::gtest_main
)

if(RAFTKV_TSAN_TEST_LAUNCHER)
  set_property(TARGET smoke_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET common_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET common_coding_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET common_logger_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET common_mpsc_queue_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET buffer_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET poller_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET event_loop_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
endif()

include(GoogleTest)
gtest_discover_tests(smoke_test)
gtest_discover_tests(common_test)
gtest_discover_tests(common_coding_test)
gtest_discover_tests(common_logger_test)
gtest_discover_tests(common_mpsc_queue_test)
gtest_discover_tests(buffer_test)
gtest_discover_tests(poller_test)
gtest_discover_tests(event_loop_test)
