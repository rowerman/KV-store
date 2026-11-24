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

add_executable(smoke_test tests/smoke_test.cc)
target_link_libraries(smoke_test PRIVATE
  raftkv_build_options
  GTest::gtest_main
  protobuf::libprotobuf
  Threads::Threads
)

if(RAFTKV_TSAN_TEST_LAUNCHER)
  set_property(TARGET smoke_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
endif()

include(GoogleTest)
gtest_discover_tests(smoke_test)
