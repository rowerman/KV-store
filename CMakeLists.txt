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

add_library(raftkv_storage STATIC
  src/storage/bloom_filter.cc
  src/storage/compaction.cc
  src/storage/lru_cache.cc
  src/storage/lsm_tree.cc
  src/storage/memtable.cc
  src/storage/merging_iterator.cc
  src/storage/sstable.cc
  src/storage/skiplist.cc
  src/storage/version_set.cc
  src/storage/wal.cc
)
target_link_libraries(raftkv_storage PUBLIC raftkv_common)

add_library(raftkv_net STATIC
  src/net/buffer.cc
  src/net/event_loop.cc
  src/net/poller.cc
  src/net/tcp_client.cc
  src/net/tcp_connection.cc
  src/net/tcp_server.cc
  src/net/timer_queue.cc
)
target_link_libraries(raftkv_net PUBLIC raftkv_common)

# 在构建目录生成 KV 与 Raft 的 Protobuf C++ 源码。
protobuf_generate_cpp(RAFTKV_PROTO_SRCS RAFTKV_PROTO_HEADERS
  proto/rpc_kv.proto
  proto/rpc_raft.proto
)

add_library(raftkv_proto STATIC ${RAFTKV_PROTO_SRCS})
target_include_directories(raftkv_proto PUBLIC
  ${CMAKE_CURRENT_BINARY_DIR}
)
# Protobuf 36 会在 ASan 下切换 ClassData ABI，必须与预编译库保持一致。
target_compile_definitions(raftkv_proto PUBLIC
  PROTOBUF_MESSAGE_GLOBALS_TEMPORARY_OPTOUT
)
target_link_libraries(raftkv_proto PUBLIC
  raftkv_build_options
  protobuf::libprotobuf
)

add_library(raftkv_raft STATIC
  src/raft/raft_log.cc
  src/raft/raft_node.cc
  src/raft/snapshot_bundle.cc
)
target_link_libraries(raftkv_raft PUBLIC
  raftkv_common
  raftkv_proto
)

add_library(raftkv_server_lib STATIC
  src/server/kv_server.cc
  src/server/kv_state_machine.cc
  src/server/real_raft_transport.cc
  src/server/session_table.cc
)
target_link_libraries(raftkv_server_lib PUBLIC
  raftkv_raft
  raftkv_rpc
  raftkv_storage
  raftkv_proto
)

add_executable(raftkv_server
  src/server/main.cc
)
target_link_libraries(raftkv_server PRIVATE
  raftkv_server_lib
)

add_library(raftkv_rpc STATIC
  src/rpc/frame_codec.cc
  src/rpc/rpc_client.cc
  src/rpc/rpc_server.cc
)
target_link_libraries(raftkv_rpc PUBLIC
  raftkv_net
  raftkv_proto
)

add_library(raftkv_client STATIC
  src/client/kv_client.cc
)
target_link_libraries(raftkv_client PUBLIC
  raftkv_rpc
)

add_executable(raftkv_cli
  src/client/cli_main.cc
)
target_link_libraries(raftkv_cli PRIVATE
  raftkv_client
)

add_test(NAME raftkv_cli_help COMMAND raftkv_cli --help)
add_test(NAME raftkv_cli_requires_endpoints COMMAND raftkv_cli)
set_tests_properties(raftkv_cli_requires_endpoints PROPERTIES
  WILL_FAIL TRUE
)

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

add_executable(tcp_connection_test tests/tcp_connection_test.cc)
target_link_libraries(tcp_connection_test PRIVATE
  raftkv_net
  GTest::gtest_main
)

add_executable(tcp_server_test tests/tcp_server_test.cc)
target_link_libraries(tcp_server_test PRIVATE
  raftkv_net
  GTest::gtest_main
)

add_executable(tcp_client_test tests/tcp_client_test.cc)
target_link_libraries(tcp_client_test PRIVATE
  raftkv_net
  GTest::gtest_main
)

add_executable(frame_codec_test tests/frame_codec_test.cc)
target_link_libraries(frame_codec_test PRIVATE
  raftkv_rpc
  GTest::gtest_main
)

add_executable(rpc_test tests/rpc_test.cc)
target_link_libraries(rpc_test PRIVATE
  raftkv_rpc
  GTest::gtest_main
)

add_executable(session_table_test tests/session_table_test.cc)
target_link_libraries(session_table_test PRIVATE
  raftkv_server_lib
  GTest::gtest_main
)

add_executable(kv_state_machine_test tests/kv_state_machine_test.cc)
target_link_libraries(kv_state_machine_test PRIVATE
  raftkv_server_lib
  GTest::gtest_main
)

add_executable(real_raft_transport_test tests/real_raft_transport_test.cc)
target_link_libraries(real_raft_transport_test PRIVATE
  raftkv_server_lib
  GTest::gtest_main
)

add_executable(kv_server_test tests/kv_server_test.cc)
target_link_libraries(kv_server_test PRIVATE
  raftkv_server_lib
  GTest::gtest_main
)

add_executable(kv_client_test tests/kv_client_test.cc)
target_link_libraries(kv_client_test PRIVATE
  raftkv_client
  raftkv_server_lib
  GTest::gtest_main
)

add_executable(memtable_test tests/memtable_test.cc)
target_link_libraries(memtable_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(wal_test tests/wal_test.cc)
target_link_libraries(wal_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(bloom_lru_test tests/bloom_lru_test.cc)
target_link_libraries(bloom_lru_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(sstable_test tests/sstable_test.cc)
target_link_libraries(sstable_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(merging_iterator_test tests/merging_iterator_test.cc)
target_link_libraries(merging_iterator_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(version_set_test tests/version_set_test.cc)
target_link_libraries(version_set_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(lsm_tree_test tests/lsm_tree_test.cc)
target_link_libraries(lsm_tree_test PRIVATE
  raftkv_storage
  GTest::gtest_main
)

add_executable(raft_log_test tests/raft_log_test.cc)
target_link_libraries(raft_log_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_transport_test tests/raft_transport_test.cc)
target_include_directories(raft_transport_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_transport_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_node_test tests/raft_node_test.cc)
target_include_directories(raft_node_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_node_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_election_test tests/raft_election_test.cc)
target_include_directories(raft_election_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_election_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_replication_test tests/raft_replication_test.cc)
target_include_directories(raft_replication_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_replication_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_read_index_test tests/raft_read_index_test.cc)
target_include_directories(raft_read_index_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_read_index_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(snapshot_bundle_test tests/snapshot_bundle_test.cc)
target_link_libraries(snapshot_bundle_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_snapshot_test tests/raft_snapshot_test.cc)
target_include_directories(raft_snapshot_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_snapshot_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

add_executable(raft_conf_change_test tests/raft_conf_change_test.cc)
target_include_directories(raft_conf_change_test PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/tests
)
target_link_libraries(raft_conf_change_test PRIVATE
  raftkv_raft
  GTest::gtest_main
)

if(RAFTKV_TSAN_TEST_LAUNCHER)
  set_property(TARGET raftkv_cli PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
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
  set_property(TARGET tcp_connection_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET tcp_server_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET tcp_client_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET frame_codec_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET rpc_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET session_table_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET kv_state_machine_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET real_raft_transport_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET kv_server_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET kv_client_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET memtable_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET wal_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET bloom_lru_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET sstable_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET merging_iterator_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET version_set_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET lsm_tree_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_log_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_transport_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_node_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_election_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_replication_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_read_index_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET snapshot_bundle_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_snapshot_test PROPERTY CROSSCOMPILING_EMULATOR
    "${RAFTKV_TSAN_TEST_LAUNCHER}"
  )
  set_property(TARGET raft_conf_change_test PROPERTY CROSSCOMPILING_EMULATOR
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
gtest_discover_tests(tcp_connection_test)
gtest_discover_tests(tcp_server_test)
gtest_discover_tests(tcp_client_test)
gtest_discover_tests(frame_codec_test)
gtest_discover_tests(rpc_test)
gtest_discover_tests(session_table_test)
gtest_discover_tests(kv_state_machine_test)
gtest_discover_tests(real_raft_transport_test)
gtest_discover_tests(kv_server_test)
gtest_discover_tests(kv_client_test)
gtest_discover_tests(memtable_test)
gtest_discover_tests(wal_test)
gtest_discover_tests(bloom_lru_test)
gtest_discover_tests(sstable_test)
gtest_discover_tests(merging_iterator_test)
gtest_discover_tests(version_set_test)
gtest_discover_tests(lsm_tree_test)
gtest_discover_tests(raft_log_test)
gtest_discover_tests(raft_transport_test)
gtest_discover_tests(raft_node_test)
gtest_discover_tests(raft_election_test)
gtest_discover_tests(raft_replication_test)
gtest_discover_tests(raft_read_index_test)
gtest_discover_tests(snapshot_bundle_test)
gtest_discover_tests(raft_snapshot_test)
gtest_discover_tests(raft_conf_change_test)
