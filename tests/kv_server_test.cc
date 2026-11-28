#include "raftkv/server/kv_server.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/net/event_loop.h"
#include "raftkv/rpc/rpc_client.h"
#include "rpc_kv.pb.h"

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-kv-server-XXXXXX";
    const char* created = ::mkdtemp(pattern);
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed");
    }
    path_ = created;
  }

  ~TempDir() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  std::string Path(const std::string& name) const {
    return path_ + "/" + name;
  }

 private:
  std::string path_;
};

uint16_t ReserveUnusedPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return 0;
  }
  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address),
                    &length) != 0) {
    ::close(fd);
    return 0;
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

KVServerOptions SingleNodeOptions(
    const TempDir& directory, uint32_t node_id = 1) {
  KVServerOptions options;
  options.local_node = {node_id, "127.0.0.1", 0};
  options.listen_ip = "127.0.0.1";
  options.data_directory = directory.Path("node-" +
      std::to_string(node_id));
  options.raft_options.election_timeout_min_ms = 100;
  options.raft_options.election_timeout_max_ms = 200;
  options.raft_options.heartbeat_interval_ms = 30;
  options.raft_options.read_index_timeout_ms = 1000;
  return options;
}

TEST(KVServerTest, ValidatesOptionsAndLifecycle) {
  TempDir directory;
  EventLoop loop;
  std::unique_ptr<KVServer> server;

  KVServerOptions options = SingleNodeOptions(directory);
  EXPECT_EQ(
      KVServer::Create(&loop, options, nullptr).code(),
      Code::kInvalidArgument);

  options.local_node.node_id = 0;
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  options.local_node.host = "bad-host";
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  options.listen_ip = "bad-listen";
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  options.data_directory.clear();
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  options.rpc_io_threads = -1;
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  options.raft_rpc_timeout_ms = 0;
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  options.peers = {{2, "127.0.0.1", 8002}};
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options.local_node.port = 8001;
  options.peers = {{1, "127.0.0.1", 8001}};
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options.peers = {
      {2, "127.0.0.1", 8002},
      {2, "127.0.0.1", 8003}};
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options.peers = {{2, "127.0.0.1", 0}};
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options.peers = {{2, "bad-host", 8002}};
  EXPECT_EQ(
      KVServer::Create(&loop, options, &server).code(),
      Code::kInvalidArgument);

  options = SingleNodeOptions(directory);
  ASSERT_TRUE(
      KVServer::Create(&loop, options, &server).ok());
  EXPECT_EQ(server->Port(), 0);
  ASSERT_TRUE(server->Start().ok());
  EXPECT_NE(server->Port(), 0);
  EXPECT_TRUE(server->GetRaftStatus().running);
  ASSERT_TRUE(server->Stop().ok());
  EXPECT_TRUE(server->Stop().ok());
  EXPECT_EQ(server->Start().code(), Code::kInvalidArgument);
}

TEST(KVServerTest, ThreeNodesServeKvRequestsOverTcp) {
  constexpr uint32_t kNodeCount = 3;
  TempDir directory;
  EventLoop loop;
  std::vector<uint16_t> ports;
  for (uint32_t index = 0; index < kNodeCount; ++index) {
    const uint16_t port = ReserveUnusedPort();
    ASSERT_NE(port, 0);
    ports.push_back(port);
  }
  for (uint32_t left = 0; left < kNodeCount; ++left) {
    for (uint32_t right = left + 1; right < kNodeCount; ++right) {
      ASSERT_NE(ports[left], ports[right]);
    }
  }

  std::vector<std::unique_ptr<KVServer>> servers;
  for (uint32_t index = 0; index < kNodeCount; ++index) {
    const uint32_t node_id = index + 1;
    KVServerOptions options = SingleNodeOptions(directory, node_id);
    options.local_node.port = ports[index];
    options.data_directory =
        directory.Path("cluster-node-" + std::to_string(node_id));
    options.raft_options.random_seed = 100 + node_id;
    for (uint32_t peer_index = 0;
         peer_index < kNodeCount; ++peer_index) {
      if (peer_index == index) {
        continue;
      }
      options.peers.push_back(
          {peer_index + 1, "127.0.0.1", ports[peer_index]});
    }
    std::unique_ptr<KVServer> server;
    ASSERT_TRUE(
        KVServer::Create(&loop, std::move(options), &server).ok());
    servers.push_back(std::move(server));
  }
  for (auto& server : servers) {
    ASSERT_TRUE(server->Start().ok());
  }

  std::unique_ptr<RpcClient> leader_client;
  std::unique_ptr<RpcClient> follower_client;
  uint32_t leader_id = 0;
  uint32_t follower_id = 0;
  bool finished = false;
  std::string failure;
  std::string stage = "waiting for initial leader";

  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    if (leader_client != nullptr) {
      leader_client->Disconnect();
    }
    if (follower_client != nullptr) {
      follower_client->Disconnect();
    }
    for (auto& server : servers) {
      server->Stop();
    }
    loop.Quit();
  };
  const auto fail = [&](const std::string& message) {
    if (failure.empty()) {
      failure = message;
    }
    finish();
  };

  std::vector<std::function<void()>> steps;
  size_t next_step = 0;
  std::function<void()> proceed;

  auto parse_response =
      [&](Status status, const RpcFrame& frame,
          auto* response) {
        return status.ok() &&
               response->ParseFromString(frame.payload);
      };

  steps.push_back([&] {
    proto::KvGetRequest request;
    request.set_key("hello");
    request.set_linearizable(true);
    follower_client->Call(
        kKvGetRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvGetResponse response;
          if (!parse_response(status, frame, &response)) {
            fail("follower get did not return a business response");
            return;
          }
          if (response.status() != proto::STATUS_CODE_NOT_LEADER ||
              response.leader_hint() !=
                  "127.0.0.1:" + std::to_string(ports[leader_id - 1])) {
            fail("follower get missing NotLeader/leader hint");
            return;
          }
          proceed();
        },
        2000);
  });

  steps.push_back([&] {
    proto::KvPutRequest request;
    request.set_client_id(100);
    request.set_seq_num(1);
    request.set_key("hello");
    request.set_value("world");
    leader_client->Call(
        kKvPutRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvPutResponse response;
          if (!parse_response(status, frame, &response) ||
              response.status() != proto::STATUS_CODE_OK) {
            fail("put hello failed");
            return;
          }
          proceed();
        },
        2000);
  });

  steps.push_back([&] {
    proto::KvPutRequest request;
    request.set_client_id(100);
    request.set_seq_num(1);
    request.set_key("hello");
    request.set_value("must-not-win");
    leader_client->Call(
        kKvPutRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvPutResponse response;
          if (!parse_response(status, frame, &response) ||
              response.status() != proto::STATUS_CODE_OK) {
            fail("duplicate put failed");
            return;
          }
          proceed();
        },
        2000);
  });

  steps.push_back([&] {
    proto::KvGetRequest request;
    request.set_key("hello");
    request.set_linearizable(true);
    leader_client->Call(
        kKvGetRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvGetResponse response;
          if (!parse_response(status, frame, &response) ||
              response.status() != proto::STATUS_CODE_OK ||
              response.value() != "world") {
            fail("duplicate put overwrote the first value");
            return;
          }
          proceed();
        },
        2000);
  });

  const auto append_put =
      [&](uint64_t seq_num, const std::string& key,
          const std::string& value) {
        steps.push_back([&, seq_num, key, value] {
          proto::KvPutRequest request;
          request.set_client_id(101);
          request.set_seq_num(seq_num);
          request.set_key(key);
          request.set_value(value);
          leader_client->Call(
              kKvPutRpcMethod, request,
              [&](Status status, const RpcFrame& frame) {
                proto::KvPutResponse response;
                if (!parse_response(status, frame, &response) ||
                    response.status() != proto::STATUS_CODE_OK) {
                  fail("scan fixture put failed");
                  return;
                }
                proceed();
              },
              2000);
        });
      };
  append_put(1, "a1", "v1");
  append_put(2, "a2", "v2");
  append_put(3, "b1", "v3");

  steps.push_back([&] {
    proto::KvScanRequest request;
    request.set_start_key("a");
    request.set_end_key("b");
    request.set_limit(1);
    leader_client->Call(
        kKvScanRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvScanResponse response;
          if (!parse_response(status, frame, &response) ||
              response.status() != proto::STATUS_CODE_OK ||
              response.entries_size() != 1 ||
              response.entries(0).key() != "a1" ||
              !response.has_more()) {
            fail("scan limit/has_more mismatch");
            return;
          }
          proceed();
        },
        2000);
  });

  steps.push_back([&] {
    proto::KvDeleteRequest request;
    request.set_client_id(100);
    request.set_seq_num(2);
    request.set_key("hello");
    leader_client->Call(
        kKvDeleteRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvDeleteResponse response;
          if (!parse_response(status, frame, &response) ||
              response.status() != proto::STATUS_CODE_OK) {
            fail("delete hello failed");
            return;
          }
          proceed();
        },
        2000);
  });

  steps.push_back([&] {
    proto::KvGetRequest request;
    request.set_key("hello");
    request.set_linearizable(true);
    leader_client->Call(
        kKvGetRpcMethod, request,
        [&](Status status, const RpcFrame& frame) {
          proto::KvGetResponse response;
          if (!parse_response(status, frame, &response) ||
              response.status() != proto::STATUS_CODE_NOT_FOUND) {
            fail("deleted key was still visible");
            return;
          }
          proceed();
        },
        2000);
  });

  const std::vector<std::string> step_names = {
      "follower linearizable get",
      "initial put",
      "duplicate put",
      "linearizable get after duplicate",
      "scan fixture put a1",
      "scan fixture put a2",
      "scan fixture put b1",
      "bounded scan",
      "delete",
      "get deleted key"};
  proceed = [&] {
    if (next_step == steps.size()) {
      finish();
      return;
    }
    const size_t current = next_step++;
    stage = step_names[current];
    steps[current]();
  };

  auto leader_timer = std::make_shared<TimerId>(0);
  *leader_timer = loop.RunEvery(
      10, [&, leader_timer] {
        if (finished) {
          return;
        }
        for (uint32_t index = 0; index < kNodeCount; ++index) {
          const RaftNodeStatus status =
              servers[index]->GetRaftStatus();
          if (status.role == Role::kLeader) {
            leader_id = index + 1;
            break;
          }
        }
        if (leader_id == 0) {
          return;
        }
        stage = "waiting for followers to observe leader";
        for (uint32_t index = 0; index < kNodeCount; ++index) {
          if (index + 1 == leader_id) {
            continue;
          }
          if (servers[index]->GetRaftStatus().leader_id != leader_id) {
            return;
          }
          follower_id = index + 1;
        }
        if (follower_id == 0) {
          return;
        }

        stage = "connecting clients";
        loop.CancelTimer(*leader_timer);
        leader_client = std::make_unique<RpcClient>(
            &loop, "kv-leader-client", "127.0.0.1",
            ports[leader_id - 1]);
        follower_client = std::make_unique<RpcClient>(
            &loop, "kv-follower-client", "127.0.0.1",
            ports[follower_id - 1]);
        leader_client->Connect();
        follower_client->Connect();

        auto connected_timer = std::make_shared<TimerId>(0);
        *connected_timer = loop.RunEvery(
            10, [&, connected_timer] {
              if (!leader_client->Connected() ||
                  !follower_client->Connected()) {
                return;
              }
              loop.CancelTimer(*connected_timer);
              stage = "ready to run kv steps";
              proceed();
            });
      });

  loop.RunAfter(20000, [&] {
    std::ostringstream message;
    message << "three-node kv server test timed out at " << stage;
    for (uint32_t index = 0; index < kNodeCount; ++index) {
      const RaftNodeStatus status =
          servers[index]->GetRaftStatus();
      message << " node" << index + 1
              << "{role=" << static_cast<int>(status.role)
              << ",term=" << status.current_term
              << ",leader=" << status.leader_id
              << ",commit=" << status.commit_index
              << ",applied=" << status.last_applied << "}";
    }
    fail(message.str());
  });
  loop.Loop();

  for (auto& server : servers) {
    server->Stop();
  }
  EXPECT_TRUE(failure.empty()) << failure;
}

}  // namespace
}  // namespace raftkv
