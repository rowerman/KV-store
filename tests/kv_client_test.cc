#include "raftkv/client/kv_client.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/net/event_loop.h"
#include "raftkv/rpc/kv_methods.h"
#include "raftkv/rpc/rpc_server.h"
#include "raftkv/server/kv_server.h"
#include "rpc_kv.pb.h"

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-kv-client-XXXXXX";
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

KVServerOptions ServerOptions(
    const TempDir& directory, uint32_t node_id) {
  KVServerOptions options;
  options.local_node = {node_id, "127.0.0.1", 0};
  options.listen_ip = "127.0.0.1";
  options.data_directory =
      directory.Path("node-" + std::to_string(node_id));
  options.raft_options.election_timeout_min_ms = 100;
  options.raft_options.election_timeout_max_ms = 200;
  options.raft_options.heartbeat_interval_ms = 30;
  options.raft_options.read_index_timeout_ms = 1000;
  return options;
}

KVClientOptions ClientOptions(uint16_t port) {
  KVClientOptions options;
  options.endpoints.push_back({"127.0.0.1", port});
  options.request_timeout_ms = 500;
  options.max_retries = 3;
  options.tcp_client_options.initial_retry_delay_ms = 5;
  options.tcp_client_options.max_retry_delay_ms = 20;
  return options;
}

void ReplyNotLeader(
    RpcReplyCallback done, const std::string& leader_hint) {
  proto::KvPutResponse response;
  response.set_status(proto::STATUS_CODE_NOT_LEADER);
  response.set_message("not leader");
  response.set_leader_hint(leader_hint);
  done(Status::OK(), &response);
}

TEST(KVClientTest, ValidatesOptionsAndLifecycle) {
  KVClientOptions options = ClientOptions(8001);
  std::unique_ptr<KVClient> client;
  EXPECT_EQ(
      KVClient::Create(nullptr, options, &client).code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      KVClient::Create(nullptr, std::move(options), nullptr).code(),
      Code::kInvalidArgument);

  {
    EventLoop loop;
    options = ClientOptions(8001);
    options.endpoints.clear();
    EXPECT_EQ(
        KVClient::Create(&loop, std::move(options), &client).code(),
        Code::kInvalidArgument);

    options = ClientOptions(8001);
    options.endpoints[0].host = "bad-host";
    EXPECT_EQ(
        KVClient::Create(&loop, std::move(options), &client).code(),
        Code::kInvalidArgument);

    options = ClientOptions(8001);
    options.endpoints[0].port = 0;
    EXPECT_EQ(
        KVClient::Create(&loop, std::move(options), &client).code(),
        Code::kInvalidArgument);

    options = ClientOptions(8001);
    options.endpoints.push_back(options.endpoints[0]);
    EXPECT_EQ(
        KVClient::Create(&loop, std::move(options), &client).code(),
        Code::kInvalidArgument);

    options = ClientOptions(8001);
    options.request_timeout_ms = 0;
    EXPECT_EQ(
        KVClient::Create(&loop, std::move(options), &client).code(),
        Code::kInvalidArgument);
  }

  EventLoop loop;
  options = ClientOptions(8001);
  ASSERT_TRUE(
      KVClient::Create(&loop, std::move(options), &client).ok());
  EXPECT_NE(client->ClientId(), 0U);
  const uint64_t client_id = client->ClientId();
  EXPECT_EQ(client->ClientId(), client_id);

  EXPECT_TRUE(client->Start().ok());
  EXPECT_TRUE(client->Start().ok());
  client->Stop();
  client->Stop();
  EXPECT_EQ(client->Start().code(), Code::kInvalidArgument);

  loop.RunAfter(20, [&loop] { loop.Quit(); });
  loop.Loop();
}

TEST(KVClientTest, RetriesWriteByLeaderHintWithStableSequence) {
  EventLoop loop;
  RpcServer follower(&loop, "follower", 0);
  RpcServer leader(&loop, "leader", 0);

  const std::string leader_hint =
      "127.0.0.1:" + std::to_string(leader.Port());
  uint64_t follower_client_id = 0;
  uint64_t follower_seq = 0;
  uint64_t leader_client_id = 0;
  uint64_t leader_seq = 0;
  int follower_calls = 0;
  int leader_calls = 0;

  ASSERT_TRUE(follower.RegisterMethod(
      kKvPutRpcMethod,
      [&](const RpcFrame& frame, RpcReplyCallback done) {
        proto::KvPutRequest request;
        ASSERT_TRUE(request.ParseFromString(frame.payload));
        ++follower_calls;
        follower_client_id = request.client_id();
        follower_seq = request.seq_num();
        ReplyNotLeader(std::move(done), leader_hint);
      }).ok());
  ASSERT_TRUE(leader.RegisterMethod(
      kKvPutRpcMethod,
      [&](const RpcFrame& frame, RpcReplyCallback done) {
        proto::KvPutRequest request;
        ASSERT_TRUE(request.ParseFromString(frame.payload));
        ++leader_calls;
        leader_client_id = request.client_id();
        leader_seq = request.seq_num();
        proto::KvPutResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(follower.Start().ok());
  ASSERT_TRUE(leader.Start().ok());

  KVClientOptions options = ClientOptions(follower.Port());
  options.endpoints.push_back({"127.0.0.1", leader.Port()});
  std::unique_ptr<KVClient> client;
  ASSERT_TRUE(
      KVClient::Create(&loop, std::move(options), &client).ok());
  ASSERT_TRUE(client->Start().ok());

  bool finished = false;
  std::string failure;
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    client->Stop();
    follower.Stop();
    leader.Stop();
    loop.Quit();
  };

  loop.RunAfter(80, [&] {
    Status status = client->Put(
        "hello", "world",
        [&](Status result) {
          if (!result.ok()) {
            failure = result.ToString();
          }
          finish();
        });
    if (!status.ok()) {
      failure = status.ToString();
      finish();
    }
  });
  loop.RunAfter(3000, [&] {
    failure = "leader hint write timed out";
    finish();
  });
  loop.Loop();

  EXPECT_TRUE(failure.empty()) << failure;
  EXPECT_EQ(follower_calls, 1);
  EXPECT_EQ(leader_calls, 1);
  EXPECT_NE(follower_client_id, 0U);
  EXPECT_EQ(follower_client_id, leader_client_id);
  EXPECT_NE(follower_seq, 0U);
  EXPECT_EQ(follower_seq, leader_seq);
  EXPECT_EQ(follower_client_id, client->ClientId());
}

TEST(KVClientTest, RetriesTimeoutOnNextEndpointWithStableSequence) {
  EventLoop loop;
  RpcServer slow_server(&loop, "slow", 0);
  RpcServer fast_server(&loop, "fast", 0);

  uint64_t slow_client_id = 0;
  uint64_t slow_seq = 0;
  uint64_t fast_client_id = 0;
  uint64_t fast_seq = 0;
  int slow_calls = 0;
  int fast_calls = 0;

  ASSERT_TRUE(slow_server.RegisterMethod(
      kKvPutRpcMethod,
      [&](const RpcFrame& frame, RpcReplyCallback) {
        proto::KvPutRequest request;
        ASSERT_TRUE(request.ParseFromString(frame.payload));
        ++slow_calls;
        slow_client_id = request.client_id();
        slow_seq = request.seq_num();
      }).ok());
  ASSERT_TRUE(fast_server.RegisterMethod(
      kKvPutRpcMethod,
      [&](const RpcFrame& frame, RpcReplyCallback done) {
        proto::KvPutRequest request;
        ASSERT_TRUE(request.ParseFromString(frame.payload));
        ++fast_calls;
        fast_client_id = request.client_id();
        fast_seq = request.seq_num();
        proto::KvPutResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(slow_server.Start().ok());
  ASSERT_TRUE(fast_server.Start().ok());

  KVClientOptions options = ClientOptions(slow_server.Port());
  options.endpoints.push_back({"127.0.0.1", fast_server.Port()});
  options.request_timeout_ms = 30;
  options.max_retries = 2;
  std::unique_ptr<KVClient> client;
  ASSERT_TRUE(
      KVClient::Create(&loop, std::move(options), &client).ok());
  ASSERT_TRUE(client->Start().ok());

  bool finished = false;
  std::string failure;
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    client->Stop();
    slow_server.Stop();
    fast_server.Stop();
    loop.Quit();
  };

  loop.RunAfter(80, [&] {
    Status status = client->Put(
        "timeout-key", "value",
        [&](Status result) {
          if (!result.ok()) {
            failure = result.ToString();
          }
          finish();
        });
    if (!status.ok()) {
      failure = status.ToString();
      finish();
    }
  });
  loop.RunAfter(3000, [&] {
    failure = "timeout retry write timed out";
    finish();
  });
  loop.Loop();

  EXPECT_TRUE(failure.empty()) << failure;
  EXPECT_EQ(slow_calls, 1);
  EXPECT_EQ(fast_calls, 1);
  EXPECT_NE(slow_client_id, 0U);
  EXPECT_EQ(slow_client_id, fast_client_id);
  EXPECT_NE(slow_seq, 0U);
  EXPECT_EQ(slow_seq, fast_seq);
}

TEST(KVClientTest, SerializesWritesAndReportsEachCallbackOnce) {
  EventLoop loop;
  RpcServer server(&loop, "serialized-writes", 0);

  bool first_pending = false;
  bool overlap = false;
  std::vector<uint64_t> sequences;
  int calls = 0;
  ASSERT_TRUE(server.RegisterMethod(
      kKvPutRpcMethod,
      [&](const RpcFrame& frame, RpcReplyCallback done) {
        proto::KvPutRequest request;
        ASSERT_TRUE(request.ParseFromString(frame.payload));
        ++calls;
        sequences.push_back(request.seq_num());
        if (calls == 1) {
          first_pending = true;
          loop.RunAfter(
              80, [&, done = std::move(done)]() mutable {
                first_pending = false;
                proto::KvPutResponse response;
                response.set_status(proto::STATUS_CODE_OK);
                done(Status::OK(), &response);
              });
          return;
        }
        overlap = overlap || first_pending;
        proto::KvPutResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(server.Start().ok());

  std::unique_ptr<KVClient> client;
  ASSERT_TRUE(KVClient::Create(
      &loop, ClientOptions(server.Port()), &client).ok());
  ASSERT_TRUE(client->Start().ok());

  bool finished = false;
  std::string failure;
  int completed = 0;
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    client->Stop();
    server.Stop();
    loop.Quit();
  };
  const auto on_write = [&](Status status) {
    if (!status.ok() && failure.empty()) {
      failure = status.ToString();
    }
    ++completed;
    if (completed == 2) {
      finish();
    }
  };

  loop.RunAfter(80, [&] {
    Status first = client->Put("first", "one", on_write);
    Status second = client->Put("second", "two", on_write);
    if (!first.ok() && failure.empty()) {
      failure = first.ToString();
    }
    if (!second.ok() && failure.empty()) {
      failure = second.ToString();
    }
  });
  loop.RunAfter(3000, [&] {
    failure = "serialized writes timed out";
    finish();
  });
  loop.Loop();

  EXPECT_TRUE(failure.empty()) << failure;
  EXPECT_EQ(completed, 2);
  ASSERT_EQ(sequences.size(), 2U);
  EXPECT_LT(sequences[0], sequences[1]);
  EXPECT_FALSE(overlap);
}

TEST(KVClientTest, RetriesGetAndScanThroughDynamicLeaderHint) {
  EventLoop loop;
  RpcServer follower(&loop, "read-follower", 0);
  RpcServer leader(&loop, "read-leader", 0);
  std::string leader_hint;
  int follower_gets = 0;
  int follower_scans = 0;

  ASSERT_TRUE(follower.RegisterMethod(
      kKvGetRpcMethod,
      [&](const RpcFrame&, RpcReplyCallback done) {
        ++follower_gets;
        proto::KvGetResponse response;
        response.set_status(proto::STATUS_CODE_NOT_LEADER);
        response.set_message("not leader");
        response.set_leader_hint(leader_hint);
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(follower.RegisterMethod(
      kKvScanRpcMethod,
      [&](const RpcFrame&, RpcReplyCallback done) {
        ++follower_scans;
        proto::KvScanResponse response;
        response.set_status(proto::STATUS_CODE_NOT_LEADER);
        response.set_message("not leader");
        response.set_leader_hint(leader_hint);
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(leader.RegisterMethod(
      kKvGetRpcMethod,
      [](const RpcFrame&, RpcReplyCallback done) {
        proto::KvGetResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        response.set_value("value");
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(leader.RegisterMethod(
      kKvScanRpcMethod,
      [](const RpcFrame&, RpcReplyCallback done) {
        proto::KvScanResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        response.set_has_more(true);
        proto::KvEntry* first = response.add_entries();
        first->set_key("a");
        first->set_value("1");
        proto::KvEntry* second = response.add_entries();
        second->set_key("b");
        second->set_value("2");
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(follower.Start().ok());
  ASSERT_TRUE(leader.Start().ok());
  leader_hint =
      "127.0.0.1:" + std::to_string(leader.Port());

  std::unique_ptr<KVClient> client;
  ASSERT_TRUE(KVClient::Create(
      &loop, ClientOptions(follower.Port()), &client).ok());
  ASSERT_TRUE(client->Start().ok());

  bool finished = false;
  std::string failure;
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    client->Stop();
    follower.Stop();
    leader.Stop();
    loop.Quit();
  };

  loop.RunAfter(80, [&] {
    Status status = client->Get(
        "key", true,
        [&](Status get_status, std::string value) {
          if (!get_status.ok() || value != "value") {
            failure = "get retry failed: " + get_status.ToString();
            finish();
            return;
          }
          Status scan_status = client->Scan(
              "a", "c", 10,
              [&](Status result, KVClientScanResult scan_result) {
                if (!result.ok() ||
                    !scan_result.has_more ||
                    scan_result.entries.size() != 2 ||
                    scan_result.entries[0].first != "a" ||
                    scan_result.entries[1].second != "2") {
                  failure = "scan retry failed: " +
                            result.ToString();
                }
                finish();
              });
          if (!scan_status.ok()) {
            failure = scan_status.ToString();
            finish();
          }
        });
    if (!status.ok()) {
      failure = status.ToString();
      finish();
    }
  });
  loop.RunAfter(3000, [&] {
    failure = "read retries timed out";
    finish();
  });
  loop.Loop();

  EXPECT_TRUE(failure.empty()) << failure;
  EXPECT_GE(follower_gets, 1);
  EXPECT_GE(follower_scans, 1);
}

TEST(KVClientTest, BlocksFurtherWritesAfterUnknownWriteOutcome) {
  EventLoop loop;
  RpcServer first(&loop, "unknown-first", 0);
  RpcServer second(&loop, "unknown-second", 0);
  int first_calls = 0;
  int second_calls = 0;
  const RpcHandler no_reply =
      [&](const RpcFrame&, RpcReplyCallback) {
        ++first_calls;
      };
  ASSERT_TRUE(first.RegisterMethod(kKvPutRpcMethod, no_reply).ok());
  ASSERT_TRUE(second.RegisterMethod(
      kKvPutRpcMethod,
      [&](const RpcFrame&, RpcReplyCallback) {
        ++second_calls;
      }).ok());
  ASSERT_TRUE(first.Start().ok());
  ASSERT_TRUE(second.Start().ok());

  KVClientOptions options = ClientOptions(first.Port());
  options.endpoints.push_back({"127.0.0.1", second.Port()});
  options.request_timeout_ms = 10;
  options.max_retries = 1;
  std::unique_ptr<KVClient> client;
  ASSERT_TRUE(
      KVClient::Create(&loop, std::move(options), &client).ok());
  ASSERT_TRUE(client->Start().ok());

  bool finished = false;
  std::string failure;
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    client->Stop();
    first.Stop();
    second.Stop();
    loop.Quit();
  };

  loop.RunAfter(80, [&] {
    Status accepted = client->Put(
        "unknown", "value",
        [&](Status result) {
          if (result.code() != Code::kTimeout) {
            failure = "unknown write should time out: " +
                      result.ToString();
          }
          Status next = client->Put("next", "value", [](Status) {});
          if (next.code() != Code::kTimeout) {
            failure = "write after unknown outcome was accepted";
          }
          finish();
        });
    if (!accepted.ok()) {
      failure = accepted.ToString();
      finish();
    }
  });
  loop.RunAfter(2000, [&] {
    failure = "unknown outcome test timed out";
    finish();
  });
  loop.Loop();

  EXPECT_TRUE(failure.empty()) << failure;
  EXPECT_EQ(first_calls, 1);
  EXPECT_EQ(second_calls, 1);
}

TEST(KVClientTest, ThreeNodesServeKvRequestsThroughSdk) {
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
    KVServerOptions options = ServerOptions(directory, node_id);
    options.local_node.port = ports[index];
    options.data_directory =
        directory.Path("cluster-node-" + std::to_string(node_id));
    options.raft_options.random_seed = 200 + node_id;
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

  std::unique_ptr<KVClient> client;
  bool finished = false;
  std::string failure;
  std::string stage = "waiting for leader";
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    if (client != nullptr) {
      client->Stop();
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
  const auto check_status = [&](const Status& status, const char* step) {
    if (!status.ok()) {
      fail(std::string(step) + ": " + status.ToString());
      return false;
    }
    return true;
  };

  std::vector<std::function<void()>> steps;
  size_t next_step = 0;
  std::function<void()> proceed;

  steps.push_back([&] {
    Status status = client->Put(
        "hello", "world",
        [&](Status result) {
          if (!check_status(result, "put hello")) {
            return;
          }
          proceed();
        });
    check_status(status, "accept put hello");
  });
  steps.push_back([&] {
    Status status = client->Get(
        "hello", true,
        [&](Status result, std::string value) {
          if (!check_status(result, "get hello") ||
              value != "world") {
            fail("get hello returned the wrong value");
            return;
          }
          proceed();
        });
    check_status(status, "accept get hello");
  });
  steps.push_back([&] {
    Status status = client->Delete(
        "hello",
        [&](Status result) {
          if (!check_status(result, "delete hello")) {
            return;
          }
          proceed();
        });
    check_status(status, "accept delete hello");
  });
  steps.push_back([&] {
    Status status = client->Get(
        "hello", true,
        [&](Status result, std::string) {
          if (result.code() != Code::kNotFound) {
            fail("deleted key is still visible");
            return;
          }
          proceed();
        });
    check_status(status, "accept get deleted key");
  });
  const auto append_put =
      [&](const std::string& key, const std::string& value) {
        steps.push_back([&, key, value] {
          Status status = client->Put(
              key, value,
              [&, key](Status result) {
                if (!check_status(result, key.c_str())) {
                  return;
                }
                proceed();
              });
          check_status(status, "accept scan fixture put");
        });
      };
  append_put("a1", "v1");
  append_put("a2", "v2");
  append_put("b1", "v3");
  steps.push_back([&] {
    Status status = client->Scan(
        "a", "b", 1,
        [&](Status result, KVClientScanResult scan_result) {
          if (!check_status(result, "scan")) {
            return;
          }
          if (scan_result.entries.size() != 1 ||
              scan_result.entries[0].first != "a1" ||
              scan_result.entries[0].second != "v1" ||
              !scan_result.has_more) {
            fail("scan result mismatch");
            return;
          }
          finish();
        });
    check_status(status, "accept scan");
  });

  const std::vector<std::string> step_names = {
      "put hello", "get hello", "delete hello",
      "get deleted hello", "put a1", "put a2", "put b1", "scan"};
  proceed = [&] {
    if (finished) {
      return;
    }
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
        uint32_t leader_id = 0;
        for (uint32_t index = 0; index < kNodeCount; ++index) {
          const RaftNodeStatus status =
              servers[index]->GetRaftStatus();
          if (status.role == Role::kLeader) {
            leader_id = index + 1;
          }
        }
        if (leader_id == 0) {
          return;
        }
        uint32_t follower_id = 0;
        for (uint32_t index = 0; index < kNodeCount; ++index) {
          const RaftNodeStatus status =
              servers[index]->GetRaftStatus();
          if (index + 1 != leader_id &&
              status.leader_id == leader_id) {
            follower_id = index + 1;
            break;
          }
        }
        if (follower_id == 0) {
          return;
        }

        loop.CancelTimer(*leader_timer);
        KVClientOptions options =
            ClientOptions(ports[follower_id - 1]);
        options.max_retries = 5;
        Status status = KVClient::Create(
            &loop, std::move(options), &client);
        if (!status.ok()) {
          fail("create client: " + status.ToString());
          return;
        }
        status = client->Start();
        if (!status.ok()) {
          fail("start client: " + status.ToString());
          return;
        }
        loop.RunAfter(100, proceed);
      });

  loop.RunAfter(30000, [&] {
    fail("three-node KVClient test timed out at " + stage);
  });
  loop.Loop();

  for (auto& server : servers) {
    server->Stop();
  }
  EXPECT_TRUE(failure.empty()) << failure;
}

TEST(KVClientTest, StopCompletesPendingRequestOnce) {
  EventLoop loop;
  RpcServer server(&loop, "stop-pending", 0);
  ASSERT_TRUE(server.RegisterMethod(
      kKvPutRpcMethod,
      [](const RpcFrame&, RpcReplyCallback) {}).ok());
  ASSERT_TRUE(server.Start().ok());

  KVClientOptions options = ClientOptions(server.Port());
  options.request_timeout_ms = 5000;
  std::unique_ptr<KVClient> client;
  ASSERT_TRUE(
      KVClient::Create(&loop, std::move(options), &client).ok());
  ASSERT_TRUE(client->Start().ok());

  bool finished = false;
  bool callback_called = false;
  std::string failure;
  const auto finish = [&] {
    if (finished) {
      return;
    }
    finished = true;
    client->Stop();
    server.Stop();
    loop.Quit();
  };

  loop.RunAfter(80, [&] {
    Status status = client->Put(
        "pending", "value",
        [&](Status result) {
          callback_called = true;
          if (result.code() != Code::kTimeout) {
            failure = result.ToString();
          }
          finish();
        });
    if (!status.ok()) {
      failure = status.ToString();
      finish();
    }
  });
  loop.RunAfter(120, [&] { client->Stop(); });
  loop.RunAfter(2000, [&] {
    failure = "stop pending test timed out";
    finish();
  });
  loop.Loop();

  EXPECT_TRUE(failure.empty()) << failure;
  EXPECT_TRUE(callback_called);
}

}  // namespace
}  // namespace raftkv
