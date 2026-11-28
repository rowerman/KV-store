#include "raftkv/server/real_raft_transport.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include <google/protobuf/wrappers.pb.h>

#include "raftkv/net/event_loop.h"
#include "raftkv/rpc/rpc_client.h"
#include "raftkv/rpc/rpc_server.h"

namespace raftkv {
namespace {

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

RealRaftTransportOptions Options(
    uint32_t local_node_id,
    std::vector<RaftPeerAddress> peers = {}) {
  RealRaftTransportOptions options;
  options.local_node_id = local_node_id;
  options.peers = std::move(peers);
  options.rpc_timeout_ms = 1000;
  return options;
}

void ArmWatchdog(EventLoop* loop, uint64_t timeout_ms) {
  loop->RunAfter(timeout_ms, [loop] { loop->Quit(); });
}

TEST(RealRaftTransportTest, ValidatesConfigurationAndRegistration) {
  EventLoop loop;
  std::unique_ptr<RealRaftTransport> transport;

  EXPECT_EQ(
      RealRaftTransport::Create(nullptr, Options(1), &transport).code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      RealRaftTransport::Create(
          &loop, Options(1, {{2, "127.0.0.1", 0}}), &transport)
          .code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      RealRaftTransport::Create(
          &loop, Options(1, {{1, "127.0.0.1", 8001}}), &transport)
          .code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      RealRaftTransport::Create(
          &loop, Options(1, {{2, "not-an-ip", 8001}}), &transport)
          .code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      RealRaftTransport::Create(
          &loop,
          Options(
              1,
              {{2, "127.0.0.1", 8001},
               {2, "127.0.0.1", 8002}}),
          &transport)
          .code(),
      Code::kInvalidArgument);

  RealRaftTransportOptions zero_local = Options(0);
  EXPECT_EQ(
      RealRaftTransport::Create(&loop, std::move(zero_local), &transport)
          .code(),
      Code::kInvalidArgument);
  RealRaftTransportOptions zero_timeout = Options(1);
  zero_timeout.rpc_timeout_ms = 0;
  EXPECT_EQ(
      RealRaftTransport::Create(
          &loop, std::move(zero_timeout), &transport)
          .code(),
      Code::kInvalidArgument);

  ASSERT_TRUE(
      RealRaftTransport::Create(&loop, Options(1), &transport).ok());
  EXPECT_NE(transport, nullptr);
  EXPECT_EQ(
      transport->RegisterRpcMethods(nullptr).code(),
      Code::kInvalidArgument);

  RpcServer server(&loop, "registration", 0);
  ASSERT_TRUE(transport->RegisterRpcMethods(&server).ok());
  EXPECT_EQ(
      transport->RegisterRpcMethods(&server).code(),
      Code::kInvalidArgument);

  std::unique_ptr<RealRaftTransport> started;
  ASSERT_TRUE(
      RealRaftTransport::Create(&loop, Options(2), &started).ok());
  started->Start();
  RpcServer second_server(&loop, "started-registration", 0);
  EXPECT_EQ(
      started->RegisterRpcMethods(&second_server).code(),
      Code::kInvalidArgument);
  started->Stop();
}

TEST(RealRaftTransportTest, RoundTripsAllRaftRpcsOverTcp) {
  const uint16_t port1 = ReserveUnusedPort();
  const uint16_t port2 = ReserveUnusedPort();
  ASSERT_NE(port1, 0);
  ASSERT_NE(port2, 0);
  ASSERT_NE(port1, port2);

  EventLoop loop;
  RpcServer server1(&loop, "raft-node-1", port1);
  RpcServer server2(&loop, "raft-node-2", port2);

  std::unique_ptr<RealRaftTransport> transport1;
  std::unique_ptr<RealRaftTransport> transport2;
  ASSERT_TRUE(
      RealRaftTransport::Create(
          &loop, Options(1, {{2, "127.0.0.1", port2}}), &transport1)
          .ok());
  ASSERT_TRUE(
      RealRaftTransport::Create(
          &loop, Options(2, {{1, "127.0.0.1", port1}}), &transport2)
          .ok());
  ASSERT_TRUE(transport1->RegisterRpcMethods(&server1).ok());
  ASSERT_TRUE(transport2->RegisterRpcMethods(&server2).ok());

  bool vote_ok = false;
  bool append_ok = false;
  bool snapshot_ok = false;
  int completed = 0;
  transport2->SetRequestVoteHandler(
      [&](uint32_t from_peer_id,
          const proto::RequestVoteRequest& request,
          RequestVoteResponseCallback done) {
        proto::RequestVoteResponse response;
        response.set_term(request.term());
        response.set_vote_granted(from_peer_id == 1);
        done(Status::OK(), response);
      });
  transport2->SetAppendEntriesHandler(
      [&](uint32_t from_peer_id,
          const proto::AppendEntriesRequest& request,
          AppendEntriesResponseCallback done) {
        proto::AppendEntriesResponse response;
        response.set_term(request.term());
        response.set_success(
            from_peer_id == 1 &&
            request.entries_size() == 1 &&
            request.entries(0).index() == 9);
        done(Status::OK(), response);
      });
  transport2->SetInstallSnapshotHandler(
      [&](uint32_t from_peer_id,
          const proto::InstallSnapshotRequest& request,
          InstallSnapshotResponseCallback done) {
        proto::InstallSnapshotResponse response;
        response.set_term(request.term());
        response.set_success(
            from_peer_id == 1 &&
            request.data() == "snapshot" &&
            request.done());
        response.set_received_offset(
            static_cast<uint64_t>(request.data().size()));
        done(Status::OK(), response);
      });

  ASSERT_TRUE(server1.Start().ok());
  ASSERT_TRUE(server2.Start().ok());
  transport1->Start();
  transport2->Start();

  const auto finish = [&] {
    if (completed != 3) {
      return;
    }
    transport1->Stop();
    transport2->Stop();
    server1.Stop();
    server2.Stop();
    loop.Quit();
  };

  loop.RunAfter(50, [&] {
    proto::RequestVoteRequest vote;
    vote.set_term(4);
    vote.set_candidate_id(1);
    transport1->SendRequestVote(
        2, vote,
        [&](Status status, proto::RequestVoteResponse response) {
          vote_ok =
              status.ok() && response.term() == 4 &&
              response.vote_granted();
          ++completed;
          finish();
        });

    proto::AppendEntriesRequest append;
    append.set_term(4);
    append.set_leader_id(1);
    proto::LogEntry* entry = append.add_entries();
    entry->set_term(4);
    entry->set_index(9);
    transport1->SendAppendEntries(
        2, append,
        [&](Status status, proto::AppendEntriesResponse response) {
          append_ok =
              status.ok() && response.term() == 4 &&
              response.success();
          ++completed;
          finish();
        });

    proto::InstallSnapshotRequest snapshot;
    snapshot.set_term(4);
    snapshot.set_leader_id(1);
    snapshot.set_last_included_index(9);
    snapshot.set_last_included_term(4);
    snapshot.set_data("snapshot");
    snapshot.set_done(true);
    transport1->SendInstallSnapshot(
        2, snapshot,
        [&](Status status, proto::InstallSnapshotResponse response) {
          snapshot_ok =
              status.ok() && response.term() == 4 &&
              response.success() &&
              response.received_offset() == 8;
          ++completed;
          finish();
        });
  });

  ArmWatchdog(&loop, 4000);
  loop.Loop();
  EXPECT_EQ(completed, 3);
  EXPECT_TRUE(vote_ok);
  EXPECT_TRUE(append_ok);
  EXPECT_TRUE(snapshot_ok);
}

TEST(RealRaftTransportTest, PropagatesHandlerErrorsAndMissingHandler) {
  const uint16_t port1 = ReserveUnusedPort();
  const uint16_t port2 = ReserveUnusedPort();
  ASSERT_NE(port1, 0);
  ASSERT_NE(port2, 0);
  ASSERT_NE(port1, port2);

  EventLoop loop;
  RpcServer server1(&loop, "raft-error-node-1", port1);
  RpcServer server2(&loop, "raft-error-node-2", port2);
  std::unique_ptr<RealRaftTransport> transport1;
  std::unique_ptr<RealRaftTransport> transport2;
  ASSERT_TRUE(
      RealRaftTransport::Create(
          &loop, Options(1, {{2, "127.0.0.1", port2}}), &transport1)
          .ok());
  ASSERT_TRUE(
      RealRaftTransport::Create(
          &loop, Options(2, {{1, "127.0.0.1", port1}}), &transport2)
          .ok());
  ASSERT_TRUE(transport1->RegisterRpcMethods(&server1).ok());
  ASSERT_TRUE(transport2->RegisterRpcMethods(&server2).ok());
  ASSERT_TRUE(server1.Start().ok());
  ASSERT_TRUE(server2.Start().ok());
  transport1->Start();
  transport2->Start();

  Code first_code = Code::kOk;
  Code second_code = Code::kOk;
  std::string first_message;
  std::string second_message;
  int completed = 0;
  const auto finish = [&] {
    transport1->Stop();
    transport2->Stop();
    server1.Stop();
    server2.Stop();
    loop.Quit();
  };

  loop.RunAfter(50, [&] {
    proto::AppendEntriesRequest request;
    request.set_term(3);
    request.set_leader_id(1);
    transport1->SendAppendEntries(
        2, request,
        [&, request](Status status, proto::AppendEntriesResponse) {
          first_code = status.code();
          first_message = status.message();
          ++completed;
          transport2->SetAppendEntriesHandler(
              [](uint32_t, const proto::AppendEntriesRequest&,
                 AppendEntriesResponseCallback done) {
                done(Status::NotLeader("leader changed"),
                     proto::AppendEntriesResponse{});
              });
          transport1->SendAppendEntries(
              2, request,
              [&](Status status, proto::AppendEntriesResponse) {
                second_code = status.code();
                second_message = status.message();
                ++completed;
                finish();
              });
        });
  });

  ArmWatchdog(&loop, 4000);
  loop.Loop();
  EXPECT_EQ(completed, 2);
  EXPECT_EQ(first_code, Code::kInvalidArgument) << first_message;
  EXPECT_EQ(second_code, Code::kNotLeader) << second_message;
}

TEST(RealRaftTransportTest, InvalidPayloadDoesNotCloseRpcConnection) {
  const uint16_t port = ReserveUnusedPort();
  ASSERT_NE(port, 0);

  EventLoop loop;
  RpcServer server(&loop, "raft-payload-node", port);
  std::unique_ptr<RealRaftTransport> transport;
  ASSERT_TRUE(
      RealRaftTransport::Create(&loop, Options(2), &transport).ok());
  ASSERT_TRUE(transport->RegisterRpcMethods(&server).ok());
  transport->SetAppendEntriesHandler(
      [](uint32_t, const proto::AppendEntriesRequest&,
         AppendEntriesResponseCallback done) {
        proto::AppendEntriesResponse response;
        response.set_success(true);
        done(Status::OK(), response);
      });
  ASSERT_TRUE(server.Start().ok());

  RpcClient client(&loop, "invalid-payload-client", "127.0.0.1", port);
  bool invalid_rejected = false;
  bool connection_reused = false;
  loop.RunAfter(50, [&] {
    google::protobuf::StringValue invalid;
    invalid.set_value(std::string("\xff", 1));
    client.Call(
        kRaftRequestVoteRpcMethod, invalid,
        [&](Status status, const RpcFrame&) {
          invalid_rejected =
              status.code() == Code::kInvalidArgument;

          proto::AppendEntriesRequest request;
          request.set_term(2);
          request.set_leader_id(1);
          client.Call(
              kRaftAppendEntriesRpcMethod, request,
              [&](Status append_status, const RpcFrame& frame) {
                proto::AppendEntriesResponse response;
                connection_reused =
                    append_status.ok() &&
                    response.ParseFromString(frame.payload) &&
                    response.success();
                client.Disconnect();
                transport->Stop();
                server.Stop();
                loop.Quit();
              },
              1000);
        },
        1000);
  });

  ArmWatchdog(&loop, 4000);
  client.Connect();
  loop.Loop();
  client.Disconnect();
  transport->Stop();
  server.Stop();
  EXPECT_TRUE(invalid_rejected);
  EXPECT_TRUE(connection_reused);
}

TEST(RealRaftTransportTest, StopCompletesPendingCall) {
  const uint16_t port1 = ReserveUnusedPort();
  const uint16_t port2 = ReserveUnusedPort();
  ASSERT_NE(port1, 0);
  ASSERT_NE(port2, 0);
  ASSERT_NE(port1, port2);

  EventLoop loop;
  RpcServer server1(&loop, "raft-stop-node-1", port1);
  RpcServer server2(&loop, "raft-stop-node-2", port2);
  std::unique_ptr<RealRaftTransport> transport1;
  std::unique_ptr<RealRaftTransport> transport2;
  ASSERT_TRUE(
      RealRaftTransport::Create(
          &loop, Options(1, {{2, "127.0.0.1", port2}}), &transport1)
          .ok());
  ASSERT_TRUE(
      RealRaftTransport::Create(
          &loop, Options(2, {{1, "127.0.0.1", port1}}), &transport2)
          .ok());
  ASSERT_TRUE(transport1->RegisterRpcMethods(&server1).ok());
  ASSERT_TRUE(transport2->RegisterRpcMethods(&server2).ok());

  bool request_received = false;
  bool completed_with_timeout = false;
  transport2->SetAppendEntriesHandler(
      [&](uint32_t, const proto::AppendEntriesRequest&,
          AppendEntriesResponseCallback) {
        request_received = true;
        loop.RunAfter(30, [&] { transport1->Stop(); });
      });

  ASSERT_TRUE(server1.Start().ok());
  ASSERT_TRUE(server2.Start().ok());
  transport1->Start();
  transport2->Start();
  loop.RunAfter(50, [&] {
    proto::AppendEntriesRequest request;
    request.set_term(5);
    request.set_leader_id(1);
    transport1->SendAppendEntries(
        2, request,
        [&](Status status, proto::AppendEntriesResponse) {
          completed_with_timeout =
              status.code() == Code::kTimeout;
          transport2->Stop();
          server1.Stop();
          server2.Stop();
          loop.Quit();
        });
  });

  ArmWatchdog(&loop, 4000);
  loop.Loop();
  transport1->Stop();
  transport2->Stop();
  server1.Stop();
  server2.Stop();
  EXPECT_TRUE(request_received);
  EXPECT_TRUE(completed_with_timeout);
}

TEST(RealRaftTransportTest, RejectsUnknownPeerAndWrongSender) {
  EventLoop loop;
  std::unique_ptr<RealRaftTransport> transport;
  ASSERT_TRUE(
      RealRaftTransport::Create(&loop, Options(1), &transport).ok());

  Code unknown_peer = Code::kOk;
  proto::RequestVoteRequest vote;
  vote.set_candidate_id(1);
  transport->SendRequestVote(
      2, vote,
      [&](Status status, proto::RequestVoteResponse) {
        unknown_peer = status.code();
      });

  Code wrong_sender = Code::kOk;
  vote.set_candidate_id(3);
  transport->SendRequestVote(
      2, vote,
      [&](Status status, proto::RequestVoteResponse) {
        wrong_sender = status.code();
      });

  EXPECT_EQ(unknown_peer, Code::kNotFound);
  EXPECT_EQ(wrong_sender, Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
