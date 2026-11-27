#include "raftkv/raft/raft_node.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "chaos/sim_transport.h"

namespace raftkv {
namespace {

using testing::SimNetwork;
using testing::SimTransport;
using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-raft-node-XXXXXX";
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

Status CreateNode(
    const std::shared_ptr<SimNetwork>& network,
    const std::string& directory, uint32_t node_id,
    std::vector<uint32_t> peers, uint64_t tick_interval_ms,
    uint64_t initial_term, std::unique_ptr<RaftNode>* node) {
  std::unique_ptr<RaftLog> log;
  Status status = RaftLog::Open(directory, &log);
  if (!status.ok()) {
    return status;
  }
  if (initial_term != 0) {
    status = log->SaveHardState(initial_term, std::nullopt);
    if (!status.ok()) {
      return status;
    }
  }

  auto transport = std::make_unique<SimTransport>(
      network, node_id);
  RaftNodeOptions options;
  options.node_id = node_id;
  options.peers = std::move(peers);
  options.tick_interval_ms = tick_interval_ms;
  return RaftNode::Create(
      std::move(options), std::move(transport),
      std::move(log), node);
}

template <typename Predicate>
bool WaitUntil(Predicate predicate,
               std::chrono::milliseconds timeout = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

void DriveNetworkUntil(
    const std::shared_ptr<SimNetwork>& network,
    const std::function<bool()>& predicate,
    std::chrono::milliseconds timeout = 2s) {
  ASSERT_TRUE(WaitUntil(
      [&]() {
        network->AdvanceBy(1ms);
        return predicate();
      },
      timeout));
}

TEST(RaftNodeLifecycleTest, StartsStopsAndRejectsRestart) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("node"), 2, {1},
                  1000, 7, &node)
                  .ok());

  RaftNodeStatus status = node->GetStatus();
  EXPECT_FALSE(status.running);
  EXPECT_EQ(status.role, Role::kFollower);
  EXPECT_EQ(status.current_term, 7U);

  ASSERT_TRUE(node->Start().ok());
  EXPECT_TRUE(WaitUntil([&]() {
    return node->GetStatus().running;
  }));
  EXPECT_TRUE(node->Start().ok());

  ASSERT_TRUE(node->Stop().ok());
  EXPECT_FALSE(node->GetStatus().running);
  EXPECT_TRUE(node->Stop().ok());
  EXPECT_EQ(node->Start().code(), Code::kInvalidArgument);
  EXPECT_EQ(node->PostEvent(TickEvent{}).code(),
            Code::kInvalidArgument);
}

TEST(RaftNodeLifecycleTest, StopsCleanlyBeforeStart) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("node"), 2, {1},
                  10, 0, &node)
                  .ok());

  EXPECT_TRUE(node->Stop().ok());
  EXPECT_TRUE(node->Stop().ok());
  EXPECT_EQ(node->Start().code(), Code::kInvalidArgument);
  EXPECT_EQ(node->PostEvent(TickEvent{}).code(),
            Code::kInvalidArgument);
}

TEST(RaftNodeActorTest, ProcessesEventsInFifoOrderOnActorThread) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("node"), 2, {1},
                  1000, 0, &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  constexpr int kEventCount = 100;
  std::atomic<int> completed{0};
  std::atomic<bool> all_actor_thread{true};
  std::vector<int> order;
  std::mutex order_mutex;
  for (int i = 0; i < kEventCount; ++i) {
    ASSERT_TRUE(node->PostEvent(ProposeEvent{
        "command-" + std::to_string(i),
        [&, i](Status status) {
          EXPECT_EQ(status.code(), Code::kNotLeader);
          if (!node->IsActorThread()) {
            all_actor_thread.store(false);
          }
          {
            std::lock_guard<std::mutex> lock(order_mutex);
            order.push_back(i);
          }
          ++completed;
        }}).ok());
  }

  ASSERT_TRUE(WaitUntil([&]() {
    return completed.load() == kEventCount;
  }));
  EXPECT_TRUE(all_actor_thread.load());
  ASSERT_EQ(order.size(), static_cast<size_t>(kEventCount));
  for (int i = 0; i < kEventCount; ++i) {
    EXPECT_EQ(order[static_cast<size_t>(i)], i);
  }
  EXPECT_GE(node->GetStatus().processed_events,
            static_cast<uint64_t>(kEventCount));
  EXPECT_TRUE(node->Stop().ok());
}

TEST(RaftNodeActorTest, GeneratesPeriodicTicks) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("node"), 2, {1},
                  5, 0, &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  ASSERT_TRUE(WaitUntil([&]() {
    return node->GetStatus().tick_count >= 2;
  }));
  EXPECT_TRUE(node->Stop().ok());
}

TEST(RaftNodeTransportTest, BridgesAllRequestsAndStoppedNodeTimesOut) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  network->SetDelay(0ms);
  SimTransport sender(network, 1);

  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("node"), 2, {1},
                  1000, 9, &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  std::atomic<int> vote_callbacks{0};
  std::atomic<int> append_callbacks{0};
  std::atomic<int> snapshot_callbacks{0};
  std::atomic<int> timeouts{0};

  proto::RequestVoteRequest vote;
  vote.set_term(9);
  sender.SendRequestVote(
      2, vote,
      [&](Status status, proto::RequestVoteResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response.term(), 9U);
        EXPECT_FALSE(response.vote_granted());
        ++vote_callbacks;
      });

  proto::AppendEntriesRequest append;
  append.set_term(9);
  sender.SendAppendEntries(
      2, append,
      [&](Status status, proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response.term(), 9U);
        EXPECT_FALSE(response.success());
        ++append_callbacks;
      });

  proto::InstallSnapshotRequest snapshot;
  snapshot.set_term(9);
  sender.SendInstallSnapshot(
      2, snapshot,
      [&](Status status,
          proto::InstallSnapshotResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response.term(), 9U);
        EXPECT_FALSE(response.success());
        ++snapshot_callbacks;
      });

  DriveNetworkUntil(network, [&]() {
    return vote_callbacks.load() == 1 &&
           append_callbacks.load() == 1 &&
           snapshot_callbacks.load() == 1;
  });
  EXPECT_EQ(node->GetStatus().processed_events >= 3, true);

  ASSERT_TRUE(node->Stop().ok());
  sender.SendRequestVote(
      2, vote,
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++timeouts;
      });
  DriveNetworkUntil(network, [&]() {
    return timeouts.load() == 1;
  });
}

TEST(RaftNodeActorTest, SupportsConcurrentEventProducers) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("node"), 2, {1},
                  1000, 0, &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  constexpr int kThreads = 4;
  constexpr int kEventsPerThread = 50;
  std::atomic<int> completed{0};
  std::atomic<bool> all_actor_thread{true};
  std::vector<std::thread> threads;
  for (int thread = 0; thread < kThreads; ++thread) {
    threads.emplace_back([&, thread]() {
      for (int i = 0; i < kEventsPerThread; ++i) {
        EXPECT_TRUE(node->PostEvent(ProposeEvent{
            std::to_string(thread) + ":" + std::to_string(i),
            [&](Status) {
              if (!node->IsActorThread()) {
                all_actor_thread.store(false);
              }
              ++completed;
            }}).ok());
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }

  ASSERT_TRUE(WaitUntil([&]() {
    return completed.load() == kThreads * kEventsPerThread;
  }));
  EXPECT_TRUE(all_actor_thread.load());
  EXPECT_TRUE(node->Stop().ok());
}

TEST(RaftNodeCreateTest, RejectsInvalidOptionsAndPreservesOutput) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>();
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateNode(
                  network, directory.Path("valid"), 2, {1},
                  10, 0, &node)
                  .ok());
  ASSERT_NE(node, nullptr);

  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(
      RaftLog::Open(directory.Path("invalid"), &log).ok());
  auto transport = std::make_unique<SimTransport>(network, 3);

  RaftNodeOptions options;
  options.node_id = 0;
  EXPECT_EQ(RaftNode::Create(
                options, std::move(transport), std::move(log),
                &node)
                .code(),
            Code::kInvalidArgument);
  ASSERT_NE(node, nullptr);

  EXPECT_EQ(RaftNode::Create(
                RaftNodeOptions{}, nullptr, nullptr, nullptr)
                .code(),
            Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
