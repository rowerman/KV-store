#include "raftkv/raft/raft_node.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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
using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-read-index-XXXXXX";
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

struct ReadResult {
  void Record(Status result, bool actor_thread, bool applied) {
    std::lock_guard<std::mutex> lock(mutex);
    ++calls;
    status = std::move(result);
    ran_on_actor = actor_thread;
    applied_reached = applied;
  }

  int Calls() const {
    std::lock_guard<std::mutex> lock(mutex);
    return calls;
  }

  Status ResultStatus() const {
    std::lock_guard<std::mutex> lock(mutex);
    return status;
  }

  bool RanOnActor() const {
    std::lock_guard<std::mutex> lock(mutex);
    return ran_on_actor;
  }

  bool AppliedReached() const {
    std::lock_guard<std::mutex> lock(mutex);
    return applied_reached;
  }

 private:
  mutable std::mutex mutex;
  int calls = 0;
  Status status;
  bool ran_on_actor = false;
  bool applied_reached = false;
};

struct ClusterOptions {
  uint64_t tick_interval_ms = 5;
  uint64_t heartbeat_interval_ms = 10;
  uint64_t election_timeout_min_ms = 40;
  uint64_t election_timeout_max_ms = 80;
  uint64_t read_index_timeout_ms = 500;
  uint64_t random_seed = 20261128;
};

class Cluster {
 public:
  Cluster(size_t node_count, ClusterOptions cluster_options)
      : network_(std::make_shared<SimNetwork>(
            cluster_options.random_seed)),
        options_(cluster_options),
        nodes_(node_count) {
    network_->SetDelay(1ms);
    for (size_t index = 0; index < node_count; ++index) {
      const uint32_t node_id =
          static_cast<uint32_t>(index + 1);
      std::vector<uint32_t> peers;
      for (size_t peer_index = 0; peer_index < node_count;
           ++peer_index) {
        if (peer_index != index) {
          peers.push_back(
              static_cast<uint32_t>(peer_index + 1));
        }
      }

      RaftNodeOptions node_options;
      node_options.node_id = node_id;
      node_options.peers = std::move(peers);
      node_options.tick_interval_ms = options_.tick_interval_ms;
      node_options.heartbeat_interval_ms =
          options_.heartbeat_interval_ms;
      node_options.election_timeout_min_ms =
          options_.election_timeout_min_ms;
      node_options.election_timeout_max_ms =
          options_.election_timeout_max_ms;
      node_options.read_index_timeout_ms =
          options_.read_index_timeout_ms;
      node_options.random_seed = options_.random_seed + node_id;

      std::unique_ptr<RaftLog> log;
      Status status = RaftLog::Open(
          directory_.Path("node-" + std::to_string(node_id)),
          &log);
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
      auto transport =
          std::make_unique<testing::SimTransport>(network_, node_id);
      status = RaftNode::Create(
          std::move(node_options), std::move(transport),
          std::move(log), &nodes_[index]);
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
    }
  }

  ~Cluster() {
    for (std::unique_ptr<RaftNode>& node : nodes_) {
      if (node != nullptr) {
        node->Stop();
      }
    }
    DrainNetwork();
  }

  Cluster(const Cluster&) = delete;
  Cluster& operator=(const Cluster&) = delete;

  void Start() {
    for (std::unique_ptr<RaftNode>& node : nodes_) {
      const Status status = node->Start();
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
    }
  }

  RaftNode* Node(uint32_t node_id) {
    return nodes_.at(static_cast<size_t>(node_id - 1)).get();
  }

  const std::shared_ptr<SimNetwork>& network() const {
    return network_;
  }

  std::optional<uint32_t> LeaderId() const {
    std::optional<uint32_t> leader;
    for (size_t index = 0; index < nodes_.size(); ++index) {
      if (nodes_[index]->GetStatus().role != Role::kLeader) {
        continue;
      }
      if (leader.has_value()) {
        return std::nullopt;
      }
      leader = static_cast<uint32_t>(index + 1);
    }
    return leader;
  }

  bool WaitForStableLeader(
      std::chrono::milliseconds timeout = 3s) {
    return DriveUntil(
        [&]() {
          const std::optional<uint32_t> leader = LeaderId();
          if (!leader.has_value()) {
            return false;
          }
          const RaftNodeStatus leader_status =
              nodes_[*leader - 1]->GetStatus();
          if (leader_status.commit_index == 0 ||
              leader_status.last_applied <
                  leader_status.commit_index) {
            return false;
          }
          for (std::unique_ptr<RaftNode>& node : nodes_) {
            const RaftNodeStatus status = node->GetStatus();
            if (status.current_term !=
                    leader_status.current_term ||
                status.leader_id != *leader) {
              return false;
            }
          }
          return true;
        },
        timeout);
  }

  bool DriveUntil(
      const std::function<bool()>& predicate,
      std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      network_->AdvanceBy(1ms);
      if (predicate()) {
        return true;
      }
      std::this_thread::sleep_for(1ms);
    }
    return predicate();
  }

  void DriveFor(std::chrono::milliseconds duration) {
    const auto deadline = std::chrono::steady_clock::now() + duration;
    while (std::chrono::steady_clock::now() < deadline) {
      network_->AdvanceBy(1ms);
      std::this_thread::sleep_for(1ms);
    }
  }

 private:
  void DrainNetwork() {
    for (int attempt = 0;
         attempt < 100 && network_->PendingMessageCount() != 0;
         ++attempt) {
      network_->AdvanceBy(1s);
    }
  }

  std::shared_ptr<SimNetwork> network_;
  ClusterOptions options_;
  TempDir directory_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
};

std::vector<uint32_t> Followers(
    uint32_t leader_id, size_t node_count) {
  std::vector<uint32_t> followers;
  for (size_t index = 0; index < node_count; ++index) {
    const uint32_t node_id = static_cast<uint32_t>(index + 1);
    if (node_id != leader_id) {
      followers.push_back(node_id);
    }
  }
  return followers;
}

TEST(RaftReadIndexTest, LeaderCompletesAfterMajorityAndApply) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t leader_id = cluster.LeaderId().value();
  RaftNode* leader = cluster.Node(leader_id);
  const RaftNodeStatus before = leader->GetStatus();
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(leader->PostEvent(ReadIndexEvent{
      17,
      [leader, before, result](Status status) {
        result->Record(
            status, leader->IsActorThread(),
            leader->GetStatus().last_applied >= before.commit_index);
      }}).ok());

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return result->Calls() == 1; }));
  EXPECT_TRUE(result->ResultStatus().ok())
      << result->ResultStatus().ToString();
  EXPECT_TRUE(result->RanOnActor());
  EXPECT_TRUE(result->AppliedReached());
}

TEST(RaftReadIndexTest, SingleNodeCompletesWithSelfAck) {
  Cluster cluster(1, ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  RaftNode* leader = cluster.Node(1);
  const RaftNodeStatus before = leader->GetStatus();
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(leader->PostEvent(ReadIndexEvent{
      16,
      [leader, before, result](Status status) {
        result->Record(
            status, leader->IsActorThread(),
            leader->GetStatus().last_applied >= before.commit_index);
      }}).ok());

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return result->Calls() == 1; }));
  EXPECT_TRUE(result->ResultStatus().ok())
      << result->ResultStatus().ToString();
  EXPECT_TRUE(result->RanOnActor());
  EXPECT_TRUE(result->AppliedReached());
}

TEST(RaftReadIndexTest, FollowerRejectsReadIndex) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t leader_id = cluster.LeaderId().value();
  const uint32_t follower_id = Followers(leader_id, 3).front();
  RaftNode* follower = cluster.Node(follower_id);
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(follower->PostEvent(ReadIndexEvent{
      18,
      [follower, result](Status status) {
        result->Record(
            status, follower->IsActorThread(), true);
      }}).ok());

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return result->Calls() == 1; }));
  EXPECT_EQ(result->ResultStatus().code(), Code::kNotLeader);
  EXPECT_TRUE(result->RanOnActor());
}

TEST(RaftReadIndexTest, IsolatedLeaderTimesOut) {
  ClusterOptions options;
  options.read_index_timeout_ms = 150;
  Cluster cluster(3, options);
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t old_leader_id = cluster.LeaderId().value();
  const std::vector<uint32_t> remaining =
      Followers(old_leader_id, 3);
  cluster.network()->Partition({old_leader_id}, remaining);
  ASSERT_TRUE(cluster.DriveUntil(
      [&]() {
        for (uint32_t node_id : remaining) {
          if (cluster.Node(node_id)->GetStatus().role ==
              Role::kLeader) {
            return true;
          }
        }
        return false;
      },
      2s));

  RaftNode* old_leader = cluster.Node(old_leader_id);
  ASSERT_EQ(old_leader->GetStatus().role, Role::kLeader);
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(old_leader->PostEvent(ReadIndexEvent{
      19,
      [old_leader, result](Status status) {
        result->Record(
            status, old_leader->IsActorThread(), true);
      }}).ok());

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return result->Calls() == 1; }, 2s));
  EXPECT_EQ(result->ResultStatus().code(), Code::kTimeout);
  EXPECT_TRUE(result->RanOnActor());
}

TEST(RaftReadIndexTest, HigherTermFailsPendingRead) {
  ClusterOptions options;
  options.read_index_timeout_ms = 1000;
  Cluster cluster(3, options);
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t old_leader_id = cluster.LeaderId().value();
  const std::vector<uint32_t> remaining =
      Followers(old_leader_id, 3);
  cluster.network()->Partition({old_leader_id}, remaining);

  RaftNode* old_leader = cluster.Node(old_leader_id);
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(old_leader->PostEvent(ReadIndexEvent{
      20,
      [old_leader, result](Status status) {
        result->Record(
            status, old_leader->IsActorThread(), true);
      }}).ok());
  cluster.DriveFor(100ms);
  ASSERT_EQ(result->Calls(), 0);

  cluster.network()->Heal();
  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return result->Calls() == 1; }, 2s));
  EXPECT_EQ(result->ResultStatus().code(), Code::kNotLeader);
}

TEST(RaftReadIndexTest, ActorStopFailsPendingRead) {
  ClusterOptions options;
  options.read_index_timeout_ms = 1000;
  Cluster cluster(3, options);
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t old_leader_id = cluster.LeaderId().value();
  cluster.network()->Partition(
      {old_leader_id}, Followers(old_leader_id, 3));

  RaftNode* old_leader = cluster.Node(old_leader_id);
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(old_leader->PostEvent(ReadIndexEvent{
      23,
      [old_leader, result](Status status) {
        result->Record(
            status, old_leader->IsActorThread(), true);
      }}).ok());
  cluster.DriveFor(20ms);
  ASSERT_EQ(result->Calls(), 0);

  ASSERT_TRUE(old_leader->Stop().ok());
  EXPECT_EQ(result->Calls(), 1);
  EXPECT_EQ(result->ResultStatus().code(), Code::kTimeout);
}

TEST(RaftReadIndexTest, FollowerEchoesContextOnlyOnSuccess) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t leader_id = cluster.LeaderId().value();
  const uint32_t follower_id = Followers(leader_id, 3).front();
  const uint64_t term =
      cluster.Node(leader_id)->GetStatus().current_term;
  RaftNode* follower = cluster.Node(follower_id);

  auto success_callbacks = std::make_shared<std::atomic<int>>(0);
  proto::AppendEntriesRequest success;
  success.set_term(term);
  success.set_leader_id(leader_id);
  success.set_read_ctx_id(41);
  ASSERT_TRUE(follower->PostEvent(AppendEntriesRequestEvent{
      leader_id, success,
      [success_callbacks](
          Status status,
          proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.success());
        EXPECT_EQ(response.read_ctx_id(), 41U);
        ++(*success_callbacks);
      }}).ok());
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return success_callbacks->load() == 1;
  }));

  auto failed_callbacks = std::make_shared<std::atomic<int>>(0);
  proto::AppendEntriesRequest failed;
  failed.set_term(term);
  failed.set_leader_id(leader_id);
  failed.set_prev_log_index(1000);
  failed.set_prev_log_term(term);
  failed.set_read_ctx_id(42);
  ASSERT_TRUE(follower->PostEvent(AppendEntriesRequestEvent{
      leader_id, failed,
      [failed_callbacks](
          Status status,
          proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_FALSE(response.success());
        EXPECT_EQ(response.read_ctx_id(), 0U);
        ++(*failed_callbacks);
      }}).ok());
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return failed_callbacks->load() == 1;
  }));
}

TEST(RaftReadIndexTest, WrongContextCannotFormMajority) {
  ClusterOptions options;
  options.read_index_timeout_ms = 150;
  Cluster cluster(3, options);
  cluster.Start();
  ASSERT_TRUE(cluster.WaitForStableLeader());

  const uint32_t leader_id = cluster.LeaderId().value();
  const uint32_t peer_id = Followers(leader_id, 3).front();
  cluster.network()->Partition({leader_id}, Followers(leader_id, 3));
  cluster.DriveFor(20ms);

  RaftNode* leader = cluster.Node(leader_id);
  auto result = std::make_shared<ReadResult>();
  ASSERT_TRUE(leader->PostEvent(ReadIndexEvent{
      21,
      [leader, result](Status status) {
        result->Record(
            status, leader->IsActorThread(), true);
      }}).ok());

  proto::AppendEntriesResponse response;
  response.set_term(leader->GetStatus().current_term);
  response.set_success(true);
  response.set_read_ctx_id(999999);
  ASSERT_TRUE(leader->PostEvent(AppendEntriesResponseEvent{
      peer_id, 0, 0, 0, Status::OK(), response}).ok());

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return result->Calls() == 1; }, 2s));
  EXPECT_EQ(result->ResultStatus().code(), Code::kTimeout);
}

TEST(RaftReadIndexOptionsTest, RejectsInvalidTimeout) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(20261128);
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(directory.Path("node"), &log).ok());
  auto transport = std::make_unique<testing::SimTransport>(network, 1);

  RaftNodeOptions options;
  options.node_id = 1;
  options.read_index_timeout_ms = 0;
  std::unique_ptr<RaftNode> node;
  EXPECT_EQ(RaftNode::Create(
                options, std::move(transport), std::move(log),
                &node)
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(node, nullptr);
}

}  // namespace
}  // namespace raftkv
