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
using testing::SimTransport;
using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-raft-election-XXXXXX";
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

struct ClusterOptions {
  uint64_t tick_interval_ms = 5;
  uint64_t heartbeat_interval_ms = 15;
  uint64_t election_timeout_min_ms = 40;
  uint64_t election_timeout_max_ms = 80;
  uint64_t random_seed = 20261127;
};

void DrainNetwork(const std::shared_ptr<SimNetwork>& network) {
  for (int attempt = 0;
       attempt < 100 && network->PendingMessageCount() != 0;
       ++attempt) {
    network->AdvanceBy(1s);
  }
}

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
      node_options.tick_interval_ms =
          options_.tick_interval_ms;
      node_options.heartbeat_interval_ms =
          options_.heartbeat_interval_ms;
      node_options.election_timeout_min_ms =
          options_.election_timeout_min_ms;
      node_options.election_timeout_max_ms =
          options_.election_timeout_max_ms;
      node_options.random_seed =
          options_.random_seed + node_id;

      std::unique_ptr<RaftLog> log;
      Status status = RaftLog::Open(
          directory_.Path("node-" + std::to_string(node_id)),
          &log);
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
      auto transport =
          std::make_unique<SimTransport>(network_, node_id);
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
    DrainNetwork(network_);
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

  std::vector<uint32_t> LeaderIds() const {
    std::vector<uint32_t> leaders;
    for (size_t index = 0; index < nodes_.size(); ++index) {
      if (nodes_[index]->GetStatus().role == Role::kLeader) {
        leaders.push_back(static_cast<uint32_t>(index + 1));
      }
    }
    return leaders;
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
  std::shared_ptr<SimNetwork> network_;
  ClusterOptions options_;
  TempDir directory_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
};

struct SingleNodeOptions {
  uint32_t node_id = 1;
  std::vector<uint32_t> peers;
  ClusterOptions cluster;
  uint64_t initial_term = 0;
  std::vector<proto::LogEntry> entries;
};

Status CreateSingleNode(
    const std::shared_ptr<SimNetwork>& network,
    const std::string& directory,
    const SingleNodeOptions& test_options,
    std::unique_ptr<RaftNode>* node) {
  std::unique_ptr<RaftLog> log;
  Status status = RaftLog::Open(directory, &log);
  if (!status.ok()) {
    return status;
  }
  if (test_options.initial_term != 0) {
    status = log->SaveHardState(
        test_options.initial_term, std::nullopt);
    if (!status.ok()) {
      return status;
    }
  }
  if (!test_options.entries.empty()) {
    status = log->Append(test_options.entries);
    if (!status.ok()) {
      return status;
    }
  }

  RaftNodeOptions options;
  options.node_id = test_options.node_id;
  options.peers = test_options.peers;
  options.tick_interval_ms =
      test_options.cluster.tick_interval_ms;
  options.heartbeat_interval_ms =
      test_options.cluster.heartbeat_interval_ms;
  options.election_timeout_min_ms =
      test_options.cluster.election_timeout_min_ms;
  options.election_timeout_max_ms =
      test_options.cluster.election_timeout_max_ms;
  options.random_seed = test_options.cluster.random_seed;
  auto transport = std::make_unique<SimTransport>(
      network, test_options.node_id);
  return RaftNode::Create(
      std::move(options), std::move(transport),
      std::move(log), node);
}

proto::RequestVoteRequest VoteRequest(
    uint64_t term, uint32_t candidate_id,
    uint64_t last_log_term, uint64_t last_log_index,
    bool is_pre_vote) {
  proto::RequestVoteRequest request;
  request.set_term(term);
  request.set_candidate_id(candidate_id);
  request.set_last_log_term(last_log_term);
  request.set_last_log_index(last_log_index);
  request.set_is_pre_vote(is_pre_vote);
  return request;
}

proto::AppendEntriesRequest HeartbeatRequest(
    uint64_t term, uint32_t leader_id) {
  proto::AppendEntriesRequest request;
  request.set_term(term);
  request.set_leader_id(leader_id);
  return request;
}

proto::LogEntry Entry(
    uint64_t term, uint64_t index,
    proto::EntryType type = proto::ENTRY_TYPE_NORMAL) {
  proto::LogEntry entry;
  entry.set_term(term);
  entry.set_index(index);
  entry.set_entry_type(type);
  entry.set_payload(type == proto::ENTRY_TYPE_NO_OP
                        ? std::string()
                        : "entry-" + std::to_string(index));
  return entry;
}

TEST(RaftElectionTest, ElectsSingleLeaderWithinOneSecond) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() {
        const std::vector<uint32_t> leaders =
            cluster.LeaderIds();
        if (leaders.size() != 1) {
          return false;
        }
        const uint32_t leader_id = leaders.front();
        const uint64_t leader_term =
            cluster.Node(leader_id)->GetStatus().current_term;
        for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
          const RaftNodeStatus status =
              cluster.Node(node_id)->GetStatus();
          if (status.current_term != leader_term ||
              status.leader_id != leader_id) {
            return false;
          }
        }
        return true;
      },
      1s));
  const uint32_t leader_id = cluster.LeaderIds().front();
  const uint64_t leader_term =
      cluster.Node(leader_id)->GetStatus().current_term;
  EXPECT_GT(leader_term, 0U);
  EXPECT_EQ(cluster.LeaderIds().size(), 1U);

  cluster.DriveFor(200ms);
  EXPECT_EQ(cluster.LeaderIds().size(), 1U);
  EXPECT_EQ(
      cluster.Node(leader_id)->GetStatus().current_term,
      leader_term);
}

TEST(RaftElectionTest, SingleNodeElectsItself) {
  Cluster cluster(1, ClusterOptions{});
  cluster.Start();

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return cluster.LeaderIds().size() == 1; }, 500ms));
  const RaftNodeStatus status = cluster.Node(1)->GetStatus();
  EXPECT_EQ(status.role, Role::kLeader);
  EXPECT_EQ(status.current_term, 1U);
  EXPECT_EQ(status.leader_id, 1U);
  ASSERT_TRUE(status.voted_for.has_value());
  EXPECT_EQ(status.voted_for.value(), 1U);
}

TEST(RaftElectionTest, IsolatedPreVoteDoesNotAdvanceTerm) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(17);
  network->SetDelay(1ms);
  SingleNodeOptions test_options;
  test_options.node_id = 1;
  test_options.peers = {2};
  test_options.cluster.tick_interval_ms = 5;
  test_options.cluster.heartbeat_interval_ms = 20;
  test_options.cluster.election_timeout_min_ms = 30;
  test_options.cluster.election_timeout_max_ms = 40;

  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateSingleNode(
                  network, directory.Path("node"), test_options,
                  &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  const auto deadline = std::chrono::steady_clock::now() + 500ms;
  while (std::chrono::steady_clock::now() < deadline &&
         node->GetStatus().role != Role::kPreCandidate) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(node->GetStatus().role, Role::kPreCandidate);
  EXPECT_EQ(node->GetStatus().current_term, 0U);

  const auto settle_deadline =
      std::chrono::steady_clock::now() + 200ms;
  while (std::chrono::steady_clock::now() < settle_deadline) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  EXPECT_EQ(node->GetStatus().current_term, 0U);
  EXPECT_EQ(node->GetStatus().role, Role::kPreCandidate);
  EXPECT_TRUE(node->Stop().ok());
  DrainNetwork(network);
}

TEST(RaftElectionTest, RecentLeaderRejectsPreVote) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(23);
  network->SetDelay(1ms);
  SimTransport leader_sender(network, 2);
  SimTransport candidate_sender(network, 3);

  SingleNodeOptions test_options;
  test_options.node_id = 1;
  test_options.peers = {2, 3};
  test_options.cluster.tick_interval_ms = 1000;
  test_options.cluster.heartbeat_interval_ms = 1000;
  test_options.cluster.election_timeout_min_ms = 1000;
  test_options.cluster.election_timeout_max_ms = 1000;

  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateSingleNode(
                  network, directory.Path("node"), test_options,
                  &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  std::atomic<int> heartbeat_callbacks{0};
  std::atomic<int> pre_vote_callbacks{0};
  leader_sender.SendAppendEntries(
      1, HeartbeatRequest(5, 2),
      [&](Status status, proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.success());
        ++heartbeat_callbacks;
      });
  const auto heartbeat_deadline =
      std::chrono::steady_clock::now() + 1s;
  while (std::chrono::steady_clock::now() < heartbeat_deadline &&
         heartbeat_callbacks.load() != 1) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(heartbeat_callbacks.load(), 1);
  EXPECT_EQ(node->GetStatus().leader_id, 2U);

  candidate_sender.SendRequestVote(
      1, VoteRequest(6, 3, 0, 0, true),
      [&](Status status, proto::RequestVoteResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_FALSE(response.vote_granted());
        ++pre_vote_callbacks;
      });
  const auto pre_vote_deadline =
      std::chrono::steady_clock::now() + 1s;
  while (std::chrono::steady_clock::now() < pre_vote_deadline &&
         pre_vote_callbacks.load() != 1) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(pre_vote_callbacks.load(), 1);
  EXPECT_EQ(node->GetStatus().current_term, 5U);
  EXPECT_EQ(node->GetStatus().leader_id, 2U);
  EXPECT_TRUE(node->Stop().ok());
  DrainNetwork(network);
}

TEST(RaftElectionTest, PersistsOneVotePerTerm) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(31);
  network->SetDelay(1ms);
  SimTransport first_candidate(network, 1);
  SimTransport second_candidate(network, 3);

  SingleNodeOptions test_options;
  test_options.node_id = 2;
  test_options.peers = {1, 3};
  test_options.cluster.tick_interval_ms = 1000;
  test_options.cluster.heartbeat_interval_ms = 1000;
  test_options.cluster.election_timeout_min_ms = 1000;
  test_options.cluster.election_timeout_max_ms = 1000;

  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateSingleNode(
                  network, directory.Path("node"), test_options,
                  &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  std::atomic<int> completed{0};
  std::atomic<int> granted{0};
  std::atomic<int> rejected{0};
  const auto record_response =
      [&](Status status, proto::RequestVoteResponse response,
          bool expected) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response.vote_granted(), expected);
        if (response.vote_granted()) {
          ++granted;
        } else {
          ++rejected;
        }
        ++completed;
      };

  first_candidate.SendRequestVote(
      2, VoteRequest(5, 1, 0, 0, false),
      [&](Status status, proto::RequestVoteResponse response) {
        record_response(std::move(status), response, true);
      });
  for (int repeat = 0; repeat < 2; ++repeat) {
    first_candidate.SendRequestVote(
        2, VoteRequest(5, 1, 0, 0, false),
        [&](Status status, proto::RequestVoteResponse response) {
          record_response(std::move(status), response, true);
        });
  }
  second_candidate.SendRequestVote(
      2, VoteRequest(5, 3, 0, 0, false),
      [&](Status status, proto::RequestVoteResponse response) {
        record_response(std::move(status), response, false);
      });

  const auto deadline = std::chrono::steady_clock::now() + 1s;
  while (std::chrono::steady_clock::now() < deadline &&
         completed.load() != 4) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(completed.load(), 4);
  EXPECT_EQ(granted.load(), 3);
  EXPECT_EQ(rejected.load(), 1);
  ASSERT_TRUE(node->GetStatus().voted_for.has_value());
  EXPECT_EQ(node->GetStatus().voted_for.value(), 1U);

  ASSERT_TRUE(node->Stop().ok());
  DrainNetwork(network);
  node.reset();

  std::unique_ptr<RaftLog> reopened;
  ASSERT_TRUE(RaftLog::Open(
                  directory.Path("node"), &reopened)
                  .ok());
  EXPECT_EQ(reopened->hard_state().current_term, 5U);
  ASSERT_TRUE(reopened->hard_state().voted_for.has_value());
  EXPECT_EQ(reopened->hard_state().voted_for.value(), 1U);
}

TEST(RaftElectionTest, ComparesCandidateLogTermBeforeIndex) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(41);
  network->SetDelay(1ms);
  SimTransport stale_candidate(network, 1);
  SimTransport current_candidate(network, 3);

  SingleNodeOptions test_options;
  test_options.node_id = 2;
  test_options.peers = {1, 3};
  test_options.cluster.tick_interval_ms = 1000;
  test_options.cluster.heartbeat_interval_ms = 1000;
  test_options.cluster.election_timeout_min_ms = 1000;
  test_options.cluster.election_timeout_max_ms = 1000;
  test_options.initial_term = 5;
  test_options.entries = {Entry(5, 1)};

  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateSingleNode(
                  network, directory.Path("node"), test_options,
                  &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  std::atomic<int> stale_callbacks{0};
  stale_candidate.SendRequestVote(
      2, VoteRequest(6, 1, 4, 100, false),
      [&](Status status, proto::RequestVoteResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_FALSE(response.vote_granted());
        ++stale_callbacks;
      });
  const auto stale_deadline =
      std::chrono::steady_clock::now() + 1s;
  while (std::chrono::steady_clock::now() < stale_deadline &&
         stale_callbacks.load() != 1) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(stale_callbacks.load(), 1);
  EXPECT_EQ(node->GetStatus().current_term, 6U);
  EXPECT_FALSE(node->GetStatus().voted_for.has_value());

  std::atomic<int> current_callbacks{0};
  current_candidate.SendRequestVote(
      2, VoteRequest(6, 3, 5, 1, false),
      [&](Status status, proto::RequestVoteResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.vote_granted());
        ++current_callbacks;
      });
  const auto current_deadline =
      std::chrono::steady_clock::now() + 1s;
  while (std::chrono::steady_clock::now() < current_deadline &&
         current_callbacks.load() != 1) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(current_callbacks.load(), 1);
  ASSERT_TRUE(node->GetStatus().voted_for.has_value());
  EXPECT_EQ(node->GetStatus().voted_for.value(), 3U);
  EXPECT_TRUE(node->Stop().ok());
  DrainNetwork(network);
}

TEST(RaftElectionTest, ReelectsAfterPartitionAndDemotesOldLeader) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return cluster.LeaderIds().size() == 1; }, 1s));
  const uint32_t old_leader = cluster.LeaderIds().front();
  const uint64_t old_term =
      cluster.Node(old_leader)->GetStatus().current_term;

  std::vector<uint32_t> remaining;
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    if (node_id != old_leader) {
      remaining.push_back(node_id);
    }
  }
  cluster.network()->Partition({old_leader}, remaining);

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() {
        std::vector<uint32_t> leaders;
        for (uint32_t node_id : remaining) {
          const RaftNodeStatus status =
              cluster.Node(node_id)->GetStatus();
          if (status.role == Role::kLeader &&
              status.current_term > old_term) {
            leaders.push_back(node_id);
          }
        }
        return leaders.size() == 1;
      },
      1s));

  uint32_t new_leader = 0;
  for (uint32_t node_id : remaining) {
    if (cluster.Node(node_id)->GetStatus().role ==
        Role::kLeader) {
      new_leader = node_id;
      break;
    }
  }
  ASSERT_NE(new_leader, 0U);
  const uint64_t new_term =
      cluster.Node(new_leader)->GetStatus().current_term;
  ASSERT_GT(new_term, old_term);

  cluster.network()->Heal();
  ASSERT_TRUE(cluster.DriveUntil(
      [&]() {
        const RaftNodeStatus status =
            cluster.Node(old_leader)->GetStatus();
        return status.role == Role::kFollower &&
               status.current_term == new_term &&
               status.leader_id == new_leader;
      },
      1s));

  cluster.DriveFor(200ms);
  const RaftNodeStatus recovered =
      cluster.Node(old_leader)->GetStatus();
  EXPECT_EQ(recovered.role, Role::kFollower);
  EXPECT_EQ(recovered.current_term, new_term);
  EXPECT_EQ(recovered.leader_id, new_leader);
}

}  // namespace
}  // namespace raftkv
