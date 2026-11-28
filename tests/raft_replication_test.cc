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
using testing::SimTransport;
using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-raft-replication-XXXXXX";
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

struct AppliedRecord {
  uint64_t index = 0;
  std::string command;
};

class ApplyRecorder {
 public:
  void SetActorCheck(std::function<bool()> check) {
    std::lock_guard<std::mutex> lock(mutex_);
    actor_check_ = std::move(check);
  }

  void Record(uint64_t index, const std::string& command) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (actor_check_ != nullptr && !actor_check_()) {
      actor_violation_ = true;
    }
    records_.push_back(AppliedRecord{index, command});
  }

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return records_.size();
  }

  std::vector<std::string> Commands() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> commands;
    commands.reserve(records_.size());
    for (const AppliedRecord& record : records_) {
      commands.push_back(record.command);
    }
    return commands;
  }

  bool ActorViolation() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return actor_violation_;
  }

 private:
  mutable std::mutex mutex_;
  std::function<bool()> actor_check_;
  std::vector<AppliedRecord> records_;
  bool actor_violation_ = false;
};

class RecordingStateMachine final : public StateMachine {
 public:
  explicit RecordingStateMachine(
      std::shared_ptr<ApplyRecorder> recorder)
      : recorder_(std::move(recorder)) {}

  Status Apply(
      uint64_t log_index, const std::string& command) override {
    recorder_->Record(log_index, command);
    return Status::OK();
  }

  Status TakeSnapshot(
      uint64_t, const std::string&) override {
    return Status::OK();
  }

  Status LoadSnapshot(const std::string&) override {
    return Status::OK();
  }

 private:
  std::shared_ptr<ApplyRecorder> recorder_;
};

struct ClusterOptions {
  uint64_t tick_interval_ms = 5;
  uint64_t heartbeat_interval_ms = 10;
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

class NetworkPump {
 public:
  explicit NetworkPump(std::shared_ptr<SimNetwork> network)
      : network_(std::move(network)),
        thread_([this]() {
          while (running_.load()) {
            network_->AdvanceBy(1ms);
            std::this_thread::sleep_for(1ms);
          }
        }) {}

  ~NetworkPump() {
    Stop();
  }

  NetworkPump(const NetworkPump&) = delete;
  NetworkPump& operator=(const NetworkPump&) = delete;

  void Stop() {
    running_.store(false);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  std::shared_ptr<SimNetwork> network_;
  std::atomic<bool> running_{true};
  std::thread thread_;
};

bool DriveNetworkUntil(
    const std::shared_ptr<SimNetwork>& network,
    const std::function<bool()>& predicate,
    std::chrono::milliseconds timeout = 2s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    network->AdvanceBy(1ms);
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}

class Cluster {
 public:
  Cluster(size_t node_count, ClusterOptions cluster_options)
      : network_(std::make_shared<SimNetwork>(
            cluster_options.random_seed)),
        options_(cluster_options),
        nodes_(node_count),
        recorders_(node_count) {
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
      recorders_[index] = std::make_shared<ApplyRecorder>();
      auto state_machine =
          std::make_unique<RecordingStateMachine>(
              recorders_[index]);
      auto transport =
          std::make_unique<SimTransport>(network_, node_id);
      status = RaftNode::Create(
          std::move(node_options), std::move(transport),
          std::move(log), std::move(state_machine),
          &nodes_[index]);
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
    for (size_t index = 0; index < nodes_.size(); ++index) {
      std::shared_ptr<ApplyRecorder> recorder = recorders_[index];
      RaftNode* node = nodes_[index].get();
      recorder->SetActorCheck(
          [node]() { return node->IsActorThread(); });
      const Status status = node->Start();
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
    }
  }

  RaftNode* Node(uint32_t node_id) {
    return nodes_.at(static_cast<size_t>(node_id - 1)).get();
  }

  std::shared_ptr<ApplyRecorder> Recorder(uint32_t node_id) const {
    return recorders_.at(static_cast<size_t>(node_id - 1));
  }

  const std::shared_ptr<SimNetwork>& network() const {
    return network_;
  }

  std::optional<uint32_t> LeaderId() const {
    std::optional<uint32_t> leader;
    for (size_t index = 0; index < nodes_.size(); ++index) {
      if (nodes_[index]->GetStatus().role == Role::kLeader) {
        if (leader.has_value()) {
          return std::nullopt;
        }
        leader = static_cast<uint32_t>(index + 1);
      }
    }
    return leader;
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
  std::vector<std::shared_ptr<ApplyRecorder>> recorders_;
};

bool WaitForStableLeader(Cluster* cluster) {
  return cluster->DriveUntil(
      [&]() {
        const std::optional<uint32_t> leader = cluster->LeaderId();
        if (!leader.has_value()) {
          return false;
        }
        const RaftNodeStatus leader_status =
            cluster->Node(*leader)->GetStatus();
        if (leader_status.commit_index == 0 ||
            leader_status.last_applied == 0) {
          return false;
        }
        for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
          const RaftNodeStatus status =
              cluster->Node(node_id)->GetStatus();
          if (status.current_term != leader_status.current_term ||
              status.leader_id != *leader ||
              status.commit_index <
                  leader_status.commit_index ||
              status.last_applied <
                  leader_status.commit_index) {
            return false;
          }
        }
        return true;
      },
      3s);
}

std::string ClusterDiagnostics(Cluster* cluster) {
  std::string diagnostics;
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    const RaftNodeStatus status =
        cluster->Node(node_id)->GetStatus();
    diagnostics +=
        "node=" + std::to_string(node_id) +
        " role=" + std::to_string(static_cast<int>(status.role)) +
        " term=" + std::to_string(status.current_term) +
        " leader=" + std::to_string(status.leader_id) +
        " log=" + std::to_string(status.last_log_index) +
        " commit=" + std::to_string(status.commit_index) +
        " applied=" + std::to_string(status.last_applied) +
        " records=" +
        std::to_string(cluster->Recorder(node_id)->Size()) +
        "; ";
  }
  return diagnostics;
}

Status CreateDirectNode(
    const std::shared_ptr<SimNetwork>& network,
    const std::string& directory, uint32_t node_id,
    std::vector<uint32_t> peers, uint64_t initial_term,
    std::vector<proto::LogEntry> entries,
    std::shared_ptr<ApplyRecorder> recorder,
    std::unique_ptr<RaftNode>* node) {
  std::unique_ptr<RaftLog> log;
  Status status = RaftLog::Open(directory, &log);
  if (!status.ok()) {
    return status;
  }
  status = log->SaveHardState(initial_term, std::nullopt);
  if (!status.ok()) {
    return status;
  }
  if (!entries.empty()) {
    status = log->Append(std::move(entries));
    if (!status.ok()) {
      return status;
    }
  }

  RaftNodeOptions options;
  options.node_id = node_id;
  options.peers = std::move(peers);
  options.tick_interval_ms = 1000;
  options.heartbeat_interval_ms = 1000;
  options.election_timeout_min_ms = 1000;
  options.election_timeout_max_ms = 1000;
  options.random_seed = 91;
  auto state_machine =
      std::make_unique<RecordingStateMachine>(recorder);
  auto transport =
      std::make_unique<SimTransport>(network, node_id);
  return RaftNode::Create(
      std::move(options), std::move(transport), std::move(log),
      std::move(state_machine), node);
}

proto::LogEntry Entry(
    uint64_t term, uint64_t index, const std::string& payload) {
  proto::LogEntry entry;
  entry.set_term(term);
  entry.set_index(index);
  entry.set_entry_type(proto::ENTRY_TYPE_NORMAL);
  entry.set_payload(payload);
  return entry;
}

TEST(RaftReplicationTest, LeaderCommitsNoopOnAllNodes) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();

  ASSERT_TRUE(WaitForStableLeader(&cluster));
  const uint32_t leader_id = cluster.LeaderId().value();
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    const RaftNodeStatus status =
        cluster.Node(node_id)->GetStatus();
    EXPECT_GE(status.last_log_index, 1U);
    EXPECT_GE(status.last_applied, 1U);
    EXPECT_EQ(cluster.Recorder(node_id)->Size(), 0U);
    EXPECT_FALSE(cluster.Recorder(node_id)->ActorViolation());
  }
  const RaftNodeStatus leader_status =
      cluster.Node(leader_id)->GetStatus();
  EXPECT_EQ(leader_status.role, Role::kLeader);
  EXPECT_EQ(leader_status.commit_index,
            leader_status.last_log_index);
  EXPECT_EQ(leader_status.last_applied,
            leader_status.commit_index);
}

TEST(RaftReplicationTest, ReplicatesFiveHundredEntriesThroughFaults) {
  ClusterOptions options;
  options.heartbeat_interval_ms = 5;
  options.election_timeout_min_ms = 500;
  options.election_timeout_max_ms = 900;
  Cluster cluster(3, options);
  cluster.Start();
  ASSERT_TRUE(WaitForStableLeader(&cluster));
  const uint32_t leader_id = cluster.LeaderId().value();
  const uint64_t base_last_index =
      cluster.Node(leader_id)->GetStatus().last_log_index;

  std::vector<uint32_t> followers;
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    if (node_id != leader_id) {
      followers.push_back(node_id);
    }
  }
  ASSERT_EQ(followers.size(), 2U);

  cluster.network()->SetDropRate(0.1);
  NetworkPump network_pump(cluster.network());
  constexpr int kEntryCount = 500;
  auto completed = std::make_shared<std::atomic<int>>(0);
  auto failed = std::make_shared<std::atomic<int>>(0);
  auto failure_code = std::make_shared<std::atomic<int>>(-1);
  auto failure_mutex = std::make_shared<std::mutex>();
  auto failure_message = std::make_shared<std::string>();
  std::vector<std::string> expected_commands;
  expected_commands.reserve(kEntryCount);
  for (int index = 0; index < kEntryCount; ++index) {
    const std::string command =
        "command-" + std::to_string(index);
    expected_commands.push_back(command);
    ASSERT_TRUE(cluster.Node(leader_id)->PostEvent(ProposeEvent{
        command,
        [completed, failed, failure_code, failure_mutex,
         failure_message](Status status) {
          if (status.ok()) {
            ++(*completed);
          } else {
            int expected = -1;
            if (failure_code->compare_exchange_strong(
                    expected, static_cast<int>(status.code()))) {
              std::lock_guard<std::mutex> lock(*failure_mutex);
              *failure_message = status.ToString();
            }
            ++(*failed);
          }
        }}).ok());
  }

  network_pump.Stop();
  cluster.DriveFor(20ms);
  cluster.network()->Partition(
      {followers.front()}, {leader_id, followers.back()});
  cluster.DriveFor(25ms);
  cluster.network()->Heal();
  cluster.DriveFor(20ms);
  cluster.network()->Partition(
      {followers.back()}, {leader_id, followers.front()});
  cluster.DriveFor(25ms);
  cluster.network()->Heal();

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() {
        return completed->load() + failed->load() == kEntryCount;
      },
      5s));
  ASSERT_EQ(failed->load(), 0)
      << "first failure: " << *failure_message
      << "; " << ClusterDiagnostics(&cluster);

  const bool converged = cluster.DriveUntil(
      [&]() {
        for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
          if (cluster.Recorder(node_id)->Size() !=
              static_cast<size_t>(kEntryCount)) {
            return false;
          }
        }
        return true;
      },
      15s);
  const std::string diagnostics =
      "completed=" + std::to_string(completed->load()) +
      ", failed=" + std::to_string(failed->load()) +
      "; " + ClusterDiagnostics(&cluster);
  ASSERT_TRUE(converged) << diagnostics;

  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    const RaftNodeStatus status =
        cluster.Node(node_id)->GetStatus();
    EXPECT_EQ(
        status.last_log_index,
        base_last_index + kEntryCount);
    EXPECT_EQ(status.commit_index, status.last_log_index);
    EXPECT_EQ(status.last_applied, status.commit_index);
    EXPECT_EQ(
        cluster.Recorder(node_id)->Commands(),
        expected_commands);
    EXPECT_FALSE(cluster.Recorder(node_id)->ActorViolation());
  }
}

TEST(RaftReplicationTest, RepairsConflictingSuffixFromConflictIndex) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(93);
  network->SetDelay(1ms);
  SimTransport sender(network, 2);
  auto recorder = std::make_shared<ApplyRecorder>();
  auto conflict_callbacks =
      std::make_shared<std::atomic<int>>(0);

  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(CreateDirectNode(
                  network, directory.Path("node"), 1, {2}, 4,
                  {Entry(1, 1, "base"), Entry(2, 2, "old")},
                  recorder, &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  proto::AppendEntriesRequest conflict;
  conflict.set_term(5);
  conflict.set_leader_id(2);
  conflict.set_prev_log_index(2);
  conflict.set_prev_log_term(4);
  sender.SendAppendEntries(
      1, conflict,
      [conflict_callbacks](
          Status status, proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_FALSE(response.success());
        EXPECT_EQ(response.conflict_term(), 2U);
        EXPECT_EQ(response.conflict_index(), 2U);
        ++(*conflict_callbacks);
      });
  ASSERT_TRUE(DriveNetworkUntil(
      network, [&]() { return conflict_callbacks->load() == 1; }));

  auto repair_callbacks = std::make_shared<std::atomic<int>>(0);
  proto::AppendEntriesRequest repair;
  repair.set_term(5);
  repair.set_leader_id(2);
  repair.set_prev_log_index(1);
  repair.set_prev_log_term(1);
  repair.set_leader_commit(3);
  *repair.add_entries() = Entry(5, 2, "new-2");
  *repair.add_entries() = Entry(5, 3, "new-3");
  sender.SendAppendEntries(
      1, repair,
      [repair_callbacks](
          Status status, proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.success());
        ++(*repair_callbacks);
      });

  const auto deadline = std::chrono::steady_clock::now() + 2s;
  while (std::chrono::steady_clock::now() < deadline &&
         repair_callbacks->load() != 1) {
    network->AdvanceBy(1ms);
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_EQ(repair_callbacks->load(), 1);
  EXPECT_EQ(node->GetStatus().commit_index, 3U);
  EXPECT_EQ(node->GetStatus().last_applied, 3U);
  EXPECT_EQ(
      recorder->Commands(),
      (std::vector<std::string>{"base", "new-2", "new-3"}));

  ASSERT_TRUE(node->Stop().ok());
  DrainNetwork(network);
  node.reset();

  std::unique_ptr<RaftLog> reopened;
  ASSERT_TRUE(RaftLog::Open(
                  directory.Path("node"), &reopened)
                  .ok());
  proto::LogEntry second;
  ASSERT_TRUE(reopened->EntryAt(2, &second).ok());
  EXPECT_EQ(second.term(), 5U);
  EXPECT_EQ(second.payload(), "new-2");
  proto::LogEntry third;
  ASSERT_TRUE(reopened->EntryAt(3, &third).ok());
  EXPECT_EQ(third.term(), 5U);
  EXPECT_EQ(third.payload(), "new-3");
}

TEST(RaftReplicationTest, FailsUncommittedProposalAfterLeaderStepdown) {
  Cluster cluster(3, ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(WaitForStableLeader(&cluster));
  const uint32_t old_leader = cluster.LeaderId().value();
  const uint64_t old_term =
      cluster.Node(old_leader)->GetStatus().current_term;

  std::vector<uint32_t> remaining;
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    if (node_id != old_leader) {
      remaining.push_back(node_id);
    }
  }
  cluster.network()->Partition({old_leader}, remaining);

  auto proposal_callbacks =
      std::make_shared<std::atomic<int>>(0);
  auto proposal_failures =
      std::make_shared<std::atomic<int>>(0);
  ASSERT_TRUE(cluster.Node(old_leader)->PostEvent(ProposeEvent{
      "uncommitted",
      [proposal_callbacks, proposal_failures](Status status) {
        if (!status.ok()) {
          ++(*proposal_failures);
        }
        ++(*proposal_callbacks);
      }}).ok());
  cluster.DriveFor(100ms);
  EXPECT_EQ(proposal_callbacks->load(), 0);

  ASSERT_TRUE(cluster.DriveUntil(
      [&]() {
        int leaders = 0;
        for (uint32_t node_id : remaining) {
          const RaftNodeStatus status =
              cluster.Node(node_id)->GetStatus();
          if (status.role == Role::kLeader &&
              status.current_term > old_term) {
            ++leaders;
          }
        }
        return leaders == 1;
      },
      2s));

  cluster.network()->Heal();
  ASSERT_TRUE(cluster.DriveUntil(
      [&]() { return proposal_callbacks->load() == 1; }, 2s));
  EXPECT_EQ(proposal_failures->load(), 1);
}

}  // namespace
}  // namespace raftkv
