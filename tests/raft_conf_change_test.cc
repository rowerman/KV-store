#include "raftkv/raft/raft_node.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "chaos/sim_transport.h"
#include "raftkv/common/status.h"

namespace raftkv {
namespace {

using testing::SimNetwork;
using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-conf-change-XXXXXX";
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

  std::string Path(const std::string& name) const {
    return path_ + "/" + name;
  }

 private:
  std::string path_;
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

class Completion {
 public:
  void Set(Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    status_ = std::move(status);
  }

  bool completed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_.has_value();
  }

  std::optional<Status> status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return status_;
  }

 private:
  mutable std::mutex mutex_;
  std::optional<Status> status_;
};

class SnapshotStateMachine final : public StateMachine {
 public:
  Status Apply(
      uint64_t index, const std::string& command) override {
    std::lock_guard<std::mutex> lock(mutex_);
    indices_.push_back(index);
    commands_.push_back(command);
    return Status::OK();
  }

  Status TakeSnapshot(
      uint64_t index, const std::string& directory) override {
    std::vector<std::string> commands;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      commands = commands_;
    }
    std::ofstream output(
        directory + "/state.txt",
        std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
      return Status::IOError("open test snapshot");
    }
    output << index << '\n' << commands.size() << '\n';
    for (const std::string& command : commands) {
      output << command << '\n';
    }
    if (!output.good()) {
      return Status::IOError("write test snapshot");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    ++snapshot_count_;
    return Status::OK();
  }

  Status LoadSnapshot(const std::string& directory) override {
    std::ifstream input(directory + "/state.txt");
    if (!input.is_open()) {
      return Status::IOError("open test snapshot");
    }
    uint64_t snapshot_index = 0;
    size_t count = 0;
    if (!(input >> snapshot_index >> count)) {
      return Status::Corruption("invalid test snapshot header");
    }
    input.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    std::vector<std::string> commands;
    commands.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      std::string command;
      if (!std::getline(input, command)) {
        return Status::Corruption("truncated test snapshot");
      }
      commands.push_back(std::move(command));
    }
    std::lock_guard<std::mutex> lock(mutex_);
    commands_ = std::move(commands);
    snapshot_index_ = snapshot_index;
    ++load_count_;
    return Status::OK();
  }

  std::vector<std::string> Commands() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return commands_;
  }

  uint64_t SnapshotIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_index_;
  }

  int SnapshotCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return snapshot_count_;
  }

  int LoadCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return load_count_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<uint64_t> indices_;
  std::vector<std::string> commands_;
  uint64_t snapshot_index_ = 0;
  int snapshot_count_ = 0;
  int load_count_ = 0;
};

struct ClusterOptions {
  uint64_t tick_interval_ms = 5;
  uint64_t heartbeat_interval_ms = 10;
  uint64_t election_timeout_min_ms = 180;
  uint64_t election_timeout_max_ms = 260;
  uint64_t snapshot_threshold_entries = 1000;
  uint64_t snapshot_chunk_size_bytes = 64;
  uint64_t random_seed = 20261128;
};

class Cluster {
 public:
  explicit Cluster(ClusterOptions options)
      : options_(options),
        network_(std::make_shared<SimNetwork>(options.random_seed)) {
    network_->SetDelay(1ms);
    for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
      std::vector<uint32_t> peers;
      for (uint32_t peer = 1; peer <= 3; ++peer) {
        if (peer != node_id) {
          peers.push_back(peer);
        }
      }
      AddNode(
          node_id, std::move(peers), true,
          options_.election_timeout_min_ms,
          options_.election_timeout_max_ms);
    }
  }

  ~Cluster() {
    for (std::unique_ptr<RaftNode>& node : nodes_) {
      if (node != nullptr) {
        node->Stop();
      }
    }
  }

  RaftNode* AddNode(
      uint32_t node_id, std::vector<uint32_t> peers,
      bool initially_voting, uint64_t election_min,
      uint64_t election_max) {
    if (node_id == 0) {
      throw std::invalid_argument("node id is zero");
    }
    if (nodes_.size() <= node_id) {
      nodes_.resize(node_id + 1);
      machines_.resize(node_id + 1);
    }

    RaftNodeOptions node_options;
    node_options.node_id = node_id;
    node_options.peers = std::move(peers);
    node_options.initially_voting = initially_voting;
    node_options.tick_interval_ms = options_.tick_interval_ms;
    node_options.heartbeat_interval_ms =
        options_.heartbeat_interval_ms;
    node_options.election_timeout_min_ms = election_min;
    node_options.election_timeout_max_ms = election_max;
    node_options.snapshot_threshold_entries =
        options_.snapshot_threshold_entries;
    node_options.snapshot_chunk_size_bytes =
        options_.snapshot_chunk_size_bytes;
    node_options.random_seed = options_.random_seed + node_id;

    std::unique_ptr<RaftLog> log;
    Status status = RaftLog::Open(
        directory_.Path("node-" + std::to_string(node_id)),
        &log);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    auto machine = std::make_unique<SnapshotStateMachine>();
    machines_[node_id] = machine.get();
    auto transport =
        std::make_unique<testing::SimTransport>(network_, node_id);
    status = RaftNode::Create(
        std::move(node_options), std::move(transport),
        std::move(log), std::move(machine), &nodes_[node_id]);
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    status = nodes_[node_id]->Start();
    if (!status.ok()) {
      throw std::runtime_error(status.ToString());
    }
    return nodes_[node_id].get();
  }

  bool DriveUntil(
      const std::function<bool()>& predicate,
      std::chrono::milliseconds timeout = 5s) {
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

  std::optional<uint32_t> LeaderId() const {
    std::optional<uint32_t> leader;
    for (size_t node_id = 1; node_id < nodes_.size(); ++node_id) {
      if (nodes_[node_id] == nullptr ||
          nodes_[node_id]->GetStatus().role != Role::kLeader) {
        continue;
      }
      if (leader.has_value()) {
        return std::nullopt;
      }
      leader = static_cast<uint32_t>(node_id);
    }
    return leader;
  }

  uint32_t WaitLeader(
      std::chrono::milliseconds timeout = 3s) {
    if (!DriveUntil(
            [&]() { return LeaderId().has_value(); }, timeout)) {
      throw std::runtime_error("cluster did not elect a leader");
    }
    return LeaderId().value();
  }

  Status Propose(uint32_t node_id, const std::string& command) {
    auto completion = std::make_shared<Completion>();
    Status post_status = Node(node_id)->PostEvent(ProposeEvent{
        command,
        [completion](Status status) {
          completion->Set(std::move(status));
        }});
    if (!post_status.ok()) {
      return post_status;
    }
    if (!DriveUntil([&]() { return completion->completed(); })) {
      return Status::Timeout("raft proposal did not complete");
    }
    return completion->status().value();
  }

  Status ProposeConfChange(
      uint32_t node_id, proto::ConfChangeType type,
      uint32_t changed_node) {
    proto::ConfChange change;
    change.set_type(type);
    change.set_node_id(changed_node);
    auto completion = std::make_shared<Completion>();
    Status post_status = Node(node_id)->PostEvent(
        ProposeConfChangeEvent{
            change,
            [completion](Status status) {
              completion->Set(std::move(status));
            }});
    if (!post_status.ok()) {
      return post_status;
    }
    if (!DriveUntil([&]() { return completion->completed(); })) {
      return Status::Timeout(
          "raft configuration change did not complete");
    }
    return completion->status().value();
  }

  bool WaitConfiguration(
      const std::set<uint32_t>& expected,
      std::chrono::milliseconds timeout = 5s) {
    return DriveUntil(
        [&]() {
          for (size_t node_id = 1;
               node_id < nodes_.size(); ++node_id) {
            if (nodes_[node_id] == nullptr) {
              continue;
            }
            const std::vector<uint32_t> voters =
                nodes_[node_id]->GetStatus().voters;
            if (std::set<uint32_t>(
                    voters.begin(), voters.end()) != expected) {
              return false;
            }
          }
          return true;
        },
        timeout);
  }

  RaftNode* Node(uint32_t node_id) {
    return nodes_.at(node_id).get();
  }

  SnapshotStateMachine* Machine(uint32_t node_id) const {
    return machines_.at(node_id);
  }

  const std::shared_ptr<SimNetwork>& network() const {
    return network_;
  }

 private:
  ClusterOptions options_;
  std::shared_ptr<SimNetwork> network_;
  TempDir directory_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
  std::vector<SnapshotStateMachine*> machines_;
};

proto::LogEntry MakeConfEntry(
    uint64_t index, uint64_t term,
    proto::ConfChangeType type, uint32_t node_id) {
  proto::ConfChange change;
  change.set_type(type);
  change.set_node_id(node_id);
  std::string payload;
  if (!change.SerializeToString(&payload)) {
    throw std::runtime_error("serialize configuration change");
  }
  proto::LogEntry entry;
  entry.set_index(index);
  entry.set_term(term);
  entry.set_entry_type(proto::ENTRY_TYPE_CONF_CHANGE);
  entry.set_payload(std::move(payload));
  return entry;
}

TEST(RaftConfChangeTest, RejectsInvalidConfigurationChanges) {
  Cluster cluster(ClusterOptions{});
  const uint32_t leader_id = cluster.WaitLeader();

  Status status = cluster.ProposeConfChange(
      leader_id, proto::CONF_CHANGE_REMOVE_NODE, 99);
  EXPECT_EQ(status.code(), Code::kInvalidArgument);

  status = cluster.ProposeConfChange(
      leader_id, proto::CONF_CHANGE_ADD_NODE, 0);
  EXPECT_EQ(status.code(), Code::kInvalidArgument);

  uint32_t existing_follower = 0;
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    if (node_id != leader_id) {
      existing_follower = node_id;
      break;
    }
  }
  ASSERT_NE(existing_follower, 0U);
  status = cluster.ProposeConfChange(
      leader_id, proto::CONF_CHANGE_ADD_NODE, existing_follower);
  EXPECT_EQ(status.code(), Code::kInvalidArgument);
  EXPECT_EQ(cluster.LeaderId(), leader_id);
}

TEST(RaftConfChangeTest, RejectsSecondUncommittedChange) {
  Cluster cluster(ClusterOptions{});
  const uint32_t leader_id = cluster.WaitLeader();
  cluster.network()->DropNode(leader_id);

  auto first = std::make_shared<Completion>();
  proto::ConfChange add_four;
  add_four.set_type(proto::CONF_CHANGE_ADD_NODE);
  add_four.set_node_id(4);
  ASSERT_TRUE(cluster.Node(leader_id)->PostEvent(
      ProposeConfChangeEvent{
          add_four,
          [first](Status status) {
            first->Set(std::move(status));
          }}).ok());

  Status second_status = cluster.ProposeConfChange(
      leader_id, proto::CONF_CHANGE_ADD_NODE, 5);
  EXPECT_EQ(second_status.code(), Code::kInvalidArgument);
  EXPECT_FALSE(first->completed());
}

TEST(RaftConfChangeTest, AddsNodesCatchesUpAndParticipatesInElection) {
  ClusterOptions options;
  options.snapshot_threshold_entries = 3;
  options.election_timeout_min_ms = 220;
  options.election_timeout_max_ms = 280;
  Cluster cluster(options);
  const uint32_t original_leader_id = cluster.WaitLeader();

  std::vector<std::string> commands;
  for (int i = 0; i < 12; ++i) {
    const std::string command = "value-" + std::to_string(i);
    commands.push_back(command);
    ASSERT_TRUE(
        cluster.Propose(original_leader_id, command).ok());
  }
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return cluster.Node(original_leader_id)
               ->GetStatus()
               .snapshot_index >= 6;
  }));

  cluster.AddNode(4, {1, 2, 3}, false, 40, 70);
  ASSERT_TRUE(cluster.ProposeConfChange(
                  original_leader_id,
                  proto::CONF_CHANGE_ADD_NODE, 4)
                  .ok());
  ASSERT_TRUE(cluster.WaitConfiguration({1, 2, 3, 4}));

  cluster.AddNode(5, {1, 2, 3, 4}, false, 70, 100);
  ASSERT_TRUE(cluster.ProposeConfChange(
                  original_leader_id,
                  proto::CONF_CHANGE_ADD_NODE, 5)
                  .ok());
  ASSERT_TRUE(cluster.WaitConfiguration({1, 2, 3, 4, 5}));

  const std::string final_command = "after-expansion";
  ASSERT_TRUE(
      cluster.Propose(original_leader_id, final_command).ok());
  commands.push_back(final_command);
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return cluster.Machine(4)->Commands() == commands &&
           cluster.Machine(5)->Commands() == commands;
  }, 10s));
  EXPECT_GE(cluster.Machine(4)->LoadCount(), 1);
  EXPECT_GE(cluster.Machine(4)->SnapshotIndex(), 6U);

  cluster.network()->DropNode(original_leader_id);
  for (uint32_t first = 1; first <= 3; ++first) {
    for (uint32_t second = first + 1; second <= 3; ++second) {
      cluster.network()->SetLinkBlocked(first, second, true);
      cluster.network()->SetLinkBlocked(second, first, true);
    }
  }
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    const RaftNodeStatus four = cluster.Node(4)->GetStatus();
    const RaftNodeStatus five = cluster.Node(5)->GetStatus();
    return four.role == Role::kLeader ||
           five.role == Role::kLeader ||
           (four.leader_id != 0 &&
            four.leader_id != original_leader_id) ||
           (five.leader_id != 0 &&
            five.leader_id != original_leader_id);
  }, 10s)) << [&]() {
    std::string details;
    for (uint32_t node_id = 1; node_id <= 5; ++node_id) {
      const RaftNodeStatus status =
          cluster.Node(node_id)->GetStatus();
      details +=
          "node=" + std::to_string(node_id) +
          " role=" + std::to_string(static_cast<int>(status.role)) +
          " term=" + std::to_string(status.current_term) +
          " voted_for=" +
          std::to_string(status.voted_for.value_or(0)) +
          " leader=" + std::to_string(status.leader_id) +
          " commit=" + std::to_string(status.commit_index) +
          " applied=" + std::to_string(status.last_applied) +
          " last_log=" + std::to_string(status.last_log_index) + "\n";
    }
    return details;
  }();

  const RaftNodeStatus four = cluster.Node(4)->GetStatus();
  const RaftNodeStatus five = cluster.Node(5)->GetStatus();
  const uint32_t elected_leader =
      four.role == Role::kLeader
          ? 4
          : (five.role == Role::kLeader
                 ? 5
                 : std::max(four.leader_id, five.leader_id));
  ASSERT_NE(elected_leader, 0U);
  EXPECT_NE(elected_leader, original_leader_id);
  EXPECT_TRUE(
      four.voted_for.value_or(0) == elected_leader ||
      five.voted_for.value_or(0) == elected_leader ||
      four.role == Role::kLeader || five.role == Role::kLeader);
}

TEST(RaftConfChangeTest, RemovingLeaderStepsDownAfterCommit) {
  Cluster cluster(ClusterOptions{});
  const uint32_t leader_id = cluster.WaitLeader();

  ASSERT_TRUE(cluster.ProposeConfChange(
                  leader_id, proto::CONF_CHANGE_REMOVE_NODE,
                  leader_id)
                  .ok());
  const RaftNodeStatus removed = cluster.Node(leader_id)->GetStatus();
  EXPECT_EQ(removed.role, Role::kFollower);
  EXPECT_EQ(
      std::find(
          removed.voters.begin(), removed.voters.end(), leader_id),
      removed.voters.end());

  uint32_t new_leader = 0;
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    const std::optional<uint32_t> leader = cluster.LeaderId();
    if (!leader.has_value() || *leader == leader_id) {
      return false;
    }
    new_leader = *leader;
    return true;
  }));
  ASSERT_TRUE(
      cluster.Propose(new_leader, "after-leader-removal").ok());
}

TEST(RaftConfChangeStartupTest, RestoresConfigurationFromLogTail) {
  TempDir directory;
  const std::string log_directory = directory.Path("node-1");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(log_directory, &log).ok());
  ASSERT_TRUE(log->SaveHardState(1, std::nullopt).ok());

  std::vector<proto::LogEntry> entries;
  proto::LogEntry noop;
  noop.set_index(1);
  noop.set_term(1);
  noop.set_entry_type(proto::ENTRY_TYPE_NO_OP);
  entries.push_back(std::move(noop));
  entries.push_back(MakeConfEntry(
      2, 1, proto::CONF_CHANGE_REMOVE_NODE, 1));
  ASSERT_TRUE(log->Append(std::move(entries)).ok());

  auto network = std::make_shared<SimNetwork>(1);
  auto transport =
      std::make_unique<testing::SimTransport>(network, 1);
  auto machine = std::make_unique<SnapshotStateMachine>();
  RaftNodeOptions options;
  options.node_id = 1;
  options.peers = {2};
  options.election_timeout_min_ms = 20;
  options.election_timeout_max_ms = 30;
  std::unique_ptr<RaftNode> node;
  const Status create_status = RaftNode::Create(
      options, std::move(transport), std::move(log),
      std::move(machine), &node);
  ASSERT_TRUE(create_status.ok()) << create_status.ToString();

  const RaftNodeStatus status = node->GetStatus();
  EXPECT_EQ(status.configuration_index, 2U);
  EXPECT_EQ(status.voters, (std::vector<uint32_t>{2}));
}

TEST(RaftConfChangeFollowerTest, RestoresConfigurationAfterSuffixReplace) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(1);
  network->SetDelay(1ms);
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(
      RaftLog::Open(directory.Path("node-3"), &log).ok());
  auto transport =
      std::make_unique<testing::SimTransport>(network, 3);
  auto machine = std::make_unique<SnapshotStateMachine>();
  RaftNodeOptions options;
  options.node_id = 3;
  options.peers = {1, 2};
  options.tick_interval_ms = 5;
  options.heartbeat_interval_ms = 10;
  options.election_timeout_min_ms = 200;
  options.election_timeout_max_ms = 300;
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(RaftNode::Create(
                  options, std::move(transport), std::move(log),
                  std::move(machine), &node)
                  .ok());
  ASSERT_TRUE(node->Start().ok());

  auto sender_one =
      std::make_unique<testing::SimTransport>(network, 1);
  auto first_done = std::make_shared<Completion>();
  proto::AppendEntriesRequest add;
  add.set_term(1);
  add.set_leader_id(1);
  add.set_prev_log_index(0);
  add.set_prev_log_term(0);
  *add.add_entries() = MakeConfEntry(
      1, 1, proto::CONF_CHANGE_ADD_NODE, 4);
  sender_one->SendAppendEntries(
      3, add,
      [first_done](
          Status status, proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.success());
        first_done->Set(status);
      });
  ASSERT_TRUE(DriveNetworkUntil(
      network, [&]() { return first_done->completed(); }));
  EXPECT_EQ(
      node->GetStatus().voters,
      (std::vector<uint32_t>{1, 2, 3, 4}));

  auto sender_two =
      std::make_unique<testing::SimTransport>(network, 2);
  auto replacement_done = std::make_shared<Completion>();
  proto::AppendEntriesRequest replacement;
  replacement.set_term(2);
  replacement.set_leader_id(2);
  replacement.set_prev_log_index(0);
  replacement.set_prev_log_term(0);
  proto::LogEntry noop;
  noop.set_index(1);
  noop.set_term(2);
  noop.set_entry_type(proto::ENTRY_TYPE_NO_OP);
  *replacement.add_entries() = std::move(noop);
  sender_two->SendAppendEntries(
      3, replacement,
      [replacement_done](
          Status status, proto::AppendEntriesResponse response) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.success());
        replacement_done->Set(status);
      });
  ASSERT_TRUE(DriveNetworkUntil(
      network,
      [&]() { return replacement_done->completed(); }));
  EXPECT_EQ(
      node->GetStatus().voters,
      (std::vector<uint32_t>{1, 2, 3}));
}

TEST(RaftConfChangeOptionsTest, RejectsNonVotingNodeWithoutPeers) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(1);
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(
      RaftLog::Open(directory.Path("node-1"), &log).ok());
  auto transport =
      std::make_unique<testing::SimTransport>(network, 1);
  auto machine = std::make_unique<SnapshotStateMachine>();
  RaftNodeOptions options;
  options.node_id = 1;
  options.initially_voting = false;
  std::unique_ptr<RaftNode> node;
  const Status status = RaftNode::Create(
      options, std::move(transport), std::move(log),
      std::move(machine), &node);
  EXPECT_EQ(status.code(), Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
