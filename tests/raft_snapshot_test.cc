#include "raftkv/raft/raft_node.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/raft/snapshot_bundle.h"

namespace raftkv {
namespace {

using testing::SimNetwork;
using namespace std::chrono_literals;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-raft-snapshot-XXXXXX";
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

void AppendUint32(std::string* output, uint32_t value) {
  const size_t offset = output->size();
  output->resize(offset + sizeof(value));
  EncodeFixed32(output->data() + offset, value);
}

void AppendUint64(std::string* output, uint64_t value) {
  const size_t offset = output->size();
  output->resize(offset + sizeof(value));
  EncodeFixed64(output->data() + offset, value);
}

bool ReadUint32(const std::string& input, size_t* offset,
                uint32_t* value) {
  if (*offset > input.size() ||
      input.size() - *offset < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input.data() + *offset);
  *offset += sizeof(uint32_t);
  return true;
}

bool ReadUint64(const std::string& input, size_t* offset,
                uint64_t* value) {
  if (*offset > input.size() ||
      input.size() - *offset < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input.data() + *offset);
  *offset += sizeof(uint64_t);
  return true;
}

void WriteFile(const std::string& path, const std::string& data) {
  std::ofstream output(
      path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.is_open());
  output.write(data.data(), static_cast<std::streamsize>(data.size()));
  ASSERT_TRUE(output.good());
}

class SnapshotStateMachine final : public StateMachine {
 public:
  Status Apply(uint64_t index, const std::string& command) override {
    std::lock_guard<std::mutex> lock(mutex_);
    commands_.push_back(command);
    applied_indices_.push_back(index);
    return Status::OK();
  }

  Status TakeSnapshot(
      uint64_t index, const std::string& directory) override {
    std::vector<std::string> commands;
    std::vector<uint64_t> indices;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      commands = commands_;
      indices = applied_indices_;
    }
    std::string encoded;
    encoded.append("RFTSTATE", 8);
    AppendUint64(&encoded, index);
    AppendUint64(&encoded, commands.size());
    for (size_t i = 0; i < commands.size(); ++i) {
      AppendUint64(&encoded, indices[i]);
      AppendUint32(
          &encoded, static_cast<uint32_t>(commands[i].size()));
      encoded.append(commands[i]);
    }
    const size_t checksum_offset = encoded.size();
    encoded.resize(checksum_offset + sizeof(uint32_t));
    EncodeFixed32(
        encoded.data() + checksum_offset,
        Crc32(encoded.data(), checksum_offset));

    const std::string path =
        directory + "/state.bin";
    std::ofstream output(
        path, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
      return Status::IOError("open snapshot state");
    }
    output.write(
        encoded.data(), static_cast<std::streamsize>(encoded.size()));
    if (!output.good()) {
      return Status::IOError("write snapshot state");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    last_snapshot_index_ = index;
    ++snapshot_count_;
    return Status::OK();
  }

  Status LoadSnapshot(const std::string& directory) override {
    std::ifstream input(
        directory + "/state.bin", std::ios::binary);
    if (!input.is_open()) {
      return Status::IOError("open snapshot state");
    }
    std::string encoded{
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()};
    if (encoded.size() < 8 + sizeof(uint64_t) * 2 +
                              sizeof(uint32_t) ||
        encoded.compare(0, 8, "RFTSTATE") != 0) {
      return Status::Corruption("invalid snapshot state");
    }
    const size_t checksum_offset =
        encoded.size() - sizeof(uint32_t);
    const uint32_t checksum =
        DecodeFixed32(encoded.data() + checksum_offset);
    if (Crc32(encoded.data(), checksum_offset) != checksum) {
      return Status::Corruption("snapshot state checksum");
    }

    size_t offset = 8;
    uint64_t last_index = 0;
    uint64_t count = 0;
    if (!ReadUint64(encoded, &offset, &last_index) ||
        !ReadUint64(encoded, &offset, &count)) {
      return Status::Corruption("truncated snapshot state");
    }
    std::vector<std::string> commands;
    std::vector<uint64_t> indices;
    for (uint64_t i = 0; i < count; ++i) {
      uint64_t index = 0;
      uint32_t length = 0;
      if (!ReadUint64(encoded, &offset, &index) ||
          !ReadUint32(encoded, &offset, &length) ||
          offset > checksum_offset ||
          checksum_offset - offset < length) {
        return Status::Corruption("truncated snapshot command");
      }
      commands.emplace_back(encoded.data() + offset, length);
      indices.push_back(index);
      offset += length;
    }
    if (offset != checksum_offset) {
      return Status::Corruption("trailing snapshot state");
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      commands_ = std::move(commands);
      applied_indices_ = std::move(indices);
      last_snapshot_index_ = last_index;
      ++load_count_;
    }
    return Status::OK();
  }

  std::vector<std::string> Commands() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return commands_;
  }

  uint64_t LastSnapshotIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_snapshot_index_;
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
  std::vector<std::string> commands_;
  std::vector<uint64_t> applied_indices_;
  uint64_t last_snapshot_index_ = 0;
  int snapshot_count_ = 0;
  int load_count_ = 0;
};

struct ClusterOptions {
  uint64_t tick_interval_ms = 5;
  uint64_t heartbeat_interval_ms = 10;
  uint64_t election_timeout_min_ms = 50;
  uint64_t election_timeout_max_ms = 100;
  uint64_t snapshot_threshold_entries = 4;
  uint64_t snapshot_chunk_size_bytes = 64;
  uint64_t random_seed = 20261128;
};

class Cluster {
 public:
  explicit Cluster(ClusterOptions cluster_options)
      : network_(std::make_shared<SimNetwork>(
            cluster_options.random_seed)),
        options_(cluster_options),
        machines_(3),
        nodes_(3) {
    network_->SetDelay(1ms);
    for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
      std::vector<uint32_t> peers;
      for (uint32_t peer = 1; peer <= 3; ++peer) {
        if (peer != node_id) {
          peers.push_back(peer);
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
      machines_[node_id - 1] = machine.get();
      auto transport =
          std::make_unique<testing::SimTransport>(network_, node_id);
      status = RaftNode::Create(
          std::move(node_options), std::move(transport),
          std::move(log), std::move(machine),
          &nodes_[node_id - 1]);
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
    }
  }

  ~Cluster() {
    for (std::unique_ptr<RaftNode>& node : nodes_) {
      node->Stop();
    }
    for (int attempt = 0;
         attempt < 100 && network_->PendingMessageCount() != 0;
         ++attempt) {
      network_->AdvanceBy(1s);
    }
  }

  void Start() {
    for (std::unique_ptr<RaftNode>& node : nodes_) {
      const Status status = node->Start();
      if (!status.ok()) {
        throw std::runtime_error(status.ToString());
      }
    }
  }

  RaftNode* Node(uint32_t node_id) {
    return nodes_.at(node_id - 1).get();
  }

  SnapshotStateMachine* Machine(
      uint32_t node_id) const {
    return machines_.at(node_id - 1);
  }

  std::optional<uint32_t> LeaderId() const {
    std::optional<uint32_t> leader;
    for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
      if (nodes_[node_id - 1]->GetStatus().role != Role::kLeader) {
        continue;
      }
      if (leader.has_value()) {
        return std::nullopt;
      }
      leader = node_id;
    }
    return leader;
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

  const std::shared_ptr<SimNetwork>& network() const {
    return network_;
  }

 private:
  std::shared_ptr<SimNetwork> network_;
  ClusterOptions options_;
  TempDir directory_;
  std::vector<SnapshotStateMachine*> machines_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
};

TEST(RaftSnapshotTest, OfflineFollowerCatchesUpThroughSnapshot) {
  Cluster cluster(ClusterOptions{});
  cluster.Start();
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return cluster.LeaderId().has_value();
  }));
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    const uint32_t leader_id = cluster.LeaderId().value();
    const RaftNodeStatus status =
        cluster.Node(leader_id)->GetStatus();
    return status.commit_index >= 1 &&
           status.last_applied == status.commit_index;
  }));

  const uint32_t leader_id = cluster.LeaderId().value();
  uint32_t offline_id = 0;
  for (uint32_t node_id = 1; node_id <= 3; ++node_id) {
    if (node_id != leader_id) {
      offline_id = node_id;
      break;
    }
  }
  ASSERT_NE(offline_id, 0U);
  cluster.network()->DropNode(offline_id);

  constexpr int kCommandCount = 11;
  auto completed = std::make_shared<std::atomic<int>>(0);
  auto failed = std::make_shared<std::atomic<int>>(0);
  std::vector<std::string> expected;
  for (int i = 0; i < kCommandCount; ++i) {
    const std::string command = "command-" + std::to_string(i);
    expected.push_back(command);
    ASSERT_TRUE(cluster.Node(leader_id)->PostEvent(ProposeEvent{
        command,
        [completed, failed](Status status) {
          if (status.ok()) {
            ++(*completed);
          } else {
            ++(*failed);
          }
        }}).ok());
  }

  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return completed->load() + failed->load() == kCommandCount;
  }));
  ASSERT_EQ(failed->load(), 0);
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return cluster.Node(leader_id)->GetStatus().snapshot_index >= 8;
  }));

  cluster.network()->RestoreNode(offline_id);
  ASSERT_TRUE(cluster.DriveUntil([&]() {
    return cluster.Machine(offline_id)->Commands() == expected &&
           cluster.Node(offline_id)->GetStatus().snapshot_index >= 8;
  }, 10s));
  EXPECT_GE(
      cluster.Node(offline_id)->GetStatus().snapshot_index,
      8U);
  EXPECT_GE(cluster.Machine(leader_id)->SnapshotCount(), 2);
  EXPECT_GE(cluster.Machine(offline_id)->LoadCount(), 1);
  EXPECT_EQ(
      cluster.Machine(offline_id)->Commands(), expected);
  EXPECT_EQ(
      cluster.Machine(leader_id)->Commands(), expected);
}

TEST(RaftSnapshotTest, RecoversPersistedInstallTransaction) {
  TempDir directory;
  const std::string log_directory = directory.Path("node");
  std::filesystem::create_directories(log_directory);
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(log_directory, &log).ok());
  ASSERT_TRUE(log->SaveHardState(5, std::nullopt).ok());

  auto machine = std::make_unique<SnapshotStateMachine>();
  ASSERT_TRUE(machine->Apply(2, "a").ok());
  ASSERT_TRUE(machine->Apply(3, "b").ok());
  const std::string snapshot_root =
      log_directory + "/snapshots";
  const std::string snapshot_directory =
      snapshot_root + "/snapshot-5-1";
  const std::string state_directory =
      snapshot_directory + "/state";
  std::filesystem::create_directories(state_directory);
  ASSERT_TRUE(
      machine->TakeSnapshot(5, state_directory).ok());
  ASSERT_TRUE(PackSnapshotDirectory(
                  state_directory,
                  snapshot_directory + "/snapshot.bundle")
                  .ok());

  std::string marker;
  marker.append("RFTINST1", 8);
  AppendUint32(&marker, 1);
  AppendUint64(&marker, 5);
  AppendUint64(&marker, 1);
  const size_t checksum_offset = marker.size();
  marker.resize(checksum_offset + sizeof(uint32_t));
  EncodeFixed32(
      marker.data() + checksum_offset,
      Crc32(marker.data(), checksum_offset));
  WriteFile(snapshot_root + "/INSTALLING", marker);

  SnapshotStateMachine* machine_ptr = machine.get();
  auto network = std::make_shared<SimNetwork>(1);
  auto transport =
      std::make_unique<testing::SimTransport>(network, 1);
  RaftNodeOptions options;
  options.node_id = 1;
  options.snapshot_threshold_entries = 4;
  std::unique_ptr<RaftNode> node;
  ASSERT_TRUE(RaftNode::Create(
                  options, std::move(transport), std::move(log),
                  std::move(machine), &node)
                  .ok());
  const RaftNodeStatus status = node->GetStatus();
  EXPECT_EQ(status.snapshot_index, 5U);
  EXPECT_EQ(status.commit_index, 5U);
  EXPECT_EQ(status.last_applied, 5U);
  EXPECT_EQ(machine_ptr->LoadCount(), 1);
  EXPECT_EQ(
      machine_ptr->Commands(),
      (std::vector<std::string>{"a", "b"}));
  EXPECT_FALSE(std::filesystem::exists(
      snapshot_root + "/INSTALLING"));
}

TEST(RaftSnapshotOptionsTest, RejectsInvalidThresholdAndChunkSize) {
  TempDir directory;
  auto network = std::make_shared<SimNetwork>(1);

  auto verify = [&](uint64_t threshold, uint64_t chunk_size) {
    std::unique_ptr<RaftLog> log;
    EXPECT_TRUE(
        RaftLog::Open(
            directory.Path(
                "node-" + std::to_string(threshold) + "-" +
                std::to_string(chunk_size)),
            &log)
            .ok());
    auto transport =
        std::make_unique<testing::SimTransport>(network, 1);
    auto machine = std::make_unique<SnapshotStateMachine>();
    RaftNodeOptions options;
    options.node_id = 1;
    options.snapshot_threshold_entries = threshold;
    options.snapshot_chunk_size_bytes = chunk_size;
    std::unique_ptr<RaftNode> node;
    EXPECT_EQ(RaftNode::Create(
                  options, std::move(transport), std::move(log),
                  std::move(machine), &node)
                  .code(),
              Code::kInvalidArgument);
  };
  verify(0, 64);
  verify(4, 0);
  verify(4, kMaxSnapshotChunkSize + 1);
}

}  // namespace
}  // namespace raftkv
