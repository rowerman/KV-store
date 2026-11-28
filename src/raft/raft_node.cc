#include "raftkv/raft/raft_node.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <system_error>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/common/mpsc_queue.h"
#include "raftkv/raft/snapshot_bundle.h"

namespace raftkv {
namespace {

enum class Lifecycle {
  kCreated,
  kRunning,
  kStopping,
  kStopped,
};

inline constexpr uint64_t kMaxTimerIntervalMs =
    24ULL * 60ULL * 60ULL * 1000ULL;
inline constexpr uint64_t kMaxEntriesPerAppend = 64;
inline constexpr char kSnapshotDirectoryName[] = "snapshots";
inline constexpr char kInstallMarkerName[] = "INSTALLING";
inline constexpr char kIncomingSnapshotName[] = "incoming.bundle";
inline constexpr char kSnapshotStateName[] = "state";
inline constexpr char kSnapshotBundleName[] = "snapshot.bundle";
inline constexpr char kConfigurationFileName[] = "raft_conf.bin";
inline constexpr char kInstallMagic[] = "RFTINST1";
inline constexpr char kConfigurationMagic[] = "RFTCONF1";
inline constexpr uint32_t kConfigurationFormatVersion = 1;

namespace fs = std::filesystem;

struct RaftConfiguration {
  uint64_t index = 0;
  std::set<uint32_t> voters;
};

Status ValidateConfChange(const proto::ConfChange& change) {
  if (change.node_id() == 0) {
    return Status::InvalidArgument(
        "raft configuration node id cannot be zero");
  }
  switch (change.type()) {
    case proto::CONF_CHANGE_ADD_NODE:
    case proto::CONF_CHANGE_REMOVE_NODE:
      return Status::OK();
    default:
      return Status::InvalidArgument(
          "raft configuration change type is invalid");
  }
}

Status EncodeConfChangePayload(
    const proto::ConfChange& change, std::string* payload) {
  if (payload == nullptr) {
    return Status::InvalidArgument(
        "raft configuration payload output is null");
  }
  Status status = ValidateConfChange(change);
  if (!status.ok()) {
    return status;
  }
  std::string encoded;
  if (!change.SerializeToString(&encoded)) {
    return Status::Corruption(
        "serialize raft configuration change");
  }
  payload->swap(encoded);
  return Status::OK();
}

Status DecodeConfChangePayload(
    const std::string& payload, proto::ConfChange* change) {
  if (change == nullptr) {
    return Status::InvalidArgument(
        "raft configuration change output is null");
  }
  proto::ConfChange decoded;
  if (!decoded.ParseFromString(payload)) {
    return Status::Corruption(
        "parse raft configuration change");
  }
  Status status = ValidateConfChange(decoded);
  if (!status.ok()) {
    return Status::Corruption(status.message());
  }
  *change = std::move(decoded);
  return Status::OK();
}

Status ApplyConfChange(
    const proto::ConfChange& change, uint64_t index,
    RaftConfiguration* configuration) {
  if (configuration == nullptr) {
    return Status::InvalidArgument(
        "raft configuration output is null");
  }
  Status status = ValidateConfChange(change);
  if (!status.ok()) {
    return status;
  }

  RaftConfiguration updated = *configuration;
  switch (change.type()) {
    case proto::CONF_CHANGE_ADD_NODE:
      if (!updated.voters.insert(change.node_id()).second) {
        return Status::InvalidArgument(
            "raft configuration node already exists");
      }
      break;
    case proto::CONF_CHANGE_REMOVE_NODE:
      if (updated.voters.erase(change.node_id()) == 0) {
        return Status::InvalidArgument(
            "raft configuration node does not exist");
      }
      if (updated.voters.empty()) {
        return Status::InvalidArgument(
            "raft configuration cannot remove the last voter");
      }
      break;
    default:
      return Status::InvalidArgument(
          "raft configuration change type is invalid");
  }
  updated.index = index;
  *configuration = std::move(updated);
  return Status::OK();
}

Status EncodeConfiguration(
    const RaftConfiguration& configuration, std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument(
        "raft configuration output is null");
  }
  if (configuration.voters.empty() ||
      configuration.voters.count(0) != 0 ||
      configuration.voters.size() >
          std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument(
        "raft configuration voters are invalid");
  }

  const size_t voter_bytes =
      configuration.voters.size() * sizeof(uint32_t);
  if (voter_bytes >
      std::numeric_limits<size_t>::max() -
          (sizeof(kConfigurationMagic) - 1 +
           sizeof(uint32_t) * 3 + sizeof(uint64_t))) {
    return Status::InvalidArgument(
        "raft configuration is too large");
  }
  std::string encoded;
  encoded.reserve(
      sizeof(kConfigurationMagic) - 1 + sizeof(uint32_t) +
      sizeof(uint64_t) + sizeof(uint32_t) + voter_bytes +
      sizeof(uint32_t));
  encoded.append(
      kConfigurationMagic, sizeof(kConfigurationMagic) - 1);

  const auto append_u32 = [&](uint32_t value) {
    const size_t offset = encoded.size();
    encoded.resize(offset + sizeof(uint32_t));
    EncodeFixed32(encoded.data() + offset, value);
  };
  const auto append_u64 = [&](uint64_t value) {
    const size_t offset = encoded.size();
    encoded.resize(offset + sizeof(uint64_t));
    EncodeFixed64(encoded.data() + offset, value);
  };

  append_u32(kConfigurationFormatVersion);
  append_u64(configuration.index);
  append_u32(
      static_cast<uint32_t>(configuration.voters.size()));
  for (uint32_t voter : configuration.voters) {
    append_u32(voter);
  }
  const size_t checksum_offset = encoded.size();
  append_u32(Crc32(encoded.data(), checksum_offset));
  output->swap(encoded);
  return Status::OK();
}

Status DecodeConfiguration(
    const std::string& encoded,
    RaftConfiguration* configuration) {
  if (configuration == nullptr) {
    return Status::InvalidArgument(
        "raft configuration output is null");
  }
  constexpr size_t kFixedSize =
      sizeof(kConfigurationMagic) - 1 + sizeof(uint32_t) +
      sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
  if (encoded.size() < kFixedSize ||
      std::memcmp(
          encoded.data(), kConfigurationMagic,
          sizeof(kConfigurationMagic) - 1) != 0) {
    return Status::Corruption("invalid raft configuration file");
  }

  size_t offset = sizeof(kConfigurationMagic) - 1;
  const uint32_t version = DecodeFixed32(encoded.data() + offset);
  offset += sizeof(uint32_t);
  if (version != kConfigurationFormatVersion) {
    return Status::Corruption(
        "unsupported raft configuration version");
  }
  const uint64_t index = DecodeFixed64(encoded.data() + offset);
  offset += sizeof(uint64_t);
  const uint32_t voter_count =
      DecodeFixed32(encoded.data() + offset);
  offset += sizeof(uint32_t);
  if (voter_count == 0 ||
      voter_count >
          (std::numeric_limits<size_t>::max() - kFixedSize) /
              sizeof(uint32_t)) {
    return Status::Corruption(
        "invalid raft configuration voter count");
  }
  const size_t voter_bytes =
      static_cast<size_t>(voter_count) * sizeof(uint32_t);
  if (encoded.size() != kFixedSize + voter_bytes) {
    return Status::Corruption(
        "invalid raft configuration file size");
  }

  RaftConfiguration decoded;
  decoded.index = index;
  for (uint32_t i = 0; i < voter_count; ++i) {
    const uint32_t voter = DecodeFixed32(encoded.data() + offset);
    offset += sizeof(uint32_t);
    if (voter == 0 ||
        (!decoded.voters.empty() &&
         voter <= *decoded.voters.rbegin())) {
      return Status::Corruption(
          "invalid raft configuration voter list");
    }
    decoded.voters.insert(voter);
  }
  const uint32_t checksum =
      DecodeFixed32(encoded.data() + offset);
  if (Crc32(encoded.data(), offset) != checksum) {
    return Status::Corruption(
        "raft configuration checksum mismatch");
  }
  *configuration = std::move(decoded);
  return Status::OK();
}

class ActorEventSink {
 public:
  bool Post(RaftEvent event) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!open_) {
      return false;
    }
    events_.Push(std::move(event));
    return true;
  }

  bool PopBatch(std::vector<RaftEvent>& output, uint64_t timeout_ms) {
    return events_.PopBatch(output, timeout_ms);
  }

  void Close() {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = false;
  }

 private:
  std::mutex mutex_;
  bool open_ = true;
  MpscQueue<RaftEvent> events_;
};

uint64_t MakeElectionSeed(const RaftNodeOptions& options) {
  uint64_t seed = options.random_seed;
  if (seed == 0) {
    std::random_device random;
    seed = (static_cast<uint64_t>(random()) << 32U) ^
           static_cast<uint64_t>(random());
  }
  return seed ^
         (static_cast<uint64_t>(options.node_id) *
          0x9E3779B97F4A7C15ULL);
}

bool IsLogAtLeastAsUpToDate(
    uint64_t candidate_last_term, uint64_t candidate_last_index,
    uint64_t local_last_term, uint64_t local_last_index) {
  if (candidate_last_term != local_last_term) {
    return candidate_last_term > local_last_term;
  }
  return candidate_last_index >= local_last_index;
}

class NoopStateMachine final : public StateMachine {
 public:
  Status Apply(uint64_t, const std::string&) override {
    return Status::OK();
  }

  Status TakeSnapshot(uint64_t, const std::string&) override {
    return Status::OK();
  }

  Status LoadSnapshot(const std::string&) override {
    return Status::OK();
  }
};

std::string JoinPath(const std::string& directory,
                     const std::string& name) {
  if (!directory.empty() && directory.back() == '/') {
    return directory + name;
  }
  return directory + "/" + name;
}

Status MakeIoError(const char* operation) {
  const int saved_errno = errno;
  return Status::IOError(
      std::string(operation) + ": " +
      std::strerror(saved_errno));
}

Status SyncDirectory(const std::string& directory) {
  const int fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open snapshot directory");
  }
  Status status = Status::OK();
  while (::fsync(fd) < 0) {
    if (errno == EINTR) {
      continue;
    }
    status = MakeIoError("fsync snapshot directory");
    break;
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    return MakeIoError("close snapshot directory");
  }
  return status;
}

Status WriteAll(int fd, const char* data, size_t size,
                const char* operation) {
  size_t written = 0;
  while (written < size) {
    const ssize_t result =
        ::write(fd, data + written, size - written);
    if (result > 0) {
      written += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

Status WriteAtAll(int fd, const char* data, size_t size,
                  uint64_t offset, const char* operation) {
  size_t written = 0;
  while (written < size) {
    const ssize_t result = ::pwrite(
        fd, data + written, size - written,
        static_cast<off_t>(offset + written));
    if (result > 0) {
      written += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

Status ReadAtExact(int fd, char* data, size_t size,
                   uint64_t offset, const char* operation) {
  size_t read_size = 0;
  while (read_size < size) {
    const ssize_t result = ::pread(
        fd, data + read_size, size - read_size,
        static_cast<off_t>(offset + read_size));
    if (result > 0) {
      read_size += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result == 0) {
      return Status::Corruption(
          std::string(operation) + ": unexpected end of file");
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

Status ReadFile(const std::string& path, std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("snapshot file output is null");
  }
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open snapshot metadata");
  }
  struct stat file_stat {};
  if (::fstat(fd, &file_stat) < 0 || file_stat.st_size < 0) {
    const Status status = MakeIoError("stat snapshot metadata");
    ::close(fd);
    return status;
  }
  std::string data(
      static_cast<size_t>(file_stat.st_size), '\0');
  size_t read_size = 0;
  while (read_size < data.size()) {
    const ssize_t result = ::read(
        fd, data.data() + read_size, data.size() - read_size);
    if (result > 0) {
      read_size += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result == 0) {
      ::close(fd);
      return Status::Corruption(
          "truncated snapshot metadata");
    }
    const Status status = MakeIoError("read snapshot metadata");
    ::close(fd);
    return status;
  }
  const int close_result = ::close(fd);
  if (close_result < 0) {
    return MakeIoError("close snapshot metadata");
  }
  output->swap(data);
  return Status::OK();
}

Status WriteConfigurationFile(
    const std::string& path,
    const RaftConfiguration& configuration) {
  std::string encoded;
  Status status = EncodeConfiguration(configuration, &encoded);
  if (!status.ok()) {
    return status;
  }

  const std::string temporary = path + ".tmp";
  const int fd = ::open(
      temporary.c_str(),
      O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return MakeIoError("create raft configuration file");
  }
  status = WriteAll(
      fd, encoded.data(), encoded.size(),
      "write raft configuration file");
  if (status.ok() && ::fsync(fd) < 0) {
    status = MakeIoError("fsync raft configuration file");
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close raft configuration file");
  }
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), path.c_str()) < 0) {
    status = MakeIoError("rename raft configuration file");
    ::unlink(temporary.c_str());
    return status;
  }

  const fs::path parent = fs::path(path).parent_path();
  return SyncDirectory(
      parent.empty() ? std::string(".") : parent.string());
}

Status ReadConfigurationFile(
    const std::string& path, bool* exists,
    RaftConfiguration* configuration) {
  if (exists == nullptr || configuration == nullptr) {
    return Status::InvalidArgument(
        "raft configuration file output is null");
  }
  std::error_code error;
  *exists = fs::exists(path, error);
  if (error) {
    return Status::IOError(
        "inspect raft configuration file: " + error.message());
  }
  if (!*exists) {
    return Status::OK();
  }
  std::string encoded;
  Status status = ReadFile(path, &encoded);
  if (!status.ok()) {
    return status;
  }
  return DecodeConfiguration(encoded, configuration);
}

std::string SnapshotDirectoryName(
    uint64_t last_included_index, uint64_t last_included_term) {
  return "snapshot-" + std::to_string(last_included_index) +
         "-" + std::to_string(last_included_term);
}

Status EncodeInstallMarker(
    uint64_t last_included_index, uint64_t last_included_term,
    std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument(
        "snapshot install marker output is null");
  }
  std::string encoded;
  encoded.reserve(sizeof(kInstallMagic) - 1 + sizeof(uint32_t) +
                  sizeof(uint64_t) * 2 + sizeof(uint32_t));
  encoded.append(kInstallMagic, sizeof(kInstallMagic) - 1);
  const size_t version_offset = encoded.size();
  encoded.resize(version_offset + sizeof(uint32_t));
  EncodeFixed32(encoded.data() + version_offset, 1);
  const size_t index_offset = encoded.size();
  encoded.resize(index_offset + sizeof(uint64_t));
  EncodeFixed64(encoded.data() + index_offset, last_included_index);
  const size_t term_offset = encoded.size();
  encoded.resize(term_offset + sizeof(uint64_t));
  EncodeFixed64(encoded.data() + term_offset, last_included_term);
  const size_t crc_offset = encoded.size();
  encoded.resize(crc_offset + sizeof(uint32_t));
  EncodeFixed32(
      encoded.data() + crc_offset,
      Crc32(encoded.data(), crc_offset));
  output->swap(encoded);
  return Status::OK();
}

Status DecodeInstallMarker(
    const std::string& encoded, uint64_t* last_included_index,
    uint64_t* last_included_term) {
  if (last_included_index == nullptr ||
      last_included_term == nullptr) {
    return Status::InvalidArgument(
        "snapshot install marker output is null");
  }
  const size_t expected_size =
      sizeof(kInstallMagic) - 1 + sizeof(uint32_t) +
      sizeof(uint64_t) * 2 + sizeof(uint32_t);
  if (encoded.size() != expected_size ||
      std::memcmp(
          encoded.data(), kInstallMagic,
          sizeof(kInstallMagic) - 1) != 0) {
    return Status::Corruption("invalid snapshot install marker");
  }
  size_t offset = sizeof(kInstallMagic) - 1;
  const uint32_t version = DecodeFixed32(encoded.data() + offset);
  offset += sizeof(uint32_t);
  if (version != 1) {
    return Status::Corruption(
        "unsupported snapshot install marker version");
  }
  const uint64_t index = DecodeFixed64(encoded.data() + offset);
  offset += sizeof(uint64_t);
  const uint64_t term = DecodeFixed64(encoded.data() + offset);
  offset += sizeof(uint64_t);
  const uint32_t checksum = DecodeFixed32(encoded.data() + offset);
  if (index == 0 || term == 0 ||
      Crc32(encoded.data(), offset) != checksum) {
    return Status::Corruption("invalid snapshot install marker");
  }
  *last_included_index = index;
  *last_included_term = term;
  return Status::OK();
}

}  // namespace

struct RaftNode::Impl {
  using Clock = std::chrono::steady_clock;

  Impl(RaftNodeOptions node_options,
       std::unique_ptr<RaftTransport> node_transport,
       std::unique_ptr<RaftLog> node_log,
       std::unique_ptr<StateMachine> node_state_machine)
      : options(std::move(node_options)),
        transport(std::move(node_transport)),
        log(std::move(node_log)),
        state_machine(std::move(node_state_machine)),
        current_term(log->hard_state().current_term),
        voted_for(log->hard_state().voted_for),
        commit_index(log->snapshot_marker().last_included_index),
        last_applied(commit_index),
        status_role(Role::kFollower),
        status_term(current_term),
        status_voted_for(voted_for.value_or(0)),
        status_leader(0),
        status_commit(commit_index),
        status_last_log(log->LastIndex()),
        status_last_applied(last_applied),
        status_snapshot(
            log->snapshot_marker().last_included_index) {
    bootstrap_configuration_.index = 0;
    bootstrap_configuration_.voters.insert(
        options.peers.begin(), options.peers.end());
    if (options.initially_voting) {
      bootstrap_configuration_.voters.insert(options.node_id);
    }
    configuration_ = bootstrap_configuration_;
    election_random.seed(MakeElectionSeed(options));
    InstallHandlers();
    PublishStatus();
  }

  Status Start();
  Status Stop();
  Status PostEvent(RaftEvent event);
  RaftNodeStatus GetStatus() const;
  bool IsActorThread() const;

  void Run();
  Status Step(const RaftEvent& event);
  void InstallHandlers();
  void ClearHandlers();
  Status OnTick(Clock::time_point now);
  Status StartPreVote(Clock::time_point now);
  Status BecomeCandidate(Clock::time_point now);
  Status BecomeLeader(Clock::time_point now);
  Status ObserveHigherTerm(uint64_t term, Clock::time_point now);
  Status BroadcastVoteRequests(bool is_pre_vote);
  Status BroadcastHeartbeat();
  Status ReplicateToPeer(uint32_t peer_id);
  Status AppendNoop();
  Status HandlePropose(const ProposeEvent& event);
  Status HandleProposeConfChange(
      const ProposeConfChangeEvent& event);
  Status AdvanceLeaderCommit();
  void ResetReplicationProgress();
  void FailPendingProposals(const Status& status);
  void FailPendingConfChange(const Status& status);
  Status ComputeConfigurationAt(
      uint64_t target_index,
      RaftConfiguration* configuration) const;
  Status RebuildCurrentConfiguration();
  void MarkConfigurationCommitted();
  Status StepDownAfterSelfRemoval();
  Status HasUncommittedConfChange(bool* pending) const;
  Status HandleReadIndex(const ReadIndexEvent& event);
  Status StartPendingReads();
  Status SendReadIndexHeartbeat(
      uint64_t read_ctx_id, uint64_t read_index);
  void HandleReadIndexResponse(
      uint32_t peer_id,
      const proto::AppendEntriesResponse& response);
  void TryCompletePendingReads();
  void ExpirePendingReads(Clock::time_point now);
  void FailPendingReads(const Status& status);
  Status HasCommittedEntryInCurrentTerm(bool* committed) const;
  Status ValidateAppendEntries(
      const proto::AppendEntriesRequest& request) const;
  Status BuildConflictResponse(
      uint64_t prev_log_index,
      proto::AppendEntriesResponse* response) const;
  Status RepairFollowerLog(
      const proto::AppendEntriesRequest& request,
      uint64_t* matched_index);
  Status AdvanceCommitIndex(uint64_t candidate_index);
  Status ApplyCommitted();
  Status RetryPeerAfterConflict(
      uint32_t peer_id,
      uint64_t request_prev_log_index,
      const proto::AppendEntriesResponse& response);
  Status OnRequestVoteRequest(const RequestVoteRequestEvent& event);
  Status OnAppendEntriesRequest(
      const AppendEntriesRequestEvent& event);
  Status OnInstallSnapshotRequest(
      const InstallSnapshotRequestEvent& event);
  Status OnRequestVoteResponse(
      const RequestVoteResponseEvent& event);
  Status OnAppendEntriesResponse(
      const AppendEntriesResponseEvent& event);
  Status OnInstallSnapshotResponse(
      const InstallSnapshotResponseEvent& event);
  Status RecoverSnapshotState();
  Status MaybeCreateSnapshot();
  Status CreateLocalSnapshot();
  Status SendInstallSnapshotChunk(uint32_t peer_id);
  Status InstallIncomingSnapshot();
  Status PersistInstallMarker(
      uint64_t last_included_index, uint64_t last_included_term);
  Status RemoveInstallMarker();

  std::chrono::milliseconds NextElectionTimeout();
  void ResetElectionDeadline(Clock::time_point now);
  void ResetHeartbeatDeadline(Clock::time_point now);
  bool ElectionDeadlineReached(Clock::time_point now) const;
  bool HeartbeatDeadlineReached(Clock::time_point now) const;
  bool HasRecentLeader(Clock::time_point now) const;
  bool IsConfiguredPeer(uint32_t peer_id) const;
  bool IsAcceptedLeader(uint32_t peer_id) const;
  bool IsSelfVoter() const;
  bool HasMajority(
      const std::unordered_set<uint32_t>& votes) const;
  void ClearElectionVotes();
  Status CandidateLogIsUpToDate(
      const proto::RequestVoteRequest& request,
      bool* up_to_date) const;
  Status PersistHardState(
      uint64_t term, std::optional<uint32_t> new_voted_for);

  bool IsStopping() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    return lifecycle == Lifecycle::kStopping ||
           lifecycle == Lifecycle::kStopped;
  }

  void PublishStatus() {
    status_role.store(role, std::memory_order_relaxed);
    status_term.store(current_term, std::memory_order_relaxed);
    status_voted_for.store(
        voted_for.value_or(0), std::memory_order_relaxed);
    status_leader.store(leader_id, std::memory_order_relaxed);
    status_commit.store(commit_index, std::memory_order_relaxed);
    status_last_log.store(
        log->LastIndex(), std::memory_order_relaxed);
    status_last_applied.store(
        last_applied, std::memory_order_relaxed);
    status_snapshot.store(
        log->snapshot_marker().last_included_index,
        std::memory_order_relaxed);
    status_configuration_index.store(
        configuration_.index, std::memory_order_relaxed);
    auto voters = std::make_shared<const std::vector<uint32_t>>(
        configuration_.voters.begin(), configuration_.voters.end());
    std::atomic_store(&status_voters, std::move(voters));
  }

  RaftNodeOptions options;
  std::unique_ptr<RaftTransport> transport;
  std::unique_ptr<RaftLog> log;
  std::unique_ptr<StateMachine> state_machine;
  std::shared_ptr<ActorEventSink> event_sink =
      std::make_shared<ActorEventSink>();

  mutable std::mutex lifecycle_mutex;
  std::condition_variable lifecycle_cv;
  std::mutex join_mutex;
  Lifecycle lifecycle = Lifecycle::kCreated;
  std::thread actor_thread;
  std::thread::id actor_id;
  std::atomic<bool> running{false};

  // 以下核心字段只在 Actor 线程访问。
  Role role = Role::kFollower;
  uint64_t current_term = 0;
  std::optional<uint32_t> voted_for;
  uint32_t leader_id = 0;
  uint64_t commit_index = 0;
  uint64_t last_applied = 0;
  std::mt19937_64 election_random;
  Clock::time_point election_deadline{};
  Clock::time_point heartbeat_deadline{};
  std::unordered_set<uint32_t> pre_votes_received;
  std::unordered_set<uint32_t> votes_received;
  RaftConfiguration bootstrap_configuration_;
  RaftConfiguration configuration_;
  std::set<uint32_t> transitional_voters_;
  std::optional<uint64_t> uncommitted_conf_change_index_;

  std::atomic<Role> status_role;
  std::atomic<uint64_t> status_term;
  std::atomic<uint32_t> status_voted_for;
  std::atomic<uint32_t> status_leader;
  std::atomic<uint64_t> status_commit;
  std::atomic<uint64_t> status_last_log;
  std::atomic<uint64_t> status_last_applied;
  std::atomic<uint64_t> status_snapshot;
  std::atomic<uint64_t> status_configuration_index{0};
  std::shared_ptr<const std::vector<uint32_t>> status_voters =
      std::make_shared<const std::vector<uint32_t>>();

  std::atomic<uint64_t> processed_events{0};
  std::atomic<uint64_t> tick_count{0};

  struct PeerReplicationState {
    enum class InflightKind {
      kNone,
      kAppendEntries,
      kInstallSnapshot,
    };

    uint64_t next_index = 1;
    uint64_t match_index = 0;
    uint64_t inflight_sequence = 0;
    uint64_t snapshot_offset = 0;
    uint64_t snapshot_transfer_index = 0;
    InflightKind inflight_kind = InflightKind::kNone;
  };

  struct PendingRead {
    uint64_t read_id = 0;
    uint64_t read_index = 0;
    Clock::time_point deadline{};
    std::unordered_set<uint32_t> acks;
    ReadIndexCallback done;
    bool started = false;
  };

  struct PendingConfChange {
    uint64_t index = 0;
    ConfChangeCallback done;
  };

  struct LocalSnapshot {
    uint64_t last_included_index = 0;
    uint64_t last_included_term = 0;
    RaftConfiguration configuration;
    std::string directory;
    std::string bundle_path;
    uint64_t bundle_size = 0;
  };

  struct IncomingSnapshot {
    uint32_t leader_id = 0;
    uint64_t last_included_index = 0;
    uint64_t last_included_term = 0;
    uint64_t next_offset = 0;
    RaftConfiguration configuration;
    std::string bundle_path;
  };

  std::unordered_map<uint32_t, PeerReplicationState> replication_;
  std::map<uint64_t, ProposeCallback> pending_proposals_;
  std::map<uint64_t, PendingRead> pending_reads_;
  std::optional<LocalSnapshot> local_snapshot_;
  std::optional<IncomingSnapshot> incoming_snapshot_;
  std::optional<PendingConfChange> pending_conf_change_;
  uint64_t next_append_sequence_ = 1;
  uint64_t next_read_ctx_id_ = 1;
  uint64_t next_snapshot_sequence_ = 1;
};

void RaftNode::Impl::InstallHandlers() {
  const std::shared_ptr<ActorEventSink> sink = event_sink;
  transport->SetRequestVoteHandler(
      [sink](uint32_t from_peer_id,
             const proto::RequestVoteRequest& request,
             RequestVoteResponseCallback done) {
        if (!sink->Post(RequestVoteRequestEvent{
                from_peer_id, request, done}) &&
            done != nullptr) {
          done(Status::Timeout(
                   "raft actor stopped before request delivery"),
               proto::RequestVoteResponse());
        }
      });
  transport->SetAppendEntriesHandler(
      [sink](uint32_t from_peer_id,
             const proto::AppendEntriesRequest& request,
             AppendEntriesResponseCallback done) {
        if (!sink->Post(AppendEntriesRequestEvent{
                from_peer_id, request, done}) &&
            done != nullptr) {
          done(Status::Timeout(
                   "raft actor stopped before request delivery"),
               proto::AppendEntriesResponse());
        }
      });
  transport->SetInstallSnapshotHandler(
      [sink](uint32_t from_peer_id,
             const proto::InstallSnapshotRequest& request,
             InstallSnapshotResponseCallback done) {
        if (!sink->Post(InstallSnapshotRequestEvent{
                from_peer_id, request, done}) &&
            done != nullptr) {
          done(Status::Timeout(
                   "raft actor stopped before request delivery"),
               proto::InstallSnapshotResponse());
        }
      });
}

void RaftNode::Impl::ClearHandlers() {
  transport->SetRequestVoteHandler(RequestVoteHandler());
  transport->SetAppendEntriesHandler(AppendEntriesHandler());
  transport->SetInstallSnapshotHandler(InstallSnapshotHandler());
}

std::chrono::milliseconds
RaftNode::Impl::NextElectionTimeout() {
  std::uniform_int_distribution<uint64_t> distribution(
      options.election_timeout_min_ms,
      options.election_timeout_max_ms);
  return std::chrono::milliseconds(distribution(election_random));
}

void RaftNode::Impl::ResetElectionDeadline(Clock::time_point now) {
  election_deadline = now + NextElectionTimeout();
}

void RaftNode::Impl::ResetHeartbeatDeadline(Clock::time_point now) {
  heartbeat_deadline =
      now + std::chrono::milliseconds(options.heartbeat_interval_ms);
}

bool RaftNode::Impl::ElectionDeadlineReached(
    Clock::time_point now) const {
  return now >= election_deadline;
}

bool RaftNode::Impl::HeartbeatDeadlineReached(
    Clock::time_point now) const {
  return now >= heartbeat_deadline;
}

bool RaftNode::Impl::HasRecentLeader(Clock::time_point now) const {
  return leader_id != 0 && now < election_deadline;
}

Status RaftNode::Impl::CandidateLogIsUpToDate(
    const proto::RequestVoteRequest& request,
    bool* up_to_date) const {
  if (up_to_date == nullptr) {
    return Status::InvalidArgument(
        "raft vote comparison output is null");
  }
  uint64_t local_last_term = 0;
  Status status = log->LastTerm(&local_last_term);
  if (!status.ok()) {
    return status;
  }
  *up_to_date = IsLogAtLeastAsUpToDate(
      request.last_log_term(), request.last_log_index(),
      local_last_term, log->LastIndex());
  return Status::OK();
}

Status RaftNode::Impl::PersistHardState(
    uint64_t term, std::optional<uint32_t> new_voted_for) {
  if (term < current_term) {
    return Status::InvalidArgument(
        "raft hard state term cannot move backwards");
  }

  // 磁盘提交成功后才发布新的任期和投票，失败时内存状态不变。
  Status status = log->SaveHardState(term, new_voted_for);
  if (!status.ok()) {
    return status;
  }
  current_term = term;
  voted_for = new_voted_for;
  PublishStatus();
  return Status::OK();
}

Status RaftNode::Impl::RecoverSnapshotState() {
  const std::string snapshot_root =
      JoinPath(log->directory(), kSnapshotDirectoryName);
  const std::string incoming_directory =
      JoinPath(snapshot_root, "incoming");
  std::error_code error;
  fs::create_directories(incoming_directory, error);
  if (error) {
    return Status::IOError(
        "create raft snapshot directory: " + error.message());
  }

  bool loaded_state_directory = false;
  const std::string install_marker_path =
      JoinPath(snapshot_root, kInstallMarkerName);
  if (fs::exists(install_marker_path, error)) {
    // 先重放安装事务，再允许 Actor 启动。
    if (error) {
      return Status::IOError(
          "inspect snapshot install marker: " + error.message());
    }
    std::string encoded;
    Status status = ReadFile(install_marker_path, &encoded);
    if (!status.ok()) {
      return status;
    }
    uint64_t snapshot_index = 0;
    uint64_t snapshot_term = 0;
    status = DecodeInstallMarker(
        encoded, &snapshot_index, &snapshot_term);
    if (!status.ok()) {
      return status;
    }
    const std::string snapshot_directory = JoinPath(
        snapshot_root,
        SnapshotDirectoryName(snapshot_index, snapshot_term));
    const std::string state_directory =
        JoinPath(snapshot_directory, kSnapshotStateName);
    if (!fs::is_directory(state_directory, error) || error) {
      return Status::Corruption(
          "snapshot install state directory is missing");
    }
    status = state_machine->LoadSnapshot(state_directory);
    if (!status.ok()) {
      return status;
    }
    status =
        log->ResetToSnapshot(snapshot_index, snapshot_term);
    if (!status.ok()) {
      return status;
    }
    commit_index = snapshot_index;
    last_applied = snapshot_index;
    PublishStatus();
    status = RemoveInstallMarker();
    if (!status.ok()) {
      return status;
    }
    loaded_state_directory = true;
  } else if (error) {
    return Status::IOError(
        "inspect snapshot install marker: " + error.message());
  }

  const RaftSnapshotMarker& marker = log->snapshot_marker();
  if (marker.last_included_index != 0) {
    // 本地持久化状态机从当前快照恢复，并保留可发送的 bundle。
    const std::string directory = JoinPath(
        snapshot_root,
        SnapshotDirectoryName(
            marker.last_included_index,
            marker.last_included_term));
    const std::string bundle_path =
        JoinPath(directory, kSnapshotBundleName);
    uint64_t bundle_size = 0;
    Status status =
        ValidateSnapshotBundle(bundle_path, &bundle_size);
    if (!status.ok()) {
      return status;
    }
    if (!loaded_state_directory) {
      status = state_machine->LoadSnapshot(
          JoinPath(directory, kSnapshotStateName));
      if (!status.ok()) {
        return status;
      }
    }
    RaftConfiguration snapshot_configuration;
    bool configuration_exists = false;
    status = ReadConfigurationFile(
        JoinPath(directory, kConfigurationFileName),
        &configuration_exists, &snapshot_configuration);
    if (!status.ok()) {
      return status;
    }
    if (!configuration_exists) {
      snapshot_configuration = bootstrap_configuration_;
      snapshot_configuration.index =
          marker.last_included_index;
    } else if (
        snapshot_configuration.index !=
        marker.last_included_index) {
      return Status::Corruption(
          "raft snapshot configuration index mismatch");
    }
    local_snapshot_ = LocalSnapshot{
        marker.last_included_index,
        marker.last_included_term,
        std::move(snapshot_configuration),
        directory,
        bundle_path,
        bundle_size};
  }

  for (const fs::directory_entry& entry :
       fs::directory_iterator(snapshot_root, error)) {
    if (error) {
      return Status::IOError(
          "scan raft snapshot directory: " + error.message());
    }
    const std::string name = entry.path().filename().string();
    if (name == "incoming" ||
        (local_snapshot_.has_value() &&
         entry.path() == local_snapshot_->directory)) {
      continue;
    }
    fs::remove_all(entry.path(), error);
    if (error) {
      return Status::IOError(
          "clean stale raft snapshot: " + error.message());
    }
  }
  fs::remove_all(incoming_directory, error);
  if (error) {
    return Status::IOError(
        "clean incoming raft snapshot: " + error.message());
  }
  fs::create_directories(incoming_directory, error);
  if (error) {
    return Status::IOError(
        "recreate incoming raft snapshot: " + error.message());
  }
  return Status::OK();
}

Status RaftNode::Impl::MaybeCreateSnapshot() {
  const uint64_t last_snapshot_index =
      log->snapshot_marker().last_included_index;
  if (last_applied <= last_snapshot_index ||
      last_applied - last_snapshot_index <
          options.snapshot_threshold_entries) {
    return Status::OK();
  }
  return CreateLocalSnapshot();
}

Status RaftNode::Impl::CreateLocalSnapshot() {
  const uint64_t snapshot_index = last_applied;
  uint64_t snapshot_term = 0;
  Status status = log->TermAt(snapshot_index, &snapshot_term);
  if (!status.ok()) {
    return status;
  }
  const std::string snapshot_root =
      JoinPath(log->directory(), kSnapshotDirectoryName);
  const std::string snapshot_name =
      SnapshotDirectoryName(snapshot_index, snapshot_term);
  const std::string final_directory =
      JoinPath(snapshot_root, snapshot_name);
  const std::string temporary_directory = JoinPath(
      snapshot_root,
      snapshot_name + ".tmp-" + std::to_string(::getpid()));
  const std::string state_directory =
      JoinPath(temporary_directory, kSnapshotStateName);
  const std::string bundle_path =
      JoinPath(temporary_directory, kSnapshotBundleName);
  RaftConfiguration snapshot_configuration;
  status = ComputeConfigurationAt(
      snapshot_index, &snapshot_configuration);
  if (!status.ok()) {
    return status;
  }

  std::error_code error;
  fs::remove_all(temporary_directory, error);
  error.clear();
  fs::create_directories(state_directory, error);
  if (error) {
    return Status::IOError(
        "create temporary raft snapshot: " + error.message());
  }
  const auto cleanup = [&]() {
    std::error_code ignored;
    fs::remove_all(temporary_directory, ignored);
  };

  // 先完成快照目录和 bundle，再推进 RaftLog 边界。
  status = state_machine->TakeSnapshot(
      snapshot_index, state_directory);
  if (!status.ok()) {
    cleanup();
    return status;
  }
  status = WriteConfigurationFile(
      JoinPath(temporary_directory, kConfigurationFileName),
      snapshot_configuration);
  if (!status.ok()) {
    cleanup();
    return status;
  }
  status = PackSnapshotDirectory(state_directory, bundle_path);
  if (!status.ok()) {
    cleanup();
    return status;
  }
  uint64_t bundle_size = 0;
  status = ValidateSnapshotBundle(bundle_path, &bundle_size);
  if (!status.ok()) {
    cleanup();
    return status;
  }

  fs::remove_all(final_directory, error);
  error.clear();
  fs::rename(temporary_directory, final_directory, error);
  if (error) {
    cleanup();
    return Status::IOError(
        "publish raft snapshot: " + error.message());
  }
  status = SyncDirectory(snapshot_root);
  if (!status.ok()) {
    return status;
  }

  // 快照已持久化，此时才能删除对应日志前缀。
  status =
      log->CompactPrefix(snapshot_index, snapshot_term);
  if (!status.ok()) {
    return status;
  }
  local_snapshot_ = LocalSnapshot{
      snapshot_index,
      snapshot_term,
      std::move(snapshot_configuration),
      final_directory,
      JoinPath(final_directory, kSnapshotBundleName),
      bundle_size};
  PublishStatus();

  for (const fs::directory_entry& entry :
       fs::directory_iterator(snapshot_root, error)) {
    if (error) {
      return Status::IOError(
          "scan old raft snapshots: " + error.message());
    }
    if (entry.path() == final_directory ||
        entry.path().filename() == "incoming") {
      continue;
    }
    fs::remove_all(entry.path(), error);
    if (error) {
      return Status::IOError(
          "remove old raft snapshot: " + error.message());
    }
  }
  return Status::OK();
}

Status RaftNode::Impl::PersistInstallMarker(
    uint64_t last_included_index, uint64_t last_included_term) {
  std::string encoded;
  Status status = EncodeInstallMarker(
      last_included_index, last_included_term, &encoded);
  if (!status.ok()) {
    return status;
  }
  const std::string snapshot_root =
      JoinPath(log->directory(), kSnapshotDirectoryName);
  const std::string target =
      JoinPath(snapshot_root, kInstallMarkerName);
  const std::string temporary = target + ".tmp";
  const int fd = ::open(
      temporary.c_str(),
      O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return MakeIoError("create snapshot install marker");
  }
  status = WriteAll(
      fd, encoded.data(), encoded.size(),
      "write snapshot install marker");
  if (status.ok() && ::fsync(fd) < 0) {
    status = MakeIoError("fsync snapshot install marker");
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close snapshot install marker");
  }
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), target.c_str()) < 0) {
    status = MakeIoError("rename snapshot install marker");
    ::unlink(temporary.c_str());
    return status;
  }
  return SyncDirectory(snapshot_root);
}

Status RaftNode::Impl::RemoveInstallMarker() {
  const std::string snapshot_root =
      JoinPath(log->directory(), kSnapshotDirectoryName);
  const std::string marker =
      JoinPath(snapshot_root, kInstallMarkerName);
  if (::unlink(marker.c_str()) < 0 && errno != ENOENT) {
    return MakeIoError("remove snapshot install marker");
  }
  return SyncDirectory(snapshot_root);
}

Status RaftNode::Impl::ComputeConfigurationAt(
    uint64_t target_index,
    RaftConfiguration* configuration) const {
  if (configuration == nullptr) {
    return Status::InvalidArgument(
        "raft configuration output is null");
  }

  RaftConfiguration computed = bootstrap_configuration_;
  uint64_t scan_index = 1;
  const uint64_t snapshot_index =
      log->snapshot_marker().last_included_index;
  if (local_snapshot_.has_value() &&
      local_snapshot_->last_included_index <= target_index) {
    computed = local_snapshot_->configuration;
    scan_index = computed.index + 1;
  } else if (snapshot_index != 0) {
    // F4.8 之前的快照没有配置 sidecar，此时启动参数仍是有效配置。
    computed.index = snapshot_index;
    scan_index = snapshot_index + 1;
  }

  if (target_index < computed.index ||
      target_index > log->LastIndex()) {
    return Status::InvalidArgument(
        "raft configuration target index is invalid");
  }
  for (uint64_t index = scan_index;
       index <= target_index; ++index) {
    proto::LogEntry entry;
    Status status = log->EntryAt(index, &entry);
    if (!status.ok()) {
      return status;
    }
    if (entry.entry_type() == proto::ENTRY_TYPE_CONF_CHANGE) {
      proto::ConfChange change;
      status = DecodeConfChangePayload(entry.payload(), &change);
      if (!status.ok()) {
        return status;
      }
      status = ApplyConfChange(change, index, &computed);
      if (!status.ok()) {
        return Status::Corruption(status.message());
      }
    }
    computed.index = index;
    if (index == std::numeric_limits<uint64_t>::max()) {
      break;
    }
  }
  *configuration = std::move(computed);
  return Status::OK();
}

Status RaftNode::Impl::RebuildCurrentConfiguration() {
  // 配置按日志顺序重建，未提交变更额外保留旧成员以接收其收官请求。
  RaftConfiguration rebuilt;
  Status status =
      ComputeConfigurationAt(log->LastIndex(), &rebuilt);
  if (!status.ok()) {
    return status;
  }

  std::optional<uint64_t> uncommitted_index;
  for (uint64_t index = commit_index + 1;
       index <= log->LastIndex(); ++index) {
    proto::LogEntry entry;
    status = log->EntryAt(index, &entry);
    if (!status.ok()) {
      return status;
    }
    if (entry.entry_type() != proto::ENTRY_TYPE_CONF_CHANGE) {
      if (index == std::numeric_limits<uint64_t>::max()) {
        break;
      }
      continue;
    }
    if (uncommitted_index.has_value()) {
      return Status::Corruption(
          "raft has multiple uncommitted configuration changes");
    }
    uncommitted_index = index;
    if (index == std::numeric_limits<uint64_t>::max()) {
      break;
    }
  }

  std::set<uint32_t> transitional;
  if (uncommitted_index.has_value()) {
    RaftConfiguration previous;
    status = ComputeConfigurationAt(
        *uncommitted_index - 1, &previous);
    if (!status.ok()) {
      return status;
    }
    transitional = std::move(previous.voters);
  }
  configuration_ = std::move(rebuilt);
  transitional_voters_ = std::move(transitional);
  uncommitted_conf_change_index_ = uncommitted_index;
  return Status::OK();
}

void RaftNode::Impl::MarkConfigurationCommitted() {
  if (!uncommitted_conf_change_index_.has_value() ||
      *uncommitted_conf_change_index_ > commit_index) {
    return;
  }
  transitional_voters_.clear();
  uncommitted_conf_change_index_.reset();
}

Status RaftNode::Impl::HasUncommittedConfChange(
    bool* pending) const {
  if (pending == nullptr) {
    return Status::InvalidArgument(
        "raft configuration pending output is null");
  }
  *pending = false;
  for (uint64_t index = commit_index + 1;
       index <= log->LastIndex(); ++index) {
    proto::LogEntry entry;
    Status status = log->EntryAt(index, &entry);
    if (!status.ok()) {
      return status;
    }
    if (entry.entry_type() == proto::ENTRY_TYPE_CONF_CHANGE) {
      *pending = true;
      return Status::OK();
    }
    if (index == std::numeric_limits<uint64_t>::max()) {
      break;
    }
  }
  return Status::OK();
}

Status RaftNode::Impl::StepDownAfterSelfRemoval() {
  // 移除自身的配置已提交，本节点立即停止 Leader 职责。
  const bool was_leader = role == Role::kLeader;
  role = Role::kFollower;
  leader_id = 0;
  replication_.clear();
  if (was_leader) {
    FailPendingProposals(
        Status::NotLeader("raft leader was removed"));
  }
  FailPendingReads(
      Status::NotLeader("raft leader was removed"));
  ResetElectionDeadline(Clock::now());
  PublishStatus();
  return Status::OK();
}

void RaftNode::Impl::ResetReplicationProgress() {
  replication_.clear();
  const uint64_t next_index = log->LastIndex() + 1;
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    replication_[peer_id].next_index = next_index;
  }
}

Status RaftNode::Impl::AppendNoop() {
  if (log->LastIndex() >=
      std::numeric_limits<uint64_t>::max() - 1) {
    return Status::Corruption("raft log index is exhausted");
  }
  proto::LogEntry entry;
  entry.set_term(current_term);
  entry.set_index(log->LastIndex() + 1);
  entry.set_entry_type(proto::ENTRY_TYPE_NO_OP);
  std::vector<proto::LogEntry> entries;
  entries.push_back(std::move(entry));
  Status status = log->Append(std::move(entries));
  if (!status.ok()) {
    return status;
  }
  PublishStatus();
  return Status::OK();
}

Status RaftNode::Impl::HandlePropose(const ProposeEvent& event) {
  if (role != Role::kLeader) {
    if (event.done != nullptr) {
      event.done(Status::NotLeader("raft node is not leader"));
    }
    return Status::OK();
  }
  if (log->LastIndex() >=
      std::numeric_limits<uint64_t>::max() - 1) {
    const Status status =
        Status::Corruption("raft log index is exhausted");
    if (event.done != nullptr) {
      event.done(status);
    }
    return status;
  }

  proto::LogEntry entry;
  entry.set_term(current_term);
  entry.set_index(log->LastIndex() + 1);
  entry.set_entry_type(proto::ENTRY_TYPE_NORMAL);
  entry.set_payload(event.command);
  const uint64_t index = entry.index();
  std::vector<proto::LogEntry> entries;
  entries.push_back(std::move(entry));
  Status status = log->Append(std::move(entries));
  if (!status.ok()) {
    if (event.done != nullptr) {
      event.done(status);
    }
    return status;
  }

  if (event.done != nullptr) {
    pending_proposals_[index] = event.done;
  }
  PublishStatus();
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    status = ReplicateToPeer(peer_id);
    if (!status.ok()) {
      return status;
    }
  }
  return AdvanceLeaderCommit();
}

void RaftNode::Impl::FailPendingProposals(const Status& status) {
  std::map<uint64_t, ProposeCallback> pending =
      std::move(pending_proposals_);
  pending_proposals_.clear();
  for (auto& entry : pending) {
    if (entry.second != nullptr) {
      entry.second(status);
    }
  }
}

void RaftNode::Impl::FailPendingConfChange(
    const Status& status) {
  if (!pending_conf_change_.has_value()) {
    return;
  }
  ConfChangeCallback done =
      std::move(pending_conf_change_->done);
  pending_conf_change_.reset();
  if (done != nullptr) {
    done(status);
  }
}

Status RaftNode::Impl::HandleProposeConfChange(
    const ProposeConfChangeEvent& event) {
  if (role != Role::kLeader) {
    if (event.done != nullptr) {
      event.done(Status::NotLeader("raft node is not leader"));
    }
    return Status::OK();
  }
  if (log->LastIndex() >=
      std::numeric_limits<uint64_t>::max() - 1) {
    const Status status =
        Status::Corruption("raft log index is exhausted");
    if (event.done != nullptr) {
      event.done(status);
    }
    return status;
  }

  Status status = ValidateConfChange(event.change);
  if (!status.ok()) {
    if (event.done != nullptr) {
      event.done(status);
    }
    return Status::OK();
  }
  RaftConfiguration updated = configuration_;
  const uint64_t index = log->LastIndex() + 1;
  status = ApplyConfChange(event.change, index, &updated);
  if (!status.ok()) {
    if (event.done != nullptr) {
      event.done(status);
    }
    return Status::OK();
  }

  bool has_uncommitted = false;
  status = HasUncommittedConfChange(&has_uncommitted);
  if (!status.ok()) {
    return status;
  }
  if (has_uncommitted || pending_conf_change_.has_value()) {
    if (event.done != nullptr) {
      event.done(Status::InvalidArgument(
          "raft already has an uncommitted configuration change"));
    }
    return Status::OK();
  }

  std::string payload;
  status = EncodeConfChangePayload(event.change, &payload);
  if (!status.ok()) {
    if (event.done != nullptr) {
      event.done(status);
    }
    return Status::OK();
  }

  proto::LogEntry entry;
  entry.set_term(current_term);
  entry.set_index(index);
  entry.set_entry_type(proto::ENTRY_TYPE_CONF_CHANGE);
  entry.set_payload(std::move(payload));
  std::vector<proto::LogEntry> entries;
  entries.push_back(std::move(entry));
  status = log->Append(std::move(entries));
  if (!status.ok()) {
    if (event.done != nullptr) {
      event.done(status);
    }
    return status;
  }

  // 配置日志落盘后立即切换到新成员集合。
  const std::set<uint32_t> previous_voters =
      configuration_.voters;
  configuration_ = std::move(updated);
  transitional_voters_ = previous_voters;
  uncommitted_conf_change_index_ = index;
  pending_conf_change_ =
      PendingConfChange{index, event.done};

  if (event.change.type() == proto::CONF_CHANGE_ADD_NODE) {
    PeerReplicationState& added =
        replication_[event.change.node_id()];
    added = PeerReplicationState();
    added.next_index = 1;
  } else {
    replication_.erase(event.change.node_id());
  }
  PublishStatus();
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    status = ReplicateToPeer(peer_id);
    if (!status.ok()) {
      return status;
    }
  }
  return AdvanceLeaderCommit();
}

Status RaftNode::Impl::HasCommittedEntryInCurrentTerm(
    bool* committed) const {
  if (committed == nullptr) {
    return Status::InvalidArgument(
        "raft read index state output is null");
  }
  uint64_t committed_term = 0;
  Status status = log->TermAt(commit_index, &committed_term);
  if (!status.ok()) {
    *committed = false;
    return status;
  }
  *committed = committed_term == current_term;
  return Status::OK();
}

Status RaftNode::Impl::HandleReadIndex(
    const ReadIndexEvent& event) {
  if (role != Role::kLeader) {
    if (event.done != nullptr) {
      event.done(Status::NotLeader("raft node is not leader"));
    }
    return Status::OK();
  }
  if (next_read_ctx_id_ == std::numeric_limits<uint64_t>::max()) {
    const Status status =
        Status::Corruption("raft read context is exhausted");
    if (event.done != nullptr) {
      event.done(status);
    }
    return status;
  }

  const uint64_t read_ctx_id = next_read_ctx_id_++;
  PendingRead pending;
  pending.read_id = event.read_id;
  pending.deadline =
      Clock::now() +
      std::chrono::milliseconds(options.read_index_timeout_ms);
  pending.acks.insert(options.node_id);
  pending.done = event.done;
  pending_reads_.emplace(read_ctx_id, std::move(pending));

  Status status = StartPendingReads();
  if (!status.ok()) {
    return status;
  }
  TryCompletePendingReads();
  return Status::OK();
}

Status RaftNode::Impl::StartPendingReads() {
  if (role != Role::kLeader) {
    return Status::OK();
  }
  bool committed = false;
  Status status = HasCommittedEntryInCurrentTerm(&committed);
  if (!status.ok() || !committed) {
    return status;
  }

  std::vector<std::pair<uint64_t, uint64_t>> contexts;
  for (auto& entry : pending_reads_) {
    PendingRead& pending = entry.second;
    if (pending.started) {
      continue;
    }
    pending.read_index = commit_index;
    pending.started = true;
    contexts.emplace_back(entry.first, pending.read_index);
  }
  for (const auto& context : contexts) {
    status =
        SendReadIndexHeartbeat(context.first, context.second);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status RaftNode::Impl::SendReadIndexHeartbeat(
    uint64_t read_ctx_id, uint64_t read_index) {
  if (role != Role::kLeader) {
    return Status::OK();
  }
  uint64_t read_term = 0;
  Status status = log->TermAt(read_index, &read_term);
  if (!status.ok()) {
    return status;
  }

  proto::AppendEntriesRequest request;
  request.set_term(current_term);
  request.set_leader_id(options.node_id);
  request.set_prev_log_index(read_index);
  request.set_prev_log_term(read_term);
  request.set_leader_commit(commit_index);
  request.set_read_ctx_id(read_ctx_id);

  const uint64_t prev_log_index = request.prev_log_index();
  const std::weak_ptr<ActorEventSink> weak_sink = event_sink;
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    transport->SendAppendEntries(
        peer_id, request,
        [weak_sink, peer_id, prev_log_index](
            Status response_status,
            proto::AppendEntriesResponse response) {
          if (const auto sink = weak_sink.lock()) {
            sink->Post(AppendEntriesResponseEvent{
                peer_id, 0, prev_log_index, 0,
                std::move(response_status), std::move(response)});
          }
        });
  }
  return Status::OK();
}

void RaftNode::Impl::HandleReadIndexResponse(
    uint32_t peer_id,
    const proto::AppendEntriesResponse& response) {
  if (role != Role::kLeader ||
      !IsConfiguredPeer(peer_id) ||
      response.term() != current_term ||
      !response.success() ||
      response.read_ctx_id() == 0) {
    return;
  }
  const auto pending = pending_reads_.find(response.read_ctx_id());
  if (pending == pending_reads_.end() ||
      !pending->second.started) {
    return;
  }
  pending->second.acks.insert(peer_id);
  TryCompletePendingReads();
}

void RaftNode::Impl::TryCompletePendingReads() {
  if (role != Role::kLeader) {
    return;
  }
  std::vector<ReadIndexCallback> completed;
  for (auto iterator = pending_reads_.begin();
       iterator != pending_reads_.end();) {
    PendingRead& pending = iterator->second;
    if (!pending.started ||
        last_applied < pending.read_index ||
        !HasMajority(pending.acks)) {
      ++iterator;
      continue;
    }
    completed.push_back(std::move(pending.done));
    iterator = pending_reads_.erase(iterator);
  }
  for (ReadIndexCallback& done : completed) {
    if (done != nullptr) {
      done(Status::OK());
    }
  }
}

void RaftNode::Impl::ExpirePendingReads(Clock::time_point now) {
  std::vector<ReadIndexCallback> expired;
  for (auto iterator = pending_reads_.begin();
       iterator != pending_reads_.end();) {
    if (now < iterator->second.deadline) {
      ++iterator;
      continue;
    }
    expired.push_back(std::move(iterator->second.done));
    iterator = pending_reads_.erase(iterator);
  }
  const Status status = Status::Timeout("raft read index timed out");
  for (ReadIndexCallback& done : expired) {
    if (done != nullptr) {
      done(status);
    }
  }
}

void RaftNode::Impl::FailPendingReads(const Status& status) {
  std::map<uint64_t, PendingRead> pending =
      std::move(pending_reads_);
  pending_reads_.clear();
  for (auto& entry : pending) {
    if (entry.second.done != nullptr) {
      entry.second.done(status);
    }
  }
}

Status RaftNode::Impl::ApplyCommitted() {
  if (last_applied > commit_index) {
    return Status::Corruption(
        "raft last applied exceeds commit index");
  }
  while (last_applied < commit_index) {
    const uint64_t index = last_applied + 1;
    proto::LogEntry entry;
    Status status = log->EntryAt(index, &entry);
    if (!status.ok()) {
      return status;
    }
    if (entry.entry_type() == proto::ENTRY_TYPE_NORMAL) {
      status = state_machine->Apply(index, entry.payload());
      if (!status.ok()) {
        return status;
      }
    } else if (
        entry.entry_type() == proto::ENTRY_TYPE_CONF_CHANGE) {
      proto::ConfChange change;
      status =
          DecodeConfChangePayload(entry.payload(), &change);
      if (!status.ok()) {
        return status;
      }
    }
    last_applied = index;
    status_last_applied.store(
        last_applied, std::memory_order_relaxed);
    TryCompletePendingReads();

    const auto pending = pending_proposals_.find(index);
    if (pending != pending_proposals_.end()) {
      ProposeCallback done = std::move(pending->second);
      pending_proposals_.erase(pending);
      if (done != nullptr) {
        done(Status::OK());
      }
    }
    if (entry.entry_type() == proto::ENTRY_TYPE_CONF_CHANGE &&
        pending_conf_change_.has_value() &&
        pending_conf_change_->index == index) {
      ConfChangeCallback done =
          std::move(pending_conf_change_->done);
      pending_conf_change_.reset();
      if (role == Role::kLeader && !IsSelfVoter()) {
        status = StepDownAfterSelfRemoval();
        if (!status.ok()) {
          return status;
        }
      }
      if (done != nullptr) {
        done(Status::OK());
      }
    }
    Status snapshot_status = MaybeCreateSnapshot();
    if (!snapshot_status.ok()) {
      return snapshot_status;
    }
  }
  MarkConfigurationCommitted();
  return Status::OK();
}

Status RaftNode::Impl::AdvanceCommitIndex(uint64_t candidate_index) {
  if (candidate_index <= commit_index) {
    return Status::OK();
  }
  if (candidate_index > log->LastIndex()) {
    return Status::InvalidArgument(
        "raft commit index exceeds last log index");
  }
  commit_index = candidate_index;
  status_commit.store(commit_index, std::memory_order_relaxed);
  Status status = ApplyCommitted();
  if (!status.ok()) {
    return status;
  }
  MarkConfigurationCommitted();
  return StartPendingReads();
}

Status RaftNode::Impl::AdvanceLeaderCommit() {
  if (role != Role::kLeader) {
    return Status::OK();
  }

  std::vector<uint64_t> matched_indices;
  matched_indices.reserve(configuration_.voters.size());
  if (IsSelfVoter()) {
    matched_indices.push_back(log->LastIndex());
  }
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    const auto replication = replication_.find(peer_id);
    matched_indices.push_back(
        replication == replication_.end()
            ? 0
            : replication->second.match_index);
  }
  std::sort(
      matched_indices.begin(), matched_indices.end(),
      std::greater<uint64_t>());
  const size_t majority = matched_indices.size() / 2 + 1;
  const uint64_t candidate_index = matched_indices[majority - 1];
  if (candidate_index <= commit_index) {
    return Status::OK();
  }

  uint64_t candidate_term = 0;
  Status status = log->TermAt(candidate_index, &candidate_term);
  if (!status.ok()) {
    return status;
  }
  if (candidate_term != current_term) {
    return Status::OK();
  }
  return AdvanceCommitIndex(candidate_index);
}

Status RaftNode::Impl::ValidateAppendEntries(
    const proto::AppendEntriesRequest& request) const {
  if (request.entries_size() == 0) {
    return Status::OK();
  }
  if (request.prev_log_index() ==
      std::numeric_limits<uint64_t>::max()) {
    return Status::InvalidArgument(
        "append entries prev index is exhausted");
  }

  uint64_t expected_index = request.prev_log_index() + 1;
  uint64_t previous_term = request.prev_log_term();
  for (int index = 0; index < request.entries_size(); ++index) {
    const proto::LogEntry& entry = request.entries(index);
    if (entry.index() != expected_index ||
        entry.term() == 0 ||
        entry.term() > request.term() ||
        entry.term() < previous_term) {
      return Status::InvalidArgument(
          "append entries payload is invalid");
    }
    if (entry.entry_type() ==
        proto::ENTRY_TYPE_CONF_CHANGE) {
      proto::ConfChange change;
      Status status =
          DecodeConfChangePayload(entry.payload(), &change);
      if (!status.ok()) {
        return status;
      }
    }
    previous_term = entry.term();
    if (expected_index ==
        std::numeric_limits<uint64_t>::max()) {
      if (index + 1 != request.entries_size()) {
        return Status::InvalidArgument(
            "append entries index is exhausted");
      }
    } else {
      ++expected_index;
    }
  }
  return Status::OK();
}

Status RaftNode::Impl::BuildConflictResponse(
    uint64_t prev_log_index,
    proto::AppendEntriesResponse* response) const {
  if (response == nullptr) {
    return Status::InvalidArgument(
        "append entries response is null");
  }
  const uint64_t last_index = log->LastIndex();
  if (prev_log_index > last_index) {
    response->set_conflict_term(0);
    response->set_conflict_index(last_index + 1);
    return Status::OK();
  }

  uint64_t conflict_term = 0;
  Status status = log->TermAt(prev_log_index, &conflict_term);
  if (!status.ok()) {
    if (status.code() != Code::kNotFound) {
      return status;
    }
    response->set_conflict_term(0);
    response->set_conflict_index(
        log->snapshot_marker().last_included_index);
    return Status::OK();
  }

  uint64_t conflict_index = prev_log_index;
  while (conflict_index > log->FirstIndex()) {
    uint64_t previous_term = 0;
    status = log->TermAt(conflict_index - 1, &previous_term);
    if (!status.ok() || previous_term != conflict_term) {
      break;
    }
    --conflict_index;
  }
  response->set_conflict_term(conflict_term);
  response->set_conflict_index(conflict_index);
  return Status::OK();
}

Status RaftNode::Impl::RepairFollowerLog(
    const proto::AppendEntriesRequest& request,
    uint64_t* matched_index) {
  if (matched_index == nullptr) {
    return Status::InvalidArgument(
        "append entries match output is null");
  }
  if (request.entries_size() == 0) {
    *matched_index = request.prev_log_index();
    return Status::OK();
  }

  const uint64_t last_index = log->LastIndex();
  size_t append_from = static_cast<size_t>(request.entries_size());
  for (int index = 0; index < request.entries_size(); ++index) {
    const proto::LogEntry& incoming = request.entries(index);
    if (incoming.index() > last_index) {
      if (incoming.index() != last_index + 1) {
        return Status::Corruption(
            "append entries left a log gap");
      }
      append_from = static_cast<size_t>(index);
      break;
    }

    proto::LogEntry local;
    Status status = log->EntryAt(incoming.index(), &local);
    if (!status.ok()) {
      return status;
    }
    if (local.term() == incoming.term()) {
      continue;
    }
    status = log->TruncateSuffix(incoming.index());
    if (!status.ok()) {
      return status;
    }
    append_from = static_cast<size_t>(index);
    break;
  }

  if (append_from <
      static_cast<size_t>(request.entries_size())) {
    std::vector<proto::LogEntry> suffix;
    suffix.reserve(
        static_cast<size_t>(request.entries_size()) - append_from);
    for (size_t index = append_from;
         index < static_cast<size_t>(request.entries_size());
         ++index) {
      suffix.push_back(request.entries(static_cast<int>(index)));
    }
    Status status = log->Append(std::move(suffix));
    if (!status.ok()) {
      return status;
    }
  }

  *matched_index =
      request.prev_log_index() +
      static_cast<uint64_t>(request.entries_size());
  if (log->LastIndex() < *matched_index) {
    return Status::Corruption(
        "append entries matched index mismatch");
  }
  PublishStatus();
  return Status::OK();
}

Status RaftNode::Impl::RetryPeerAfterConflict(
    uint32_t peer_id,
    uint64_t request_prev_log_index,
    const proto::AppendEntriesResponse& response) {
  const auto replication = replication_.find(peer_id);
  if (replication == replication_.end()) {
    return Status::OK();
  }

  uint64_t next_index = response.conflict_index();
  if (response.conflict_term() != 0) {
    uint64_t search_index =
        std::min(request_prev_log_index, log->LastIndex());
    while (search_index >= log->FirstIndex()) {
      uint64_t term = 0;
      Status status = log->TermAt(search_index, &term);
      if (!status.ok()) {
        if (status.code() == Code::kNotFound) {
          break;
        }
        return status;
      }
      if (term == response.conflict_term()) {
        next_index = search_index + 1;
        break;
      }
      if (search_index == 0 ||
          search_index == log->FirstIndex()) {
        break;
      }
      --search_index;
    }
  }

  next_index = std::max<uint64_t>(1, next_index);
  next_index = std::min(next_index, log->LastIndex() + 1);
  replication->second.next_index = next_index;
  return ReplicateToPeer(peer_id);
}

bool RaftNode::Impl::IsConfiguredPeer(uint32_t peer_id) const {
  return peer_id != options.node_id &&
         configuration_.voters.count(peer_id) != 0;
}

bool RaftNode::Impl::IsAcceptedLeader(uint32_t peer_id) const {
  return peer_id != options.node_id &&
         (configuration_.voters.count(peer_id) != 0 ||
          transitional_voters_.count(peer_id) != 0);
}

bool RaftNode::Impl::IsSelfVoter() const {
  return configuration_.voters.count(options.node_id) != 0;
}

bool RaftNode::Impl::HasMajority(
    const std::unordered_set<uint32_t>& votes) const {
  size_t voter_acks = 0;
  for (uint32_t voter : configuration_.voters) {
    if (votes.count(voter) != 0) {
      ++voter_acks;
    }
  }
  return voter_acks >= configuration_.voters.size() / 2 + 1;
}

void RaftNode::Impl::ClearElectionVotes() {
  pre_votes_received.clear();
  votes_received.clear();
}

Status RaftNode::Impl::OnTick(Clock::time_point now) {
  ExpirePendingReads(now);
  if (role == Role::kLeader) {
    if (!HeartbeatDeadlineReached(now)) {
      return Status::OK();
    }
    ResetHeartbeatDeadline(now);
    return BroadcastHeartbeat();
  }
  if (!IsSelfVoter()) {
    return Status::OK();
  }
  if (!ElectionDeadlineReached(now)) {
    return Status::OK();
  }
  return StartPreVote(now);
}

Status RaftNode::Impl::StartPreVote(Clock::time_point now) {
  if (!IsSelfVoter()) {
    return Status::OK();
  }
  if (current_term == std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("raft term is exhausted");
  }
  FailPendingProposals(
      Status::NotLeader("raft leader stepped down"));
  FailPendingConfChange(
      Status::NotLeader("raft leader stepped down"));
  FailPendingReads(
      Status::NotLeader("raft leader stepped down"));
  role = Role::kPreCandidate;
  leader_id = 0;
  ClearElectionVotes();
  pre_votes_received.insert(options.node_id);
  ResetElectionDeadline(now);
  PublishStatus();
  if (HasMajority(pre_votes_received)) {
    return BecomeCandidate(now);
  }
  return BroadcastVoteRequests(true);
}

Status RaftNode::Impl::BecomeCandidate(Clock::time_point now) {
  if (!IsSelfVoter()) {
    return Status::OK();
  }
  if (current_term == std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("raft term is exhausted");
  }
  FailPendingProposals(
      Status::NotLeader("raft leader stepped down"));
  FailPendingConfChange(
      Status::NotLeader("raft leader stepped down"));
  FailPendingReads(
      Status::NotLeader("raft leader stepped down"));
  Status status =
      PersistHardState(current_term + 1, options.node_id);
  if (!status.ok()) {
    return status;
  }
  role = Role::kCandidate;
  leader_id = 0;
  ClearElectionVotes();
  votes_received.insert(options.node_id);
  ResetElectionDeadline(now);
  PublishStatus();
  if (HasMajority(votes_received)) {
    return BecomeLeader(now);
  }
  return BroadcastVoteRequests(false);
}

Status RaftNode::Impl::BecomeLeader(Clock::time_point now) {
  Status status = AppendNoop();
  if (!status.ok()) {
    return status;
  }
  role = Role::kLeader;
  leader_id = options.node_id;
  ClearElectionVotes();
  ResetReplicationProgress();
  ResetElectionDeadline(now);
  ResetHeartbeatDeadline(now);
  PublishStatus();
  status = AdvanceLeaderCommit();
  if (!status.ok()) {
    return status;
  }
  return BroadcastHeartbeat();
}

Status RaftNode::Impl::ObserveHigherTerm(
    uint64_t term, Clock::time_point now) {
  if (term <= current_term) {
    return Status::OK();
  }
  const bool was_leader = role == Role::kLeader;
  Status status = PersistHardState(term, std::nullopt);
  if (!status.ok()) {
    return status;
  }
  role = Role::kFollower;
  leader_id = 0;
  ClearElectionVotes();
  replication_.clear();
  if (was_leader) {
    FailPendingProposals(
        Status::NotLeader("raft leader stepped down"));
    FailPendingConfChange(
        Status::NotLeader("raft leader stepped down"));
  }
  FailPendingReads(
      Status::NotLeader("raft leader stepped down"));
  ResetElectionDeadline(now);
  PublishStatus();
  return Status::OK();
}

Status RaftNode::Impl::BroadcastVoteRequests(bool is_pre_vote) {
  uint64_t last_log_term = 0;
  Status status = log->LastTerm(&last_log_term);
  if (!status.ok()) {
    return status;
  }

  proto::RequestVoteRequest request;
  request.set_term(
      is_pre_vote ? current_term + 1 : current_term);
  request.set_candidate_id(options.node_id);
  request.set_last_log_index(log->LastIndex());
  request.set_last_log_term(last_log_term);
  request.set_is_pre_vote(is_pre_vote);

  const std::weak_ptr<ActorEventSink> weak_sink = event_sink;
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    transport->SendRequestVote(
        peer_id, request,
        [weak_sink, peer_id, is_pre_vote](
            Status response_status,
            proto::RequestVoteResponse response) {
          if (const auto sink = weak_sink.lock()) {
            sink->Post(RequestVoteResponseEvent{
                peer_id, is_pre_vote, std::move(response_status),
                std::move(response)});
          }
        });
  }
  return Status::OK();
}

Status RaftNode::Impl::ReplicateToPeer(uint32_t peer_id) {
  if (role != Role::kLeader || !IsConfiguredPeer(peer_id)) {
    return Status::OK();
  }
  const auto replication = replication_.find(peer_id);
  if (replication == replication_.end()) {
    return Status::Corruption("raft peer replication state is missing");
  }
  PeerReplicationState& progress = replication->second;
  if (progress.inflight_kind !=
      PeerReplicationState::InflightKind::kNone) {
    return Status::OK();
  }
  if (progress.next_index <=
      log->snapshot_marker().last_included_index) {
    // 所需日志已被压缩时，必须先安装快照。
    if (progress.snapshot_transfer_index !=
        log->snapshot_marker().last_included_index) {
      progress.snapshot_transfer_index =
          log->snapshot_marker().last_included_index;
      progress.snapshot_offset = 0;
    }
    return SendInstallSnapshotChunk(peer_id);
  }
  if (next_append_sequence_ ==
      std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("raft append sequence is exhausted");
  }

  progress.next_index = std::max<uint64_t>(1, progress.next_index);
  progress.next_index =
      std::min(progress.next_index, log->LastIndex() + 1);
  const uint64_t prev_log_index = progress.next_index - 1;
  uint64_t prev_log_term = 0;
  Status status = log->TermAt(prev_log_index, &prev_log_term);
  if (!status.ok()) {
    return status;
  }

  proto::AppendEntriesRequest request;
  request.set_term(current_term);
  request.set_leader_id(options.node_id);
  request.set_prev_log_index(prev_log_index);
  request.set_prev_log_term(prev_log_term);
  request.set_leader_commit(commit_index);

  const uint64_t last_index = log->LastIndex();
  uint64_t next_index = progress.next_index;
  uint64_t entry_count = 0;
  while (next_index <= last_index &&
         entry_count < kMaxEntriesPerAppend) {
    proto::LogEntry entry;
    status = log->EntryAt(next_index, &entry);
    if (!status.ok()) {
      return status;
    }
    *request.add_entries() = std::move(entry);
    if (next_index == std::numeric_limits<uint64_t>::max()) {
      break;
    }
    ++next_index;
    ++entry_count;
  }

  const uint64_t sequence = next_append_sequence_++;
  progress.inflight_kind =
      PeerReplicationState::InflightKind::kAppendEntries;
  progress.inflight_sequence = sequence;
  const std::weak_ptr<ActorEventSink> weak_sink = event_sink;
  transport->SendAppendEntries(
      peer_id, request,
      [weak_sink, peer_id, sequence, prev_log_index, entry_count](
          Status response_status,
          proto::AppendEntriesResponse response) {
        if (const auto sink = weak_sink.lock()) {
          sink->Post(AppendEntriesResponseEvent{
              peer_id, sequence, prev_log_index, entry_count,
              std::move(response_status), std::move(response)});
        }
      });
  return Status::OK();
}

Status RaftNode::Impl::SendInstallSnapshotChunk(
    uint32_t peer_id) {
  if (role != Role::kLeader || !IsConfiguredPeer(peer_id)) {
    return Status::OK();
  }
  const auto replication = replication_.find(peer_id);
  if (replication == replication_.end()) {
    return Status::Corruption(
        "raft peer snapshot state is missing");
  }
  PeerReplicationState& progress = replication->second;
  if (progress.inflight_kind !=
      PeerReplicationState::InflightKind::kNone) {
    return Status::OK();
  }
  if (!local_snapshot_.has_value() ||
      local_snapshot_->last_included_index !=
          log->snapshot_marker().last_included_index ||
      local_snapshot_->last_included_term !=
          log->snapshot_marker().last_included_term) {
    return Status::Corruption(
        "raft snapshot source is unavailable");
  }
  if (next_snapshot_sequence_ ==
      std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption(
        "raft snapshot sequence is exhausted");
  }

  const int fd = ::open(
      local_snapshot_->bundle_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open raft snapshot bundle");
  }
  const uint64_t offset = progress.snapshot_offset;
  if (offset > local_snapshot_->bundle_size) {
    ::close(fd);
    return Status::Corruption(
        "raft snapshot offset exceeds bundle size");
  }
  const uint64_t remaining =
      local_snapshot_->bundle_size - offset;
  const uint64_t data_size = std::min<uint64_t>(
      remaining, options.snapshot_chunk_size_bytes);
  std::string data(static_cast<size_t>(data_size), '\0');
  Status status = data.empty()
                      ? Status::OK()
                      : ReadAtExact(
                            fd, data.data(), data.size(), offset,
                            "read raft snapshot chunk");
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close raft snapshot bundle");
  }
  if (!status.ok()) {
    return status;
  }

  proto::InstallSnapshotRequest request;
  request.set_term(current_term);
  request.set_leader_id(options.node_id);
  request.set_last_included_index(
      local_snapshot_->last_included_index);
  request.set_last_included_term(
      local_snapshot_->last_included_term);
  request.set_offset(offset);
  request.set_data(std::move(data));
  for (uint32_t voter : local_snapshot_->configuration.voters) {
    request.add_voters(voter);
  }
  const bool done = offset + data_size == local_snapshot_->bundle_size;
  request.set_done(done);

  const uint64_t sequence = next_snapshot_sequence_++;
  progress.inflight_kind =
      PeerReplicationState::InflightKind::kInstallSnapshot;
  progress.inflight_sequence = sequence;
  progress.snapshot_offset = offset;
  const uint64_t data_size_copy = data_size;
  const uint64_t snapshot_index =
      local_snapshot_->last_included_index;
  const uint64_t snapshot_term =
      local_snapshot_->last_included_term;
  const std::weak_ptr<ActorEventSink> weak_sink = event_sink;
  transport->SendInstallSnapshot(
      peer_id, request,
      [weak_sink, peer_id, sequence, snapshot_index, snapshot_term,
       offset, data_size_copy, done](
          Status response_status,
          proto::InstallSnapshotResponse response) {
        if (const auto sink = weak_sink.lock()) {
          sink->Post(InstallSnapshotResponseEvent{
              peer_id, sequence, snapshot_index, snapshot_term,
              offset, data_size_copy, done,
              std::move(response_status), std::move(response)});
        }
      });
  return Status::OK();
}

Status RaftNode::Impl::InstallIncomingSnapshot() {
  if (!incoming_snapshot_.has_value()) {
    return Status::Corruption(
        "incoming raft snapshot state is missing");
  }
  const uint64_t snapshot_index =
      incoming_snapshot_->last_included_index;
  const uint64_t snapshot_term =
      incoming_snapshot_->last_included_term;
  const std::string incoming_bundle =
      incoming_snapshot_->bundle_path;
  uint64_t bundle_size = 0;
  Status status =
      ValidateSnapshotBundle(incoming_bundle, &bundle_size);
  if (!status.ok()) {
    return status;
  }

  const std::string snapshot_root =
      JoinPath(log->directory(), kSnapshotDirectoryName);
  const std::string snapshot_name =
      SnapshotDirectoryName(snapshot_index, snapshot_term);
  const std::string final_directory =
      JoinPath(snapshot_root, snapshot_name);
  const std::string temporary_directory = JoinPath(
      snapshot_root,
      snapshot_name + ".tmp-install-" + std::to_string(::getpid()));
  std::error_code error;
  fs::remove_all(temporary_directory, error);
  error.clear();
  const std::string temporary_state =
      JoinPath(temporary_directory, kSnapshotStateName);
  status =
      UnpackSnapshotDirectory(incoming_bundle, temporary_state);
  if (!status.ok()) {
    fs::remove_all(temporary_directory, error);
    return status;
  }
  status = WriteConfigurationFile(
      JoinPath(temporary_directory, kConfigurationFileName),
      incoming_snapshot_->configuration);
  if (!status.ok()) {
    fs::remove_all(temporary_directory, error);
    return status;
  }
  const std::string temporary_bundle =
      JoinPath(temporary_directory, kSnapshotBundleName);
  fs::rename(incoming_bundle, temporary_bundle, error);
  if (error) {
    fs::remove_all(temporary_directory, error);
    return Status::IOError(
        "move incoming raft snapshot: " + error.message());
  }
  fs::remove_all(final_directory, error);
  error.clear();
  fs::rename(temporary_directory, final_directory, error);
  if (error) {
    fs::remove_all(temporary_directory, error);
    return Status::IOError(
        "publish incoming raft snapshot: " + error.message());
  }
  status = SyncDirectory(snapshot_root);
  if (!status.ok()) {
    return status;
  }
  // 安装事务覆盖“加载状态机”和“重置日志”之间的崩溃窗口。
  status =
      PersistInstallMarker(snapshot_index, snapshot_term);
  if (!status.ok()) {
    return status;
  }

  const std::string final_state =
      JoinPath(final_directory, kSnapshotStateName);
  status = state_machine->LoadSnapshot(final_state);
  if (!status.ok()) {
    return status;
  }
  status = log->ResetToSnapshot(snapshot_index, snapshot_term);
  if (!status.ok()) {
    return status;
  }
  commit_index = snapshot_index;
  last_applied = snapshot_index;
  configuration_ = incoming_snapshot_->configuration;
  transitional_voters_.clear();
  uncommitted_conf_change_index_.reset();
  local_snapshot_ = LocalSnapshot{
      snapshot_index,
      snapshot_term,
      incoming_snapshot_->configuration,
      final_directory,
      JoinPath(final_directory, kSnapshotBundleName),
      bundle_size};
  incoming_snapshot_.reset();
  PublishStatus();

  status = RemoveInstallMarker();
  if (!status.ok()) {
    return status;
  }
  for (const fs::directory_entry& entry :
       fs::directory_iterator(snapshot_root, error)) {
    if (error) {
      return Status::IOError(
          "scan installed raft snapshots: " + error.message());
    }
    if (entry.path() == final_directory ||
        entry.path().filename() == "incoming") {
      continue;
    }
    fs::remove_all(entry.path(), error);
    if (error) {
      return Status::IOError(
          "remove replaced raft snapshot: " + error.message());
    }
  }
  const std::string incoming_directory =
      JoinPath(snapshot_root, "incoming");
  fs::remove_all(incoming_directory, error);
  error.clear();
  fs::create_directories(incoming_directory, error);
  if (error) {
    return Status::IOError(
        "reset incoming raft snapshot directory: " + error.message());
  }
  return Status::OK();
}

Status RaftNode::Impl::BroadcastHeartbeat() {
  if (role != Role::kLeader) {
    return Status::OK();
  }
  uint64_t last_log_term = 0;
  Status status = log->LastTerm(&last_log_term);
  if (!status.ok()) {
    return status;
  }

  proto::AppendEntriesRequest request;
  request.set_term(current_term);
  request.set_leader_id(options.node_id);
  request.set_prev_log_index(log->LastIndex());
  request.set_prev_log_term(last_log_term);
  request.set_leader_commit(commit_index);
  const uint64_t prev_log_index = request.prev_log_index();
  const std::weak_ptr<ActorEventSink> weak_sink = event_sink;
  for (uint32_t peer_id : configuration_.voters) {
    if (peer_id == options.node_id) {
      continue;
    }
    const auto replication = replication_.find(peer_id);
    if (replication == replication_.end() ||
        replication->second.inflight_kind ==
            PeerReplicationState::InflightKind::kNone) {
      Status replicate_status = ReplicateToPeer(peer_id);
      if (!replicate_status.ok()) {
        return replicate_status;
      }
      continue;
    }
    transport->SendAppendEntries(
        peer_id, request,
        [weak_sink, peer_id, prev_log_index](
            Status response_status,
            proto::AppendEntriesResponse response) {
          if (const auto sink = weak_sink.lock()) {
            sink->Post(AppendEntriesResponseEvent{
                peer_id, 0, prev_log_index, 0,
                std::move(response_status), std::move(response)});
          }
        });
  }
  return Status::OK();
}

Status RaftNode::Impl::OnRequestVoteRequest(
    const RequestVoteRequestEvent& event) {
  const auto now = Clock::now();
  proto::RequestVoteResponse response;
  response.set_term(current_term);
  response.set_vote_granted(false);
  const auto complete = [&](Status status, bool granted) {
    response.set_vote_granted(granted);
    if (event.done != nullptr) {
      event.done(std::move(status), response);
    }
  };

  if (event.request.term() == 0 ||
      event.request.candidate_id() == 0 ||
      event.request.candidate_id() != event.from_peer_id ||
      !IsSelfVoter() ||
      !IsConfiguredPeer(event.request.candidate_id()) ||
      event.request.last_log_term() > event.request.term() ||
      (event.request.last_log_term() == 0 &&
       event.request.last_log_index() != 0) ||
      (event.request.last_log_index() == 0 &&
       event.request.last_log_term() != 0)) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }

  if (event.request.is_pre_vote()) {
    if (event.request.term() <= current_term) {
      complete(Status::OK(), false);
      PublishStatus();
      return Status::OK();
    }
    bool up_to_date = false;
    Status status =
        CandidateLogIsUpToDate(event.request, &up_to_date);
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    const bool can_vote =
        role != Role::kLeader && !HasRecentLeader(now);
    complete(Status::OK(), up_to_date && can_vote);
    PublishStatus();
    return Status::OK();
  }

  if (event.request.term() < current_term) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() > current_term) {
    Status status = ObserveHigherTerm(event.request.term(), now);
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    response.set_term(current_term);
  }

  bool up_to_date = false;
  Status status =
      CandidateLogIsUpToDate(event.request, &up_to_date);
  if (!status.ok()) {
    complete(status, false);
    return status;
  }
  const bool already_voted_for_candidate =
      !voted_for.has_value() ||
      voted_for.value() == event.request.candidate_id();
  if (!up_to_date || HasRecentLeader(now) ||
      !already_voted_for_candidate) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }

  status = PersistHardState(
      current_term, event.request.candidate_id());
  response.set_term(current_term);
  if (!status.ok()) {
    complete(status, false);
    return status;
  }
  ResetElectionDeadline(now);
  complete(Status::OK(), true);
  return Status::OK();
}

Status RaftNode::Impl::OnAppendEntriesRequest(
    const AppendEntriesRequestEvent& event) {
  const auto now = Clock::now();
  proto::AppendEntriesResponse response;
  response.set_term(current_term);
  response.set_success(false);
  const auto complete = [&](Status status, bool success) {
    response.set_success(success);
    if (success) {
      response.set_read_ctx_id(event.request.read_ctx_id());
    }
    if (event.done != nullptr) {
      event.done(std::move(status), response);
    }
  };

  if (event.request.term() == 0 ||
      event.request.leader_id() == 0 ||
      event.request.leader_id() != event.from_peer_id ||
      !IsAcceptedLeader(event.request.leader_id())) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() < current_term) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() > current_term) {
    Status status = ObserveHigherTerm(event.request.term(), now);
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    response.set_term(current_term);
  }
  if (role == Role::kLeader) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }

  role = Role::kFollower;
  leader_id = event.request.leader_id();
  ClearElectionVotes();
  ResetElectionDeadline(now);
  PublishStatus();

  if (!ValidateAppendEntries(event.request).ok()) {
    complete(Status::OK(), false);
    return Status::OK();
  }
  if (event.request.prev_log_index() > log->LastIndex()) {
    Status status = BuildConflictResponse(
        event.request.prev_log_index(), &response);
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    complete(Status::OK(), false);
    return Status::OK();
  }

  uint64_t previous_term = 0;
  Status status = log->TermAt(
      event.request.prev_log_index(), &previous_term);
  if (!status.ok() ||
      previous_term != event.request.prev_log_term()) {
    status = BuildConflictResponse(
        event.request.prev_log_index(), &response);
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    complete(Status::OK(), false);
    return Status::OK();
  }

  uint64_t matched_index = 0;
  status = RepairFollowerLog(event.request, &matched_index);
  if (!status.ok()) {
    complete(status, false);
    return status;
  }
  if (event.request.entries_size() > 0) {
    status = RebuildCurrentConfiguration();
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    PublishStatus();
  }

  const uint64_t follower_commit =
      std::min(event.request.leader_commit(), matched_index);
  status = AdvanceCommitIndex(follower_commit);
  if (!status.ok()) {
    complete(status, false);
    return status;
  }
  complete(Status::OK(), true);
  return Status::OK();
}

Status RaftNode::Impl::OnInstallSnapshotRequest(
    const InstallSnapshotRequestEvent& event) {
  const auto now = Clock::now();
  proto::InstallSnapshotResponse response;
  response.set_term(current_term);
  response.set_success(false);
  const auto complete = [&](Status status, bool success) {
    response.set_success(success);
    if (event.done != nullptr) {
      event.done(std::move(status), response);
    }
  };

  if (event.request.term() == 0 ||
      event.request.leader_id() == 0 ||
      event.request.leader_id() != event.from_peer_id ||
      !IsAcceptedLeader(event.request.leader_id()) ||
      event.request.last_included_index() == 0 ||
      event.request.last_included_index() ==
          std::numeric_limits<uint64_t>::max() ||
      event.request.last_included_term() == 0 ||
      event.request.last_included_term() > event.request.term()) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() < current_term) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() > current_term) {
    Status status = ObserveHigherTerm(event.request.term(), now);
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
    response.set_term(current_term);
  }
  if (role == Role::kLeader) {
    complete(Status::OK(), false);
    PublishStatus();
    return Status::OK();
  }

  role = Role::kFollower;
  leader_id = event.request.leader_id();
  ClearElectionVotes();
  ResetElectionDeadline(now);
  PublishStatus();

  const uint64_t snapshot_index =
      event.request.last_included_index();
  const uint64_t snapshot_term =
      event.request.last_included_term();
  RaftConfiguration request_configuration;
  request_configuration.index = snapshot_index;
  for (int index = 0;
       index < event.request.voters_size(); ++index) {
    const uint32_t voter = event.request.voters(index);
    if (voter == 0 ||
        (!request_configuration.voters.empty() &&
         voter <= *request_configuration.voters.rbegin())) {
      complete(Status::OK(), false);
      return Status::OK();
    }
    request_configuration.voters.insert(voter);
  }
  if (request_configuration.voters.empty() ||
      request_configuration.voters.count(
          event.request.leader_id()) == 0) {
    complete(Status::OK(), false);
    return Status::OK();
  }
  if (snapshot_index == log->snapshot_marker().last_included_index &&
      snapshot_term == log->snapshot_marker().last_included_term) {
    if (local_snapshot_.has_value() &&
        local_snapshot_->configuration.voters !=
            request_configuration.voters) {
      complete(Status::OK(), false);
      return Status::OK();
    }
    if (event.request.offset() >
        std::numeric_limits<uint64_t>::max() -
            event.request.data().size()) {
      complete(Status::OK(), false);
      return Status::OK();
    }
    response.set_received_offset(
        event.request.offset() + event.request.data().size());
    complete(Status::OK(), true);
    return Status::OK();
  }
  if (snapshot_index < commit_index ||
      snapshot_index <=
          log->snapshot_marker().last_included_index) {
    complete(Status::OK(), false);
    return Status::OK();
  }
  if (event.request.data().size() > kMaxSnapshotChunkSize ||
      (event.request.data().empty() && !event.request.done())) {
    complete(Status::OK(), false);
    return Status::OK();
  }

  const std::string snapshot_root =
      JoinPath(log->directory(), kSnapshotDirectoryName);
  const std::string incoming_directory =
      JoinPath(snapshot_root, "incoming");
  const std::string bundle_path =
      JoinPath(incoming_directory, kIncomingSnapshotName);
  std::error_code error;
  if (event.request.offset() == 0) {
    // 新快照从 offset 0 开始，旧半成品直接丢弃。
    fs::remove_all(incoming_directory, error);
    error.clear();
    fs::create_directories(incoming_directory, error);
    if (error) {
      const Status status = Status::IOError(
          "reset incoming raft snapshot: " + error.message());
      complete(status, false);
      return status;
    }
    incoming_snapshot_ = IncomingSnapshot{
        event.request.leader_id(), snapshot_index, snapshot_term, 0,
        request_configuration, bundle_path};
  }
  if (!incoming_snapshot_.has_value() ||
      incoming_snapshot_->leader_id != event.request.leader_id() ||
      incoming_snapshot_->last_included_index != snapshot_index ||
      incoming_snapshot_->last_included_term != snapshot_term ||
      incoming_snapshot_->configuration.voters !=
          request_configuration.voters) {
    complete(Status::OK(), false);
    return Status::OK();
  }
  if (event.request.offset() != incoming_snapshot_->next_offset) {
    // 只接受连续块，并告诉 Leader 当前期望位置。
    response.set_received_offset(incoming_snapshot_->next_offset);
    complete(Status::OK(), false);
    return Status::OK();
  }
  if (event.request.offset() >
      std::numeric_limits<uint64_t>::max() -
          event.request.data().size()) {
    complete(Status::OK(), false);
    return Status::OK();
  }

  const int flags =
      event.request.offset() == 0
          ? O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC
          : O_WRONLY | O_CREAT | O_CLOEXEC;
  const int fd = ::open(bundle_path.c_str(), flags, 0644);
  if (fd < 0) {
    const Status status = MakeIoError("open incoming raft snapshot");
    complete(status, false);
    return status;
  }
  Status status = WriteAtAll(
      fd, event.request.data().data(), event.request.data().size(),
      event.request.offset(), "write incoming raft snapshot");
  if (status.ok() && event.request.done() && ::fsync(fd) < 0) {
    status = MakeIoError("fsync incoming raft snapshot");
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close incoming raft snapshot");
  }
  if (!status.ok()) {
    complete(status, false);
    return status;
  }

  const uint64_t received_offset =
      event.request.offset() + event.request.data().size();
  incoming_snapshot_->next_offset = received_offset;
  response.set_received_offset(received_offset);
  if (event.request.done()) {
    status = InstallIncomingSnapshot();
    if (!status.ok()) {
      complete(status, false);
      return status;
    }
  }
  PublishStatus();
  complete(Status::OK(), true);
  return Status::OK();
}

Status RaftNode::Impl::OnRequestVoteResponse(
    const RequestVoteResponseEvent& event) {
  if (!event.status.ok() || !IsConfiguredPeer(event.peer_id)) {
    return Status::OK();
  }
  const auto now = Clock::now();
  if (event.response.term() > current_term) {
    Status status = ObserveHigherTerm(event.response.term(), now);
    if (!status.ok()) {
      return status;
    }
  }
  if (event.response.term() < current_term ||
      !event.response.vote_granted()) {
    return Status::OK();
  }

  if (event.is_pre_vote && role == Role::kPreCandidate) {
    pre_votes_received.insert(event.peer_id);
    if (HasMajority(pre_votes_received)) {
      return BecomeCandidate(now);
    }
  } else if (!event.is_pre_vote && role == Role::kCandidate) {
    votes_received.insert(event.peer_id);
    if (HasMajority(votes_received)) {
      return BecomeLeader(now);
    }
  }
  return Status::OK();
}

Status RaftNode::Impl::OnAppendEntriesResponse(
    const AppendEntriesResponseEvent& event) {
  if (!IsConfiguredPeer(event.peer_id)) {
    return Status::OK();
  }
  if (event.status.ok() &&
      event.response.term() > current_term) {
    return ObserveHigherTerm(
        event.response.term(), Clock::now());
  }
  if (event.status.ok()) {
    HandleReadIndexResponse(event.peer_id, event.response);
  }
  const auto replication = replication_.find(event.peer_id);
  if (replication == replication_.end() ||
      replication->second.inflight_kind !=
          PeerReplicationState::InflightKind::kAppendEntries ||
      replication->second.inflight_sequence !=
          event.request_sequence) {
    return Status::OK();
  }
  replication->second.inflight_kind =
      PeerReplicationState::InflightKind::kNone;
  replication->second.inflight_sequence = 0;

  if (!event.status.ok()) {
    return Status::OK();
  }
  if (role != Role::kLeader ||
      event.response.term() < current_term) {
    return Status::OK();
  }

  if (event.response.success()) {
    if (event.request_entry_count >
        std::numeric_limits<uint64_t>::max() -
            event.request_prev_log_index) {
      return Status::Corruption("raft matched index overflow");
    }
    const uint64_t matched =
        event.request_prev_log_index + event.request_entry_count;
    if (matched > log->LastIndex()) {
      return Status::Corruption(
          "raft follower matched beyond leader log");
    }
    if (matched > replication->second.match_index) {
      replication->second.match_index = matched;
      replication->second.next_index =
          matched == std::numeric_limits<uint64_t>::max()
              ? matched
              : matched + 1;
    }
    Status status = AdvanceLeaderCommit();
    if (!status.ok()) {
      return status;
    }
    // 配置提交可能移除本 Leader，不能继续使用旧复制迭代器。
    if (role != Role::kLeader || !IsConfiguredPeer(event.peer_id)) {
      return Status::OK();
    }
    const auto current_replication =
        replication_.find(event.peer_id);
    if (current_replication == replication_.end()) {
      return Status::OK();
    }
    if (current_replication->second.next_index <=
        log->LastIndex()) {
      return ReplicateToPeer(event.peer_id);
    }
    return Status::OK();
  }

  return RetryPeerAfterConflict(
      event.peer_id, event.request_prev_log_index,
      event.response);
}

Status RaftNode::Impl::OnInstallSnapshotResponse(
    const InstallSnapshotResponseEvent& event) {
  if (!IsConfiguredPeer(event.peer_id)) {
    return Status::OK();
  }
  if (event.status.ok() &&
      event.response.term() > current_term) {
    return ObserveHigherTerm(event.response.term(), Clock::now());
  }
  const auto replication = replication_.find(event.peer_id);
  if (replication == replication_.end() ||
      replication->second.inflight_kind !=
          PeerReplicationState::InflightKind::kInstallSnapshot ||
      replication->second.inflight_sequence !=
          event.request_sequence) {
    return Status::OK();
  }
  replication->second.inflight_kind =
      PeerReplicationState::InflightKind::kNone;
  replication->second.inflight_sequence = 0;
  if (!event.status.ok() || role != Role::kLeader ||
      event.response.term() != current_term ||
      !event.response.success()) {
    return Status::OK();
  }
  if (event.request_offset >
      std::numeric_limits<uint64_t>::max() -
          event.request_data_size) {
    return Status::Corruption("raft snapshot offset overflow");
  }
  const uint64_t expected_offset =
      event.request_offset + event.request_data_size;
  if (event.response.received_offset() != expected_offset) {
    return Status::OK();
  }

  if (event.request_done) {
    const uint64_t matched = event.request_last_included_index;
    if (!local_snapshot_.has_value() ||
        local_snapshot_->last_included_index != matched ||
        local_snapshot_->last_included_term !=
            event.request_last_included_term) {
      return Status::OK();
    }
    replication->second.match_index =
        std::max(replication->second.match_index, matched);
    replication->second.next_index =
        matched == std::numeric_limits<uint64_t>::max()
            ? matched
            : matched + 1;
    replication->second.snapshot_offset = 0;
    replication->second.snapshot_transfer_index = 0;
    return ReplicateToPeer(event.peer_id);
  }
  replication->second.snapshot_offset = expected_offset;
  return ReplicateToPeer(event.peer_id);
}

Status RaftNode::Impl::Start() {
  std::lock_guard<std::mutex> join_lock(join_mutex);
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex);
  if (lifecycle == Lifecycle::kRunning) {
    return Status::OK();
  }
  if (lifecycle != Lifecycle::kCreated ||
      actor_thread.joinable()) {
    return Status::InvalidArgument(
        "raft node cannot be restarted");
  }

  try {
    actor_thread = std::thread([this]() { Run(); });
  } catch (const std::system_error& error) {
    return Status::IOError(
        std::string("start raft actor thread: ") + error.what());
  }
  actor_id = actor_thread.get_id();
  lifecycle = Lifecycle::kRunning;
  running.store(true, std::memory_order_release);
  return Status::OK();
}

Status RaftNode::Impl::Stop() {
  if (IsActorThread()) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (lifecycle == Lifecycle::kRunning) {
      lifecycle = Lifecycle::kStopping;
      running.store(false, std::memory_order_release);
      event_sink->Post(StopEvent{});
      event_sink->Close();
    }
    return Status::OK();
  }

  std::lock_guard<std::mutex> join_lock(join_mutex);
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (lifecycle == Lifecycle::kStopped) {
      return Status::OK();
    }
    if (lifecycle == Lifecycle::kCreated) {
      lifecycle = Lifecycle::kStopped;
      running.store(false, std::memory_order_release);
      event_sink->Close();
      lifecycle_cv.notify_all();
      return Status::OK();
    }
    if (lifecycle == Lifecycle::kRunning) {
      lifecycle = Lifecycle::kStopping;
      running.store(false, std::memory_order_release);
      event_sink->Post(StopEvent{});
      event_sink->Close();
    }
  }

  if (actor_thread.joinable()) {
    actor_thread.join();
  }
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    lifecycle = Lifecycle::kStopped;
    running.store(false, std::memory_order_release);
  }
  lifecycle_cv.notify_all();
  return Status::OK();
}

Status RaftNode::Impl::PostEvent(RaftEvent event) {
  std::lock_guard<std::mutex> lock(lifecycle_mutex);
  if (lifecycle != Lifecycle::kCreated &&
      lifecycle != Lifecycle::kRunning) {
    return Status::InvalidArgument("raft node is not running");
  }
  if (!event_sink->Post(std::move(event))) {
    return Status::InvalidArgument("raft event sink is closed");
  }
  return Status::OK();
}

RaftNodeStatus RaftNode::Impl::GetStatus() const {
  RaftNodeStatus status;
  status.role = status_role.load(std::memory_order_relaxed);
  status.current_term =
      status_term.load(std::memory_order_relaxed);
  const uint32_t voted_for =
      status_voted_for.load(std::memory_order_relaxed);
  if (voted_for != 0) {
    status.voted_for = voted_for;
  }
  status.leader_id =
      status_leader.load(std::memory_order_relaxed);
  status.commit_index =
      status_commit.load(std::memory_order_relaxed);
  status.last_log_index =
      status_last_log.load(std::memory_order_relaxed);
  status.last_applied =
      status_last_applied.load(std::memory_order_relaxed);
  status.snapshot_index =
      status_snapshot.load(std::memory_order_relaxed);
  status.configuration_index =
      status_configuration_index.load(std::memory_order_relaxed);
  const std::shared_ptr<const std::vector<uint32_t>> voters =
      std::atomic_load(&status_voters);
  if (voters != nullptr) {
    status.voters = *voters;
  }
  status.processed_events =
      processed_events.load(std::memory_order_relaxed);
  status.tick_count =
      tick_count.load(std::memory_order_relaxed);
  status.running = running.load(std::memory_order_acquire);
  return status;
}

bool RaftNode::Impl::IsActorThread() const {
  std::lock_guard<std::mutex> lock(lifecycle_mutex);
  return actor_id != std::thread::id() &&
         std::this_thread::get_id() == actor_id;
}

Status RaftNode::Impl::Step(const RaftEvent& event) {
  if (std::holds_alternative<TickEvent>(event)) {
    return OnTick(Clock::now());
  }
  if (const auto* request_event =
          std::get_if<RequestVoteRequestEvent>(&event)) {
    return OnRequestVoteRequest(*request_event);
  }
  if (const auto* request_event =
          std::get_if<AppendEntriesRequestEvent>(&event)) {
    return OnAppendEntriesRequest(*request_event);
  }
  if (const auto* request_event =
          std::get_if<InstallSnapshotRequestEvent>(&event)) {
    return OnInstallSnapshotRequest(*request_event);
  }
  if (const auto* response_event =
          std::get_if<RequestVoteResponseEvent>(&event)) {
    return OnRequestVoteResponse(*response_event);
  }
  if (const auto* response_event =
          std::get_if<AppendEntriesResponseEvent>(&event)) {
    return OnAppendEntriesResponse(*response_event);
  }
  if (const auto* response_event =
          std::get_if<InstallSnapshotResponseEvent>(&event)) {
    return OnInstallSnapshotResponse(*response_event);
  }
  if (const auto* proposal = std::get_if<ProposeEvent>(&event)) {
    return HandlePropose(*proposal);
  }
  if (const auto* proposal =
          std::get_if<ProposeConfChangeEvent>(&event)) {
    return HandleProposeConfChange(*proposal);
  }
  if (const auto* read = std::get_if<ReadIndexEvent>(&event)) {
    return HandleReadIndex(*read);
  }
  if (std::holds_alternative<StopEvent>(event)) {
    return Status::OK();
  }
  return Status::Corruption("unknown raft event");
}

void RaftNode::Impl::Run() {
  const auto interval =
      std::chrono::milliseconds(options.tick_interval_ms);
  const Clock::time_point started_at = Clock::now();
  ResetElectionDeadline(started_at);
  ResetHeartbeatDeadline(started_at);
  auto next_tick = started_at + interval;

  while (true) {
    std::vector<RaftEvent> batch;
    const auto now = std::chrono::steady_clock::now();
    const auto remaining =
        next_tick > now
            ? std::chrono::duration_cast<std::chrono::milliseconds>(
                  next_tick - now)
            : std::chrono::milliseconds::zero();
    uint64_t wait_ms =
        static_cast<uint64_t>(remaining.count());
    if (remaining < next_tick - now) {
      ++wait_ms;
    }

    const bool received = event_sink->PopBatch(batch, wait_ms);
    if (received) {
      bool should_stop = false;
      for (const RaftEvent& event : batch) {
        if (std::holds_alternative<StopEvent>(event)) {
          should_stop = true;
          break;
        }
        const Status step_status = Step(event);
        processed_events.fetch_add(1, std::memory_order_relaxed);
        if (std::holds_alternative<TickEvent>(event)) {
          tick_count.fetch_add(1, std::memory_order_relaxed);
        }
        if (!step_status.ok()) {
          should_stop = true;
          break;
        }

        const auto after_event = Clock::now();
        if (after_event >= next_tick) {
          const TickEvent implicit_tick;
          const Status tick_status = Step(implicit_tick);
          processed_events.fetch_add(1, std::memory_order_relaxed);
          tick_count.fetch_add(1, std::memory_order_relaxed);
          if (!tick_status.ok()) {
            should_stop = true;
            break;
          }
          next_tick = after_event + interval;
        }
        if (IsStopping()) {
          should_stop = true;
          break;
        }
      }
      if (should_stop) {
        break;
      }
    }

    const auto after_batch = std::chrono::steady_clock::now();
    if (after_batch >= next_tick) {
      const TickEvent tick;
      const Status tick_status = Step(tick);
      processed_events.fetch_add(1, std::memory_order_relaxed);
      tick_count.fetch_add(1, std::memory_order_relaxed);
      if (!tick_status.ok()) {
        break;
      }
      next_tick = after_batch + interval;
    }
  }

  FailPendingProposals(
      Status::NotLeader("raft actor stopped"));
  FailPendingConfChange(
      Status::Timeout("raft actor stopped"));
  FailPendingReads(
      Status::Timeout("raft actor stopped"));
  event_sink->Close();
  {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    lifecycle = Lifecycle::kStopped;
    running.store(false, std::memory_order_release);
  }
  lifecycle_cv.notify_all();
}

RaftNode::RaftNode(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

RaftNode::~RaftNode() {
  Stop();
  impl_->ClearHandlers();
}

Status RaftNode::Create(
    RaftNodeOptions options,
    std::unique_ptr<RaftTransport> transport,
    std::unique_ptr<RaftLog> log,
    std::unique_ptr<RaftNode>* node) {
  return Create(
      std::move(options), std::move(transport), std::move(log),
      std::make_unique<NoopStateMachine>(), node);
}

Status RaftNode::Create(
    RaftNodeOptions options,
    std::unique_ptr<RaftTransport> transport,
    std::unique_ptr<RaftLog> log,
    std::unique_ptr<StateMachine> state_machine,
    std::unique_ptr<RaftNode>* node) {
  if (node == nullptr || transport == nullptr || log == nullptr ||
      state_machine == nullptr) {
    return Status::InvalidArgument("raft node dependency is null");
  }
  if (options.node_id == 0) {
    return Status::InvalidArgument("raft node id cannot be zero");
  }
  if (options.tick_interval_ms == 0 ||
      options.tick_interval_ms > kMaxTimerIntervalMs) {
    return Status::InvalidArgument(
        "raft tick interval is invalid");
  }
  if (options.heartbeat_interval_ms == 0 ||
      options.heartbeat_interval_ms > kMaxTimerIntervalMs) {
    return Status::InvalidArgument(
        "raft heartbeat interval is invalid");
  }
  if (options.election_timeout_min_ms == 0 ||
      options.election_timeout_max_ms <
          options.election_timeout_min_ms ||
      options.election_timeout_max_ms > kMaxTimerIntervalMs) {
    return Status::InvalidArgument(
        "raft election timeout range is invalid");
  }
  if (options.read_index_timeout_ms == 0 ||
      options.read_index_timeout_ms > kMaxTimerIntervalMs) {
    return Status::InvalidArgument(
        "raft read index timeout is invalid");
  }
  if (options.snapshot_threshold_entries == 0) {
    return Status::InvalidArgument(
        "raft snapshot threshold is invalid");
  }
  if (options.snapshot_chunk_size_bytes == 0 ||
      options.snapshot_chunk_size_bytes >
          kMaxSnapshotChunkSize) {
    return Status::InvalidArgument(
        "raft snapshot chunk size is invalid");
  }

  std::unordered_set<uint32_t> peers;
  for (uint32_t peer : options.peers) {
    if (peer == 0 || peer == options.node_id ||
        !peers.insert(peer).second) {
      return Status::InvalidArgument(
          "raft peer list is invalid");
    }
  }
  if (!options.initially_voting && peers.empty()) {
    return Status::InvalidArgument(
        "raft non-voting node requires known peers");
  }

  auto impl = std::make_unique<Impl>(
      std::move(options), std::move(transport), std::move(log),
      std::move(state_machine));
  Status status = impl->RecoverSnapshotState();
  if (!status.ok()) {
    return status;
  }
  status = impl->RebuildCurrentConfiguration();
  if (!status.ok()) {
    return status;
  }
  impl->PublishStatus();
  *node = std::unique_ptr<RaftNode>(
      new RaftNode(std::move(impl)));
  return Status::OK();
}

Status RaftNode::Start() {
  return impl_->Start();
}

Status RaftNode::Stop() {
  return impl_->Stop();
}

Status RaftNode::PostEvent(RaftEvent event) {
  return impl_->PostEvent(std::move(event));
}

RaftNodeStatus RaftNode::GetStatus() const {
  return impl_->GetStatus();
}

bool RaftNode::IsActorThread() const {
  return impl_->IsActorThread();
}

}  // namespace raftkv
