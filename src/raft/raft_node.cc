#include "raftkv/raft/raft_node.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "raftkv/common/mpsc_queue.h"

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
};

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
        status_last_applied(last_applied) {
    election_random.seed(MakeElectionSeed(options));
    InstallHandlers();
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
  Status AdvanceLeaderCommit();
  void ResetReplicationProgress();
  void FailPendingProposals(const Status& status);
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

  std::chrono::milliseconds NextElectionTimeout();
  void ResetElectionDeadline(Clock::time_point now);
  void ResetHeartbeatDeadline(Clock::time_point now);
  bool ElectionDeadlineReached(Clock::time_point now) const;
  bool HeartbeatDeadlineReached(Clock::time_point now) const;
  bool HasRecentLeader(Clock::time_point now) const;
  bool IsConfiguredPeer(uint32_t peer_id) const;
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

  std::atomic<Role> status_role;
  std::atomic<uint64_t> status_term;
  std::atomic<uint32_t> status_voted_for;
  std::atomic<uint32_t> status_leader;
  std::atomic<uint64_t> status_commit;
  std::atomic<uint64_t> status_last_log;
  std::atomic<uint64_t> status_last_applied;

  std::atomic<uint64_t> processed_events{0};
  std::atomic<uint64_t> tick_count{0};

  struct PeerReplicationState {
    uint64_t next_index = 1;
    uint64_t match_index = 0;
    uint64_t inflight_sequence = 0;
    bool inflight = false;
  };

  struct PendingRead {
    uint64_t read_id = 0;
    uint64_t read_index = 0;
    Clock::time_point deadline{};
    std::unordered_set<uint32_t> acks;
    ReadIndexCallback done;
    bool started = false;
  };

  std::unordered_map<uint32_t, PeerReplicationState> replication_;
  std::map<uint64_t, ProposeCallback> pending_proposals_;
  std::map<uint64_t, PendingRead> pending_reads_;
  uint64_t next_append_sequence_ = 1;
  uint64_t next_read_ctx_id_ = 1;
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

void RaftNode::Impl::ResetReplicationProgress() {
  replication_.clear();
  const uint64_t next_index = log->LastIndex() + 1;
  for (uint32_t peer_id : options.peers) {
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
  for (uint32_t peer_id : options.peers) {
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
  for (uint32_t peer_id : options.peers) {
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
  }
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
  return StartPendingReads();
}

Status RaftNode::Impl::AdvanceLeaderCommit() {
  if (role != Role::kLeader) {
    return Status::OK();
  }

  std::vector<uint64_t> matched_indices;
  matched_indices.reserve(options.peers.size() + 1);
  matched_indices.push_back(log->LastIndex());
  for (uint32_t peer_id : options.peers) {
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
    response->set_conflict_term(
        log->snapshot_marker().last_included_term);
    response->set_conflict_index(log->FirstIndex());
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
  return std::find(
             options.peers.begin(), options.peers.end(), peer_id) !=
         options.peers.end();
}

bool RaftNode::Impl::HasMajority(
    const std::unordered_set<uint32_t>& votes) const {
  const size_t cluster_size = options.peers.size() + 1;
  return votes.size() >= cluster_size / 2 + 1;
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
  if (!ElectionDeadlineReached(now)) {
    return Status::OK();
  }
  return StartPreVote(now);
}

Status RaftNode::Impl::StartPreVote(Clock::time_point now) {
  if (current_term == std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("raft term is exhausted");
  }
  FailPendingProposals(
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
  if (current_term == std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("raft term is exhausted");
  }
  FailPendingProposals(
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
  for (uint32_t peer_id : options.peers) {
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
  if (progress.inflight) {
    return Status::OK();
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
  progress.inflight = true;
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
  for (uint32_t peer_id : options.peers) {
    const auto replication = replication_.find(peer_id);
    if (replication == replication_.end() ||
        !replication->second.inflight) {
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
      !IsConfiguredPeer(event.request.leader_id())) {
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
  const auto complete = [&](Status status) {
    if (event.done != nullptr) {
      event.done(std::move(status), response);
    }
  };

  if (event.request.term() == 0 ||
      event.request.leader_id() == 0 ||
      event.request.leader_id() != event.from_peer_id ||
      !IsConfiguredPeer(event.request.leader_id())) {
    complete(Status::OK());
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() < current_term) {
    complete(Status::OK());
    PublishStatus();
    return Status::OK();
  }
  if (event.request.term() > current_term) {
    Status status = ObserveHigherTerm(event.request.term(), now);
    if (!status.ok()) {
      complete(status);
      return status;
    }
    response.set_term(current_term);
  }
  if (role != Role::kLeader) {
    role = Role::kFollower;
    leader_id = event.request.leader_id();
    ClearElectionVotes();
    ResetElectionDeadline(now);
  }
  PublishStatus();
  complete(Status::OK());
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
      !replication->second.inflight ||
      replication->second.inflight_sequence !=
          event.request_sequence) {
    return Status::OK();
  }
  replication->second.inflight = false;
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
    if (replication->second.next_index <= log->LastIndex()) {
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
  if (!event.status.ok() || !IsConfiguredPeer(event.peer_id)) {
    return Status::OK();
  }
  if (event.response.term() > current_term) {
    return ObserveHigherTerm(event.response.term(), Clock::now());
  }
  return Status::OK();
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

  std::unordered_set<uint32_t> peers;
  for (uint32_t peer : options.peers) {
    if (peer == 0 || peer == options.node_id ||
        !peers.insert(peer).second) {
      return Status::InvalidArgument(
          "raft peer list is invalid");
    }
  }

  auto impl = std::make_unique<Impl>(
      std::move(options), std::move(transport), std::move(log),
      std::move(state_machine));
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
