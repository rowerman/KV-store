#include "raftkv/raft/raft_node.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <system_error>
#include <thread>
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

}  // namespace

struct RaftNode::Impl {
  using Clock = std::chrono::steady_clock;

  Impl(RaftNodeOptions node_options,
       std::unique_ptr<RaftTransport> node_transport,
       std::unique_ptr<RaftLog> node_log)
      : options(std::move(node_options)),
        transport(std::move(node_transport)),
        log(std::move(node_log)),
        current_term(log->hard_state().current_term),
        voted_for(log->hard_state().voted_for),
        commit_index(log->LastIndex()),
        status_role(Role::kFollower),
        status_term(current_term),
        status_voted_for(voted_for.value_or(0)),
        status_leader(0),
        status_commit(commit_index) {
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
  }

  RaftNodeOptions options;
  std::unique_ptr<RaftTransport> transport;
  std::unique_ptr<RaftLog> log;
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

  std::atomic<uint64_t> processed_events{0};
  std::atomic<uint64_t> tick_count{0};
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
  role = Role::kLeader;
  leader_id = options.node_id;
  ClearElectionVotes();
  ResetElectionDeadline(now);
  ResetHeartbeatDeadline(now);
  PublishStatus();
  return BroadcastHeartbeat();
}

Status RaftNode::Impl::ObserveHigherTerm(
    uint64_t term, Clock::time_point now) {
  if (term <= current_term) {
    return Status::OK();
  }
  Status status = PersistHardState(term, std::nullopt);
  if (!status.ok()) {
    return status;
  }
  role = Role::kFollower;
  leader_id = 0;
  ClearElectionVotes();
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

Status RaftNode::Impl::BroadcastHeartbeat() {
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

  const std::weak_ptr<ActorEventSink> weak_sink = event_sink;
  for (uint32_t peer_id : options.peers) {
    transport->SendAppendEntries(
        peer_id, request,
        [weak_sink, peer_id](
            Status response_status,
            proto::AppendEntriesResponse response) {
          if (const auto sink = weak_sink.lock()) {
            sink->Post(AppendEntriesResponseEvent{
                peer_id, std::move(response_status),
                std::move(response)});
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

  // F4.4 只广播空心跳，日志复制由 F4.5 处理。
  if (event.request.entries_size() != 0) {
    complete(Status::OK(), false);
    return Status::OK();
  }
  if (event.request.prev_log_index() > log->LastIndex()) {
    complete(Status::OK(), false);
    return Status::OK();
  }

  uint64_t previous_term = 0;
  const Status term_status = log->TermAt(
      event.request.prev_log_index(), &previous_term);
  if (!term_status.ok() ||
      previous_term != event.request.prev_log_term()) {
    complete(Status::OK(), false);
    return Status::OK();
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
  if (!event.status.ok() || !IsConfiguredPeer(event.peer_id)) {
    return Status::OK();
  }
  if (event.response.term() > current_term) {
    return ObserveHigherTerm(event.response.term(), Clock::now());
  }
  return Status::OK();
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
    if (proposal->done != nullptr) {
      proposal->done(Status::NotLeader(
          "raft node has not elected a leader"));
    }
    PublishStatus();
    return Status::OK();
  }
  if (const auto* read = std::get_if<ReadIndexEvent>(&event)) {
    if (read->done != nullptr) {
      read->done(Status::NotLeader(
          "raft node has not elected a leader"));
    }
    PublishStatus();
    return Status::OK();
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
        if (!step_status.ok() || IsStopping()) {
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
  if (node == nullptr || transport == nullptr || log == nullptr) {
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

  std::unordered_set<uint32_t> peers;
  for (uint32_t peer : options.peers) {
    if (peer == 0 || peer == options.node_id ||
        !peers.insert(peer).second) {
      return Status::InvalidArgument(
          "raft peer list is invalid");
    }
  }

  auto impl = std::make_unique<Impl>(
      std::move(options), std::move(transport), std::move(log));
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
