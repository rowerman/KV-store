#include "raftkv/raft/raft_node.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
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

inline constexpr uint64_t kMaxTickIntervalMs =
    24ULL * 60ULL * 60ULL * 1000ULL;

}  // namespace

struct RaftNode::Impl {
  Impl(RaftNodeOptions node_options,
       std::unique_ptr<RaftTransport> node_transport,
       std::unique_ptr<RaftLog> node_log)
      : options(std::move(node_options)),
        transport(std::move(node_transport)),
        log(std::move(node_log)),
        current_term(log->hard_state().current_term),
        commit_index(log->LastIndex()),
        status_role(Role::kFollower),
        status_term(current_term),
        status_leader(0),
        status_commit(commit_index) {
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

  bool IsStopping() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    return lifecycle == Lifecycle::kStopping ||
           lifecycle == Lifecycle::kStopped;
  }

  void PublishStatus() {
    status_role.store(role, std::memory_order_relaxed);
    status_term.store(current_term, std::memory_order_relaxed);
    status_leader.store(leader_id, std::memory_order_relaxed);
    status_commit.store(commit_index, std::memory_order_relaxed);
  }

  RaftNodeOptions options;
  std::unique_ptr<RaftTransport> transport;
  std::unique_ptr<RaftLog> log;
  MpscQueue<RaftEvent> events;

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
  uint32_t leader_id = 0;
  uint64_t commit_index = 0;

  std::atomic<Role> status_role;
  std::atomic<uint64_t> status_term;
  std::atomic<uint32_t> status_leader;
  std::atomic<uint64_t> status_commit;
  std::atomic<uint64_t> processed_events{0};
  std::atomic<uint64_t> tick_count{0};
};

void RaftNode::Impl::InstallHandlers() {
  transport->SetRequestVoteHandler(
      [this](uint32_t from_peer_id,
             const proto::RequestVoteRequest& request,
             RequestVoteResponseCallback done) {
        const Status status = PostEvent(RequestVoteRequestEvent{
            from_peer_id, request, done});
        if (!status.ok() && done != nullptr) {
          done(Status::Timeout(
                   "raft actor stopped before request delivery"),
               proto::RequestVoteResponse());
        }
      });
  transport->SetAppendEntriesHandler(
      [this](uint32_t from_peer_id,
             const proto::AppendEntriesRequest& request,
             AppendEntriesResponseCallback done) {
        const Status status = PostEvent(AppendEntriesRequestEvent{
            from_peer_id, request, done});
        if (!status.ok() && done != nullptr) {
          done(Status::Timeout(
                   "raft actor stopped before request delivery"),
               proto::AppendEntriesResponse());
        }
      });
  transport->SetInstallSnapshotHandler(
      [this](uint32_t from_peer_id,
             const proto::InstallSnapshotRequest& request,
             InstallSnapshotResponseCallback done) {
        const Status status = PostEvent(InstallSnapshotRequestEvent{
            from_peer_id, request, done});
        if (!status.ok() && done != nullptr) {
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
      events.Push(StopEvent{});
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
      lifecycle_cv.notify_all();
      return Status::OK();
    }
    if (lifecycle == Lifecycle::kRunning) {
      lifecycle = Lifecycle::kStopping;
      running.store(false, std::memory_order_release);
      events.Push(StopEvent{});
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
  events.Push(std::move(event));
  return Status::OK();
}

RaftNodeStatus RaftNode::Impl::GetStatus() const {
  RaftNodeStatus status;
  status.role = status_role.load(std::memory_order_relaxed);
  status.current_term =
      status_term.load(std::memory_order_relaxed);
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
    PublishStatus();
    return Status::OK();
  }
  if (const auto* request =
          std::get_if<RequestVoteRequestEvent>(&event)) {
    proto::RequestVoteResponse response;
    response.set_term(current_term);
    response.set_vote_granted(false);
    if (request->done != nullptr) {
      request->done(Status::OK(), response);
    }
    PublishStatus();
    return Status::OK();
  }
  if (const auto* request =
          std::get_if<AppendEntriesRequestEvent>(&event)) {
    proto::AppendEntriesResponse response;
    response.set_term(current_term);
    response.set_success(false);
    if (request->done != nullptr) {
      request->done(Status::OK(), response);
    }
    PublishStatus();
    return Status::OK();
  }
  if (const auto* request =
          std::get_if<InstallSnapshotRequestEvent>(&event)) {
    proto::InstallSnapshotResponse response;
    response.set_term(current_term);
    response.set_success(false);
    if (request->done != nullptr) {
      request->done(Status::OK(), response);
    }
    PublishStatus();
    return Status::OK();
  }
  if (std::holds_alternative<RequestVoteResponseEvent>(event) ||
      std::holds_alternative<AppendEntriesResponseEvent>(event) ||
      std::holds_alternative<InstallSnapshotResponseEvent>(event)) {
    PublishStatus();
    return Status::OK();
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
  auto next_tick = std::chrono::steady_clock::now() + interval;

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

    const bool received = events.PopBatch(batch, wait_ms);
    if (received) {
      bool should_stop = false;
      for (const RaftEvent& event : batch) {
        if (std::holds_alternative<StopEvent>(event)) {
          should_stop = true;
          break;
        }
        Step(event);
        processed_events.fetch_add(1, std::memory_order_relaxed);
        if (std::holds_alternative<TickEvent>(event)) {
          tick_count.fetch_add(1, std::memory_order_relaxed);
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
      Step(tick);
      processed_events.fetch_add(1, std::memory_order_relaxed);
      tick_count.fetch_add(1, std::memory_order_relaxed);
      next_tick = after_batch + interval;
    }
  }

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
  impl_->ClearHandlers();
  Stop();
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
      options.tick_interval_ms > kMaxTickIntervalMs) {
    return Status::InvalidArgument(
        "raft tick interval is invalid");
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
