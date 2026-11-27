#ifndef RAFTKV_RAFT_RAFT_NODE_H_
#define RAFTKV_RAFT_RAFT_NODE_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "raftkv/common/status.h"
#include "raftkv/raft/raft_log.h"
#include "raftkv/raft/transport.h"

namespace raftkv {

enum class Role {
  kFollower,
  kPreCandidate,
  kCandidate,
  kLeader,
};

struct RaftNodeOptions {
  uint32_t node_id = 0;
  std::vector<uint32_t> peers;
  uint64_t tick_interval_ms = 10;
  uint64_t heartbeat_interval_ms = 50;
  uint64_t election_timeout_min_ms = 150;
  uint64_t election_timeout_max_ms = 300;
  // 0 表示使用运行时随机源；测试使用非 0 值获得可重复结果。
  uint64_t random_seed = 0;
};

// 线程安全的状态镜像，供 Server 和测试观察。
struct RaftNodeStatus {
  Role role = Role::kFollower;
  uint64_t current_term = 0;
  std::optional<uint32_t> voted_for;
  uint32_t leader_id = 0;
  uint64_t commit_index = 0;
  uint64_t processed_events = 0;
  uint64_t tick_count = 0;
  bool running = false;
};

struct TickEvent {};

struct RequestVoteRequestEvent {
  uint32_t from_peer_id = 0;
  proto::RequestVoteRequest request;
  RequestVoteResponseCallback done;
};

struct AppendEntriesRequestEvent {
  uint32_t from_peer_id = 0;
  proto::AppendEntriesRequest request;
  AppendEntriesResponseCallback done;
};

struct InstallSnapshotRequestEvent {
  uint32_t from_peer_id = 0;
  proto::InstallSnapshotRequest request;
  InstallSnapshotResponseCallback done;
};

struct RequestVoteResponseEvent {
  uint32_t peer_id = 0;
  bool is_pre_vote = false;
  Status status;
  proto::RequestVoteResponse response;
};

struct AppendEntriesResponseEvent {
  uint32_t peer_id = 0;
  Status status;
  proto::AppendEntriesResponse response;
};

struct InstallSnapshotResponseEvent {
  uint32_t peer_id = 0;
  Status status;
  proto::InstallSnapshotResponse response;
};

struct ProposeEvent {
  std::string command;
  std::function<void(Status)> done;
};

struct ReadIndexEvent {
  uint64_t read_id = 0;
  std::function<void(Status)> done;
};

struct StopEvent {};

using RaftEvent = std::variant<
    TickEvent,
    RequestVoteRequestEvent,
    AppendEntriesRequestEvent,
    InstallSnapshotRequestEvent,
    RequestVoteResponseEvent,
    AppendEntriesResponseEvent,
    InstallSnapshotResponseEvent,
    ProposeEvent,
    ReadIndexEvent,
    StopEvent>;

// 单线程 Raft 状态机；所有核心状态只由 Actor 线程修改。
class RaftNode final {
 public:
  static Status Create(
      RaftNodeOptions options,
      std::unique_ptr<RaftTransport> transport,
      std::unique_ptr<RaftLog> log,
      std::unique_ptr<RaftNode>* node);

  ~RaftNode();

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // 幂等启动；只允许启动一次。
  Status Start();

  // 停止并等待线程结束；可从 Actor 线程内调用。
  Status Stop();

  // 可从任意线程投递，Actor 按 FIFO 处理。
  Status PostEvent(RaftEvent event);

  RaftNodeStatus GetStatus() const;

  // 用于确认回调或测试代码是否运行在 Actor 线程。
  bool IsActorThread() const;

 private:
  struct Impl;
  explicit RaftNode(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_RAFT_RAFT_NODE_H_
