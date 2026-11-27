#ifndef RAFTKV_RAFT_TRANSPORT_H_
#define RAFTKV_RAFT_TRANSPORT_H_

#include <cstdint>
#include <functional>

#include "raftkv/common/status.h"
#include "rpc_raft.pb.h"

namespace raftkv {

using RequestVoteResponseCallback = std::function<void(
    Status status, proto::RequestVoteResponse response)>;
using AppendEntriesResponseCallback = std::function<void(
    Status status, proto::AppendEntriesResponse response)>;
using InstallSnapshotResponseCallback = std::function<void(
    Status status, proto::InstallSnapshotResponse response)>;

// from_peer_id 是请求发送节点；done 只允许生效一次。
using RequestVoteHandler = std::function<void(
    uint32_t from_peer_id,
    const proto::RequestVoteRequest& request,
    RequestVoteResponseCallback done)>;
using AppendEntriesHandler = std::function<void(
    uint32_t from_peer_id,
    const proto::AppendEntriesRequest& request,
    AppendEntriesResponseCallback done)>;
using InstallSnapshotHandler = std::function<void(
    uint32_t from_peer_id,
    const proto::InstallSnapshotRequest& request,
    InstallSnapshotResponseCallback done)>;

// 真实网络和内存模拟网络共用的 Raft 传输接口。
class RaftTransport {
 public:
  virtual ~RaftTransport() = default;

  virtual void SendRequestVote(
      uint32_t peer_id,
      const proto::RequestVoteRequest& request,
      RequestVoteResponseCallback callback) = 0;
  virtual void SendAppendEntries(
      uint32_t peer_id,
      const proto::AppendEntriesRequest& request,
      AppendEntriesResponseCallback callback) = 0;
  virtual void SendInstallSnapshot(
      uint32_t peer_id,
      const proto::InstallSnapshotRequest& request,
      InstallSnapshotResponseCallback callback) = 0;

  // 注册收到的请求处理器；传空函数可取消注册。
  virtual void SetRequestVoteHandler(
      RequestVoteHandler handler) = 0;
  virtual void SetAppendEntriesHandler(
      AppendEntriesHandler handler) = 0;
  virtual void SetInstallSnapshotHandler(
      InstallSnapshotHandler handler) = 0;
};

}  // namespace raftkv

#endif  // RAFTKV_RAFT_TRANSPORT_H_
