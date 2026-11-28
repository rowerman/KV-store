#ifndef RAFTKV_SERVER_REAL_RAFT_TRANSPORT_H_
#define RAFTKV_SERVER_REAL_RAFT_TRANSPORT_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "raftkv/net/tcp_client.h"
#include "raftkv/raft/transport.h"

namespace raftkv {

class EventLoop;
class RpcServer;

inline constexpr uint32_t kRaftRequestVoteRpcMethod = 101;
inline constexpr uint32_t kRaftAppendEntriesRpcMethod = 102;
inline constexpr uint32_t kRaftInstallSnapshotRpcMethod = 103;

// 真实网络中的 Peer RPC 地址。
struct RaftPeerAddress {
  uint32_t node_id = 0;
  std::string host;
  uint16_t port = 0;
};

struct RealRaftTransportOptions {
  uint32_t local_node_id = 0;
  std::vector<RaftPeerAddress> peers;
  uint64_t rpc_timeout_ms = 1000;
  TcpClientOptions tcp_client_options;
};

// 通过真实 TCP RPC 实现 RaftTransport。
class RealRaftTransport final : public RaftTransport {
 public:
  static Status Create(
      EventLoop* loop,
      RealRaftTransportOptions options,
      std::unique_ptr<RealRaftTransport>* transport);

  ~RealRaftTransport() override;

  RealRaftTransport(const RealRaftTransport&) = delete;
  RealRaftTransport& operator=(const RealRaftTransport&) = delete;

  // 在 Start 前注册三个内部 Raft RPC；只允许成功调用一次。
  Status RegisterRpcMethods(RpcServer* server);

  // 连接全部 Peer；连接异步建立，可重复调用。
  void Start();

  // 断开全部 Peer；幂等，可从任意线程调用。
  void Stop();

  void SendRequestVote(
      uint32_t peer_id,
      const proto::RequestVoteRequest& request,
      RequestVoteResponseCallback callback) override;

  void SendAppendEntries(
      uint32_t peer_id,
      const proto::AppendEntriesRequest& request,
      AppendEntriesResponseCallback callback) override;

  void SendInstallSnapshot(
      uint32_t peer_id,
      const proto::InstallSnapshotRequest& request,
      InstallSnapshotResponseCallback callback) override;

  void SetRequestVoteHandler(
      RequestVoteHandler handler) override;
  void SetAppendEntriesHandler(
      AppendEntriesHandler handler) override;
  void SetInstallSnapshotHandler(
      InstallSnapshotHandler handler) override;

 private:
  struct Impl;

  explicit RealRaftTransport(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_SERVER_REAL_RAFT_TRANSPORT_H_
