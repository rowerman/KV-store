#ifndef RAFTKV_SERVER_KV_SERVER_H_
#define RAFTKV_SERVER_KV_SERVER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "raftkv/raft/raft_node.h"
#include "raftkv/server/real_raft_transport.h"
#include "raftkv/storage/lsm_tree.h"

namespace raftkv {

class EventLoop;

inline constexpr uint32_t kKvPutRpcMethod = 201;
inline constexpr uint32_t kKvDeleteRpcMethod = 202;
inline constexpr uint32_t kKvGetRpcMethod = 203;
inline constexpr uint32_t kKvScanRpcMethod = 204;

// 完整服务的节点地址；Port 同时也是本地 RPC 监听端口。
struct KVServerOptions {
  RaftPeerAddress local_node;
  std::vector<RaftPeerAddress> peers;
  std::string listen_ip = "0.0.0.0";
  std::string data_directory;
  int rpc_io_threads = 0;
  uint64_t raft_rpc_timeout_ms = 1000;
  TcpClientOptions tcp_client_options;
  RaftNodeOptions raft_options;
  LSMTOptions storage_options;
};

// 组装 RPC、Raft 与 LSM 的完整 KV 服务。
class KVServer final {
 public:
  static Status Create(
      EventLoop* loop,
      KVServerOptions options,
      std::unique_ptr<KVServer>* server);

  ~KVServer();

  KVServer(const KVServer&) = delete;
  KVServer& operator=(const KVServer&) = delete;

  // 启动 RPC 监听与 Raft Actor；只允许启动一次。
  Status Start();

  // 停止接收请求并等待 Raft Actor 退出；幂等。
  Status Stop();

  // Start 前返回配置端口；Start 后返回实际监听端口。
  uint16_t Port() const;

  // 线程安全地返回 Raft 状态镜像。
  RaftNodeStatus GetRaftStatus() const;

 private:
  struct Impl;

  explicit KVServer(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_SERVER_KV_SERVER_H_
