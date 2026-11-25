#ifndef RAFTKV_RPC_RPC_CLIENT_H_
#define RAFTKV_RPC_RPC_CLIENT_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <google/protobuf/message.h>

#include "raftkv/common/status.h"
#include "raftkv/net/tcp_client.h"
#include "raftkv/rpc/frame_codec.h"

namespace raftkv {

class EventLoop;

namespace detail {
class RpcClientImpl;
}

// response 仅在回调期间有效；载荷由上层按 method_id 解析。
using RpcResponseCallback = std::function<void(
    Status status, const RpcFrame& response)>;

// 在单条长连接上复用 request_id 的异步 RPC 客户端。
class RpcClient {
 public:
  RpcClient(EventLoop* loop, std::string name, std::string host,
            uint16_t port, TcpClientOptions options = {});

  // 主动断开，并用 kTimeout 完成全部在途请求。
  ~RpcClient();

  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;

  // 发起或恢复连接。
  void Connect();

  // 主动断开并停止自动重连。
  void Disconnect();

  // 线程安全地返回当前是否已连接。
  bool Connected() const;

  // req 在返回前完成序列化；回调始终在 EventLoop 线程执行。
  void Call(uint32_t method_id,
            const google::protobuf::Message& request,
            RpcResponseCallback callback, uint64_t timeout_ms);

 private:
  std::shared_ptr<detail::RpcClientImpl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_RPC_RPC_CLIENT_H_
