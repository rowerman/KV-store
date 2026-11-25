#ifndef RAFTKV_RPC_RPC_SERVER_H_
#define RAFTKV_RPC_RPC_SERVER_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <google/protobuf/message.h>

#include "raftkv/common/status.h"
#include "raftkv/rpc/frame_codec.h"

namespace raftkv {

class EventLoop;

namespace detail {
class RpcServerImpl;
}

// 异步处理器通过 done 返回成功响应或错误。
using RpcReplyCallback = std::function<void(
    Status status, const google::protobuf::Message* response)>;

// request 仅在 handler 执行期间有效；done 可跨线程且只生效一次。
using RpcHandler = std::function<void(
    const RpcFrame& request, RpcReplyCallback done)>;

// 监听端口并按 method_id 分发 RPC 请求。
class RpcServer {
 public:
  RpcServer(EventLoop* base_loop, std::string name, uint16_t port,
            int io_threads = 0,
            std::string listen_ip = "127.0.0.1");

  // 停止服务并释放未完成处理器持有的服务器引用。
  ~RpcServer();

  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  // 仅在 Start 前注册；重复 method_id 返回 InvalidArgument。
  Status RegisterMethod(uint32_t method_id, RpcHandler handler);

  // 启动监听；重复调用成功时直接返回 OK。
  Status Start();

  // 停止监听和全部连接。
  void Stop();

  // 返回实际监听端口。
  uint16_t Port() const;

 private:
  std::shared_ptr<detail::RpcServerImpl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_RPC_RPC_SERVER_H_
