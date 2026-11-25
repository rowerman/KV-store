#ifndef RAFTKV_NET_TCP_CLIENT_H_
#define RAFTKV_NET_TCP_CLIENT_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "raftkv/net/tcp_connection.h"

namespace raftkv {

class EventLoop;

namespace detail {
class TcpClientImpl;
}

// 自动重连参数。
struct TcpClientOptions {
  // 第一次重连等待时间。
  uint64_t initial_retry_delay_ms = 50;
  // 指数退避的最大等待时间。
  uint64_t max_retry_delay_ms = 500;
  // 是否在连接失败或断开后自动重连。
  bool reconnect = true;
};

// 运行在既有 EventLoop 上的异步 TCP 客户端。
class TcpClient {
 public:
  // host 必须是数字 IPv4 地址。
  TcpClient(EventLoop* loop, std::string name, std::string host,
            uint16_t port, TcpClientOptions options = {});

  // 停止重连并关闭当前连接；不拥有 EventLoop。
  ~TcpClient();

  TcpClient(const TcpClient&) = delete;
  TcpClient& operator=(const TcpClient&) = delete;

  // 返回所属事件循环。
  EventLoop* GetLoop() const;

  // 线程安全地返回当前是否已连接。
  bool Connected() const;

  // 回调配置和发送缓冲区上限必须在首次 Connect 前完成。
  void SetConnectionCallback(ConnectionCallback callback);
  void SetMessageCallback(MessageCallback callback);
  void SetHighWaterMarkCallback(HighWaterMarkCallback callback);
  void SetWriteCompleteCallback(WriteCompleteCallback callback);
  void SetHighWaterMark(size_t bytes);

  // 开始异步连接；可重复调用。
  void Connect();

  // 主动断开并停止自动重连；可重复调用。
  void Disconnect();
  // 关闭当前连接或正在建立的 socket，保留自动重连意图。
  void CloseConnection();

  // 发送数据；未连接时直接丢弃。
  void Send(Slice data);

 private:
  std::shared_ptr<detail::TcpClientImpl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_TCP_CLIENT_H_
