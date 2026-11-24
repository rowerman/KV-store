#ifndef RAFTKV_NET_TCP_CONNECTION_H_
#define RAFTKV_NET_TCP_CONNECTION_H_

#include <cstddef>
#include <functional>
#include <memory>
#include <string>

#include "raftkv/common/slice.h"
#include "raftkv/net/buffer.h"
#include "raftkv/net/channel.h"

namespace raftkv {

class EventLoop;
class TcpConnection;

using TcpConnectionPtr = std::shared_ptr<TcpConnection>;
using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
using MessageCallback =
    std::function<void(const TcpConnectionPtr&, Buffer*)>;
using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
using HighWaterMarkCallback =
    std::function<void(const TcpConnectionPtr&, size_t)>;
using WriteCompleteCallback =
    std::function<void(const TcpConnectionPtr&)>;

// TCP 连接状态。
enum class TcpConnectionState {
  kConnecting,
  kConnected,
  kDisconnecting,
  kDisconnected,
};

// 由 shared_ptr 管理的 TCP 连接。
class TcpConnection
    : public std::enable_shared_from_this<TcpConnection> {
 public:
  // 接收已 accept 的非阻塞 fd，并设置对端地址文本。
  TcpConnection(EventLoop* loop, std::string name, int fd,
                std::string peer_address);

  // 先注销 Channel，再关闭连接 fd。
  ~TcpConnection();

  TcpConnection(const TcpConnection&) = delete;
  TcpConnection& operator=(const TcpConnection&) = delete;

  // 返回连接基本信息。
  EventLoop* GetLoop() const { return loop_; }
  const std::string& Name() const { return name_; }
  const std::string& PeerAddress() const { return peer_address_; }
  int fd() const { return fd_; }
  TcpConnectionState State() const { return state_; }
  bool Connected() const {
    return state_ == TcpConnectionState::kConnected;
  }

  // 配置回调，必须在 ConnectEstablished 前完成。
  void SetConnectionCallback(ConnectionCallback callback);
  void SetMessageCallback(MessageCallback callback);
  void SetCloseCallback(CloseCallback callback);
  void SetHighWaterMarkCallback(HighWaterMarkCallback callback);
  void SetWriteCompleteCallback(WriteCompleteCallback callback);

  // 设置输出缓冲区高水位，默认 64 MiB。
  void SetHighWaterMark(size_t bytes);

  // 在所属 EventLoop 中启用读事件并通知连接建立。
  void ConnectEstablished();

  // 任意线程可调用；数据会安全投递到连接所属 EventLoop。
  void Send(Slice data);

  // 半关闭写方向；待发送数据先排空。
  void Shutdown();

  // 立即关闭连接。
  void ForceClose();

 private:
  void HandleRead();
  void HandleWrite();
  void HandleClose();
  void HandleError();
  void SendInLoop(const char* data, size_t len);
  void ShutdownInLoop();
  void ForceCloseInLoop();

  EventLoop* loop_;
  std::string name_;
  int fd_;
  std::string peer_address_;
  Channel channel_;
  Buffer input_buffer_;
  Buffer output_buffer_;
  TcpConnectionState state_;
  size_t high_water_mark_ = 64 * 1024 * 1024;
  bool high_water_mark_reached_ = false;
  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  CloseCallback close_callback_;
  HighWaterMarkCallback high_water_mark_callback_;
  WriteCompleteCallback write_complete_callback_;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_TCP_CONNECTION_H_
