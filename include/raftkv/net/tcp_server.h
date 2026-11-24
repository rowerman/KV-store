#ifndef RAFTKV_NET_TCP_SERVER_H_
#define RAFTKV_NET_TCP_SERVER_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "raftkv/common/status.h"
#include "raftkv/net/tcp_connection.h"

namespace raftkv {

class Channel;
class EventLoop;

using NewConnectionCallback =
    std::function<void(int fd, std::string peer_address)>;

// 负责监听并接受非阻塞 TCP 连接。
class Acceptor {
 public:
  Acceptor(EventLoop* loop, NewConnectionCallback callback);
  ~Acceptor();

  Acceptor(const Acceptor&) = delete;
  Acceptor& operator=(const Acceptor&) = delete;

  // 创建监听 socket 并注册 accept 回调。
  Status Start(const std::string& ip, uint16_t port);

  // 停止监听并关闭 listen fd。
  void Stop();

  // 返回实际监听端口。
  uint16_t Port() const { return port_; }

 private:
  void HandleRead();

  EventLoop* loop_;
  NewConnectionCallback new_connection_callback_;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::unique_ptr<Channel> channel_;
};

// 管理子 Reactor 线程并按轮询分配连接。
class EventLoopThreadPool {
 public:
  explicit EventLoopThreadPool(EventLoop* base_loop);
  ~EventLoopThreadPool();

  EventLoopThreadPool(const EventLoopThreadPool&) = delete;
  EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

  // 线程数为 0 时使用基 EventLoop。
  void Start(int thread_count);

  // 返回下一个 IO EventLoop。
  EventLoop* NextLoop();

  // 请求全部子 EventLoop 退出并 join。
  void Stop();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// 主从 Reactor TCP 服务端。
class TcpServer {
 public:
  TcpServer(EventLoop* base_loop, std::string name, uint16_t port,
            int io_threads,
            std::string listen_ip = "127.0.0.1");
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  // 配置回调，必须在 Start 前完成。
  void SetConnectionCallback(ConnectionCallback callback);
  void SetMessageCallback(MessageCallback callback);
  void SetHighWaterMarkCallback(HighWaterMarkCallback callback);
  void SetWriteCompleteCallback(WriteCompleteCallback callback);
  void SetHighWaterMark(size_t bytes);

  // 启动线程池和监听器；失败时回滚线程池。
  Status Start();

  // 在基 EventLoop 线程停止 accept、关闭连接并停止 IO 线程。
  void Stop();

  // 返回实际监听端口。
  uint16_t Port() const { return acceptor_.Port(); }

 private:
  using ConnectionMap =
      std::unordered_map<std::string, TcpConnectionPtr>;

  void NewConnection(int fd, std::string peer_address);
  void RemoveConnection(const std::string& name);

  EventLoop* base_loop_;
  std::string name_;
  uint16_t port_;
  int io_threads_;
  std::string listen_ip_;
  Acceptor acceptor_;
  EventLoopThreadPool thread_pool_;
  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  HighWaterMarkCallback high_water_mark_callback_;
  WriteCompleteCallback write_complete_callback_;
  size_t high_water_mark_ = 64 * 1024 * 1024;
  ConnectionMap connections_;
  std::mutex connections_mutex_;
  uint64_t next_connection_id_ = 1;
  bool started_ = false;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_TCP_SERVER_H_
