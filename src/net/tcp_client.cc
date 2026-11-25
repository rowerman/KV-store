#include "raftkv/net/tcp_client.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

#include "raftkv/net/channel.h"
#include "raftkv/net/event_loop.h"

namespace raftkv {
namespace detail {

class TcpClientImpl : public std::enable_shared_from_this<TcpClientImpl> {
 public:
  TcpClientImpl(EventLoop* loop, std::string name, std::string host,
                uint16_t port, TcpClientOptions options)
      : loop_(loop),
        name_(std::move(name)),
        host_(std::move(host)),
        port_(port),
        options_(options) {
    if (loop_ == nullptr) {
      throw std::invalid_argument("tcp client loop is null");
    }
    if (options_.initial_retry_delay_ms == 0) {
      options_.initial_retry_delay_ms = 1;
    }
    if (options_.max_retry_delay_ms <
        options_.initial_retry_delay_ms) {
      options_.max_retry_delay_ms =
          options_.initial_retry_delay_ms;
    }

    server_address_.sin_family = AF_INET;
    server_address_.sin_port = htons(port_);
    if (::inet_pton(AF_INET, host_.c_str(),
                    &server_address_.sin_addr) != 1) {
      throw std::invalid_argument("invalid server ip: " + host_);
    }
  }

  ~TcpClientImpl() {
    // 正常情况下 fd 已在 EventLoop 中关闭，这里只兜底未执行的清理。
    connecting_channel_.reset();
    if (connecting_fd_ >= 0) {
      ::close(connecting_fd_);
    }
  }

  EventLoop* loop() const { return loop_; }

  bool connected() const {
    return connected_.load(std::memory_order_acquire);
  }

  void SetConnectionCallback(ConnectionCallback callback) {
    connection_callback_ = std::move(callback);
  }

  void SetMessageCallback(MessageCallback callback) {
    message_callback_ = std::move(callback);
  }

  void SetHighWaterMarkCallback(
      HighWaterMarkCallback callback) {
    high_water_mark_callback_ = std::move(callback);
  }

  void SetWriteCompleteCallback(
      WriteCompleteCallback callback) {
    write_complete_callback_ = std::move(callback);
  }

  void SetHighWaterMark(size_t bytes) {
    high_water_mark_ = bytes;
  }

  void Connect() {
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->ConnectInLoop(); });
  }

  void Disconnect() {
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->DisconnectInLoop(true); });
  }

  void CloseConnection() {
    auto self = shared_from_this();
    loop_->RunInLoop(
        [self] { self->CloseConnectionInLoop(); });
  }

  void Destroy() {
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->DisconnectInLoop(false); });
  }

  void Send(Slice data) {
    if (data.empty()) {
      return;
    }
    if (loop_->IsInLoopThread()) {
      SendInLoop(data.data(), data.size());
      return;
    }
    if (!connected()) {
      return;
    }

    auto self = shared_from_this();
    std::string copy(data.data(), data.size());
    loop_->RunInLoop(
        [self, data = std::move(copy)] {
          self->SendInLoop(data.data(), data.size());
        });
  }

 private:
  enum class State {
    kDisconnected,
    kConnecting,
    kConnected,
  };

  void ConnectInLoop() {
    if (keep_connected_) {
      return;
    }
    keep_connected_ = true;
    retry_attempt_ = 0;
    CancelRetryInLoop();
    StartConnectInLoop();
  }

  void StartConnectInLoop() {
    if (!keep_connected_ || state_ != State::kDisconnected) {
      return;
    }

    const int fd =
        ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                 0);
    if (fd < 0) {
      ScheduleReconnectInLoop();
      return;
    }

    if (::connect(fd,
                  reinterpret_cast<const sockaddr*>(&server_address_),
                  sizeof(server_address_)) == 0) {
      state_ = State::kConnecting;
      EstablishConnectionInLoop(fd);
      return;
    }
    if (errno != EINPROGRESS && errno != EWOULDBLOCK) {
      ::close(fd);
      ScheduleReconnectInLoop();
      return;
    }

    std::shared_ptr<Channel> channel;
    try {
      channel = std::make_shared<Channel>(&loop_->poller_, fd);
    } catch (...) {
      ::close(fd);
      ScheduleReconnectInLoop();
      return;
    }

    const std::weak_ptr<TcpClientImpl> weak = weak_from_this();
    channel->SetWriteCallback([weak] {
      if (auto self = weak.lock()) {
        self->HandleConnectEventInLoop();
      }
    });
    channel->SetErrorCallback([weak] {
      if (auto self = weak.lock()) {
        self->HandleConnectEventInLoop();
      }
    });
    channel->SetCloseCallback([weak] {
      if (auto self = weak.lock()) {
        self->HandleConnectEventInLoop();
      }
    });

    connecting_fd_ = fd;
    connecting_channel_ = std::move(channel);
    state_ = State::kConnecting;
    try {
      connecting_channel_->EnableWriting();
    } catch (...) {
      connecting_channel_.reset();
      ::close(connecting_fd_);
      connecting_fd_ = -1;
      state_ = State::kDisconnected;
      connected_.store(false, std::memory_order_release);
      ScheduleReconnectInLoop();
    }
  }

  void HandleConnectEventInLoop() {
    if (state_ != State::kConnecting ||
        connecting_channel_ == nullptr) {
      return;
    }

    int socket_error = 0;
    socklen_t error_length = sizeof(socket_error);
    const bool got_error =
        ::getsockopt(connecting_fd_, SOL_SOCKET, SO_ERROR,
                     &socket_error, &error_length) == 0;
    const int fd = connecting_fd_;
    connecting_fd_ = -1;

    // 当前进程正在执行该 Channel 的回调，不能立刻销毁 Channel。
    std::shared_ptr<Channel> channel =
        std::move(connecting_channel_);
    try {
      channel->DisableAll();
    } catch (...) {
      // fd 可能即将关闭，后续延迟析构仍会清理陈旧注册。
    }

    if (!got_error || socket_error != 0) {
      ::close(fd);
      state_ = State::kDisconnected;
      connected_.store(false, std::memory_order_release);
      loop_->QueueInLoop(
          [channel = std::move(channel)]() mutable {
            channel.reset();
          });
      ScheduleReconnectInLoop();
      return;
    }

    // 先销毁旧 Channel，再创建占用同一 fd 的 TcpConnection。
    auto self = shared_from_this();
    loop_->QueueInLoop(
        [self, channel = std::move(channel), fd]() mutable {
          channel.reset();
          self->EstablishConnectionInLoop(fd);
        });
  }

  void EstablishConnectionInLoop(int fd) {
    if (!keep_connected_ || state_ != State::kConnecting) {
      ::close(fd);
      state_ = State::kDisconnected;
      connected_.store(false, std::memory_order_release);
      return;
    }

    TcpConnectionPtr connection;
    try {
      connection = std::make_shared<TcpConnection>(
          loop_, name_ + "-" + std::to_string(connection_sequence_++),
          fd, host_ + ":" + std::to_string(port_));
    } catch (...) {
      ::close(fd);
      state_ = State::kDisconnected;
      connected_.store(false, std::memory_order_release);
      ScheduleReconnectInLoop();
      return;
    }

    const std::weak_ptr<TcpClientImpl> weak = weak_from_this();
    connection->SetConnectionCallback(
        [weak](const TcpConnectionPtr& current) {
          if (auto self = weak.lock()) {
            self->HandleConnectionEventInLoop(current);
          }
        });
    connection->SetMessageCallback(message_callback_);
    connection->SetHighWaterMarkCallback(
        high_water_mark_callback_);
    connection->SetWriteCompleteCallback(
        write_complete_callback_);
    connection->SetHighWaterMark(high_water_mark_);
    connection->SetCloseCallback(
        [weak](const TcpConnectionPtr& closed) {
          if (auto self = weak.lock()) {
            self->HandleConnectionClosedInLoop(closed);
          }
        });

    connection_ = std::move(connection);
    retry_attempt_ = 0;
    state_ = State::kConnected;
    connected_.store(true, std::memory_order_release);
    connection_->ConnectEstablished();
  }

  void HandleConnectionEventInLoop(
      const TcpConnectionPtr& connection) {
    if (connection_ != connection) {
      return;
    }

    if (connection->State() ==
        TcpConnectionState::kDisconnected) {
      state_ = State::kDisconnected;
      connected_.store(false, std::memory_order_release);
    } else {
      state_ = State::kConnected;
      connected_.store(true, std::memory_order_release);
    }
    if (connection_callback_) {
      connection_callback_(connection);
    }
  }

  void HandleConnectionClosedInLoop(
      const TcpConnectionPtr& connection) {
    if (connection_ != connection) {
      return;
    }
    connection_.reset();
    state_ = State::kDisconnected;
    connected_.store(false, std::memory_order_release);
    ScheduleReconnectInLoop();
  }

  void SendInLoop(const char* data, size_t length) {
    if (state_ != State::kConnected || connection_ == nullptr) {
      return;
    }
    connection_->Send(Slice(data, length));
  }

  void DisconnectInLoop(bool notify) {
    keep_connected_ = false;
    CancelRetryInLoop();
    CloseConnectingSocketInLoop();

    TcpConnectionPtr connection = connection_;
    if (!notify) {
      connection_callback_ = nullptr;
      message_callback_ = nullptr;
      high_water_mark_callback_ = nullptr;
      write_complete_callback_ = nullptr;
    }
    if (connection != nullptr) {
      connection->ForceClose();
    }
    if (connection_ == connection) {
      connection_.reset();
    }

    // 用户可在断开回调中重新 Connect，不能覆盖新连接状态。
    if (!keep_connected_) {
      state_ = State::kDisconnected;
      connected_.store(false, std::memory_order_release);
    }
  }

  void CloseConnectionInLoop() {
    if (state_ == State::kConnecting) {
      CloseConnectingSocketInLoop();
      ScheduleReconnectInLoop();
      return;
    }
    TcpConnectionPtr connection = connection_;
    if (state_ == State::kConnected && connection != nullptr) {
      // 正常关闭回调会统一更新状态并安排下一次重连。
      connection->ForceClose();
    }
  }


  void CloseConnectingSocketInLoop() {
    std::shared_ptr<Channel> channel =
        std::move(connecting_channel_);
    if (channel != nullptr) {
      try {
        channel->DisableAll();
      } catch (...) {
        // 延迟析构会再次尝试删除 Poller 中的注册。
      }
    }
    if (connecting_fd_ >= 0) {
      ::close(connecting_fd_);
      connecting_fd_ = -1;
    }
    state_ = State::kDisconnected;
    connected_.store(false, std::memory_order_release);
    if (channel != nullptr) {
      loop_->QueueInLoop(
          [channel = std::move(channel)]() mutable {
            channel.reset();
          });
    }
  }

  uint64_t RetryDelayInLoop() const {
    uint64_t delay = options_.initial_retry_delay_ms;
    for (uint64_t i = 0;
         i < retry_attempt_ && delay < options_.max_retry_delay_ms;
         ++i) {
      if (delay > options_.max_retry_delay_ms / 2) {
        return options_.max_retry_delay_ms;
      }
      delay *= 2;
    }
    return delay < options_.max_retry_delay_ms
               ? delay
               : options_.max_retry_delay_ms;
  }

  void ScheduleReconnectInLoop() {
    if (!keep_connected_ || !options_.reconnect ||
        state_ != State::kDisconnected ||
        retry_timer_id_ != 0 || connecting_channel_ != nullptr ||
        connection_ != nullptr) {
      return;
    }

    const uint64_t delay = RetryDelayInLoop();
    ++retry_attempt_;
    const std::weak_ptr<TcpClientImpl> weak = weak_from_this();
    retry_timer_id_ = loop_->RunAfter(delay, [weak] {
      if (auto self = weak.lock()) {
        self->retry_timer_id_ = 0;
        self->StartConnectInLoop();
      }
    });
  }

  void CancelRetryInLoop() {
    if (retry_timer_id_ != 0) {
      loop_->CancelTimer(retry_timer_id_);
      retry_timer_id_ = 0;
    }
  }

  EventLoop* const loop_;
  const std::string name_;
  const std::string host_;
  const uint16_t port_;
  TcpClientOptions options_;
  sockaddr_in server_address_ {};

  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
  HighWaterMarkCallback high_water_mark_callback_;
  WriteCompleteCallback write_complete_callback_;
  size_t high_water_mark_ = 64 * 1024 * 1024;

  State state_ = State::kDisconnected;
  bool keep_connected_ = false;
  uint64_t retry_attempt_ = 0;
  uint64_t retry_timer_id_ = 0;
  uint64_t connection_sequence_ = 1;
  std::shared_ptr<Channel> connecting_channel_;
  int connecting_fd_ = -1;
  TcpConnectionPtr connection_;
  std::atomic<bool> connected_ {false};
};

}  // namespace detail

TcpClient::TcpClient(EventLoop* loop, std::string name,
                     std::string host, uint16_t port,
                     TcpClientOptions options)
    : impl_(std::make_shared<detail::TcpClientImpl>(
          loop, std::move(name), std::move(host), port, options)) {}

TcpClient::~TcpClient() {
  impl_->Destroy();
}

EventLoop* TcpClient::GetLoop() const {
  return impl_->loop();
}

bool TcpClient::Connected() const {
  return impl_->connected();
}

void TcpClient::SetConnectionCallback(
    ConnectionCallback callback) {
  impl_->SetConnectionCallback(std::move(callback));
}

void TcpClient::SetMessageCallback(MessageCallback callback) {
  impl_->SetMessageCallback(std::move(callback));
}

void TcpClient::SetHighWaterMarkCallback(
    HighWaterMarkCallback callback) {
  impl_->SetHighWaterMarkCallback(std::move(callback));
}

void TcpClient::SetWriteCompleteCallback(
    WriteCompleteCallback callback) {
  impl_->SetWriteCompleteCallback(std::move(callback));
}

void TcpClient::SetHighWaterMark(size_t bytes) {
  impl_->SetHighWaterMark(bytes);
}

void TcpClient::Connect() {
  impl_->Connect();
}

void TcpClient::Disconnect() {
  impl_->Disconnect();
}

void TcpClient::CloseConnection() {
  impl_->CloseConnection();
}

void TcpClient::Send(Slice data) {
  impl_->Send(data);
}

}  // namespace raftkv
