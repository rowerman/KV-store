#include "raftkv/net/tcp_connection.h"

#include <cerrno>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

#include "raftkv/net/event_loop.h"

namespace raftkv {

TcpConnection::TcpConnection(EventLoop* loop, std::string name, int fd,
                             std::string peer_address)
    : loop_(loop),
      name_(std::move(name)),
      fd_(fd),
      peer_address_(std::move(peer_address)),
      channel_(&loop->poller_, fd),
      state_(TcpConnectionState::kConnecting) {}

TcpConnection::~TcpConnection() {
  // Channel 必须先注销，之后才能关闭它引用的 fd。
  try {
    channel_.Remove();
  } catch (...) {
    // 析构阶段不再传播 Poller 删除错误。
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

void TcpConnection::SetConnectionCallback(ConnectionCallback callback) {
  connection_callback_ = std::move(callback);
}

void TcpConnection::SetMessageCallback(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void TcpConnection::SetCloseCallback(CloseCallback callback) {
  close_callback_ = std::move(callback);
}

void TcpConnection::SetHighWaterMarkCallback(
    HighWaterMarkCallback callback) {
  high_water_mark_callback_ = std::move(callback);
}

void TcpConnection::SetWriteCompleteCallback(
    WriteCompleteCallback callback) {
  write_complete_callback_ = std::move(callback);
}

void TcpConnection::SetHighWaterMark(size_t bytes) {
  high_water_mark_ = bytes;
}

void TcpConnection::ConnectEstablished() {
  if (!loop_->IsInLoopThread()) {
    throw std::logic_error(
        "TcpConnection::ConnectEstablished called in wrong thread");
  }
  if (state_ != TcpConnectionState::kConnecting) {
    return;
  }

  const std::weak_ptr<TcpConnection> weak = weak_from_this();
  channel_.SetReadCallback([weak] {
    if (auto connection = weak.lock()) {
      connection->HandleRead();
    }
  });
  channel_.SetWriteCallback([weak] {
    if (auto connection = weak.lock()) {
      connection->HandleWrite();
    }
  });
  channel_.SetCloseCallback([weak] {
    if (auto connection = weak.lock()) {
      connection->HandleClose();
    }
  });
  channel_.SetErrorCallback([weak] {
    if (auto connection = weak.lock()) {
      connection->HandleError();
    }
  });

  state_ = TcpConnectionState::kConnected;
  channel_.EnableReading();
  if (connection_callback_) {
    connection_callback_(shared_from_this());
  }
}

void TcpConnection::Send(Slice data) {
  if (loop_->IsInLoopThread()) {
    SendInLoop(data.data(), data.size());
    return;
  }

  std::string copy;
  if (!data.empty()) {
    copy.assign(data.data(), data.size());
  }
  TcpConnectionPtr self = shared_from_this();
  loop_->RunInLoop(
      [self, data = std::move(copy)] {
        self->SendInLoop(data.data(), data.size());
      });
}

void TcpConnection::Shutdown() {
  if (loop_->IsInLoopThread()) {
    ShutdownInLoop();
    return;
  }

  TcpConnectionPtr self = shared_from_this();
  loop_->RunInLoop([self] { self->ShutdownInLoop(); });
}

void TcpConnection::ForceClose() {
  if (loop_->IsInLoopThread()) {
    ForceCloseInLoop();
    return;
  }

  TcpConnectionPtr self = shared_from_this();
  loop_->RunInLoop([self] { self->ForceCloseInLoop(); });
}

void TcpConnection::HandleRead() {
  TcpConnectionPtr self = shared_from_this();
  while (state_ == TcpConnectionState::kConnected ||
         state_ == TcpConnectionState::kDisconnecting) {
    int saved_errno = 0;
    const ssize_t bytes_read =
        input_buffer_.ReadFd(fd_, &saved_errno);
    if (bytes_read > 0) {
      if (message_callback_) {
        message_callback_(self, &input_buffer_);
      } else {
        input_buffer_.RetrieveAllAsString();
      }
      if (state_ == TcpConnectionState::kDisconnected) {
        return;
      }
      continue;
    }
    if (bytes_read == 0) {
      HandleClose();
      return;
    }
    if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
      return;
    }
    HandleError();
    return;
  }
}

void TcpConnection::HandleWrite() {
  if (state_ == TcpConnectionState::kDisconnected) {
    return;
  }

  while (output_buffer_.ReadableBytes() > 0) {
    const ssize_t bytes_written =
        ::write(fd_, output_buffer_.Peek(),
                output_buffer_.ReadableBytes());
    if (bytes_written > 0) {
      output_buffer_.Retrieve(static_cast<size_t>(bytes_written));
      continue;
    }
    if (bytes_written < 0 && errno == EINTR) {
      continue;
    }
    if (bytes_written < 0 &&
        (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    HandleError();
    return;
  }

  channel_.DisableWriting();
  high_water_mark_reached_ = false;
  if (state_ == TcpConnectionState::kDisconnecting) {
    ::shutdown(fd_, SHUT_WR);
  }
  if (write_complete_callback_) {
    write_complete_callback_(shared_from_this());
  }
}

void TcpConnection::HandleClose() {
  if (state_ == TcpConnectionState::kDisconnected) {
    return;
  }

  state_ = TcpConnectionState::kDisconnected;
  channel_.DisableAll();
  TcpConnectionPtr self = shared_from_this();
  if (connection_callback_) {
    connection_callback_(self);
  }
  if (close_callback_) {
    close_callback_(self);
  }
}

void TcpConnection::HandleError() {
  HandleClose();
}

void TcpConnection::SendInLoop(const char* data, size_t len) {
  if (state_ != TcpConnectionState::kConnected || len == 0) {
    return;
  }

  size_t written = 0;
  if (!channel_.IsWriting() &&
      output_buffer_.ReadableBytes() == 0) {
    while (written < len) {
      const ssize_t result =
          ::write(fd_, data + written, len - written);
      if (result > 0) {
        written += static_cast<size_t>(result);
        continue;
      }
      if (result < 0 && errno == EINTR) {
        continue;
      }
      if (result < 0 &&
          (errno == EAGAIN || errno == EWOULDBLOCK)) {
        break;
      }
      HandleError();
      return;
    }
  }

  if (written < len) {
    output_buffer_.Append(data + written, len - written);
    channel_.EnableWriting();
    if (output_buffer_.ReadableBytes() >= high_water_mark_ &&
        !high_water_mark_reached_) {
      high_water_mark_reached_ = true;
      if (high_water_mark_callback_) {
        high_water_mark_callback_(shared_from_this(),
                                  output_buffer_.ReadableBytes());
      }
    }
    return;
  }

  if (write_complete_callback_) {
    write_complete_callback_(shared_from_this());
  }
}

void TcpConnection::ShutdownInLoop() {
  if (state_ != TcpConnectionState::kConnected) {
    return;
  }

  state_ = TcpConnectionState::kDisconnecting;
  if (!channel_.IsWriting() &&
      output_buffer_.ReadableBytes() == 0) {
    if (::shutdown(fd_, SHUT_WR) < 0 && errno != ENOTCONN) {
      HandleError();
    }
  }
}

void TcpConnection::ForceCloseInLoop() {
  if (state_ == TcpConnectionState::kDisconnected) {
    return;
  }
  ::shutdown(fd_, SHUT_RDWR);
  HandleClose();
}

}  // namespace raftkv
