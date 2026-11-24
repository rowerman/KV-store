#include "raftkv/net/tcp_server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <netinet/in.h>
#include <stdexcept>
#include <system_error>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/net/channel.h"
#include "raftkv/net/event_loop.h"

namespace raftkv {
namespace {

std::string FormatPeerAddress(const sockaddr_storage& address) {
  if (address.ss_family != AF_INET) {
    return "unknown";
  }
  const auto* ipv4 =
      reinterpret_cast<const sockaddr_in*>(&address);
  char ip[INET_ADDRSTRLEN] = {};
  ::inet_ntop(AF_INET, &ipv4->sin_addr, ip, sizeof(ip));
  return std::string(ip) + ":" + std::to_string(ntohs(ipv4->sin_port));
}

// 在独立线程中创建并运行 EventLoop。
class EventLoopThread {
 public:
  EventLoopThread() = default;

  ~EventLoopThread() { Stop(); }

  EventLoop* StartLoop() {
    if (thread_.joinable()) {
      throw std::logic_error("EventLoopThread already started");
    }
    thread_ = std::thread([this] { ThreadFunc(); });

    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this] {
      return loop_ != nullptr || start_error_ != nullptr;
    });
    if (start_error_) {
      std::rethrow_exception(start_error_);
    }
    return loop_;
  }

  void Stop() {
    if (thread_.joinable()) {
      EventLoop* loop = nullptr;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        loop = loop_;
      }
      if (loop != nullptr) {
        loop->Quit();
      }
      thread_.join();
      loop_ = nullptr;
    }
  }

 private:
  void ThreadFunc() {
    try {
      EventLoop loop;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
      }
      condition_.notify_all();
      loop.Loop();
    } catch (...) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        start_error_ = std::current_exception();
      }
      condition_.notify_all();
    }
  }

  std::mutex mutex_;
  std::condition_variable condition_;
  EventLoop* loop_ = nullptr;
  std::exception_ptr start_error_;
  std::thread thread_;
};

}  // namespace

Acceptor::Acceptor(EventLoop* loop, NewConnectionCallback callback)
    : loop_(loop), new_connection_callback_(std::move(callback)) {}

Acceptor::~Acceptor() {
  Stop();
}

Status Acceptor::Start(const std::string& ip, uint16_t port) {
  if (loop_ == nullptr) {
    return Status::InvalidArgument("acceptor loop is null");
  }
  if (listen_fd_ >= 0) {
    return Status::OK();
  }

  const int fd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return Status::IOError(std::strerror(errno));
  }

  int reuse = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse,
                   sizeof(reuse)) < 0) {
    const std::string error = std::strerror(errno);
    ::close(fd);
    return Status::IOError(error);
  }

  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (ip.empty() || ip == "0.0.0.0") {
    address.sin_addr.s_addr = htonl(INADDR_ANY);
  } else if (::inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1) {
    ::close(fd);
    return Status::InvalidArgument("invalid listen ip: " + ip);
  }

  if (::bind(fd, reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) < 0) {
    const std::string error = std::strerror(errno);
    ::close(fd);
    return Status::IOError(error);
  }
  if (::listen(fd, SOMAXCONN) < 0) {
    const std::string error = std::strerror(errno);
    ::close(fd);
    return Status::IOError(error);
  }

  sockaddr_in actual_address {};
  socklen_t actual_length = sizeof(actual_address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual_address),
                    &actual_length) < 0) {
    const std::string error = std::strerror(errno);
    ::close(fd);
    return Status::IOError(error);
  }

  listen_fd_ = fd;
  port_ = ntohs(actual_address.sin_port);
  channel_ = std::make_unique<Channel>(&loop_->poller_, listen_fd_);
  channel_->SetReadCallback([this] { HandleRead(); });
  try {
    channel_->EnableReading();
  } catch (const std::system_error& error) {
    channel_.reset();
    ::close(listen_fd_);
    listen_fd_ = -1;
    port_ = 0;
    return Status::IOError(error.what());
  }
  return Status::OK();
}

void Acceptor::Stop() {
  if (channel_) {
    try {
      channel_->Remove();
    } catch (...) {
      // 停止流程优先保证 fd 被关闭。
    }
    channel_.reset();
  }
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
}

void Acceptor::HandleRead() {
  while (true) {
    sockaddr_storage peer {};
    socklen_t peer_length = sizeof(peer);
    const int fd =
        ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer),
                  &peer_length, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd >= 0) {
      try {
        new_connection_callback_(fd, FormatPeerAddress(peer));
      } catch (...) {
        ::close(fd);
        throw;
      }
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    if (errno == EINTR || errno == ECONNABORTED) {
      continue;
    }
    return;
  }
}

struct EventLoopThreadPool::Impl {
  explicit Impl(EventLoop* base) : base_loop(base) {}

  EventLoop* base_loop;
  bool started = false;
  size_t next_index = 0;
  std::vector<std::unique_ptr<EventLoopThread>> threads;
  std::vector<EventLoop*> loops;
};

EventLoopThreadPool::EventLoopThreadPool(EventLoop* base_loop)
    : impl_(std::make_unique<Impl>(base_loop)) {}

EventLoopThreadPool::~EventLoopThreadPool() {
  Stop();
}

void EventLoopThreadPool::Start(int thread_count) {
  if (impl_->started) {
    throw std::logic_error("EventLoopThreadPool already started");
  }
  impl_->started = true;
  if (thread_count <= 0) {
    impl_->loops.push_back(impl_->base_loop);
    return;
  }

  for (int i = 0; i < thread_count; ++i) {
    auto thread = std::make_unique<EventLoopThread>();
    impl_->loops.push_back(thread->StartLoop());
    impl_->threads.push_back(std::move(thread));
  }
}

EventLoop* EventLoopThreadPool::NextLoop() {
  if (impl_->loops.empty()) {
    return impl_->base_loop;
  }
  EventLoop* loop = impl_->loops[impl_->next_index];
  impl_->next_index = (impl_->next_index + 1) % impl_->loops.size();
  return loop;
}

void EventLoopThreadPool::Stop() {
  impl_->threads.clear();
  impl_->loops.clear();
  impl_->started = false;
}

TcpServer::TcpServer(EventLoop* base_loop, std::string name,
                     uint16_t port, int io_threads,
                     std::string listen_ip)
    : base_loop_(base_loop),
      name_(std::move(name)),
      port_(port),
      io_threads_(io_threads),
      listen_ip_(std::move(listen_ip)),
      acceptor_(base_loop,
                [this](int fd, std::string peer) {
                  NewConnection(fd, std::move(peer));
                }),
      thread_pool_(base_loop) {}

TcpServer::~TcpServer() {
  Stop();
}

void TcpServer::SetConnectionCallback(ConnectionCallback callback) {
  connection_callback_ = std::move(callback);
}

void TcpServer::SetMessageCallback(MessageCallback callback) {
  message_callback_ = std::move(callback);
}

void TcpServer::SetHighWaterMarkCallback(
    HighWaterMarkCallback callback) {
  high_water_mark_callback_ = std::move(callback);
}

void TcpServer::SetWriteCompleteCallback(
    WriteCompleteCallback callback) {
  write_complete_callback_ = std::move(callback);
}

void TcpServer::SetHighWaterMark(size_t bytes) {
  high_water_mark_ = bytes;
}

Status TcpServer::Start() {
  if (started_) {
    return Status::OK();
  }

  thread_pool_.Start(io_threads_);
  const Status status = acceptor_.Start(listen_ip_, port_);
  if (!status.ok()) {
    thread_pool_.Stop();
    return status;
  }
  started_ = true;
  return Status::OK();
}

void TcpServer::Stop() {
  if (!started_) {
    return;
  }

  acceptor_.Stop();
  std::vector<TcpConnectionPtr> active_connections;
  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    active_connections.reserve(connections_.size());
    for (auto& item : connections_) {
      active_connections.push_back(std::move(item.second));
    }
    connections_.clear();
  }
  for (TcpConnectionPtr& connection : active_connections) {
    EventLoop* loop = connection->GetLoop();
    loop->RunInLoop(
        [connection = std::move(connection)] {
          connection->ForceClose();
        });
  }
  thread_pool_.Stop();
  started_ = false;
}

void TcpServer::NewConnection(int fd, std::string peer_address) {
  EventLoop* io_loop = thread_pool_.NextLoop();
  const std::string connection_name =
      name_ + "-" + std::to_string(next_connection_id_++);
  TcpConnectionPtr connection = std::make_shared<TcpConnection>(
      io_loop, connection_name, fd, std::move(peer_address));
  connection->SetConnectionCallback(connection_callback_);
  connection->SetMessageCallback(message_callback_);
  connection->SetHighWaterMarkCallback(high_water_mark_callback_);
  connection->SetWriteCompleteCallback(write_complete_callback_);
  connection->SetHighWaterMark(high_water_mark_);

  connection->SetCloseCallback(
      [this, connection_name](const TcpConnectionPtr&) {
        RemoveConnection(connection_name);
      });

  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_[connection_name] = connection;
  }
  io_loop->RunInLoop(
      [connection] { connection->ConnectEstablished(); });
}

void TcpServer::RemoveConnection(const std::string& name) {
  TcpConnectionPtr removed;
  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    const auto iterator = connections_.find(name);
    if (iterator == connections_.end()) {
      return;
    }
    removed = std::move(iterator->second);
    connections_.erase(iterator);
  }
  // removed 在本函数返回时销毁，此时仍是连接所属 IO 线程。
}

}  // namespace raftkv
