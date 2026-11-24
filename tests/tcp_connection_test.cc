#include "raftkv/net/event_loop.h"
#include "raftkv/net/poller.h"
#include "raftkv/net/tcp_connection.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <future>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace raftkv {
namespace {

using namespace std::chrono_literals;

class SocketFd {
 public:
  explicit SocketFd(int fd = -1) : fd_(fd) {}
  ~SocketFd() { reset(); }

  SocketFd(const SocketFd&) = delete;
  SocketFd& operator=(const SocketFd&) = delete;

  int get() const { return fd_; }

  void reset(int fd = -1) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_;
};

struct SocketPair {
  int owned_fd = -1;
  SocketFd peer;

  bool Open() {
    int fds[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      return false;
    }
    owned_fd = fds[0];
    peer.reset(fds[1]);
    return true;
  }

  ~SocketPair() {
    if (owned_fd >= 0) {
      ::close(owned_fd);
    }
  }
};

TEST(TcpConnectionTest, ReadCallbackReceivesData) {
  EventLoop loop;
  SocketPair pair;
  ASSERT_TRUE(pair.Open());
  ASSERT_TRUE(SetNonBlocking(pair.owned_fd).ok());

  auto connection = std::make_shared<TcpConnection>(
      &loop, "connection", pair.owned_fd, "peer");
  pair.owned_fd = -1;
  std::string received;
  connection->SetMessageCallback(
      [&](const TcpConnectionPtr&, Buffer* buffer) {
        received += buffer->RetrieveAllAsString();
        loop.Quit();
      });
  connection->ConnectEstablished();

  ASSERT_EQ(::write(pair.peer.get(), "hello", 5), 5);
  loop.Loop();

  EXPECT_EQ(received, "hello");
  connection->ForceClose();
}

TEST(TcpConnectionTest, SendFromOtherThreadReachesPeer) {
  EventLoop loop;
  SocketPair pair;
  ASSERT_TRUE(pair.Open());
  ASSERT_TRUE(SetNonBlocking(pair.owned_fd).ok());

  auto connection = std::make_shared<TcpConnection>(
      &loop, "connection", pair.owned_fd, "peer");
  pair.owned_fd = -1;
  connection->ConnectEstablished();

  std::thread sender([connection] { connection->Send(Slice("payload")); });
  sender.join();

  std::promise<std::string> received;
  std::future<std::string> result = received.get_future();
  std::thread reader([&] {
    std::array<char, 7> buffer{};
    std::string data;
    while (data.size() < buffer.size()) {
      const ssize_t count =
          ::read(pair.peer.get(), buffer.data() + data.size(),
                 buffer.size() - data.size());
      if (count <= 0) {
        break;
      }
      data.append(buffer.data() + data.size(),
                  static_cast<size_t>(count));
    }
    received.set_value(data);
    loop.QueueInLoop([&loop] { loop.Quit(); });
  });

  loop.Loop();
  reader.join();

  EXPECT_EQ(result.get(), "payload");
  connection->ForceClose();
}

TEST(TcpConnectionTest, PeerCloseRunsDisconnectAndCloseCallbacksOnce) {
  EventLoop loop;
  SocketPair pair;
  ASSERT_TRUE(pair.Open());
  ASSERT_TRUE(SetNonBlocking(pair.owned_fd).ok());

  auto connection = std::make_shared<TcpConnection>(
      &loop, "connection", pair.owned_fd, "peer");
  pair.owned_fd = -1;
  int connected_callbacks = 0;
  int disconnected_callbacks = 0;
  int close_callbacks = 0;
  connection->SetConnectionCallback(
      [&](const TcpConnectionPtr& current) {
        ++connected_callbacks;
        if (current->State() == TcpConnectionState::kDisconnected) {
          ++disconnected_callbacks;
          loop.Quit();
        }
      });
  connection->SetCloseCallback(
      [&](const TcpConnectionPtr&) { ++close_callbacks; });
  connection->ConnectEstablished();
  pair.peer.reset();

  loop.Loop();

  EXPECT_EQ(connected_callbacks, 2);
  EXPECT_EQ(disconnected_callbacks, 1);
  EXPECT_EQ(close_callbacks, 1);
}

TEST(TcpConnectionTest, ShutdownClosesPeerWriteDirection) {
  EventLoop loop;
  SocketPair pair;
  ASSERT_TRUE(pair.Open());
  ASSERT_TRUE(SetNonBlocking(pair.owned_fd).ok());

  auto connection = std::make_shared<TcpConnection>(
      &loop, "connection", pair.owned_fd, "peer");
  pair.owned_fd = -1;
  connection->ConnectEstablished();
  connection->Shutdown();

  std::promise<ssize_t> result;
  std::future<ssize_t> read_result = result.get_future();
  std::thread reader([&] {
    char byte = '\0';
    const ssize_t count = ::read(pair.peer.get(), &byte, 1);
    result.set_value(count);
    loop.QueueInLoop([&loop] { loop.Quit(); });
  });

  loop.Loop();
  reader.join();

  EXPECT_EQ(read_result.get(), 0);
  connection->ForceClose();
}

TEST(TcpConnectionTest, HighWaterAndWriteCompleteCallbacksFire) {
  EventLoop loop;
  SocketPair pair;
  ASSERT_TRUE(pair.Open());
  ASSERT_TRUE(SetNonBlocking(pair.owned_fd).ok());
  int send_buffer = 4096;
  ASSERT_EQ(::setsockopt(pair.owned_fd, SOL_SOCKET, SO_SNDBUF,
                         &send_buffer, sizeof(send_buffer)),
            0);

  auto connection = std::make_shared<TcpConnection>(
      &loop, "connection", pair.owned_fd, "peer");
  pair.owned_fd = -1;
  bool high_water = false;
  bool write_complete = false;
  connection->SetHighWaterMark(1024);
  connection->SetHighWaterMarkCallback(
      [&](const TcpConnectionPtr&, size_t) { high_water = true; });
  connection->SetWriteCompleteCallback(
      [&](const TcpConnectionPtr&) {
        write_complete = true;
      });
  connection->ConnectEstablished();

  const std::string payload(1024 * 1024, 'x');
  connection->Send(Slice(payload));
  ASSERT_TRUE(high_water);

  std::thread reader([&] {
    std::array<char, 16384> buffer{};
    size_t received = 0;
    while (received < payload.size()) {
      const ssize_t count =
          ::read(pair.peer.get(), buffer.data(), buffer.size());
      if (count <= 0) {
        break;
      }
      received += static_cast<size_t>(count);
    }
    loop.QueueInLoop([&loop] { loop.Quit(); });
  });

  loop.Loop();
  reader.join();

  EXPECT_TRUE(write_complete);
  connection->ForceClose();
}

}  // namespace
}  // namespace raftkv
