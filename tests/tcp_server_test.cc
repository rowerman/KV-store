#include "raftkv/net/event_loop.h"
#include "raftkv/net/tcp_server.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <random>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace raftkv {
namespace {

using namespace std::chrono_literals;

class ClientFd {
 public:
  explicit ClientFd(int fd = -1) : fd_(fd) {}
  ~ClientFd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  ClientFd(const ClientFd&) = delete;
  ClientFd& operator=(const ClientFd&) = delete;

  int get() const { return fd_; }

 private:
  int fd_;
};

int ConnectTo(uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }

  timeval timeout {};
  timeout.tv_sec = 5;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
               sizeof(timeout));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
               sizeof(timeout));

  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                sizeof(address)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

bool SendAll(int fd, const std::string& data) {
  size_t sent = 0;
  while (sent < data.size()) {
    const ssize_t count =
        ::send(fd, data.data() + sent, data.size() - sent, 0);
    if (count <= 0) {
      return false;
    }
    sent += static_cast<size_t>(count);
  }
  return true;
}

bool ReceiveAll(int fd, std::string* data, size_t size) {
  data->clear();
  data->resize(size);
  size_t received = 0;
  while (received < size) {
    const ssize_t count =
        ::recv(fd, data->data() + received, size - received, 0);
    if (count <= 0) {
      return false;
    }
    received += static_cast<size_t>(count);
  }
  return true;
}

void RunEchoClient(uint16_t port, int client_id,
                   std::atomic<int>* successes,
                   std::atomic<int>* failures) {
  ClientFd client(ConnectTo(port));
  if (client.get() < 0) {
    ++(*failures);
    return;
  }

  std::mt19937 random(0x524B0000U + client_id);
  std::uniform_int_distribution<int> length_distribution(1, 4096);
  std::uniform_int_distribution<int> byte_distribution(0, 255);

  for (int message_index = 0; message_index < 3; ++message_index) {
    std::string message(
        static_cast<size_t>(length_distribution(random)), '\0');
    for (char& byte : message) {
      byte = static_cast<char>(byte_distribution(random));
    }

    std::string echoed;
    if (!SendAll(client.get(), message) ||
        !ReceiveAll(client.get(), &echoed, message.size()) ||
        echoed != message) {
      ++(*failures);
      return;
    }
  }
  ++(*successes);
}

TEST(TcpServerTest, EchoWithBaseLoopOnly) {
  EventLoop loop;
  TcpServer server(&loop, "base-echo", 0, 0);
  server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* buffer) {
        connection->Send(Slice(buffer->RetrieveAllAsString()));
      });

  ASSERT_TRUE(server.Start().ok());
  ASSERT_NE(server.Port(), 0);

  std::atomic<int> successes{0};
  std::atomic<int> failures{0};
  std::thread client([&] {
    RunEchoClient(server.Port(), 1, &successes, &failures);
    loop.QueueInLoop([&] {
      server.Stop();
      loop.Quit();
    });
  });

  loop.Loop();
  client.join();

  EXPECT_EQ(successes.load(), 1);
  EXPECT_EQ(failures.load(), 0);
}

TEST(TcpServerTest, HundredClientsEchoAcrossTwoIoThreads) {
  EventLoop loop;
  TcpServer server(&loop, "threaded-echo", 0, 2);
  std::atomic<int> established{0};
  std::atomic<int> disconnected{0};
  server.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (connection->Connected()) {
          ++established;
        } else if (connection->State() ==
                   TcpConnectionState::kDisconnected) {
          ++disconnected;
        }
      });
  server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* buffer) {
        connection->Send(Slice(buffer->RetrieveAllAsString()));
      });

  ASSERT_TRUE(server.Start().ok());
  ASSERT_NE(server.Port(), 0);

  std::atomic<int> successes{0};
  std::atomic<int> failures{0};
  std::thread coordinator([&] {
    std::vector<std::thread> clients;
    clients.reserve(100);
    for (int i = 0; i < 100; ++i) {
      clients.emplace_back([&, i] {
        RunEchoClient(server.Port(), i, &successes, &failures);
      });
    }
    for (std::thread& client : clients) {
      client.join();
    }
    loop.QueueInLoop([&] {
      server.Stop();
      loop.Quit();
    });
  });

  loop.Loop();
  coordinator.join();

  EXPECT_EQ(successes.load(), 100);
  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(established.load(), 100);
  EXPECT_EQ(disconnected.load(), 100);
}

TEST(TcpServerTest, StartRejectsInvalidListenIp) {
  EventLoop loop;
  TcpServer server(&loop, "invalid", 0, 0, "not-an-ip");

  const Status status = server.Start();
  EXPECT_EQ(status.code(), Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
