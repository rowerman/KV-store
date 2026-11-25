#include "raftkv/net/event_loop.h"
#include "raftkv/net/tcp_client.h"
#include "raftkv/net/tcp_server.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdint>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>

namespace raftkv {
namespace {

uint16_t ReserveUnusedPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return 0;
  }

  sockaddr_in address {};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    return 0;
  }

  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address),
                    &length) != 0) {
    ::close(fd);
    return 0;
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

void ArmWatchdog(EventLoop* loop) {
  loop->RunAfter(3000, [loop] { loop->Quit(); });
}

TEST(TcpClientTest, ConnectsEchoesAndDisconnects) {
  EventLoop loop;
  TcpServer server(&loop, "client-echo", 0, 0);
  server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* buffer) {
        connection->Send(Slice(buffer->RetrieveAllAsString()));
      });
  ASSERT_TRUE(server.Start().ok());
  ASSERT_NE(server.Port(), 0);

  TcpClient client(&loop, "client", "127.0.0.1", server.Port());
  std::string received;
  bool connected = false;
  bool disconnected = false;
  bool wrong_thread = false;
  client.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (!loop.IsInLoopThread()) {
          wrong_thread = true;
        }
        if (connection->Connected()) {
          connected = true;
          client.Send(Slice("hello"));
        } else if (connection->State() ==
                   TcpConnectionState::kDisconnected) {
          disconnected = true;
          loop.Quit();
        }
      });
  client.SetMessageCallback(
      [&](const TcpConnectionPtr&, Buffer* buffer) {
        if (!loop.IsInLoopThread()) {
          wrong_thread = true;
        }
        received = buffer->RetrieveAllAsString();
        client.Disconnect();
      });

  ArmWatchdog(&loop);
  client.Connect();
  loop.Loop();

  EXPECT_TRUE(connected);
  EXPECT_TRUE(disconnected);
  EXPECT_FALSE(wrong_thread);
  EXPECT_EQ(received, "hello");
  EXPECT_FALSE(client.Connected());
}

TEST(TcpClientTest, SendsFromOtherThreadAndRunsCallbacksInLoop) {
  EventLoop loop;
  TcpServer server(&loop, "threaded-client-echo", 0, 0);
  server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* buffer) {
        connection->Send(Slice(buffer->RetrieveAllAsString()));
      });
  ASSERT_TRUE(server.Start().ok());

  TcpClient client(&loop, "threaded-client", "127.0.0.1",
                   server.Port());
  std::string received;
  bool wrong_thread = false;
  bool write_complete = false;
  client.SetHighWaterMark(1024 * 1024);
  client.SetWriteCompleteCallback(
      [&](const TcpConnectionPtr&) {
        if (!loop.IsInLoopThread()) {
          wrong_thread = true;
        }
        write_complete = true;
      });
  client.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (!connection->Connected()) {
          loop.Quit();
          return;
        }
        if (!loop.IsInLoopThread()) {
          wrong_thread = true;
        }
        std::thread sender([&client] {
          client.Send(Slice("cross-thread"));
        });
        sender.join();
      });
  client.SetMessageCallback(
      [&](const TcpConnectionPtr&, Buffer* buffer) {
        if (!loop.IsInLoopThread()) {
          wrong_thread = true;
        }
        received = buffer->RetrieveAllAsString();
        client.Disconnect();
      });

  ArmWatchdog(&loop);
  client.Connect();
  loop.Loop();

  EXPECT_EQ(received, "cross-thread");
  EXPECT_TRUE(write_complete);
  EXPECT_FALSE(wrong_thread);
}

TEST(TcpClientTest, ReconnectsUntilServerStarts) {
  const uint16_t port = ReserveUnusedPort();
  ASSERT_NE(port, 0);

  EventLoop loop;
  TcpServer server(&loop, "late-echo", port, 0);
  server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* buffer) {
        connection->Send(Slice(buffer->RetrieveAllAsString()));
      });

  TcpClientOptions options;
  options.initial_retry_delay_ms = 20;
  options.max_retry_delay_ms = 80;
  TcpClient client(&loop, "late-client", "127.0.0.1", port,
                   options);

  Status start_status;
  std::string received;
  int connected_count = 0;
  client.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (connection->Connected()) {
          ++connected_count;
          client.Send(Slice("after-retry"));
        } else if (connection->State() ==
                   TcpConnectionState::kDisconnected) {
          if (!received.empty() || connected_count > 0) {
            loop.Quit();
          }
        }
      });
  client.SetMessageCallback(
      [&](const TcpConnectionPtr&, Buffer* buffer) {
        received = buffer->RetrieveAllAsString();
        client.Disconnect();
      });

  loop.RunAfter(120, [&] {
    start_status = server.Start();
    if (!start_status.ok()) {
      loop.Quit();
    }
  });
  ArmWatchdog(&loop);
  client.Connect();
  loop.Loop();

  EXPECT_TRUE(start_status.ok()) << start_status.ToString();
  EXPECT_EQ(connected_count, 1);
  EXPECT_EQ(received, "after-retry");
}

TEST(TcpClientTest, ReconnectsAfterPeerCloses) {
  EventLoop loop;
  TcpServer server(&loop, "reconnect-echo", 0, 0);
  bool first_server_message = true;
  server.SetMessageCallback(
      [&](const TcpConnectionPtr& connection, Buffer* buffer) {
        const std::string payload = buffer->RetrieveAllAsString();
        if (first_server_message) {
          first_server_message = false;
          connection->ForceClose();
          return;
        }
        connection->Send(Slice(payload));
      });
  ASSERT_TRUE(server.Start().ok());

  TcpClientOptions options;
  options.initial_retry_delay_ms = 10;
  options.max_retry_delay_ms = 40;
  TcpClient client(&loop, "reconnect-client", "127.0.0.1",
                   server.Port(), options);

  std::string received;
  int connected_count = 0;
  int disconnected_count = 0;
  client.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (connection->Connected()) {
          ++connected_count;
          client.Send(Slice(connected_count == 1 ? "first" : "second"));
        } else if (connection->State() ==
                   TcpConnectionState::kDisconnected) {
          ++disconnected_count;
          if (!received.empty()) {
            loop.Quit();
          }
        }
      });
  client.SetMessageCallback(
      [&](const TcpConnectionPtr&, Buffer* buffer) {
        received = buffer->RetrieveAllAsString();
        client.Disconnect();
      });

  ArmWatchdog(&loop);
  client.Connect();
  loop.Loop();

  EXPECT_EQ(connected_count, 2);
  EXPECT_GE(disconnected_count, 2);
  EXPECT_EQ(received, "second");
}

TEST(TcpClientTest, DisconnectCancelsRetryAndAllowsManualReconnect) {
  const uint16_t port = ReserveUnusedPort();
  ASSERT_NE(port, 0);

  EventLoop loop;
  TcpServer server(&loop, "manual-echo", port, 0);
  TcpClientOptions options;
  options.initial_retry_delay_ms = 10;
  options.max_retry_delay_ms = 20;
  TcpClient client(&loop, "manual-client", "127.0.0.1", port,
                   options);

  Status start_status;
  bool manual_connect_issued = false;
  bool connected_before_manual_connect = false;
  bool connected_after_manual_connect = false;
  int disconnected_count = 0;
  client.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (connection->Connected()) {
          if (!manual_connect_issued) {
            connected_before_manual_connect = true;
          } else {
            connected_after_manual_connect = true;
          }
        } else if (connection->State() ==
                   TcpConnectionState::kDisconnected) {
          ++disconnected_count;
        }
      });

  loop.RunAfter(45, [&] { client.Disconnect(); });
  loop.RunAfter(100, [&] {
    start_status = server.Start();
    if (!start_status.ok()) {
      loop.Quit();
    }
  });
  loop.RunAfter(160, [&] {
    manual_connect_issued = true;
    client.Connect();
  });
  loop.RunAfter(260, [&] { loop.Quit(); });
  ArmWatchdog(&loop);
  client.Connect();
  loop.Loop();

  EXPECT_TRUE(start_status.ok()) << start_status.ToString();
  EXPECT_FALSE(connected_before_manual_connect);
  EXPECT_TRUE(connected_after_manual_connect);
  EXPECT_TRUE(client.Connected());
  EXPECT_EQ(disconnected_count, 0);

  client.Disconnect();
}

TEST(TcpClientTest, DisabledReconnectLeavesClientDisconnected) {
  EventLoop loop;
  TcpServer server(&loop, "no-reconnect-echo", 0, 0);
  server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* buffer) {
        buffer->RetrieveAllAsString();
        connection->ForceClose();
      });
  ASSERT_TRUE(server.Start().ok());

  TcpClientOptions options;
  options.initial_retry_delay_ms = 10;
  options.max_retry_delay_ms = 10;
  options.reconnect = false;
  TcpClient client(&loop, "no-reconnect-client", "127.0.0.1",
                   server.Port(), options);

  int connected_count = 0;
  int disconnected_count = 0;
  client.SetConnectionCallback(
      [&](const TcpConnectionPtr& connection) {
        if (connection->Connected()) {
          ++connected_count;
          client.Send(Slice("close"));
        } else if (connection->State() ==
                   TcpConnectionState::kDisconnected) {
          ++disconnected_count;
        }
      });

  loop.RunAfter(200, [&] { loop.Quit(); });
  ArmWatchdog(&loop);
  client.Connect();
  loop.Loop();

  EXPECT_EQ(connected_count, 1);
  EXPECT_GE(disconnected_count, 1);
  EXPECT_FALSE(client.Connected());
}

TEST(TcpClientTest, DestructorCancelsPendingReconnect) {
  const uint16_t port = ReserveUnusedPort();
  ASSERT_NE(port, 0);

  EventLoop loop;
  TcpClientOptions options;
  options.initial_retry_delay_ms = 500;
  options.max_retry_delay_ms = 500;
  auto client = std::make_unique<TcpClient>(
      &loop, "destroy-client", "127.0.0.1", port, options);
  client->Connect();

  loop.RunAfter(30, [&] {
    client.reset();
    loop.Quit();
  });
  ArmWatchdog(&loop);
  loop.Loop();

  EXPECT_EQ(client, nullptr);
}

TEST(TcpClientTest, RejectsInvalidIpv4Address) {
  EventLoop loop;
  EXPECT_THROW(
      TcpClient(&loop, "invalid", "not-an-ipv4-address", 1234),
      std::invalid_argument);
}

}  // namespace
}  // namespace raftkv
