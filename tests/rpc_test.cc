#include "raftkv/rpc/rpc_client.h"
#include "raftkv/rpc/rpc_server.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "rpc_kv.pb.h"
#include "raftkv/net/buffer.h"
#include "raftkv/net/event_loop.h"
#include "raftkv/net/tcp_server.h"

namespace raftkv {
namespace {

constexpr uint32_t kEchoMethod = 1;
constexpr uint32_t kErrorMethod = 2;
constexpr uint32_t kThrowMethod = 3;
constexpr uint32_t kDoubleReplyMethod = 4;
constexpr uint32_t kTimeoutMethod = 5;
constexpr uint32_t kPendingMethod = 6;
constexpr uint32_t kReconnectMethod = 7;
constexpr uint32_t kCorruptMethod = 8;

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

void ArmWatchdog(EventLoop* loop, uint64_t timeout_ms) {
  loop->RunAfter(timeout_ms, [loop] { loop->Quit(); });
}

void RunWhenConnected(EventLoop* loop, RpcClient* client,
                      std::function<void()> action) {
  auto timer = std::make_shared<TimerId>(0);
  *timer = loop->RunEvery(
      5, [loop, client, timer, action = std::move(action)]() mutable {
        if (!client->Connected()) {
          return;
        }
        loop->CancelTimer(*timer);
        action();
      });
}

TEST(RpcServerTest, ValidatesMethodRegistration) {
  EventLoop loop;
  RpcServer server(&loop, "registration", 0);

  EXPECT_EQ(server.RegisterMethod(1, RpcHandler{}).code(),
            Code::kInvalidArgument);

  RpcHandler handler =
      [](const RpcFrame&, RpcReplyCallback) {};
  EXPECT_TRUE(server.RegisterMethod(1, handler).ok());
  EXPECT_EQ(server.RegisterMethod(1, handler).code(),
            Code::kInvalidArgument);

  ASSERT_TRUE(server.Start().ok());
  EXPECT_EQ(server.RegisterMethod(2, handler).code(),
            Code::kInvalidArgument);
  server.Stop();
}

TEST(RpcTest, MatchesOneThousandConcurrentCalls) {
  constexpr int kProducerCount = 4;
  constexpr int kCallsPerProducer = 250;
  constexpr int kTotalCalls =
      kProducerCount * kCallsPerProducer;

  EventLoop loop;
  RpcServer server(&loop, "concurrent-server", 0);
  ASSERT_TRUE(server.RegisterMethod(
      kEchoMethod,
      [&loop](const RpcFrame& request, RpcReplyCallback done) {
        proto::KvPutRequest parsed;
        if (!parsed.ParseFromString(request.payload)) {
          done(Status::InvalidArgument("bad put request"), nullptr);
          return;
        }

        auto response = std::make_shared<proto::KvPutResponse>();
        response->set_status(proto::STATUS_CODE_OK);
        response->set_message(parsed.key());
        const uint64_t delay_ms = parsed.seq_num() % 16;
        loop.RunAfter(
            delay_ms, [done = std::move(done), response]() mutable {
              done(Status::OK(), response.get());
            });
      }).ok());
  ASSERT_TRUE(server.Start().ok());

  RpcClient client(&loop, "concurrent-client", "127.0.0.1",
                   server.Port());
  int completed = 0;
  int failures = 0;

  RunWhenConnected(&loop, &client, [&] {
    std::vector<std::thread> producers;
    producers.reserve(kProducerCount);
    for (int producer = 0; producer < kProducerCount; ++producer) {
      producers.emplace_back([&, producer] {
        for (int index = 0; index < kCallsPerProducer; ++index) {
          const int call_index =
              producer * kCallsPerProducer + index;
          const std::string key =
              "key-" + std::to_string(call_index);
          proto::KvPutRequest request;
          request.set_client_id(1);
          request.set_seq_num(
              static_cast<uint64_t>(call_index));
          request.set_key(key);
          request.set_value("value");

          client.Call(
              kEchoMethod, request,
              [&, expected_key = key](Status status,
                                      const RpcFrame& response) {
                bool ok = status.ok();
                ok = ok &&
                     response.msg_type ==
                         static_cast<uint8_t>(
                             RpcMessageType::kResponse);
                ok = ok && response.method_id == kEchoMethod;

                proto::KvPutResponse parsed;
                ok = ok &&
                     parsed.ParseFromString(response.payload);
                ok = ok && parsed.status() ==
                               proto::STATUS_CODE_OK;
                ok = ok && parsed.message() == expected_key;
                if (!ok) {
                  ++failures;
                }

                ++completed;
                if (completed == kTotalCalls) {
                  client.Disconnect();
                  server.Stop();
                  loop.Quit();
                }
              },
              8000);
        }
      });
    }
    for (std::thread& producer : producers) {
      producer.join();
    }
  });

  ArmWatchdog(&loop, 10000);
  client.Connect();
  loop.Loop();

  client.Disconnect();
  server.Stop();
  EXPECT_EQ(completed, kTotalCalls);
  EXPECT_EQ(failures, 0);
}

TEST(RpcTest, MatchesErrorExceptionAndSingleReply) {
  EventLoop loop;
  RpcServer server(&loop, "response-server", 0);
  ASSERT_TRUE(server.RegisterMethod(
      kErrorMethod,
      [](const RpcFrame&, RpcReplyCallback done) {
        done(Status::NotLeader("leader changed"), nullptr);
      }).ok());
  ASSERT_TRUE(server.RegisterMethod(
      kThrowMethod,
      [](const RpcFrame&, RpcReplyCallback) {
        throw std::runtime_error("handler failed");
      }).ok());
  ASSERT_TRUE(server.RegisterMethod(
      kDoubleReplyMethod,
      [](const RpcFrame&, RpcReplyCallback done) {
        proto::KvPutResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        response.set_message("first");
        done(Status::OK(), &response);
        done(Status::IOError("second"), nullptr);
      }).ok());
  ASSERT_TRUE(server.Start().ok());

  RpcClient client(&loop, "response-client", "127.0.0.1",
                   server.Port());
  int completed = 0;
  bool correct = true;

  auto make_callback =
      [&](Code expected_code, bool expect_error_frame)
      -> RpcResponseCallback {
    return [&, expected_code,
            expect_error_frame](Status status,
                                const RpcFrame& response) {
      if (status.code() != expected_code) {
        correct = false;
      }
      const bool is_error_frame =
          response.msg_type ==
          static_cast<uint8_t>(RpcMessageType::kError);
      if (is_error_frame != expect_error_frame) {
        correct = false;
      }

      ++completed;
      if (completed == 4) {
        client.Disconnect();
        server.Stop();
        loop.Quit();
      }
    };
  };

  RunWhenConnected(&loop, &client, [&] {
    proto::KvPutRequest request;
    request.set_key("key");
    client.Call(kErrorMethod, request,
                make_callback(Code::kNotLeader, true), 2000);
    client.Call(kThrowMethod, request,
                make_callback(Code::kIOError, true), 2000);
    client.Call(
        kDoubleReplyMethod, request,
        [&](Status status, const RpcFrame& response) {
          if (!status.ok() ||
              response.msg_type !=
                  static_cast<uint8_t>(RpcMessageType::kResponse)) {
            correct = false;
          }
          proto::KvPutResponse parsed;
          if (!parsed.ParseFromString(response.payload) ||
              parsed.message() != "first") {
            correct = false;
          }
          ++completed;
          if (completed == 4) {
            client.Disconnect();
            server.Stop();
            loop.Quit();
          }
        },
        2000);
    client.Call(9999, request,
                make_callback(Code::kInvalidArgument, true), 2000);
  });

  ArmWatchdog(&loop, 4000);
  client.Connect();
  loop.Loop();

  client.Disconnect();
  server.Stop();
  EXPECT_EQ(completed, 4);
  EXPECT_TRUE(correct);
}

TEST(RpcTest, TimesOutAndIgnoresLateResponse) {
  EventLoop loop;
  RpcServer server(&loop, "timeout-server", 0);
  ASSERT_TRUE(server.RegisterMethod(
      kTimeoutMethod,
      [&loop](const RpcFrame&, RpcReplyCallback done) {
        auto response = std::make_shared<proto::KvGetResponse>();
        response->set_status(proto::STATUS_CODE_OK);
        response->set_value("late");
        loop.RunAfter(
            80, [done = std::move(done), response]() mutable {
              done(Status::OK(), response.get());
            });
      }).ok());
  ASSERT_TRUE(server.Start().ok());

  RpcClient client(&loop, "timeout-client", "127.0.0.1",
                   server.Port());
  int callbacks = 0;
  bool timed_out = false;

  RunWhenConnected(&loop, &client, [&] {
    proto::KvGetRequest request;
    request.set_key("missing");
    client.Call(
        kTimeoutMethod, request,
        [&](Status status, const RpcFrame&) {
          ++callbacks;
          timed_out = status.code() == Code::kTimeout;
          if (callbacks == 1) {
            loop.RunAfter(200, [&] {
              client.Disconnect();
              server.Stop();
              loop.Quit();
            });
          }
        },
        20);
  });

  ArmWatchdog(&loop, 2000);
  client.Connect();
  loop.Loop();

  client.Disconnect();
  server.Stop();
  EXPECT_EQ(callbacks, 1);
  EXPECT_TRUE(timed_out);
}

TEST(RpcTest, FailsPendingCallsWhenConnectionCloses) {
  EventLoop loop;
  RpcServer server(&loop, "pending-server", 0);
  int received = 0;
  ASSERT_TRUE(server.RegisterMethod(
      kPendingMethod,
      [&](const RpcFrame&, RpcReplyCallback) {
        ++received;
        if (received == 3) {
          loop.RunAfter(10, [&] { server.Stop(); });
        }
      }).ok());
  ASSERT_TRUE(server.Start().ok());

  RpcClient client(&loop, "pending-client", "127.0.0.1",
                   server.Port());
  int callbacks = 0;
  bool all_timeout = true;

  RunWhenConnected(&loop, &client, [&] {
    proto::KvPutRequest request;
    request.set_key("pending");
    for (int i = 0; i < 3; ++i) {
      client.Call(
          kPendingMethod, request,
          [&](Status status, const RpcFrame&) {
            if (status.code() != Code::kTimeout) {
              all_timeout = false;
            }
            ++callbacks;
            if (callbacks == 3) {
              client.Disconnect();
              loop.Quit();
            }
          },
          5000);
    }
  });

  ArmWatchdog(&loop, 3000);
  client.Connect();
  loop.Loop();

  client.Disconnect();
  server.Stop();
  EXPECT_EQ(received, 3);
  EXPECT_EQ(callbacks, 3);
  EXPECT_TRUE(all_timeout);
}

TEST(RpcTest, ReconnectsAndAcceptsNewCalls) {
  const uint16_t port = ReserveUnusedPort();
  ASSERT_NE(port, 0);

  EventLoop loop;
  RpcServer server(&loop, "reconnect-server", port);
  ASSERT_TRUE(server.RegisterMethod(
      kReconnectMethod,
      [](const RpcFrame& request, RpcReplyCallback done) {
        proto::KvGetResponse response;
        response.set_status(proto::STATUS_CODE_OK);
        response.set_value(request.payload);
        done(Status::OK(), &response);
      }).ok());
  ASSERT_TRUE(server.Start().ok());

  TcpClientOptions options;
  options.initial_retry_delay_ms = 10;
  options.max_retry_delay_ms = 40;
  RpcClient client(&loop, "reconnect-client", "127.0.0.1",
                   port, options);

  bool first_ok = false;
  bool second_ok = false;
  bool saw_disconnected = false;
  Status restart_status;

  RunWhenConnected(&loop, &client, [&] {
    proto::KvGetRequest request;
    request.set_key("before");
    client.Call(
        kReconnectMethod, request,
        [&, request](Status status, const RpcFrame& response) {
          proto::KvGetResponse parsed;
          first_ok =
              status.ok() &&
              parsed.ParseFromString(response.payload) &&
              parsed.value() == request.SerializeAsString();

          server.Stop();
          loop.RunAfter(30, [&] {
            restart_status = server.Start();
            if (!restart_status.ok()) {
              loop.Quit();
            }
          });

          auto reconnect_timer = std::make_shared<TimerId>(0);
          *reconnect_timer = loop.RunEvery(
              10, [&, reconnect_timer, request] {
                if (!client.Connected()) {
                  saw_disconnected = true;
                  return;
                }
                if (!saw_disconnected) {
                  return;
                }
                loop.CancelTimer(*reconnect_timer);

                client.Call(
                    kReconnectMethod, request,
                    [&](Status second_status,
                        const RpcFrame& second_response) {
                      proto::KvGetResponse parsed;
                      second_ok =
                          second_status.ok() &&
                          parsed.ParseFromString(
                              second_response.payload);
                      client.Disconnect();
                      server.Stop();
                      loop.Quit();
                    },
                    2000);
              });
        },
        2000);
  });

  ArmWatchdog(&loop, 4000);
  client.Connect();
  loop.Loop();

  client.Disconnect();
  server.Stop();
  EXPECT_TRUE(restart_status.ok());
  EXPECT_TRUE(saw_disconnected);
  EXPECT_TRUE(first_ok);
  EXPECT_TRUE(second_ok);
}

TEST(RpcTest, CorruptionFailsCallAndClosesConnection) {
  EventLoop loop;
  TcpServer raw_server(&loop, "raw-corrupt", 0, 0);
  raw_server.SetMessageCallback(
      [](const TcpConnectionPtr& connection, Buffer* input) {
        while (true) {
          RpcFrame request;
          const FrameDecodeResult result =
              FrameCodec::TryDecode(input, &request);
          if (result == FrameDecodeResult::kIncomplete) {
            return;
          }
          if (result == FrameDecodeResult::kCorrupt) {
            connection->ForceClose();
            return;
          }

          RpcFrame response;
          response.msg_type =
              static_cast<uint8_t>(RpcMessageType::kResponse);
          response.method_id = request.method_id;
          response.request_id = request.request_id;
          response.payload.clear();

          Buffer encoded;
          if (!FrameCodec::Encode(response, &encoded).ok()) {
            connection->ForceClose();
            return;
          }
          std::string wire(encoded.Peek(),
                           encoded.ReadableBytes());
          wire.back() =
              static_cast<char>(wire.back() ^ 0x01);
          connection->Send(Slice(wire));
        }
      });
  ASSERT_TRUE(raw_server.Start().ok());

  RpcClient client(&loop, "corrupt-client", "127.0.0.1",
                   raw_server.Port());
  int callbacks = 0;
  bool corruption = false;

  RunWhenConnected(&loop, &client, [&] {
    proto::KvGetRequest request;
    request.set_key("corrupt");
    client.Call(
        kCorruptMethod, request,
        [&](Status status, const RpcFrame&) {
          ++callbacks;
          corruption = status.code() == Code::kCorruption;
          if (callbacks == 1) {
            loop.RunAfter(80, [&] {
              client.Disconnect();
              raw_server.Stop();
              loop.Quit();
            });
          }
        },
        2000);
  });

  ArmWatchdog(&loop, 3000);
  client.Connect();
  loop.Loop();

  client.Disconnect();
  raw_server.Stop();
  EXPECT_EQ(callbacks, 1);
  EXPECT_TRUE(corruption);
}

TEST(RpcTest, DestructorCompletesPendingCall) {
  EventLoop loop;
  RpcServer server(&loop, "destroy-server", 0);
  ASSERT_TRUE(server.RegisterMethod(
      kPendingMethod,
      [](const RpcFrame&, RpcReplyCallback) {}).ok());
  ASSERT_TRUE(server.Start().ok());

  auto client = std::make_unique<RpcClient>(
      &loop, "destroy-client", "127.0.0.1", server.Port());
  int callbacks = 0;
  bool timeout_on_destroy = false;

  RunWhenConnected(&loop, client.get(), [&] {
    proto::KvPutRequest request;
    request.set_key("destroy");
    client->Call(
        kPendingMethod, request,
        [&](Status status, const RpcFrame&) {
          ++callbacks;
          timeout_on_destroy =
              status.code() == Code::kTimeout;
          loop.RunAfter(20, [&] {
            server.Stop();
            loop.Quit();
          });
        },
        2000);
    loop.RunAfter(10, [&] { client.reset(); });
  });

  ArmWatchdog(&loop, 2000);
  client->Connect();
  loop.Loop();

  server.Stop();
  EXPECT_EQ(callbacks, 1);
  EXPECT_TRUE(timeout_on_destroy);
}

}  // namespace
}  // namespace raftkv
