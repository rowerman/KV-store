#include "raftkv/rpc/rpc_server.h"

#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "rpc_kv.pb.h"
#include "raftkv/net/buffer.h"
#include "raftkv/net/event_loop.h"
#include "raftkv/net/tcp_connection.h"
#include "raftkv/net/tcp_server.h"

namespace raftkv {
namespace {

// RPC 错误响应使用协议层状态码。
proto::StatusCode ToProtoStatusCode(Code code) {
  switch (code) {
    case Code::kOk:
      return proto::STATUS_CODE_OK;
    case Code::kNotFound:
      return proto::STATUS_CODE_NOT_FOUND;
    case Code::kNotLeader:
      return proto::STATUS_CODE_NOT_LEADER;
    case Code::kTimeout:
      return proto::STATUS_CODE_TIMEOUT;
    case Code::kCorruption:
    case Code::kInvalidArgument:
      return proto::STATUS_CODE_INVALID_ARGUMENT;
    case Code::kIOError:
      return proto::STATUS_CODE_INTERNAL;
  }
  return proto::STATUS_CODE_INTERNAL;
}

bool SerializeErrorPayload(const Status& status,
                           std::string* payload) {
  proto::RpcErrorResponse error;
  error.set_status(ToProtoStatusCode(status.code()));
  error.set_message(status.message());
  return error.SerializeToString(payload);
}

}  // namespace

namespace detail {

class RpcServerImpl
    : public std::enable_shared_from_this<RpcServerImpl> {
 public:
  RpcServerImpl(EventLoop* base_loop, std::string name, uint16_t port,
                int io_threads, std::string listen_ip)
      : tcp_server_(base_loop, std::move(name), port, io_threads,
                    std::move(listen_ip)) {
    if (base_loop == nullptr) {
      throw std::invalid_argument("rpc server loop is null");
    }
  }

  ~RpcServerImpl() { Stop(); }

  Status RegisterMethod(uint32_t method_id, RpcHandler handler) {
    if (handler == nullptr) {
      return Status::InvalidArgument("rpc handler is null");
    }

    std::lock_guard<std::mutex> lock(methods_mutex_);
    if (started_) {
      return Status::InvalidArgument(
          "cannot register rpc method after start");
    }
    const auto inserted =
        methods_.emplace(method_id, std::move(handler));
    if (!inserted.second) {
      return Status::InvalidArgument("duplicate rpc method id");
    }
    return Status::OK();
  }

  Status Start() {
    if (started_) {
      return Status::OK();
    }

    const std::weak_ptr<RpcServerImpl> weak = weak_from_this();
    tcp_server_.SetMessageCallback(
        [weak](const TcpConnectionPtr& connection, Buffer* input) {
          if (auto server = weak.lock()) {
            server->HandleMessage(connection, input);
          }
        });

    const Status status = tcp_server_.Start();
    if (status.ok()) {
      std::lock_guard<std::mutex> lock(methods_mutex_);
      started_ = true;
    }
    return status;
  }

  void Stop() {
    if (!started_) {
      return;
    }
    tcp_server_.Stop();
    std::lock_guard<std::mutex> lock(methods_mutex_);
    started_ = false;
  }

  uint16_t Port() const { return tcp_server_.Port(); }

 private:
  void HandleMessage(const TcpConnectionPtr& connection,
                     Buffer* input) {
    while (connection->Connected()) {
      RpcFrame request;
      const FrameDecodeResult result =
          FrameCodec::TryDecode(input, &request);
      if (result == FrameDecodeResult::kIncomplete) {
        return;
      }
      if (result == FrameDecodeResult::kCorrupt ||
          request.msg_type !=
              static_cast<uint8_t>(RpcMessageType::kRequest) ||
          request.request_id == 0) {
        connection->ForceClose();
        return;
      }

      DispatchRequest(connection, std::move(request));
    }
  }

  void DispatchRequest(const TcpConnectionPtr& connection,
                       RpcFrame request) {
    RpcHandler handler;
    {
      std::lock_guard<std::mutex> lock(methods_mutex_);
      const auto iterator = methods_.find(request.method_id);
      if (iterator != methods_.end()) {
        handler = iterator->second;
      }
    }

    if (handler == nullptr) {
      SendError(connection, request.method_id, request.request_id,
                Status::InvalidArgument("unknown rpc method"));
      return;
    }

    auto completed = std::make_shared<std::atomic<bool>>(false);
    const std::weak_ptr<RpcServerImpl> weak_server =
        weak_from_this();
    const std::weak_ptr<TcpConnection> weak_connection = connection;
    RpcReplyCallback done =
        [weak_server, weak_connection,
         method_id = request.method_id,
         request_id = request.request_id,
         completed](Status status,
                    const google::protobuf::Message* response) {
          if (completed->exchange(true,
                                   std::memory_order_acq_rel)) {
            return;
          }

          std::string payload;
          uint8_t message_type = 0;
          if (status.ok()) {
            if (response == nullptr) {
              status = Status::InvalidArgument(
                  "successful rpc reply has no message");
            } else if (!response->SerializeToString(&payload)) {
              status = Status::IOError(
                  "failed to serialize rpc response");
            }
          }

          if (status.ok()) {
            message_type = static_cast<uint8_t>(
                RpcMessageType::kResponse);
          } else {
            if (!SerializeErrorPayload(status, &payload)) {
              return;
            }
            message_type =
                static_cast<uint8_t>(RpcMessageType::kError);
          }

          const auto server = weak_server.lock();
          const auto current_connection = weak_connection.lock();
          if (server == nullptr || current_connection == nullptr) {
            return;
          }

          RpcFrame frame;
          frame.msg_type = message_type;
          frame.method_id = method_id;
          frame.request_id = request_id;
          frame.payload = std::move(payload);
          server->SendFrame(current_connection, std::move(frame));
        };

    try {
      handler(request, done);
    } catch (const std::exception& error) {
      done(Status::IOError(
               std::string("rpc handler exception: ") + error.what()),
           nullptr);
    } catch (...) {
      done(Status::IOError("rpc handler threw unknown exception"),
           nullptr);
    }
  }

  void SendError(const TcpConnectionPtr& connection,
                 uint32_t method_id, uint64_t request_id,
                 const Status& status) {
    RpcFrame frame;
    frame.msg_type =
        static_cast<uint8_t>(RpcMessageType::kError);
    frame.method_id = method_id;
    frame.request_id = request_id;
    if (!SerializeErrorPayload(status, &frame.payload)) {
      connection->ForceClose();
      return;
    }
    SendFrame(connection, std::move(frame));
  }

  void SendFrame(const TcpConnectionPtr& connection,
                 RpcFrame frame) {
    Buffer output;
    const Status status = FrameCodec::Encode(frame, &output);
    if (!status.ok()) {
      connection->ForceClose();
      return;
    }
    connection->Send(
        Slice(output.Peek(), output.ReadableBytes()));
  }

  TcpServer tcp_server_;
  std::mutex methods_mutex_;
  std::unordered_map<uint32_t, RpcHandler> methods_;
  bool started_ = false;
};

}  // namespace detail

RpcServer::RpcServer(EventLoop* base_loop, std::string name,
                     uint16_t port, int io_threads,
                     std::string listen_ip)
    : impl_(std::make_shared<detail::RpcServerImpl>(
          base_loop, std::move(name), port, io_threads,
          std::move(listen_ip))) {}

RpcServer::~RpcServer() {
  impl_->Stop();
}

Status RpcServer::RegisterMethod(uint32_t method_id,
                                 RpcHandler handler) {
  return impl_->RegisterMethod(method_id, std::move(handler));
}

Status RpcServer::Start() {
  return impl_->Start();
}

void RpcServer::Stop() {
  impl_->Stop();
}

uint16_t RpcServer::Port() const {
  return impl_->Port();
}

}  // namespace raftkv
