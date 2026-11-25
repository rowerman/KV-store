#include "raftkv/rpc/rpc_client.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "rpc_kv.pb.h"
#include "raftkv/net/buffer.h"
#include "raftkv/net/event_loop.h"
#include "raftkv/net/tcp_connection.h"

namespace raftkv {
namespace {

Status StatusFromRpcError(const proto::RpcErrorResponse& error) {
  switch (error.status()) {
    case proto::STATUS_CODE_OK:
      return Status::Corruption(
          "rpc error response has OK status");
    case proto::STATUS_CODE_NOT_FOUND:
      return Status::NotFound(error.message());
    case proto::STATUS_CODE_NOT_LEADER:
      return Status::NotLeader(error.message());
    case proto::STATUS_CODE_TIMEOUT:
      return Status::Timeout(error.message());
    case proto::STATUS_CODE_INVALID_ARGUMENT:
      return Status::InvalidArgument(error.message());
    case proto::STATUS_CODE_INTERNAL:
      return Status::IOError(error.message());
    default:
      break;
  }
  return Status::Corruption("unknown rpc status code");
}

}  // namespace

namespace detail {

class RpcClientImpl
    : public std::enable_shared_from_this<RpcClientImpl> {
 public:
  RpcClientImpl(EventLoop* loop, std::string name, std::string host,
                uint16_t port, TcpClientOptions options)
      : loop_(loop),
        tcp_client_(loop, std::move(name), std::move(host), port,
                    options) {
    if (loop_ == nullptr) {
      throw std::invalid_argument("rpc client loop is null");
    }
  }

  void Connect() {
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->ConnectInLoop(); });
  }

  void Disconnect() {
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->DisconnectInLoop(); });
  }

  void Destroy() {
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->DestroyInLoop(); });
  }

  bool Connected() const { return tcp_client_.Connected(); }

  void Call(uint32_t method_id,
            const google::protobuf::Message& request,
            RpcResponseCallback callback, uint64_t timeout_ms) {
    if (callback == nullptr) {
      throw std::invalid_argument("rpc response callback is null");
    }

    if (timeout_ms == 0) {
      QueueCallFailure(
          Status::InvalidArgument("rpc timeout must be positive"),
          std::move(callback));
      return;
    }

    std::string payload;
    if (!request.SerializeToString(&payload)) {
      QueueCallFailure(
          Status::IOError("failed to serialize rpc request"),
          std::move(callback));
      return;
    }
    if (payload.size() > kRpcFrameMaxPayloadSize) {
      QueueCallFailure(
          Status::InvalidArgument("rpc request payload is too large"),
          std::move(callback));
      return;
    }

    RpcFrame frame;
    frame.msg_type =
        static_cast<uint8_t>(RpcMessageType::kRequest);
    frame.method_id = method_id;
    frame.request_id = AllocateRequestId();
    frame.payload = std::move(payload);

    auto self = shared_from_this();
    loop_->RunInLoop(
        [self, frame = std::move(frame),
         callback = std::move(callback), timeout_ms]() mutable {
          self->StartCallInLoop(std::move(frame),
                                std::move(callback), timeout_ms);
        });
  }

 private:
  struct PendingCall {
    uint32_t method_id = 0;
    RpcResponseCallback callback;
    TimerId timeout_timer = 0;
  };

  void ConnectInLoop() {
    InstallCallbacksInLoop();
    tcp_client_.Connect();
  }

  void DisconnectInLoop() {
    tcp_client_.Disconnect();
    FailAllPending(
        Status::Timeout("rpc client disconnected"));
  }

  void DestroyInLoop() {
    tcp_client_.SetConnectionCallback(nullptr);
    tcp_client_.SetMessageCallback(nullptr);
    FailAllPending(
        Status::Timeout("rpc client destroyed"));
    tcp_client_.Disconnect();
  }

  void InstallCallbacksInLoop() {
    if (callbacks_installed_) {
      return;
    }

    const std::weak_ptr<RpcClientImpl> weak = weak_from_this();
    tcp_client_.SetConnectionCallback(
        [weak](const TcpConnectionPtr& connection) {
          if (auto client = weak.lock()) {
            client->HandleConnectionEvent(connection);
          }
        });
    tcp_client_.SetMessageCallback(
        [weak](const TcpConnectionPtr&, Buffer* input) {
          if (auto client = weak.lock()) {
            client->HandleMessage(input);
          }
        });
    callbacks_installed_ = true;
  }

  uint64_t AllocateRequestId() {
    uint64_t id = next_request_id_.fetch_add(
        1, std::memory_order_relaxed);
    if (id == 0) {
      id = next_request_id_.fetch_add(
          1, std::memory_order_relaxed);
    }
    return id;
  }

  void QueueCallFailure(Status status,
                        RpcResponseCallback callback) {
    loop_->RunInLoop(
        [status = std::move(status),
         callback = std::move(callback)]() mutable {
          callback(std::move(status), RpcFrame{});
        });
  }

  void StartCallInLoop(RpcFrame frame,
                       RpcResponseCallback callback,
                       uint64_t timeout_ms) {
    if (!tcp_client_.Connected()) {
      callback(
          Status::Timeout("rpc connection is not established"),
          RpcFrame{});
      return;
    }

    const uint64_t request_id = frame.request_id;
    if (pending_calls_.find(request_id) != pending_calls_.end()) {
      callback(Status::IOError("rpc request id collision"),
               RpcFrame{});
      return;
    }

    Buffer output;
    const Status encode_status = FrameCodec::Encode(frame, &output);
    if (!encode_status.ok()) {
      callback(encode_status, RpcFrame{});
      return;
    }

    const std::weak_ptr<RpcClientImpl> weak = weak_from_this();
    PendingCall pending;
    pending.method_id = frame.method_id;
    pending.callback = std::move(callback);
    pending.timeout_timer = loop_->RunAfter(
        timeout_ms, [weak, request_id] {
          if (auto client = weak.lock()) {
            client->HandleTimeout(request_id);
          }
        });
    pending_calls_.emplace(request_id, std::move(pending));

    // 登记和超时都完成后才发送，写失败触发的断开能清理该请求。
    tcp_client_.Send(
        Slice(output.Peek(), output.ReadableBytes()));
  }

  void HandleTimeout(uint64_t request_id) {
    const auto iterator = pending_calls_.find(request_id);
    if (iterator == pending_calls_.end()) {
      return;
    }

    RpcResponseCallback callback =
        std::move(iterator->second.callback);
    pending_calls_.erase(iterator);
    callback(
        Status::Timeout("rpc request timed out"), RpcFrame{});
  }

  void HandleConnectionEvent(
      const TcpConnectionPtr& connection) {
    if (connection->State() ==
        TcpConnectionState::kDisconnected) {
      FailAllPending(
          Status::Timeout("rpc connection closed"));
    }
  }

  void HandleMessage(Buffer* input) {
    while (true) {
      RpcFrame frame;
      const FrameDecodeResult result =
          FrameCodec::TryDecode(input, &frame);
      if (result == FrameDecodeResult::kIncomplete) {
        return;
      }
      if (result == FrameDecodeResult::kCorrupt) {
        FailProtocol(
            Status::Corruption("invalid rpc response frame"));
        return;
      }
      if (!HandleFrame(frame)) {
        return;
      }
    }
  }

  bool HandleFrame(const RpcFrame& frame) {
    const auto iterator =
        pending_calls_.find(frame.request_id);
    if (iterator == pending_calls_.end()) {
      // 超时或主动完成后的迟到响应不再影响任何状态。
      return true;
    }
    if (iterator->second.method_id != frame.method_id) {
      FailProtocol(
          Status::Corruption("rpc response method mismatch"));
      return false;
    }

    if (frame.msg_type ==
        static_cast<uint8_t>(RpcMessageType::kResponse)) {
      CompleteRequest(frame.request_id, Status::OK(), frame);
      return true;
    }
    if (frame.msg_type !=
        static_cast<uint8_t>(RpcMessageType::kError)) {
      FailProtocol(
          Status::Corruption("unexpected rpc response type"));
      return false;
    }

    proto::RpcErrorResponse error;
    if (!error.ParseFromString(frame.payload)) {
      FailProtocol(
          Status::Corruption("invalid rpc error payload"));
      return false;
    }
    Status error_status = StatusFromRpcError(error);
    if (error_status.code() == Code::kCorruption) {
      FailProtocol(std::move(error_status));
      return false;
    }
    CompleteRequest(frame.request_id,
                    std::move(error_status), frame);
    return true;
  }

  void CompleteRequest(uint64_t request_id, Status status,
                       const RpcFrame& frame) {
    const auto iterator = pending_calls_.find(request_id);
    if (iterator == pending_calls_.end()) {
      return;
    }

    PendingCall pending = std::move(iterator->second);
    pending_calls_.erase(iterator);
    if (pending.timeout_timer != 0) {
      loop_->CancelTimer(pending.timeout_timer);
    }
    pending.callback(std::move(status), frame);
  }

  void FailAllPending(Status status) {
    std::unordered_map<uint64_t, PendingCall> pending;
    pending.swap(pending_calls_);

    for (auto& item : pending) {
      PendingCall& call = item.second;
      if (call.timeout_timer != 0) {
        loop_->CancelTimer(call.timeout_timer);
      }
      // 每个回调获得独立空帧，避免通过引用共享临时状态。
      call.callback(status, RpcFrame{});
    }
  }

  void FailProtocol(Status status) {
    FailAllPending(std::move(status));
    tcp_client_.CloseConnection();
  }

  EventLoop* const loop_;
  TcpClient tcp_client_;
  std::atomic<uint64_t> next_request_id_{1};
  std::unordered_map<uint64_t, PendingCall> pending_calls_;
  bool callbacks_installed_ = false;
};

}  // namespace detail

RpcClient::RpcClient(EventLoop* loop, std::string name,
                     std::string host, uint16_t port,
                     TcpClientOptions options)
    : impl_(std::make_shared<detail::RpcClientImpl>(
          loop, std::move(name), std::move(host), port, options)) {}

RpcClient::~RpcClient() {
  impl_->Destroy();
}

void RpcClient::Connect() {
  impl_->Connect();
}

void RpcClient::Disconnect() {
  impl_->Disconnect();
}

bool RpcClient::Connected() const {
  return impl_->Connected();
}

void RpcClient::Call(uint32_t method_id,
                     const google::protobuf::Message& request,
                     RpcResponseCallback callback,
                     uint64_t timeout_ms) {
  impl_->Call(method_id, request, std::move(callback), timeout_ms);
}

}  // namespace raftkv
