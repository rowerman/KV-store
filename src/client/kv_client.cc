#include "raftkv/client/kv_client.h"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <unistd.h>

#include "raftkv/net/event_loop.h"
#include "raftkv/rpc/frame_codec.h"
#include "raftkv/rpc/kv_methods.h"
#include "raftkv/rpc/rpc_client.h"
#include "rpc_kv.pb.h"

namespace raftkv {
namespace {

std::string FormatEndpoint(const KVClientEndpoint& endpoint) {
  return endpoint.host + ":" + std::to_string(endpoint.port);
}

Status NormalizeEndpoint(KVClientEndpoint* endpoint) {
  if (endpoint == nullptr || endpoint->host.empty()) {
    return Status::InvalidArgument(
        "kv client endpoint host is empty");
  }
  if (endpoint->port == 0) {
    return Status::InvalidArgument(
        "kv client endpoint port is zero");
  }

  in_addr address {};
  if (::inet_pton(AF_INET, endpoint->host.c_str(), &address) != 1) {
    return Status::InvalidArgument(
        "kv client endpoint host is not IPv4");
  }

  char text[INET_ADDRSTRLEN] = {};
  if (::inet_ntop(AF_INET, &address, text, sizeof(text)) == nullptr) {
    return Status::InvalidArgument(
        "kv client endpoint host cannot be normalized");
  }
  endpoint->host = text;
  return Status::OK();
}

Status NormalizeOptions(
    EventLoop* loop, KVClientOptions* options) {
  if (loop == nullptr) {
    return Status::InvalidArgument("kv client loop is null");
  }
  if (options == nullptr) {
    return Status::InvalidArgument("kv client options are null");
  }
  if (options->endpoints.empty()) {
    return Status::InvalidArgument(
        "kv client endpoint list is empty");
  }
  if (options->request_timeout_ms == 0) {
    return Status::InvalidArgument(
        "kv client request timeout is zero");
  }

  std::unordered_set<std::string> unique_endpoints;
  for (KVClientEndpoint& endpoint : options->endpoints) {
    Status status = NormalizeEndpoint(&endpoint);
    if (!status.ok()) {
      return status;
    }
    if (!unique_endpoints.insert(FormatEndpoint(endpoint)).second) {
      return Status::InvalidArgument(
          "kv client endpoint is duplicated");
    }
  }
  return Status::OK();
}

Status ParseLeaderHint(
    const std::string& hint, KVClientEndpoint* endpoint) {
  if (hint.empty() || endpoint == nullptr) {
    return Status::InvalidArgument(
        "kv client leader hint is empty");
  }
  const size_t separator = hint.find(':');
  if (separator == std::string::npos ||
      hint.find(':', separator + 1) != std::string::npos) {
    return Status::InvalidArgument(
        "kv client leader hint is invalid");
  }

  uint32_t port = 0;
  const std::string port_text = hint.substr(separator + 1);
  const auto parsed = std::from_chars(
      port_text.data(), port_text.data() + port_text.size(), port);
  if (parsed.ec != std::errc() ||
      parsed.ptr != port_text.data() + port_text.size() ||
      port == 0 ||
      port > std::numeric_limits<uint16_t>::max()) {
    return Status::InvalidArgument(
        "kv client leader hint port is invalid");
  }

  endpoint->host = hint.substr(0, separator);
  endpoint->port = static_cast<uint16_t>(port);
  return NormalizeEndpoint(endpoint);
}

Status BusinessStatus(
    proto::StatusCode code, const std::string& message,
    const std::string& leader_hint) {
  const std::string detail =
      leader_hint.empty()
          ? message
          : (message.empty()
                 ? "leader hint: " + leader_hint
                 : message + "; leader hint: " + leader_hint);
  switch (code) {
    case proto::STATUS_CODE_OK:
      return Status::OK();
    case proto::STATUS_CODE_NOT_FOUND:
      return Status::NotFound(message);
    case proto::STATUS_CODE_NOT_LEADER:
      return Status::NotLeader(detail);
    case proto::STATUS_CODE_TIMEOUT:
      return Status::Timeout(message);
    case proto::STATUS_CODE_INTERNAL:
      return Status::IOError(message);
    case proto::STATUS_CODE_INVALID_ARGUMENT:
      return Status::InvalidArgument(message);
    default:
      break;
  }
  return Status::Corruption("kv response status is unknown");
}

bool IsRetryable(const Status& status) {
  return status.code() == Code::kNotLeader ||
         status.code() == Code::kTimeout ||
         status.code() == Code::kCorruption;
}

template <typename Response>
Status DecodeRpcResponse(
    uint32_t expected_method, const RpcFrame& frame,
    Response* response) {
  if (frame.msg_type !=
      static_cast<uint8_t>(RpcMessageType::kResponse)) {
    return Status::Corruption(
        "kv rpc response type is invalid");
  }
  if (frame.method_id != expected_method) {
    return Status::Corruption(
        "kv rpc response method mismatched");
  }
  if (response == nullptr ||
      !response->ParseFromString(frame.payload)) {
    return Status::Corruption(
        "kv rpc response protobuf is invalid");
  }
  return Status::OK();
}

uint64_t MixClientId(uint64_t value) {
  value += 0x9E3779B97F4A7C15ULL;
  value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
  return value ^ (value >> 31U);
}

uint64_t GenerateClientId() {
  static std::atomic<uint64_t> next_sequence{0};
  const uint64_t sequence =
      next_sequence.fetch_add(1, std::memory_order_relaxed);
  const uint64_t timestamp = static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const uint64_t process_id = static_cast<uint64_t>(::getpid());

  std::random_device random;
  const uint64_t client_id = MixClientId(
      timestamp ^ (process_id << 32U) ^ sequence ^
      (static_cast<uint64_t>(random()) << 32U) ^
      static_cast<uint64_t>(random()));
  return client_id == 0 ? 1 : client_id;
}

}  // namespace

struct KVClient::Impl
    : public std::enable_shared_from_this<KVClient::Impl> {
 private:
  enum class Lifecycle {
    kCreated,
    kStarted,
    kStopping,
    kStopped,
  };

 public:
  struct WriteOperation {
    proto::KvCommandType type = proto::KV_COMMAND_TYPE_UNSPECIFIED;
    std::string key;
    std::string value;
    uint64_t seq_num = 0;
    uint64_t attempts = 0;
    bool completed = false;
    KVClientWriteCallback callback;
  };

  struct GetOperation {
    std::string key;
    bool linearizable = true;
    uint64_t attempts = 0;
    bool completed = false;
    KVClientGetCallback callback;
  };

  struct ScanOperation {
    std::string start_key;
    std::string end_key;
    uint32_t limit = 0;
    uint64_t attempts = 0;
    bool completed = false;
    KVClientScanCallback callback;
  };

  Impl(EventLoop* event_loop, KVClientOptions client_options)
      : loop(event_loop),
        options(std::move(client_options)),
        client_id(GenerateClientId()) {}

  ~Impl() { Stop(); }

  Status Start() {
    {
      std::lock_guard<std::mutex> lock(lifecycle_mutex);
      if (lifecycle == Lifecycle::kStarted) {
        return Status::OK();
      }
      if (lifecycle != Lifecycle::kCreated) {
        return Status::InvalidArgument(
            "kv client cannot be restarted");
      }
      lifecycle = Lifecycle::kStarted;
    }

    auto self = shared_from_this();
    loop->QueueInLoop([self] { self->StartInLoop(); });
    return Status::OK();
  }

  void Stop() {
    {
      std::lock_guard<std::mutex> lock(lifecycle_mutex);
      if (lifecycle == Lifecycle::kStopping ||
          lifecycle == Lifecycle::kStopped) {
        return;
      }
      if (lifecycle == Lifecycle::kCreated) {
        lifecycle = Lifecycle::kStopped;
        return;
      }
      lifecycle = Lifecycle::kStopping;
    }

    auto self = shared_from_this();
    loop->QueueInLoop([self] { self->StopInLoop(); });
  }

  Status CheckReadAvailable() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (lifecycle != Lifecycle::kStarted) {
      return Status::InvalidArgument(
          "kv client is not started");
    }
    return Status::OK();
  }

  Status CheckWriteAvailable() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (lifecycle != Lifecycle::kStarted) {
      return Status::InvalidArgument(
          "kv client is not started");
    }
    if (write_outcome_unknown) {
      return Status::Timeout(
          "previous write outcome is unknown; create a new client");
    }
    return Status::OK();
  }

  void EnqueueWriteInLoop(
      std::shared_ptr<WriteOperation> operation) {
    if (!IsStarted()) {
      CompleteWriteInLoop(
          std::move(operation),
          Status::Timeout("kv client is stopped"), false);
      return;
    }
    pending_writes.push_back(std::move(operation));
    StartNextWriteInLoop();
  }

  void BeginGetInLoop(
      uint64_t token, std::shared_ptr<GetOperation> operation) {
    if (!IsStarted()) {
      InvokeGetCallback(
          std::move(operation),
          Status::Timeout("kv client is stopped"), std::string());
      return;
    }
    active_gets.emplace(token, operation);
    StartGetAttemptInLoop(token, std::move(operation));
  }

  void BeginScanInLoop(
      uint64_t token, std::shared_ptr<ScanOperation> operation) {
    if (!IsStarted()) {
      InvokeScanCallback(
          std::move(operation),
          Status::Timeout("kv client is stopped"),
          KVClientScanResult{});
      return;
    }
    active_scans.emplace(token, operation);
    StartScanAttemptInLoop(token, std::move(operation));
  }

  uint64_t AllocateOperationToken() {
    uint64_t token =
        next_operation_token.fetch_add(1, std::memory_order_relaxed);
    if (token == 0) {
      token =
          next_operation_token.fetch_add(1, std::memory_order_relaxed);
    }
    return token;
  }

 private:
  bool IsStarted() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    return lifecycle == Lifecycle::kStarted;
  }

  void StartInLoop() {
    if (!IsStarted()) {
      return;
    }
    for (const KVClientEndpoint& endpoint : options.endpoints) {
      ClientForEndpointInLoop(endpoint);
    }
  }

  void StopInLoop() {
    // 主动断开会让 RpcClient 以 Timeout 完成全部在途回调。
    for (auto& item : endpoint_clients) {
      item.second->Disconnect();
    }
    FailPendingWrites();
    FailPendingReads();
    endpoint_clients.clear();

    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (lifecycle == Lifecycle::kStopping) {
      lifecycle = Lifecycle::kStopped;
    }
  }

  KVClientEndpoint NextInitialEndpointInLoop() {
    const size_t index =
        next_endpoint_index % options.endpoints.size();
    ++next_endpoint_index;
    return options.endpoints[index];
  }

  KVClientEndpoint RetryEndpointInLoop(
      const std::string& leader_hint) {
    KVClientEndpoint hinted;
    if (ParseLeaderHint(leader_hint, &hinted).ok()) {
      return hinted;
    }
    return NextInitialEndpointInLoop();
  }

  RpcClient* ClientForEndpointInLoop(
      const KVClientEndpoint& endpoint) {
    const std::string key = FormatEndpoint(endpoint);
    const auto existing = endpoint_clients.find(key);
    if (existing != endpoint_clients.end()) {
      return existing->second.get();
    }

    auto client = std::make_unique<RpcClient>(
        loop, "kv-client-" + std::to_string(client_id) + "-" + key,
        endpoint.host, endpoint.port, options.tcp_client_options);
    RpcClient* raw_client = client.get();
    endpoint_clients.emplace(key, std::move(client));
    raw_client->Connect();
    return raw_client;
  }

  uint64_t MaxAttempts() const {
    return static_cast<uint64_t>(options.max_retries) + 1;
  }

  uint64_t RetryDelayMs() const {
    return std::max<uint64_t>(
        1, options.tcp_client_options.initial_retry_delay_ms);
  }

  void StartNextWriteInLoop() {
    if (active_write != nullptr || !IsStarted()) {
      return;
    }
    if (write_outcome_unknown) {
      FailQueuedWrites();
      return;
    }
    if (pending_writes.empty()) {
      return;
    }

    active_write = std::move(pending_writes.front());
    pending_writes.pop_front();
    active_write->seq_num = next_seq_num++;
    active_write->attempts = 0;
    StartWriteAttemptInLoop(
        active_write, NextInitialEndpointInLoop());
  }

  void StartWriteAttemptInLoop(
      const std::shared_ptr<WriteOperation>& operation,
      const KVClientEndpoint& endpoint) {
    if (operation->completed) {
      return;
    }
    if (!IsStarted()) {
      CompleteWriteInLoop(
          operation, Status::Timeout("kv client is stopped"), false);
      return;
    }

    ++operation->attempts;
    RpcClient* client = ClientForEndpointInLoop(endpoint);
    const std::weak_ptr<Impl> weak = weak_from_this();
    if (operation->type == proto::KV_COMMAND_TYPE_PUT) {
      proto::KvPutRequest request;
      request.set_client_id(client_id);
      request.set_seq_num(operation->seq_num);
      request.set_key(operation->key);
      request.set_value(operation->value);
      client->Call(
          kKvPutRpcMethod, request,
          [weak, operation](
              Status status, const RpcFrame& frame) {
            if (auto impl = weak.lock()) {
              impl->HandleWriteResponse(
                  operation, std::move(status), frame);
            }
          },
          options.request_timeout_ms);
      return;
    }

    proto::KvDeleteRequest request;
    request.set_client_id(client_id);
    request.set_seq_num(operation->seq_num);
    request.set_key(operation->key);
    client->Call(
        kKvDeleteRpcMethod, request,
        [weak, operation](
            Status status, const RpcFrame& frame) {
          if (auto impl = weak.lock()) {
            impl->HandleWriteResponse(
                operation, std::move(status), frame);
          }
        },
        options.request_timeout_ms);
  }

  void HandleWriteResponse(
      const std::shared_ptr<WriteOperation>& operation,
      Status status, const RpcFrame& frame) {
    if (operation->completed) {
      return;
    }
    if (!status.ok()) {
      RetryWriteInLoop(operation, std::move(status), std::string());
      return;
    }

    Status business_status;
    std::string leader_hint;
    if (operation->type == proto::KV_COMMAND_TYPE_PUT) {
      proto::KvPutResponse response;
      status = DecodeRpcResponse(
          kKvPutRpcMethod, frame, &response);
      if (!status.ok()) {
        RetryWriteInLoop(operation, std::move(status), std::string());
        return;
      }
      leader_hint = response.leader_hint();
      business_status = BusinessStatus(
          response.status(), response.message(), leader_hint);
    } else {
      proto::KvDeleteResponse response;
      status = DecodeRpcResponse(
          kKvDeleteRpcMethod, frame, &response);
      if (!status.ok()) {
        RetryWriteInLoop(operation, std::move(status), std::string());
        return;
      }
      leader_hint = response.leader_hint();
      business_status = BusinessStatus(
          response.status(), response.message(), leader_hint);
    }

    if (!business_status.ok()) {
      RetryWriteInLoop(
          operation, std::move(business_status), leader_hint);
      return;
    }
    CompleteWriteInLoop(operation, Status::OK(), false);
  }

  void RetryWriteInLoop(
      const std::shared_ptr<WriteOperation>& operation,
      Status status, const std::string& leader_hint) {
    if (operation->completed) {
      return;
    }
    if (!IsStarted()) {
      CompleteWriteInLoop(
          operation, Status::Timeout("kv client is stopped"), false);
      return;
    }
    if (!IsRetryable(status) ||
        operation->attempts >= MaxAttempts()) {
      const bool outcome_unknown =
          status.code() == Code::kTimeout;
      CompleteWriteInLoop(
          operation, std::move(status), outcome_unknown);
      return;
    }
    const KVClientEndpoint endpoint =
        RetryEndpointInLoop(leader_hint);
    // 退避期间先发起连接，下一次尝试时通常已可发送请求。
    ClientForEndpointInLoop(endpoint);
    const std::weak_ptr<Impl> weak = weak_from_this();
    loop->RunAfter(
        RetryDelayMs(),
        [weak, operation, endpoint] {
          if (auto impl = weak.lock()) {
            impl->ResumeWriteRetryInLoop(operation, endpoint);
          }
        });
  }

  void ResumeWriteRetryInLoop(
      const std::shared_ptr<WriteOperation>& operation,
      const KVClientEndpoint& endpoint) {
    if (operation->completed) {
      return;
    }
    if (!IsStarted()) {
      CompleteWriteInLoop(
          operation, Status::Timeout("kv client is stopped"), false);
      return;
    }
    if (active_write != operation) {
      return;
    }
    StartWriteAttemptInLoop(operation, endpoint);
  }

  void CompleteWriteInLoop(
      std::shared_ptr<WriteOperation> operation, Status status,
      bool outcome_unknown) {
    if (operation == nullptr || operation->completed) {
      return;
    }
    operation->completed = true;
    if (active_write == operation) {
      active_write.reset();
    }
    if (outcome_unknown) {
      std::lock_guard<std::mutex> lock(lifecycle_mutex);
      write_outcome_unknown = true;
    }

    KVClientWriteCallback callback = std::move(operation->callback);
    operation->callback = nullptr;
    if (callback != nullptr) {
      callback(std::move(status));
    }

    if (active_write == nullptr) {
      StartNextWriteInLoop();
    }
  }

  void FailQueuedWrites() {
    std::deque<std::shared_ptr<WriteOperation>> pending;
    pending.swap(pending_writes);
    for (auto& operation : pending) {
      operation->completed = true;
      KVClientWriteCallback callback =
          std::move(operation->callback);
      operation->callback = nullptr;
      if (callback != nullptr) {
        callback(Status::Timeout(
            "previous write outcome is unknown; create a new client"));
      }
    }
  }

  void FailPendingWrites() {
    if (active_write != nullptr) {
      CompleteWriteInLoop(
          std::move(active_write),
          Status::Timeout("kv client is stopped"), false);
    }
    std::deque<std::shared_ptr<WriteOperation>> pending;
    pending.swap(pending_writes);
    for (auto& operation : pending) {
      operation->completed = true;
      KVClientWriteCallback callback =
          std::move(operation->callback);
      operation->callback = nullptr;
      if (callback != nullptr) {
        callback(Status::Timeout("kv client is stopped"));
      }
    }
  }

  void StartGetAttemptInLoop(
      uint64_t token,
      const std::shared_ptr<GetOperation>& operation) {
    if (!IsStarted()) {
      FinishGetInLoop(
          token, Status::Timeout("kv client is stopped"),
          std::string());
      return;
    }
    ++operation->attempts;

    proto::KvGetRequest request;
    request.set_key(operation->key);
    request.set_linearizable(operation->linearizable);
    const std::weak_ptr<Impl> weak = weak_from_this();
    ClientForEndpointInLoop(NextInitialEndpointInLoop())->Call(
        kKvGetRpcMethod, request,
        [weak, token, operation](
            Status status, const RpcFrame& frame) {
          if (auto impl = weak.lock()) {
            impl->HandleGetResponse(
                token, operation, std::move(status), frame);
          }
        },
        options.request_timeout_ms);
  }

  void HandleGetResponse(
      uint64_t token,
      const std::shared_ptr<GetOperation>& operation,
      Status status, const RpcFrame& frame) {
    const auto active = active_gets.find(token);
    if (active == active_gets.end() ||
        active->second != operation ||
        operation->completed) {
      return;
    }
    if (!status.ok()) {
      RetryGetInLoop(token, operation, std::move(status), std::string());
      return;
    }

    proto::KvGetResponse response;
    status = DecodeRpcResponse(kKvGetRpcMethod, frame, &response);
    if (!status.ok()) {
      RetryGetInLoop(token, operation, std::move(status), std::string());
      return;
    }
    const std::string leader_hint = response.leader_hint();
    const Status business_status = BusinessStatus(
        response.status(), response.message(), leader_hint);
    if (business_status.ok()) {
      FinishGetInLoop(token, Status::OK(), response.value());
      return;
    }
    RetryGetInLoop(
        token, operation, business_status, leader_hint);
  }

  void RetryGetInLoop(
      uint64_t token,
      const std::shared_ptr<GetOperation>& operation,
      Status status, const std::string& leader_hint) {
    const auto active = active_gets.find(token);
    if (active == active_gets.end() ||
        active->second != operation) {
      return;
    }
    if (!IsStarted()) {
      FinishGetInLoop(
          token, Status::Timeout("kv client is stopped"),
          std::string());
      return;
    }
    if (!IsRetryable(status) ||
        operation->attempts >= MaxAttempts()) {
      FinishGetInLoop(token, std::move(status), std::string());
      return;
    }

    const KVClientEndpoint endpoint =
        RetryEndpointInLoop(leader_hint);
    ClientForEndpointInLoop(endpoint);
    const std::weak_ptr<Impl> weak = weak_from_this();
    loop->RunAfter(
        RetryDelayMs(),
        [weak, token, operation, endpoint] {
          if (auto impl = weak.lock()) {
            impl->ResumeGetRetryInLoop(token, operation, endpoint);
          }
        });
  }

  void ResumeGetRetryInLoop(
      uint64_t token,
      const std::shared_ptr<GetOperation>& operation,
      const KVClientEndpoint& endpoint) {
    const auto active = active_gets.find(token);
    if (active == active_gets.end() ||
        active->second != operation) {
      return;
    }
    if (!IsStarted()) {
      FinishGetInLoop(
          token, Status::Timeout("kv client is stopped"),
          std::string());
      return;
    }
    StartGetAttemptAtEndpointInLoop(token, operation, endpoint);
  }

  void StartGetAttemptAtEndpointInLoop(
      uint64_t token,
      const std::shared_ptr<GetOperation>& operation,
      const KVClientEndpoint& endpoint) {
    ++operation->attempts;
    proto::KvGetRequest request;
    request.set_key(operation->key);
    request.set_linearizable(operation->linearizable);
    const std::weak_ptr<Impl> weak = weak_from_this();
    ClientForEndpointInLoop(endpoint)->Call(
        kKvGetRpcMethod, request,
        [weak, token, operation](
            Status response_status, const RpcFrame& frame) {
          if (auto impl = weak.lock()) {
            impl->HandleGetResponse(
                token, operation, std::move(response_status), frame);
          }
        },
        options.request_timeout_ms);
  }

  void FinishGetInLoop(
      uint64_t token, Status status, std::string value) {
    const auto active = active_gets.find(token);
    if (active == active_gets.end()) {
      return;
    }
    std::shared_ptr<GetOperation> operation =
        std::move(active->second);
    active_gets.erase(active);
    operation->completed = true;
    InvokeGetCallback(
        std::move(operation), std::move(status), std::move(value));
  }

  void InvokeGetCallback(
      std::shared_ptr<GetOperation> operation, Status status,
      std::string value) {
    operation->completed = true;
    KVClientGetCallback callback = std::move(operation->callback);
    operation->callback = nullptr;
    if (callback != nullptr) {
      callback(std::move(status), std::move(value));
    }
  }

  void StartScanAttemptInLoop(
      uint64_t token,
      const std::shared_ptr<ScanOperation>& operation) {
    if (!IsStarted()) {
      FinishScanInLoop(
          token, Status::Timeout("kv client is stopped"),
          KVClientScanResult{});
      return;
    }
    ++operation->attempts;

    proto::KvScanRequest request;
    request.set_start_key(operation->start_key);
    request.set_end_key(operation->end_key);
    request.set_limit(operation->limit);
    const std::weak_ptr<Impl> weak = weak_from_this();
    ClientForEndpointInLoop(NextInitialEndpointInLoop())->Call(
        kKvScanRpcMethod, request,
        [weak, token, operation](
            Status status, const RpcFrame& frame) {
          if (auto impl = weak.lock()) {
            impl->HandleScanResponse(
                token, operation, std::move(status), frame);
          }
        },
        options.request_timeout_ms);
  }

  void HandleScanResponse(
      uint64_t token,
      const std::shared_ptr<ScanOperation>& operation,
      Status status, const RpcFrame& frame) {
    const auto active = active_scans.find(token);
    if (active == active_scans.end() ||
        active->second != operation ||
        operation->completed) {
      return;
    }
    if (!status.ok()) {
      RetryScanInLoop(
          token, operation, std::move(status), std::string());
      return;
    }

    proto::KvScanResponse response;
    status = DecodeRpcResponse(kKvScanRpcMethod, frame, &response);
    if (!status.ok()) {
      RetryScanInLoop(
          token, operation, std::move(status), std::string());
      return;
    }
    const std::string leader_hint = response.leader_hint();
    const Status business_status = BusinessStatus(
        response.status(), response.message(), leader_hint);
    if (!business_status.ok()) {
      RetryScanInLoop(
          token, operation, business_status, leader_hint);
      return;
    }

    KVClientScanResult result;
    result.has_more = response.has_more();
    result.entries.reserve(
        static_cast<size_t>(response.entries_size()));
    for (const proto::KvEntry& entry : response.entries()) {
      result.entries.emplace_back(entry.key(), entry.value());
    }
    FinishScanInLoop(token, Status::OK(), std::move(result));
  }

  void RetryScanInLoop(
      uint64_t token,
      const std::shared_ptr<ScanOperation>& operation,
      Status status, const std::string& leader_hint) {
    const auto active = active_scans.find(token);
    if (active == active_scans.end() ||
        active->second != operation) {
      return;
    }
    if (!IsStarted()) {
      FinishScanInLoop(
          token, Status::Timeout("kv client is stopped"),
          KVClientScanResult{});
      return;
    }
    if (!IsRetryable(status) ||
        operation->attempts >= MaxAttempts()) {
      FinishScanInLoop(token, std::move(status), KVClientScanResult{});
      return;
    }

    const KVClientEndpoint endpoint =
        RetryEndpointInLoop(leader_hint);
    ClientForEndpointInLoop(endpoint);
    const std::weak_ptr<Impl> weak = weak_from_this();
    loop->RunAfter(
        RetryDelayMs(),
        [weak, token, operation, endpoint] {
          if (auto impl = weak.lock()) {
            impl->ResumeScanRetryInLoop(token, operation, endpoint);
          }
        });
  }

  void ResumeScanRetryInLoop(
      uint64_t token,
      const std::shared_ptr<ScanOperation>& operation,
      const KVClientEndpoint& endpoint) {
    const auto active = active_scans.find(token);
    if (active == active_scans.end() ||
        active->second != operation) {
      return;
    }
    if (!IsStarted()) {
      FinishScanInLoop(
          token, Status::Timeout("kv client is stopped"),
          KVClientScanResult{});
      return;
    }
    StartScanAttemptAtEndpointInLoop(token, operation, endpoint);
  }

  void StartScanAttemptAtEndpointInLoop(
      uint64_t token,
      const std::shared_ptr<ScanOperation>& operation,
      const KVClientEndpoint& endpoint) {
    ++operation->attempts;
    proto::KvScanRequest request;
    request.set_start_key(operation->start_key);
    request.set_end_key(operation->end_key);
    request.set_limit(operation->limit);
    const std::weak_ptr<Impl> weak = weak_from_this();
    ClientForEndpointInLoop(endpoint)->Call(
        kKvScanRpcMethod, request,
        [weak, token, operation](
            Status response_status, const RpcFrame& frame) {
          if (auto impl = weak.lock()) {
            impl->HandleScanResponse(
                token, operation, std::move(response_status), frame);
          }
        },
        options.request_timeout_ms);
  }

  void FinishScanInLoop(
      uint64_t token, Status status, KVClientScanResult result) {
    const auto active = active_scans.find(token);
    if (active == active_scans.end()) {
      return;
    }
    std::shared_ptr<ScanOperation> operation =
        std::move(active->second);
    active_scans.erase(active);
    operation->completed = true;
    InvokeScanCallback(
        std::move(operation), std::move(status), std::move(result));
  }

  void InvokeScanCallback(
      std::shared_ptr<ScanOperation> operation, Status status,
      KVClientScanResult result) {
    operation->completed = true;
    KVClientScanCallback callback = std::move(operation->callback);
    operation->callback = nullptr;
    if (callback != nullptr) {
      callback(std::move(status), std::move(result));
    }
  }

  void FailPendingReads() {
    std::unordered_map<uint64_t, std::shared_ptr<GetOperation>>
        gets;
    gets.swap(active_gets);
    for (auto& item : gets) {
      InvokeGetCallback(
          std::move(item.second),
          Status::Timeout("kv client is stopped"), std::string());
    }

    std::unordered_map<uint64_t, std::shared_ptr<ScanOperation>>
        scans;
    scans.swap(active_scans);
    for (auto& item : scans) {
      InvokeScanCallback(
          std::move(item.second),
          Status::Timeout("kv client is stopped"),
          KVClientScanResult{});
    }
  }

 public:
  EventLoop* const loop;
  const KVClientOptions options;
  const uint64_t client_id;

 private:
  mutable std::mutex lifecycle_mutex;
  Lifecycle lifecycle = Lifecycle::kCreated;
  bool write_outcome_unknown = false;

  // 以下成员只由 EventLoop 线程访问。
  std::unordered_map<std::string, std::unique_ptr<RpcClient>>
      endpoint_clients;
  size_t next_endpoint_index = 0;

  std::deque<std::shared_ptr<WriteOperation>> pending_writes;
  std::shared_ptr<WriteOperation> active_write;
  uint64_t next_seq_num = 1;

  std::unordered_map<uint64_t, std::shared_ptr<GetOperation>>
      active_gets;
  std::unordered_map<uint64_t, std::shared_ptr<ScanOperation>>
      active_scans;
  std::atomic<uint64_t> next_operation_token{1};
};

Status KVClient::Create(
    EventLoop* loop, KVClientOptions options,
    std::unique_ptr<KVClient>* client) {
  if (client == nullptr) {
    return Status::InvalidArgument(
        "kv client output is null");
  }
  client->reset();

  Status status = NormalizeOptions(loop, &options);
  if (!status.ok()) {
    return status;
  }

  auto impl = std::make_shared<Impl>(loop, std::move(options));
  *client = std::unique_ptr<KVClient>(
      new KVClient(std::move(impl)));
  return Status::OK();
}

KVClient::KVClient(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

KVClient::~KVClient() {
  impl_->Stop();
}

Status KVClient::Start() {
  return impl_->Start();
}

void KVClient::Stop() {
  impl_->Stop();
}

Status KVClient::Put(
    std::string key, std::string value,
    KVClientWriteCallback callback) {
  if (callback == nullptr) {
    return Status::InvalidArgument(
        "kv client put callback is null");
  }
  Status status = impl_->CheckWriteAvailable();
  if (!status.ok()) {
    return status;
  }

  auto operation =
      std::make_shared<Impl::WriteOperation>();
  operation->type = proto::KV_COMMAND_TYPE_PUT;
  operation->key = std::move(key);
  operation->value = std::move(value);
  operation->callback = std::move(callback);

  auto self = impl_;
  impl_->loop->QueueInLoop(
      [self, operation] {
        self->EnqueueWriteInLoop(operation);
      });
  return Status::OK();
}

Status KVClient::Delete(
    std::string key, KVClientWriteCallback callback) {
  if (callback == nullptr) {
    return Status::InvalidArgument(
        "kv client delete callback is null");
  }
  Status status = impl_->CheckWriteAvailable();
  if (!status.ok()) {
    return status;
  }

  auto operation =
      std::make_shared<Impl::WriteOperation>();
  operation->type = proto::KV_COMMAND_TYPE_DELETE;
  operation->key = std::move(key);
  operation->callback = std::move(callback);

  auto self = impl_;
  impl_->loop->QueueInLoop(
      [self, operation] {
        self->EnqueueWriteInLoop(operation);
      });
  return Status::OK();
}

Status KVClient::Get(
    std::string key, bool linearizable,
    KVClientGetCallback callback) {
  if (callback == nullptr) {
    return Status::InvalidArgument(
        "kv client get callback is null");
  }
  Status status = impl_->CheckReadAvailable();
  if (!status.ok()) {
    return status;
  }

  auto operation = std::make_shared<Impl::GetOperation>();
  operation->key = std::move(key);
  operation->linearizable = linearizable;
  operation->callback = std::move(callback);
  const uint64_t token = impl_->AllocateOperationToken();

  auto self = impl_;
  impl_->loop->QueueInLoop(
      [self, token, operation] {
        self->BeginGetInLoop(token, operation);
      });
  return Status::OK();
}

Status KVClient::Scan(
    std::string start_key, std::string end_key, uint32_t limit,
    KVClientScanCallback callback) {
  if (callback == nullptr) {
    return Status::InvalidArgument(
        "kv client scan callback is null");
  }
  if (limit == 0 || limit > kKvClientMaxScanLimit) {
    return Status::InvalidArgument(
        "kv client scan limit is invalid");
  }
  Status status = impl_->CheckReadAvailable();
  if (!status.ok()) {
    return status;
  }

  auto operation = std::make_shared<Impl::ScanOperation>();
  operation->start_key = std::move(start_key);
  operation->end_key = std::move(end_key);
  operation->limit = limit;
  operation->callback = std::move(callback);
  const uint64_t token = impl_->AllocateOperationToken();

  auto self = impl_;
  impl_->loop->QueueInLoop(
      [self, token, operation] {
        self->BeginScanInLoop(token, operation);
      });
  return Status::OK();
}

uint64_t KVClient::ClientId() const {
  return impl_->client_id;
}

}  // namespace raftkv
