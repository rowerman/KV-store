#include "raftkv/server/kv_server.h"

#include <arpa/inet.h>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "rpc_kv.pb.h"
#include "raftkv/net/event_loop.h"
#include "raftkv/raft/raft_log.h"
#include "raftkv/rpc/rpc_server.h"
#include "raftkv/server/kv_state_machine.h"

namespace raftkv {
namespace {

inline constexpr uint32_t kMaxScanLimit = 10000;

std::string JoinPath(const std::string& directory,
                     const char* child) {
  return (std::filesystem::path(directory) / child).string();
}

std::string FormatEndpoint(const RaftPeerAddress& peer) {
  return peer.host + ":" + std::to_string(peer.port);
}

Status ValidateIpv4(const std::string& host,
                    const char* description) {
  in_addr address {};
  if (host.empty() ||
      ::inet_pton(AF_INET, host.c_str(), &address) != 1) {
    return Status::InvalidArgument(
        std::string(description) + " is not an IPv4 address");
  }
  return Status::OK();
}

Status ValidateOptions(
    EventLoop* loop, const KVServerOptions& options) {
  if (loop == nullptr) {
    return Status::InvalidArgument("kv server loop is null");
  }
  if (options.local_node.node_id == 0) {
    return Status::InvalidArgument(
        "kv server local node id is zero");
  }
  Status status =
      ValidateIpv4(options.local_node.host, "local node host");
  if (!status.ok()) {
    return status;
  }
  status = ValidateIpv4(options.listen_ip, "listen ip");
  if (!status.ok()) {
    return status;
  }
  if (options.data_directory.empty()) {
    return Status::InvalidArgument(
        "kv server data directory is empty");
  }
  if (options.rpc_io_threads < 0) {
    return Status::InvalidArgument(
        "kv server rpc io thread count is negative");
  }
  if (options.raft_rpc_timeout_ms == 0) {
    return Status::InvalidArgument(
        "kv server raft rpc timeout is zero");
  }
  if (options.local_node.port == 0 && !options.peers.empty()) {
    return Status::InvalidArgument(
        "multi-node kv server requires a nonzero local port");
  }

  std::unordered_set<uint32_t> peer_ids;
  for (const RaftPeerAddress& peer : options.peers) {
    if (peer.node_id == 0 ||
        peer.node_id == options.local_node.node_id) {
      return Status::InvalidArgument(
          "kv server peer id is invalid");
    }
    if (!peer_ids.insert(peer.node_id).second) {
      return Status::InvalidArgument(
          "kv server peer id is duplicated");
    }
    if (peer.port == 0) {
      return Status::InvalidArgument(
          "kv server peer port is zero");
    }
    status = ValidateIpv4(peer.host, "peer host");
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

RaftNodeOptions BuildRaftOptions(
    const KVServerOptions& options) {
  RaftNodeOptions raft_options = options.raft_options;
  raft_options.node_id = options.local_node.node_id;
  raft_options.peers.clear();
  for (const RaftPeerAddress& peer : options.peers) {
    raft_options.peers.push_back(peer.node_id);
  }
  return raft_options;
}

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

template <typename Response>
void FillFailureResponse(
    Response* response, const Status& status,
    const std::string& leader_hint) {
  response->set_status(ToProtoStatusCode(status.code()));
  if (!status.ok()) {
    response->set_message(status.message());
  }
  if (status.code() == Code::kNotLeader) {
    response->set_leader_hint(leader_hint);
  }
}

}  // namespace

struct KVServer::Impl
    : public std::enable_shared_from_this<KVServer::Impl> {
  Impl(EventLoop* event_loop, RaftPeerAddress local,
       std::unordered_map<uint32_t, std::string> addresses,
       std::unique_ptr<LSMTree> storage,
       std::unique_ptr<RpcServer> rpc,
       std::unique_ptr<RaftNode> raft)
      : loop(event_loop),
        local_node(std::move(local)),
        leader_addresses(std::move(addresses)),
        lsm_tree(std::move(storage)),
        rpc_server(std::move(rpc)),
        raft_node(std::move(raft)) {}

  ~Impl() { Stop(); }

  Status RegisterClientRpcMethods() {
    const std::weak_ptr<Impl> weak = weak_from_this();
    Status status = rpc_server->RegisterMethod(
        kKvPutRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandlePut(frame, std::move(done));
          } else {
            done(Status::Timeout("kv server is stopped"), nullptr);
          }
        });
    if (!status.ok()) {
      return status;
    }
    status = rpc_server->RegisterMethod(
        kKvDeleteRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandleDelete(frame, std::move(done));
          } else {
            done(Status::Timeout("kv server is stopped"), nullptr);
          }
        });
    if (!status.ok()) {
      return status;
    }
    status = rpc_server->RegisterMethod(
        kKvGetRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandleGet(frame, std::move(done));
          } else {
            done(Status::Timeout("kv server is stopped"), nullptr);
          }
        });
    if (!status.ok()) {
      return status;
    }
    return rpc_server->RegisterMethod(
        kKvScanRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandleScan(frame, std::move(done));
          } else {
            done(Status::Timeout("kv server is stopped"), nullptr);
          }
        });
  }

  Status Start() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (started) {
      return Status::OK();
    }
    if (stopping || stopped) {
      return Status::InvalidArgument(
          "kv server cannot start after stop");
    }

    Status status = rpc_server->Start();
    if (!status.ok()) {
      return status;
    }
    if (local_node.port == 0) {
      local_node.port = rpc_server->Port();
      std::lock_guard<std::mutex> address_lock(address_mutex);
      leader_addresses[local_node.node_id] =
          FormatEndpoint(local_node);
    }
    status = raft_node->Start();
    if (!status.ok()) {
      rpc_server->Stop();
      return status;
    }

    started = true;
    return Status::OK();
  }

  Status Stop() {
    std::unique_lock<std::mutex> lock(lifecycle_mutex);
    if (stopped) {
      return stop_status;
    }
    if (stopping) {
      lifecycle_cv.wait(lock, [this] { return stopped; });
      return stop_status;
    }
    stopping = true;
    started = false;
    lock.unlock();

    // 不持生命周期锁执行 join，允许挂起回调查询 Raft 状态。
    const Status raft_status = raft_node->Stop();
    rpc_server->Stop();

    lock.lock();
    stop_status = raft_status;
    stopping = false;
    stopped = true;
    lifecycle_cv.notify_all();
    return stop_status;
  }

  uint16_t Port() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    const uint16_t port = rpc_server->Port();
    return port != 0 ? port : local_node.port;
  }

  RaftNodeStatus GetRaftStatus() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    return raft_node->GetStatus();
  }

  std::string LeaderHint(const Status& status) {
    if (status.code() != Code::kNotLeader) {
      return {};
    }
    const RaftNodeStatus raft_status = raft_node->GetStatus();
    if (raft_status.role == Role::kLeader ||
        raft_status.leader_id == 0) {
      return {};
    }
    std::lock_guard<std::mutex> lock(address_mutex);
    const auto iterator =
        leader_addresses.find(raft_status.leader_id);
    return iterator == leader_addresses.end()
               ? std::string()
               : iterator->second;
  }

  template <typename Response>
  void ReplyFailure(
      RpcReplyCallback done, const Status& status) {
    if (done == nullptr) {
      return;
    }
    Response response;
    FillFailureResponse(&response, status, LeaderHint(status));
    done(Status::OK(), &response);
  }

  template <typename Response>
  void ProposeWrite(
      proto::KvCommandType type, uint64_t client_id,
      uint64_t seq_num, std::string key, std::string value,
      RpcReplyCallback done) {
    proto::KvCommand command;
    command.set_type(type);
    command.set_client_id(client_id);
    command.set_seq_num(seq_num);
    command.set_key(std::move(key));
    command.set_value(std::move(value));
    std::string payload;
    if (!command.SerializeToString(&payload)) {
      ReplyFailure<Response>(
          std::move(done),
          Status::IOError("serialize kv command"));
      return;
    }

    const std::weak_ptr<Impl> weak = weak_from_this();
    ProposeCallback completion =
        [weak, done = std::move(done)](Status status) mutable {
          if (done == nullptr) {
            return;
          }
          auto impl = weak.lock();
          if (impl == nullptr) {
            done(Status::Timeout("kv server is stopped"), nullptr);
            return;
          }
          Response response;
          FillFailureResponse(
              &response, status, impl->LeaderHint(status));
          if (status.ok()) {
            response.set_status(proto::STATUS_CODE_OK);
            response.set_message("");
          }
          done(Status::OK(), &response);
        };

    const Status post_status = raft_node->PostEvent(
        ProposeEvent{std::move(payload), completion});
    if (!post_status.ok()) {
      completion(post_status);
    }
  }

  void HandlePut(const RpcFrame& frame, RpcReplyCallback done) {
    proto::KvPutRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::InvalidArgument("invalid put request"), nullptr);
      return;
    }
    if (request.client_id() == 0 || request.seq_num() == 0) {
      ReplyFailure<proto::KvPutResponse>(
          std::move(done),
          Status::InvalidArgument(
              "put client id or sequence is zero"));
      return;
    }
    ProposeWrite<proto::KvPutResponse>(
        proto::KV_COMMAND_TYPE_PUT, request.client_id(),
        request.seq_num(), request.key(), request.value(),
        std::move(done));
  }

  void HandleDelete(const RpcFrame& frame, RpcReplyCallback done) {
    proto::KvDeleteRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::InvalidArgument("invalid delete request"), nullptr);
      return;
    }
    if (request.client_id() == 0 || request.seq_num() == 0) {
      ReplyFailure<proto::KvDeleteResponse>(
          std::move(done),
          Status::InvalidArgument(
              "delete client id or sequence is zero"));
      return;
    }
    ProposeWrite<proto::KvDeleteResponse>(
        proto::KV_COMMAND_TYPE_DELETE, request.client_id(),
        request.seq_num(), request.key(), std::string(),
        std::move(done));
  }

  void HandleGet(const RpcFrame& frame, RpcReplyCallback done) {
    proto::KvGetRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::InvalidArgument("invalid get request"), nullptr);
      return;
    }
    if (!request.linearizable()) {
      ReadGet(request.key(), std::move(done));
      return;
    }

    const std::weak_ptr<Impl> weak = weak_from_this();
    ReadIndexCallback completion =
        [weak, key = request.key(),
         done = std::move(done)](Status status) mutable {
          auto impl = weak.lock();
          if (impl == nullptr) {
            if (done != nullptr) {
              done(Status::Timeout("kv server is stopped"), nullptr);
            }
            return;
          }
          if (!status.ok()) {
            impl->ReplyFailure<proto::KvGetResponse>(
                std::move(done), status);
            return;
          }
          impl->loop->RunInLoop(
              [weak, key = std::move(key),
               done = std::move(done)]() mutable {
                auto current = weak.lock();
                if (current == nullptr) {
                  if (done != nullptr) {
                    done(Status::Timeout("kv server is stopped"), nullptr);
                  }
                  return;
                }
                current->ReadGet(std::move(key), std::move(done));
              });
        };

    const Status post_status = raft_node->PostEvent(
        ReadIndexEvent{0, completion});
    if (!post_status.ok()) {
      completion(post_status);
    }
  }

  void ReadGet(std::string key, RpcReplyCallback done) {
    if (done == nullptr) {
      return;
    }
    proto::KvGetResponse response;
    std::string value;
    const Status status = lsm_tree->Get(key, &value);
    if (status.ok()) {
      response.set_status(proto::STATUS_CODE_OK);
      response.set_value(std::move(value));
    } else {
      FillFailureResponse(
          &response, status, LeaderHint(status));
    }
    done(Status::OK(), &response);
  }

  void HandleScan(const RpcFrame& frame, RpcReplyCallback done) {
    proto::KvScanRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::InvalidArgument("invalid scan request"), nullptr);
      return;
    }
    if (request.limit() == 0 || request.limit() > kMaxScanLimit) {
      ReplyFailure<proto::KvScanResponse>(
          std::move(done),
          Status::InvalidArgument("scan limit is invalid"));
      return;
    }

    const std::weak_ptr<Impl> weak = weak_from_this();
    ReadIndexCallback completion =
        [weak, start = request.start_key(),
         end = request.end_key(), limit = request.limit(),
         done = std::move(done)](Status status) mutable {
          auto impl = weak.lock();
          if (impl == nullptr) {
            if (done != nullptr) {
              done(Status::Timeout("kv server is stopped"), nullptr);
            }
            return;
          }
          if (!status.ok()) {
            impl->ReplyFailure<proto::KvScanResponse>(
                std::move(done), status);
            return;
          }
          impl->loop->RunInLoop(
              [weak, start = std::move(start),
               end = std::move(end), limit,
               done = std::move(done)]() mutable {
                auto current = weak.lock();
                if (current == nullptr) {
                  if (done != nullptr) {
                    done(Status::Timeout("kv server is stopped"), nullptr);
                  }
                  return;
                }
                current->ReadScan(
                    std::move(start), std::move(end), limit,
                    std::move(done));
              });
        };

    const Status post_status = raft_node->PostEvent(
        ReadIndexEvent{0, completion});
    if (!post_status.ok()) {
      completion(post_status);
    }
  }

  void ReadScan(
      std::string start, std::string end, uint32_t limit,
      RpcReplyCallback done) {
    if (done == nullptr) {
      return;
    }
    std::vector<std::pair<std::string, std::string>> rows;
    const Status status = lsm_tree->Scan(
        start, end, static_cast<size_t>(limit) + 1, &rows);
    if (!status.ok()) {
      proto::KvScanResponse response;
      FillFailureResponse(
          &response, status, LeaderHint(status));
      done(Status::OK(), &response);
      return;
    }

    proto::KvScanResponse response;
    response.set_status(proto::STATUS_CODE_OK);
    response.set_has_more(rows.size() > limit);
    if (rows.size() > limit) {
      rows.resize(limit);
    }
    for (const auto& row : rows) {
      proto::KvEntry* entry = response.add_entries();
      entry->set_key(row.first);
      entry->set_value(row.second);
    }
    done(Status::OK(), &response);
  }

  EventLoop* const loop;
  RaftPeerAddress local_node;
  mutable std::mutex address_mutex;
  std::unordered_map<uint32_t, std::string> leader_addresses;

  // 析构顺序必须保证 Raft/StateMachine 先于 LSMTree。
  std::unique_ptr<LSMTree> lsm_tree;
  std::unique_ptr<RpcServer> rpc_server;
  std::unique_ptr<RaftNode> raft_node;

  mutable std::mutex lifecycle_mutex;
  std::condition_variable lifecycle_cv;
  bool started = false;
  bool stopping = false;
  bool stopped = false;
  Status stop_status;
};

Status KVServer::Create(
    EventLoop* loop,
    KVServerOptions options,
    std::unique_ptr<KVServer>* server) {
  if (server == nullptr) {
    return Status::InvalidArgument(
        "kv server output is null");
  }
  server->reset();
  Status status = ValidateOptions(loop, options);
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<LSMTree> lsm_tree;
  status = LSMTree::Open(
      JoinPath(options.data_directory, "kv_lsm"),
      options.storage_options, &lsm_tree);
  if (!status.ok()) {
    return status;
  }
  std::unique_ptr<StateMachine> state_machine =
      std::make_unique<KVStateMachine>(lsm_tree.get());

  std::unique_ptr<RaftLog> raft_log;
  status = RaftLog::Open(
      JoinPath(options.data_directory, "raft"), &raft_log);
  if (!status.ok()) {
    return status;
  }

  auto rpc_server = std::make_unique<RpcServer>(
      loop,
      "kv-server-" + std::to_string(options.local_node.node_id),
      options.local_node.port, options.rpc_io_threads,
      options.listen_ip);

  RealRaftTransportOptions transport_options;
  transport_options.local_node_id = options.local_node.node_id;
  transport_options.peers = options.peers;
  transport_options.rpc_timeout_ms = options.raft_rpc_timeout_ms;
  transport_options.tcp_client_options =
      options.tcp_client_options;
  std::unique_ptr<RealRaftTransport> transport;
  status = RealRaftTransport::Create(
      loop, std::move(transport_options), &transport);
  if (!status.ok()) {
    return status;
  }
  status = transport->RegisterRpcMethods(rpc_server.get());
  if (!status.ok()) {
    return status;
  }
  // RealTransport 随后移交给 RaftNode；先投递全部连接任务。
  transport->Start();

  std::unique_ptr<RaftNode> raft_node;
  status = RaftNode::Create(
      BuildRaftOptions(options),
      std::move(transport),
      std::move(raft_log),
      std::move(state_machine),
      &raft_node);
  if (!status.ok()) {
    return status;
  }

  std::unordered_map<uint32_t, std::string> addresses;
  addresses.emplace(
      options.local_node.node_id,
      FormatEndpoint(options.local_node));
  for (const RaftPeerAddress& peer : options.peers) {
    addresses.emplace(peer.node_id, FormatEndpoint(peer));
  }

  auto impl = std::make_shared<Impl>(
      loop, options.local_node, std::move(addresses),
      std::move(lsm_tree), std::move(rpc_server),
      std::move(raft_node));
  status = impl->RegisterClientRpcMethods();
  if (!status.ok()) {
    return status;
  }
  *server = std::unique_ptr<KVServer>(
      new KVServer(std::move(impl)));
  return Status::OK();
}

KVServer::KVServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

KVServer::~KVServer() = default;

Status KVServer::Start() {
  return impl_->Start();
}

Status KVServer::Stop() {
  return impl_->Stop();
}

uint16_t KVServer::Port() const {
  return impl_->Port();
}

RaftNodeStatus KVServer::GetRaftStatus() const {
  return impl_->GetRaftStatus();
}

}  // namespace raftkv
