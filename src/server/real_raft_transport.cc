#include "raftkv/server/real_raft_transport.h"

#include <arpa/inet.h>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "raftkv/net/event_loop.h"
#include "raftkv/rpc/rpc_client.h"
#include "raftkv/rpc/rpc_server.h"

namespace raftkv {
namespace {

Status ValidateOptions(
    EventLoop* loop, const RealRaftTransportOptions& options) {
  if (loop == nullptr) {
    return Status::InvalidArgument(
        "real raft transport loop is null");
  }
  if (options.local_node_id == 0) {
    return Status::InvalidArgument(
        "real raft local node id is zero");
  }
  if (options.rpc_timeout_ms == 0) {
    return Status::InvalidArgument(
        "real raft rpc timeout is zero");
  }

  std::unordered_set<uint32_t> peer_ids;
  for (const RaftPeerAddress& peer : options.peers) {
    if (peer.node_id == 0 ||
        peer.node_id == options.local_node_id) {
      return Status::InvalidArgument(
          "real raft peer id is invalid");
    }
    if (!peer_ids.insert(peer.node_id).second) {
      return Status::InvalidArgument(
          "real raft peer id is duplicated");
    }
    if (peer.port == 0) {
      return Status::InvalidArgument(
          "real raft peer port is zero");
    }
    in_addr address {};
    if (peer.host.empty() ||
        ::inet_pton(AF_INET, peer.host.c_str(), &address) != 1) {
      return Status::InvalidArgument(
          "real raft peer host is not an IPv4 address");
    }
  }
  return Status::OK();
}

template <typename Response>
Status DecodeResponse(
    uint32_t expected_method, const RpcFrame& frame,
    Response* response) {
  if (frame.msg_type !=
      static_cast<uint8_t>(RpcMessageType::kResponse)) {
    return Status::Corruption(
        "unexpected raft rpc response type");
  }
  if (frame.method_id != expected_method) {
    return Status::Corruption(
        "raft rpc response method mismatch");
  }
  if (!response->ParseFromString(frame.payload)) {
    return Status::Corruption(
        "raft rpc response protobuf is invalid");
  }
  return Status::OK();
}

template <typename Response, typename Callback>
void CompleteWithError(Callback callback, Status status) {
  if (callback != nullptr) {
    callback(std::move(status), Response{});
  }
}

}  // namespace

struct RealRaftTransport::Impl
    : public std::enable_shared_from_this<RealRaftTransport::Impl> {
  Impl(EventLoop* event_loop, RealRaftTransportOptions options)
      : loop(event_loop),
        local_node_id(options.local_node_id),
        rpc_timeout_ms(options.rpc_timeout_ms) {
    const TcpClientOptions tcp_options =
        options.tcp_client_options;
    for (const RaftPeerAddress& peer : options.peers) {
      const std::string name =
          "raft-" + std::to_string(local_node_id) +
          "-peer-" + std::to_string(peer.node_id);
      clients.emplace(
          peer.node_id,
          std::make_unique<RpcClient>(
              loop, name, peer.host, peer.port, tcp_options));
    }
  }

  ~Impl() { Stop(); }

  void Start() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    for (auto& entry : clients) {
      entry.second->Connect();
    }
    running = true;
  }

  void Stop() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex);
    if (!running) {
      return;
    }
    for (auto& entry : clients) {
      entry.second->Disconnect();
    }
    running = false;
  }

  Status RegisterRpcMethods(RpcServer* server) {
    if (server == nullptr) {
      return Status::InvalidArgument(
          "real raft rpc server is null");
    }

    std::lock_guard<std::mutex> registration_lock(
        registration_mutex);
    if (methods_registered) {
      return Status::InvalidArgument(
          "real raft rpc methods are already registered");
    }
    {
      std::lock_guard<std::mutex> lifecycle_lock(
          lifecycle_mutex);
      if (running) {
        return Status::InvalidArgument(
            "real raft rpc methods must be registered before start");
      }
    }

    const std::weak_ptr<Impl> weak = weak_from_this();
    Status status = server->RegisterMethod(
        kRaftRequestVoteRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandleRequestVoteRpc(frame, std::move(done));
          } else {
            done(Status::Timeout(
                     "real raft transport is stopped"),
                 nullptr);
          }
        });
    if (!status.ok()) {
      return status;
    }

    status = server->RegisterMethod(
        kRaftAppendEntriesRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandleAppendEntriesRpc(frame, std::move(done));
          } else {
            done(Status::Timeout(
                     "real raft transport is stopped"),
                 nullptr);
          }
        });
    if (!status.ok()) {
      return status;
    }

    status = server->RegisterMethod(
        kRaftInstallSnapshotRpcMethod,
        [weak](const RpcFrame& frame, RpcReplyCallback done) {
          if (auto impl = weak.lock()) {
            impl->HandleInstallSnapshotRpc(frame, std::move(done));
          } else {
            done(Status::Timeout(
                     "real raft transport is stopped"),
                 nullptr);
          }
        });
    if (!status.ok()) {
      return status;
    }

    methods_registered = true;
    return Status::OK();
  }

  void SetRequestVoteHandler(RequestVoteHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex);
    request_vote_handler = std::move(handler);
  }

  void SetAppendEntriesHandler(AppendEntriesHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex);
    append_entries_handler = std::move(handler);
  }

  void SetInstallSnapshotHandler(
      InstallSnapshotHandler handler) {
    std::lock_guard<std::mutex> lock(handlers_mutex);
    install_snapshot_handler = std::move(handler);
  }

  void HandleRequestVoteRpc(
      const RpcFrame& frame, RpcReplyCallback done) {
    proto::RequestVoteRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::Corruption(
               "request vote request protobuf is invalid"),
           nullptr);
      return;
    }
    if (request.candidate_id() == 0 ||
        request.candidate_id() == local_node_id) {
      done(Status::InvalidArgument(
               "request vote candidate id is invalid"),
           nullptr);
      return;
    }

    RequestVoteHandler handler;
    {
      std::lock_guard<std::mutex> lock(handlers_mutex);
      handler = request_vote_handler;
    }
    if (handler == nullptr) {
      done(Status::InvalidArgument(
               "request vote handler is not installed"),
           nullptr);
      return;
    }

    handler(
        request.candidate_id(), request,
        [done = std::move(done)](
            Status status,
            proto::RequestVoteResponse response) mutable {
          if (!status.ok()) {
            done(std::move(status), nullptr);
            return;
          }
          done(Status::OK(), &response);
        });
  }

  void HandleAppendEntriesRpc(
      const RpcFrame& frame, RpcReplyCallback done) {
    proto::AppendEntriesRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::Corruption(
               "append entries request protobuf is invalid"),
           nullptr);
      return;
    }
    if (request.leader_id() == 0 ||
        request.leader_id() == local_node_id) {
      done(Status::InvalidArgument(
               "append entries leader id is invalid"),
           nullptr);
      return;
    }

    AppendEntriesHandler handler;
    {
      std::lock_guard<std::mutex> lock(handlers_mutex);
      handler = append_entries_handler;
    }
    if (handler == nullptr) {
      done(Status::InvalidArgument(
               "append entries handler is not installed"),
           nullptr);
      return;
    }

    handler(
        request.leader_id(), request,
        [done = std::move(done)](
            Status status,
            proto::AppendEntriesResponse response) mutable {
          if (!status.ok()) {
            done(std::move(status), nullptr);
            return;
          }
          done(Status::OK(), &response);
        });
  }

  void HandleInstallSnapshotRpc(
      const RpcFrame& frame, RpcReplyCallback done) {
    proto::InstallSnapshotRequest request;
    if (!request.ParseFromString(frame.payload)) {
      done(Status::Corruption(
               "install snapshot request protobuf is invalid"),
           nullptr);
      return;
    }
    if (request.leader_id() == 0 ||
        request.leader_id() == local_node_id) {
      done(Status::InvalidArgument(
               "install snapshot leader id is invalid"),
           nullptr);
      return;
    }

    InstallSnapshotHandler handler;
    {
      std::lock_guard<std::mutex> lock(handlers_mutex);
      handler = install_snapshot_handler;
    }
    if (handler == nullptr) {
      done(Status::InvalidArgument(
               "install snapshot handler is not installed"),
           nullptr);
      return;
    }

    handler(
        request.leader_id(), request,
        [done = std::move(done)](
            Status status,
            proto::InstallSnapshotResponse response) mutable {
          if (!status.ok()) {
            done(std::move(status), nullptr);
            return;
          }
          done(Status::OK(), &response);
        });
  }

  RpcClient* FindClient(uint32_t peer_id) {
    const auto iterator = clients.find(peer_id);
    return iterator == clients.end()
               ? nullptr
               : iterator->second.get();
  }

  EventLoop* const loop;
  const uint32_t local_node_id;
  const uint64_t rpc_timeout_ms;
  std::unordered_map<
      uint32_t, std::unique_ptr<RpcClient>> clients;

  std::mutex lifecycle_mutex;
  bool running = false;

  std::mutex registration_mutex;
  bool methods_registered = false;

  std::mutex handlers_mutex;
  RequestVoteHandler request_vote_handler;
  AppendEntriesHandler append_entries_handler;
  InstallSnapshotHandler install_snapshot_handler;
};

Status RealRaftTransport::Create(
    EventLoop* loop,
    RealRaftTransportOptions options,
    std::unique_ptr<RealRaftTransport>* transport) {
  if (transport == nullptr) {
    return Status::InvalidArgument(
        "real raft transport output is null");
  }
  transport->reset();
  Status status = ValidateOptions(loop, options);
  if (!status.ok()) {
    return status;
  }

  auto impl = std::make_shared<Impl>(
      loop, std::move(options));
  *transport = std::unique_ptr<RealRaftTransport>(
      new RealRaftTransport(std::move(impl)));
  return Status::OK();
}

RealRaftTransport::RealRaftTransport(
    std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

RealRaftTransport::~RealRaftTransport() = default;

Status RealRaftTransport::RegisterRpcMethods(RpcServer* server) {
  return impl_->RegisterRpcMethods(server);
}

void RealRaftTransport::Start() {
  impl_->Start();
}

void RealRaftTransport::Stop() {
  impl_->Stop();
}

void RealRaftTransport::SendRequestVote(
    uint32_t peer_id,
    const proto::RequestVoteRequest& request,
    RequestVoteResponseCallback callback) {
  if (request.candidate_id() != impl_->local_node_id) {
    CompleteWithError<proto::RequestVoteResponse>(
        std::move(callback),
        Status::InvalidArgument(
            "request vote sender id does not match local node"));
    return;
  }
  RpcClient* client = impl_->FindClient(peer_id);
  if (client == nullptr) {
    CompleteWithError<proto::RequestVoteResponse>(
        std::move(callback),
        Status::NotFound("raft peer is not configured"));
    return;
  }

  client->Call(
      kRaftRequestVoteRpcMethod, request,
      [callback = std::move(callback)](
          Status status, const RpcFrame& frame) mutable {
        if (callback == nullptr) {
          return;
        }
        if (!status.ok()) {
          callback(std::move(status),
                   proto::RequestVoteResponse{});
          return;
        }
        proto::RequestVoteResponse response;
        status = DecodeResponse(
            kRaftRequestVoteRpcMethod, frame, &response);
        callback(std::move(status), std::move(response));
      },
      impl_->rpc_timeout_ms);
}

void RealRaftTransport::SendAppendEntries(
    uint32_t peer_id,
    const proto::AppendEntriesRequest& request,
    AppendEntriesResponseCallback callback) {
  if (request.leader_id() != impl_->local_node_id) {
    CompleteWithError<proto::AppendEntriesResponse>(
        std::move(callback),
        Status::InvalidArgument(
            "append entries sender id does not match local node"));
    return;
  }
  RpcClient* client = impl_->FindClient(peer_id);
  if (client == nullptr) {
    CompleteWithError<proto::AppendEntriesResponse>(
        std::move(callback),
        Status::NotFound("raft peer is not configured"));
    return;
  }

  client->Call(
      kRaftAppendEntriesRpcMethod, request,
      [callback = std::move(callback)](
          Status status, const RpcFrame& frame) mutable {
        if (callback == nullptr) {
          return;
        }
        if (!status.ok()) {
          callback(std::move(status),
                   proto::AppendEntriesResponse{});
          return;
        }
        proto::AppendEntriesResponse response;
        status = DecodeResponse(
            kRaftAppendEntriesRpcMethod, frame, &response);
        callback(std::move(status), std::move(response));
      },
      impl_->rpc_timeout_ms);
}

void RealRaftTransport::SendInstallSnapshot(
    uint32_t peer_id,
    const proto::InstallSnapshotRequest& request,
    InstallSnapshotResponseCallback callback) {
  if (request.leader_id() != impl_->local_node_id) {
    CompleteWithError<proto::InstallSnapshotResponse>(
        std::move(callback),
        Status::InvalidArgument(
            "install snapshot sender id does not match local node"));
    return;
  }
  RpcClient* client = impl_->FindClient(peer_id);
  if (client == nullptr) {
    CompleteWithError<proto::InstallSnapshotResponse>(
        std::move(callback),
        Status::NotFound("raft peer is not configured"));
    return;
  }

  client->Call(
      kRaftInstallSnapshotRpcMethod, request,
      [callback = std::move(callback)](
          Status status, const RpcFrame& frame) mutable {
        if (callback == nullptr) {
          return;
        }
        if (!status.ok()) {
          callback(std::move(status),
                   proto::InstallSnapshotResponse{});
          return;
        }
        proto::InstallSnapshotResponse response;
        status = DecodeResponse(
            kRaftInstallSnapshotRpcMethod, frame, &response);
        callback(std::move(status), std::move(response));
      },
      impl_->rpc_timeout_ms);
}

void RealRaftTransport::SetRequestVoteHandler(
    RequestVoteHandler handler) {
  impl_->SetRequestVoteHandler(std::move(handler));
}

void RealRaftTransport::SetAppendEntriesHandler(
    AppendEntriesHandler handler) {
  impl_->SetAppendEntriesHandler(std::move(handler));
}

void RealRaftTransport::SetInstallSnapshotHandler(
    InstallSnapshotHandler handler) {
  impl_->SetInstallSnapshotHandler(std::move(handler));
}

}  // namespace raftkv
