#ifndef RAFTKV_TESTS_CHAOS_SIM_TRANSPORT_H_
#define RAFTKV_TESTS_CHAOS_SIM_TRANSPORT_H_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "raftkv/raft/transport.h"

namespace raftkv {
namespace testing {

class SimTransport;

// 无后台线程的确定性内存网络。
class SimNetwork {
 public:
  explicit SimNetwork(uint64_t random_seed = 20261127);

  void SetDelay(std::chrono::milliseconds delay);
  void SetLatency(std::chrono::milliseconds min_delay,
                  std::chrono::milliseconds max_delay);
  void SetDropRate(double rate);
  void DropNode(uint32_t node_id);
  void RestoreNode(uint32_t node_id);
  void Partition(std::vector<uint32_t> group_a,
                 std::vector<uint32_t> group_b);
  void SetLinkBlocked(uint32_t from_node,
                      uint32_t to_node, bool blocked);
  void Heal();
  void AdvanceBy(std::chrono::milliseconds duration);

  std::chrono::milliseconds Now() const;
  size_t PendingMessageCount() const;

 private:
  friend class SimTransport;

  struct Impl;
  struct NodeState {
    RequestVoteHandler request_vote;
    AppendEntriesHandler append_entries;
    InstallSnapshotHandler install_snapshot;
  };

  void RegisterNode(uint32_t node_id);
  void UnregisterNode(uint32_t node_id);

  std::shared_ptr<Impl> impl_;
};

struct SimNetwork::Impl
    : public std::enable_shared_from_this<SimNetwork::Impl> {
  using TimePoint = std::chrono::milliseconds;
  using EventKey = std::pair<TimePoint, uint64_t>;

  explicit Impl(uint64_t seed) : random(seed) {}

  static uint64_t LinkKey(uint32_t from_node, uint32_t to_node) {
    return (static_cast<uint64_t>(from_node) << 32U) |
           static_cast<uint64_t>(to_node);
  }

  TimePoint ChooseDelayLocked() {
    if (min_latency == max_latency) {
      return min_latency;
    }
    std::uniform_int_distribution<int64_t> distribution(
        min_latency.count(), max_latency.count());
    return TimePoint(distribution(random));
  }

  bool ShouldDropLocked() {
    if (drop_rate == 0.0) {
      return false;
    }
    if (drop_rate == 1.0) {
      return true;
    }
    return std::uniform_real_distribution<double>(0.0, 1.0)(
               random) < drop_rate;
  }

  bool LinkAvailableLocked(uint32_t from_node,
                           uint32_t to_node,
                           std::string* reason) const {
    if (nodes.find(from_node) == nodes.end()) {
      *reason = "source node is unavailable";
      return false;
    }
    if (nodes.find(to_node) == nodes.end()) {
      *reason = "target node is unavailable";
      return false;
    }
    if (down_nodes.find(from_node) != down_nodes.end() ||
        down_nodes.find(to_node) != down_nodes.end()) {
      *reason = "node is down";
      return false;
    }
    if (blocked_links.find(LinkKey(from_node, to_node)) !=
        blocked_links.end()) {
      *reason = "network link is blocked";
      return false;
    }
    const bool forward_partition =
        partition_a.find(from_node) != partition_a.end() &&
        partition_b.find(to_node) != partition_b.end();
    const bool reverse_partition =
        partition_b.find(from_node) != partition_b.end() &&
        partition_a.find(to_node) != partition_a.end();
    if (forward_partition || reverse_partition) {
      *reason = "network is partitioned";
      return false;
    }
    return true;
  }

  void ScheduleLocked(TimePoint delay,
                      std::function<void()> action) {
    if (delay < TimePoint::zero() ||
        delay > TimePoint::max() - now) {
      throw std::overflow_error("simulated time is invalid");
    }
    const EventKey key{now + delay, next_sequence++};
    events.emplace(key, std::move(action));
  }

  template <typename Response>
  void SendResponse(uint32_t from_node, uint32_t to_node,
                    Status handler_status, Response response,
                    std::function<void(Status, Response)> callback) {
    if (callback == nullptr) {
      return;
    }
    const std::shared_ptr<Impl> state = shared_from_this();
    TimePoint delay;
    std::string failure;
    bool available = true;
    {
      std::lock_guard<std::mutex> lock(mutex);
      delay = ChooseDelayLocked();
      available = LinkAvailableLocked(
          from_node, to_node, &failure);
      if (available && ShouldDropLocked()) {
        available = false;
        failure = "simulated packet drop";
      }
      if (!available) {
        state->ScheduleLocked(
            delay,
            [callback = std::move(callback), failure]() mutable {
              callback(Status::Timeout(failure), Response());
            });
        return;
      }
      state->ScheduleLocked(
          delay,
          [callback = std::move(callback), handler_status,
           response = std::move(response)]() mutable {
            callback(handler_status, std::move(response));
          });
    }
  }

  template <typename Request, typename Response, typename Handler>
  void DeliverRequest(
      uint32_t from_node, uint32_t to_node, Request request,
      std::function<void(Status, Response)> callback,
      Handler NodeState::*handler_member) {
    const std::shared_ptr<Impl> state = shared_from_this();
    Handler handler;
    {
      std::lock_guard<std::mutex> lock(mutex);
      const auto iterator = nodes.find(to_node);
      if (iterator != nodes.end()) {
        handler = iterator->second.*handler_member;
      }
    }
    if (handler == nullptr) {
      state->SendResponse(
          to_node, from_node,
          Status::Timeout("target handler is unavailable"),
          Response(), std::move(callback));
      return;
    }

    auto completed = std::make_shared<std::atomic_bool>(false);
    auto done = [state, from_node, to_node, completed, callback,
                 handler_member](
                    Status status, Response response) mutable {
      if (completed->exchange(true)) {
        return;
      }
      state->SendResponse(
          to_node, from_node, status, std::move(response),
          std::move(callback));
    };
    try {
      handler(from_node, request, std::move(done));
    } catch (const std::exception& error) {
      done(Status::IOError(
               std::string("simulated handler failed: ") +
               error.what()),
           Response());
    } catch (...) {
      done(Status::IOError("simulated handler failed"), Response());
    }
  }

  template <typename Request, typename Response, typename Handler>
  void SendRequest(
      uint32_t from_node, uint32_t to_node, Request request,
      std::function<void(Status, Response)> callback,
      Handler NodeState::*handler_member) {
    if (callback == nullptr) {
      throw std::invalid_argument("simulated response callback is null");
    }
    const std::shared_ptr<Impl> state = shared_from_this();
    TimePoint delay;
    std::string failure;
    bool available = true;
    {
      std::lock_guard<std::mutex> lock(mutex);
      delay = ChooseDelayLocked();
      available = LinkAvailableLocked(
          from_node, to_node, &failure);
      if (available && ShouldDropLocked()) {
        available = false;
        failure = "simulated packet drop";
      }
      if (!available) {
        state->ScheduleLocked(
            delay,
            [callback = std::move(callback), failure]() mutable {
              callback(Status::Timeout(failure), Response());
            });
        return;
      }
      state->ScheduleLocked(
          delay,
          [state, from_node, to_node,
           request = std::move(request),
           callback = std::move(callback),
           handler_member]() mutable {
            state->DeliverRequest<Request, Response, Handler>(
                from_node, to_node, std::move(request),
                std::move(callback), handler_member);
          });
    }
  }

  mutable std::mutex mutex;
  TimePoint now{0};
  TimePoint min_latency{0};
  TimePoint max_latency{0};
  double drop_rate = 0.0;
  uint64_t next_sequence = 0;
  std::mt19937_64 random;
  std::map<EventKey, std::function<void()>> events;
  std::unordered_map<uint32_t, NodeState> nodes;
  std::unordered_set<uint32_t> down_nodes;
  std::unordered_set<uint32_t> partition_a;
  std::unordered_set<uint32_t> partition_b;
  std::unordered_set<uint64_t> blocked_links;
};

inline SimNetwork::SimNetwork(uint64_t random_seed)
    : impl_(std::make_shared<Impl>(random_seed)) {}

inline void SimNetwork::SetDelay(
    std::chrono::milliseconds delay) {
  SetLatency(delay, delay);
}

inline void SimNetwork::SetLatency(
    std::chrono::milliseconds min_delay,
    std::chrono::milliseconds max_delay) {
  if (min_delay < std::chrono::milliseconds::zero() ||
      max_delay < min_delay) {
    throw std::invalid_argument("simulated latency range is invalid");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->min_latency = min_delay;
  impl_->max_latency = max_delay;
}

inline void SimNetwork::SetDropRate(double rate) {
  if (!(rate >= 0.0 && rate <= 1.0)) {
    throw std::invalid_argument("simulated drop rate is invalid");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->drop_rate = rate;
}

inline void SimNetwork::DropNode(uint32_t node_id) {
  if (node_id == 0) {
    throw std::invalid_argument("simulated node id cannot be zero");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->down_nodes.insert(node_id);
}

inline void SimNetwork::RestoreNode(uint32_t node_id) {
  if (node_id == 0) {
    throw std::invalid_argument("simulated node id cannot be zero");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->down_nodes.erase(node_id);
}

inline void SimNetwork::Partition(
    std::vector<uint32_t> group_a,
    std::vector<uint32_t> group_b) {
  if (group_a.empty() || group_b.empty()) {
    throw std::invalid_argument("partition groups cannot be empty");
  }

  std::unordered_set<uint32_t> first;
  std::unordered_set<uint32_t> second;
  for (uint32_t node_id : group_a) {
    if (node_id == 0 || !first.insert(node_id).second) {
      throw std::invalid_argument("invalid first partition group");
    }
  }
  for (uint32_t node_id : group_b) {
    if (node_id == 0 || !second.insert(node_id).second) {
      throw std::invalid_argument("invalid second partition group");
    }
    if (first.find(node_id) != first.end()) {
      throw std::invalid_argument("partition groups overlap");
    }
  }

  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->partition_a.swap(first);
  impl_->partition_b.swap(second);
}

inline void SimNetwork::SetLinkBlocked(
    uint32_t from_node, uint32_t to_node, bool blocked) {
  if (from_node == 0 || to_node == 0) {
    throw std::invalid_argument("simulated node id cannot be zero");
  }
  const uint64_t key = Impl::LinkKey(from_node, to_node);
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (blocked) {
    impl_->blocked_links.insert(key);
  } else {
    impl_->blocked_links.erase(key);
  }
}

inline void SimNetwork::Heal() {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->partition_a.clear();
  impl_->partition_b.clear();
  impl_->blocked_links.clear();
}

inline void SimNetwork::AdvanceBy(
    std::chrono::milliseconds duration) {
  if (duration < std::chrono::milliseconds::zero()) {
    throw std::invalid_argument("simulated time cannot move backwards");
  }

  std::chrono::milliseconds target;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (duration >
        std::chrono::milliseconds::max() - impl_->now) {
      throw std::overflow_error("simulated time overflow");
    }
    target = impl_->now + duration;
  }

  while (true) {
    std::function<void()> action;
    {
      std::lock_guard<std::mutex> lock(impl_->mutex);
      auto iterator = impl_->events.begin();
      if (iterator == impl_->events.end() ||
          iterator->first.first > target) {
        impl_->now = target;
        return;
      }
      impl_->now = iterator->first.first;
      action = std::move(iterator->second);
      impl_->events.erase(iterator);
    }
    action();
  }
}

inline std::chrono::milliseconds SimNetwork::Now() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->now;
}

inline size_t SimNetwork::PendingMessageCount() const {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  return impl_->events.size();
}

inline void SimNetwork::RegisterNode(uint32_t node_id) {
  if (node_id == 0) {
    throw std::invalid_argument("simulated node id cannot be zero");
  }
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (!impl_->nodes.emplace(node_id, NodeState()).second) {
    throw std::invalid_argument("simulated node id is already registered");
  }
}

inline void SimNetwork::UnregisterNode(uint32_t node_id) {
  std::lock_guard<std::mutex> lock(impl_->mutex);
  impl_->nodes.erase(node_id);
}

class SimTransport final : public RaftTransport {
 public:
  SimTransport(std::shared_ptr<SimNetwork> network,
               uint32_t node_id);
  ~SimTransport() override;

  SimTransport(const SimTransport&) = delete;
  SimTransport& operator=(const SimTransport&) = delete;

  void SendRequestVote(
      uint32_t peer_id,
      const proto::RequestVoteRequest& request,
      RequestVoteResponseCallback callback) override;
  void SendAppendEntries(
      uint32_t peer_id,
      const proto::AppendEntriesRequest& request,
      AppendEntriesResponseCallback callback) override;
  void SendInstallSnapshot(
      uint32_t peer_id,
      const proto::InstallSnapshotRequest& request,
      InstallSnapshotResponseCallback callback) override;

  void SetRequestVoteHandler(
      RequestVoteHandler handler) override;
  void SetAppendEntriesHandler(
      AppendEntriesHandler handler) override;
  void SetInstallSnapshotHandler(
      InstallSnapshotHandler handler) override;

  void SetDelay(std::chrono::milliseconds delay);
  void SetLatency(std::chrono::milliseconds min_delay,
                  std::chrono::milliseconds max_delay);
  void SetDropRate(double rate);
  void DropNode(uint32_t node_id);
  void RestoreNode(uint32_t node_id);
  void Partition(std::vector<uint32_t> group_a,
                 std::vector<uint32_t> group_b);
  void SetLinkBlocked(uint32_t from_node,
                      uint32_t to_node, bool blocked);
  void Heal();
  void AdvanceBy(std::chrono::milliseconds duration);

  std::chrono::milliseconds Now() const;
  size_t PendingMessageCount() const;
  uint32_t node_id() const { return node_id_; }

 private:
  std::shared_ptr<SimNetwork> network_;
  uint32_t node_id_;
};

inline SimTransport::SimTransport(
    std::shared_ptr<SimNetwork> network, uint32_t node_id)
    : network_(std::move(network)), node_id_(node_id) {
  if (network_ == nullptr) {
    throw std::invalid_argument("simulated network is null");
  }
  network_->RegisterNode(node_id_);
}

inline SimTransport::~SimTransport() {
  network_->UnregisterNode(node_id_);
}

inline void SimTransport::SendRequestVote(
    uint32_t peer_id,
    const proto::RequestVoteRequest& request,
    RequestVoteResponseCallback callback) {
  network_->impl_->template SendRequest<
      proto::RequestVoteRequest, proto::RequestVoteResponse,
      RequestVoteHandler>(
      node_id_, peer_id, request, std::move(callback),
      &SimNetwork::NodeState::request_vote);
}

inline void SimTransport::SendAppendEntries(
    uint32_t peer_id,
    const proto::AppendEntriesRequest& request,
    AppendEntriesResponseCallback callback) {
  network_->impl_->template SendRequest<
      proto::AppendEntriesRequest, proto::AppendEntriesResponse,
      AppendEntriesHandler>(
      node_id_, peer_id, request, std::move(callback),
      &SimNetwork::NodeState::append_entries);
}

inline void SimTransport::SendInstallSnapshot(
    uint32_t peer_id,
    const proto::InstallSnapshotRequest& request,
    InstallSnapshotResponseCallback callback) {
  network_->impl_->template SendRequest<
      proto::InstallSnapshotRequest,
      proto::InstallSnapshotResponse,
      InstallSnapshotHandler>(
      node_id_, peer_id, request, std::move(callback),
      &SimNetwork::NodeState::install_snapshot);
}

inline void SimTransport::SetRequestVoteHandler(
    RequestVoteHandler handler) {
  std::lock_guard<std::mutex> lock(network_->impl_->mutex);
  network_->impl_->nodes.at(node_id_).request_vote =
      std::move(handler);
}

inline void SimTransport::SetAppendEntriesHandler(
    AppendEntriesHandler handler) {
  std::lock_guard<std::mutex> lock(network_->impl_->mutex);
  network_->impl_->nodes.at(node_id_).append_entries =
      std::move(handler);
}

inline void SimTransport::SetInstallSnapshotHandler(
    InstallSnapshotHandler handler) {
  std::lock_guard<std::mutex> lock(network_->impl_->mutex);
  network_->impl_->nodes.at(node_id_).install_snapshot =
      std::move(handler);
}

inline void SimTransport::SetDelay(
    std::chrono::milliseconds delay) {
  network_->SetDelay(delay);
}

inline void SimTransport::SetLatency(
    std::chrono::milliseconds min_delay,
    std::chrono::milliseconds max_delay) {
  network_->SetLatency(min_delay, max_delay);
}

inline void SimTransport::SetDropRate(double rate) {
  network_->SetDropRate(rate);
}

inline void SimTransport::DropNode(uint32_t node_id) {
  network_->DropNode(node_id);
}

inline void SimTransport::RestoreNode(uint32_t node_id) {
  network_->RestoreNode(node_id);
}

inline void SimTransport::Partition(
    std::vector<uint32_t> group_a,
    std::vector<uint32_t> group_b) {
  network_->Partition(std::move(group_a), std::move(group_b));
}

inline void SimTransport::SetLinkBlocked(
    uint32_t from_node, uint32_t to_node, bool blocked) {
  network_->SetLinkBlocked(from_node, to_node, blocked);
}

inline void SimTransport::Heal() {
  network_->Heal();
}

inline void SimTransport::AdvanceBy(
    std::chrono::milliseconds duration) {
  network_->AdvanceBy(duration);
}

inline std::chrono::milliseconds SimTransport::Now() const {
  return network_->Now();
}

inline size_t SimTransport::PendingMessageCount() const {
  return network_->PendingMessageCount();
}

}  // namespace testing
}  // namespace raftkv

#endif  // RAFTKV_TESTS_CHAOS_SIM_TRANSPORT_H_
