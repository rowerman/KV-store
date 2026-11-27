#include "chaos/sim_transport.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace raftkv {
namespace testing {
namespace {

using namespace std::chrono_literals;

proto::RequestVoteRequest VoteRequest(uint32_t candidate_id) {
  proto::RequestVoteRequest request;
  request.set_term(1);
  request.set_candidate_id(candidate_id);
  return request;
}

void InstallVoteEcho(SimTransport* transport) {
  transport->SetRequestVoteHandler(
      [](uint32_t, const proto::RequestVoteRequest& request,
         RequestVoteResponseCallback done) {
        proto::RequestVoteResponse response;
        response.set_term(request.term());
        response.set_vote_granted(true);
        done(Status::OK(), response);
      });
}

class SimTransportTest : public ::testing::Test {
 protected:
  void SetUp() override {
    network_ = std::make_shared<SimNetwork>(20261127);
    first_ = std::make_unique<SimTransport>(network_, 1);
    second_ = std::make_unique<SimTransport>(network_, 2);
  }

  std::shared_ptr<SimNetwork> network_;
  std::unique_ptr<SimTransport> first_;
  std::unique_ptr<SimTransport> second_;
};

TEST_F(SimTransportTest, DeliversAllRequestTypesWithTwoWayDelay) {
  network_->SetDelay(2ms);

  std::atomic<int> vote_handled{0};
  std::atomic<int> append_handled{0};
  std::atomic<int> snapshot_handled{0};
  second_->SetRequestVoteHandler(
      [&](uint32_t from, const proto::RequestVoteRequest& request,
          RequestVoteResponseCallback done) {
        EXPECT_EQ(from, 1U);
        EXPECT_EQ(request.candidate_id(), 7U);
        ++vote_handled;
        proto::RequestVoteResponse response;
        response.set_term(4);
        response.set_vote_granted(true);
        done(Status::OK(), response);
      });
  second_->SetAppendEntriesHandler(
      [&](uint32_t, const proto::AppendEntriesRequest& request,
          AppendEntriesResponseCallback done) {
        EXPECT_EQ(request.leader_id(), 1U);
        ++append_handled;
        proto::AppendEntriesResponse response;
        response.set_term(request.term());
        response.set_success(true);
        done(Status::OK(), response);
      });
  second_->SetInstallSnapshotHandler(
      [&](uint32_t, const proto::InstallSnapshotRequest& request,
          InstallSnapshotResponseCallback done) {
        ++snapshot_handled;
        proto::InstallSnapshotResponse response;
        response.set_received_offset(
            request.offset() + request.data().size());
        done(Status::OK(), response);
      });

  int vote_callbacks = 0;
  int append_callbacks = 0;
  int snapshot_callbacks = 0;
  first_->SendRequestVote(
      2, VoteRequest(7),
      [&](Status status, proto::RequestVoteResponse response) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.vote_granted());
        ++vote_callbacks;
      });

  proto::AppendEntriesRequest append;
  append.set_term(3);
  append.set_leader_id(1);
  first_->SendAppendEntries(
      2, append,
      [&](Status status, proto::AppendEntriesResponse response) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_TRUE(response.success());
        ++append_callbacks;
      });

  proto::InstallSnapshotRequest snapshot;
  snapshot.set_term(3);
  snapshot.set_leader_id(1);
  snapshot.set_offset(5);
  snapshot.set_data("abc");
  first_->SendInstallSnapshot(
      2, snapshot,
      [&](Status status, proto::InstallSnapshotResponse response) {
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(response.received_offset(), 8U);
        ++snapshot_callbacks;
      });

  EXPECT_EQ(network_->PendingMessageCount(), 3U);
  network_->AdvanceBy(1ms);
  EXPECT_EQ(vote_handled.load(), 0);
  EXPECT_EQ(vote_callbacks, 0);

  network_->AdvanceBy(1ms);
  EXPECT_EQ(vote_handled.load(), 1);
  EXPECT_EQ(append_handled.load(), 1);
  EXPECT_EQ(snapshot_handled.load(), 1);
  EXPECT_EQ(vote_callbacks, 0);

  network_->AdvanceBy(2ms);
  EXPECT_EQ(vote_callbacks, 1);
  EXPECT_EQ(append_callbacks, 1);
  EXPECT_EQ(snapshot_callbacks, 1);
}

TEST_F(SimTransportTest, AppliesDropRateAndMissingHandlerTimeout) {
  network_->SetDelay(1ms);
  network_->SetDropRate(1.0);
  int callbacks = 0;
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++callbacks;
      });
  network_->AdvanceBy(1ms);
  EXPECT_EQ(callbacks, 1);

  network_->SetDropRate(0.0);
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        EXPECT_NE(status.message().find("handler"), std::string::npos);
        ++callbacks;
      });
  network_->AdvanceBy(2ms);
  EXPECT_EQ(callbacks, 2);
}

TEST_F(SimTransportTest, SupportsNodePartitionAndDirectionalFailures) {
  network_->SetDelay(0ms);
  InstallVoteEcho(second_.get());

  auto send_vote = [&](int* callbacks) {
    first_->SendRequestVote(
        2, VoteRequest(1),
        [callbacks](Status status, proto::RequestVoteResponse) {
          EXPECT_TRUE(status.ok()) << status.ToString();
          ++*callbacks;
        });
    network_->AdvanceBy(0ms);
  };

  int callbacks = 0;
  send_vote(&callbacks);
  EXPECT_EQ(callbacks, 1);

  network_->DropNode(2);
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++callbacks;
      });
  network_->AdvanceBy(0ms);
  EXPECT_EQ(callbacks, 2);

  network_->RestoreNode(2);
  network_->Partition({1}, {2});
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++callbacks;
      });
  network_->AdvanceBy(0ms);
  EXPECT_EQ(callbacks, 3);

  network_->Heal();
  network_->SetLinkBlocked(1, 2, true);
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++callbacks;
      });
  network_->AdvanceBy(0ms);
  EXPECT_EQ(callbacks, 4);

  int reverse_handled = 0;
  first_->SetRequestVoteHandler(
      [&](uint32_t, const proto::RequestVoteRequest&,
          RequestVoteResponseCallback done) {
        ++reverse_handled;
        proto::RequestVoteResponse response;
        response.set_vote_granted(true);
        done(Status::OK(), response);
      });
  second_->SendRequestVote(
      1, VoteRequest(2),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++callbacks;
      });
  network_->AdvanceBy(0ms);
  EXPECT_EQ(reverse_handled, 1);
  EXPECT_EQ(callbacks, 5);

  network_->SetLinkBlocked(1, 2, false);
  send_vote(&callbacks);
  EXPECT_EQ(callbacks, 6);
}

std::vector<uint64_t> RunLatencyOrder(uint64_t seed) {
  auto network = std::make_shared<SimNetwork>(seed);
  SimTransport first(network, 1);
  SimTransport second(network, 2);
  second.SetRequestVoteHandler(
      [](uint32_t, const proto::RequestVoteRequest& request,
         RequestVoteResponseCallback done) {
        proto::RequestVoteResponse response;
        response.set_term(request.candidate_id());
        response.set_vote_granted(true);
        done(Status::OK(), response);
      });

  network->SetLatency(1ms, 5ms);
  std::vector<uint64_t> order;
  for (uint32_t i = 1; i <= 20; ++i) {
    first.SendRequestVote(
        2, VoteRequest(i),
        [&](Status status, proto::RequestVoteResponse response) {
          ASSERT_TRUE(status.ok()) << status.ToString();
          order.push_back(response.term());
        });
  }
  network->AdvanceBy(10ms);
  return order;
}

TEST(SimTransportDeterminismTest, RepeatsRandomDeliveryOrder) {
  const std::vector<uint64_t> first = RunLatencyOrder(12345);
  const std::vector<uint64_t> second = RunLatencyOrder(12345);
  const std::vector<uint64_t> different_seed =
      RunLatencyOrder(54321);

  ASSERT_EQ(first.size(), 20U);
  EXPECT_EQ(first, second);
  EXPECT_EQ(different_seed.size(), first.size());
}

TEST_F(SimTransportTest, CompletesDoneOnceAndUnregistersNodes) {
  network_->SetDelay(0ms);
  second_->SetRequestVoteHandler(
      [](uint32_t, const proto::RequestVoteRequest&,
         RequestVoteResponseCallback done) {
        proto::RequestVoteResponse response;
        response.set_vote_granted(true);
        done(Status::OK(), response);
        done(Status::OK(), response);
      });

  int callbacks = 0;
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_TRUE(status.ok()) << status.ToString();
        ++callbacks;
      });
  network_->AdvanceBy(0ms);
  EXPECT_EQ(callbacks, 1);

  EXPECT_THROW(
      {
        SimTransport duplicate(network_, 1);
      },
      std::invalid_argument);

  second_.reset();
  first_->SendRequestVote(
      2, VoteRequest(1),
      [&](Status status, proto::RequestVoteResponse) {
        EXPECT_EQ(status.code(), Code::kTimeout);
        ++callbacks;
      });
  network_->AdvanceBy(0ms);
  EXPECT_EQ(callbacks, 2);
}

TEST_F(SimTransportTest, ValidatesFaultControlArguments) {
  EXPECT_THROW(network_->SetDropRate(-0.1), std::invalid_argument);
  EXPECT_THROW(network_->SetDropRate(1.1), std::invalid_argument);
  EXPECT_THROW(network_->SetLatency(2ms, 1ms),
               std::invalid_argument);
  EXPECT_THROW(network_->Partition({1}, {1}),
               std::invalid_argument);
  EXPECT_THROW(network_->Partition({}, {2}),
               std::invalid_argument);
  EXPECT_THROW(network_->DropNode(0), std::invalid_argument);
}

TEST(SimTransportConcurrencyTest, SupportsConcurrentSenders) {
  auto network = std::make_shared<SimNetwork>(20261127);
  network->SetDelay(0ms);
  SimTransport receiver(network, 10);
  InstallVoteEcho(&receiver);

  constexpr int kSenderCount = 4;
  constexpr int kRequestsPerSender = 25;
  std::vector<std::unique_ptr<SimTransport>> senders;
  for (int i = 0; i < kSenderCount; ++i) {
    senders.push_back(std::make_unique<SimTransport>(
        network, static_cast<uint32_t>(i + 1)));
  }

  std::atomic<int> successes{0};
  std::atomic<int> callbacks{0};
  std::vector<std::thread> threads;
  for (int sender = 0; sender < kSenderCount; ++sender) {
    threads.emplace_back([&, sender]() {
      for (int i = 0; i < kRequestsPerSender; ++i) {
        senders[static_cast<size_t>(sender)]->SendRequestVote(
            10, VoteRequest(static_cast<uint32_t>(sender + 1)),
            [&](Status status, proto::RequestVoteResponse response) {
              if (status.ok() && response.vote_granted()) {
                ++successes;
              }
              ++callbacks;
            });
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }

  network->AdvanceBy(0ms);
  EXPECT_EQ(callbacks.load(),
            kSenderCount * kRequestsPerSender);
  EXPECT_EQ(successes.load(),
            kSenderCount * kRequestsPerSender);
}

}  // namespace
}  // namespace testing
}  // namespace raftkv
