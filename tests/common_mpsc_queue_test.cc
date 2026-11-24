#include "raftkv/common/mpsc_queue.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

namespace raftkv {
namespace {

TEST(MpscQueueTest, PopBatchDrainsAndReplacesOutput) {
  MpscQueue<int> queue;
  queue.Push(1);
  queue.Push(2);
  queue.Push(3);
  std::vector<int> output = {99, 100};

  ASSERT_TRUE(queue.PopBatch(output, 100));

  EXPECT_EQ(output, std::vector<int>({1, 2, 3}));
  output = {42};
  EXPECT_FALSE(queue.PopBatch(output, 0));
  EXPECT_TRUE(output.empty());
}

TEST(MpscQueueTest, PopBatchTimeoutAndZeroTimeout) {
  MpscQueue<int> queue;
  std::vector<int> output;

  const auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(queue.PopBatch(output, 20));
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_GE(elapsed, std::chrono::milliseconds(10));
  EXPECT_LT(elapsed, std::chrono::seconds(1));

  const auto zero_start = std::chrono::steady_clock::now();
  EXPECT_FALSE(queue.PopBatch(output, 0));
  EXPECT_LT(std::chrono::steady_clock::now() - zero_start,
            std::chrono::milliseconds(100));
}

TEST(MpscQueueTest, SupportsMoveOnlyValues) {
  MpscQueue<std::unique_ptr<int>> queue;
  queue.Push(std::make_unique<int>(7));
  std::vector<std::unique_ptr<int>> output;

  ASSERT_TRUE(queue.PopBatch(output, 0));
  ASSERT_EQ(output.size(), 1U);
  ASSERT_NE(output.front(), nullptr);
  EXPECT_EQ(*output.front(), 7);
}

TEST(MpscQueueTest, FourProducersPreserveCountAndChecksum) {
  constexpr int kProducerCount = 4;
  constexpr int kValuesPerProducer = 10000;
  constexpr size_t kExpectedCount =
      static_cast<size_t>(kProducerCount) * kValuesPerProducer;
  constexpr uint64_t kExpectedChecksum =
      static_cast<uint64_t>(kExpectedCount) * (kExpectedCount - 1) / 2;

  MpscQueue<int> queue;
  std::vector<std::thread> producers;
  producers.reserve(kProducerCount);

  size_t received_count = 0;
  uint64_t checksum = 0;
  std::thread consumer([&queue, &received_count, &checksum] {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    std::vector<int> batch;
    while (received_count < kExpectedCount &&
           std::chrono::steady_clock::now() < deadline) {
      if (queue.PopBatch(batch, 10)) {
        for (int value : batch) {
          ++received_count;
          checksum += static_cast<uint64_t>(value);
        }
      }
    }
  });

  for (int producer_id = 0; producer_id < kProducerCount; ++producer_id) {
    producers.emplace_back([producer_id, &queue] {
      const int first = producer_id * kValuesPerProducer;
      for (int offset = 0; offset < kValuesPerProducer; ++offset) {
        queue.Push(first + offset);
      }
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }
  consumer.join();

  EXPECT_EQ(received_count, kExpectedCount);
  EXPECT_EQ(checksum, kExpectedChecksum);
}

}  // namespace
}  // namespace raftkv
