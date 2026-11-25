#include "raftkv/storage/bloom_filter.h"
#include "raftkv/storage/lru_cache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"

namespace raftkv {
namespace {

std::shared_ptr<const std::string> MakeValue(
    const std::string& value) {
  return std::make_shared<const std::string>(value);
}

size_t Charge(const std::string& key,
              const std::shared_ptr<const std::string>& value) {
  return kLruEntryOverhead + key.size() + value->size();
}

TEST(BloomFilterTest, HasNoFalseNegativesAndLowFalsePositiveRate) {
  constexpr int kKeyCount = 100000;

  std::vector<std::string> present_keys;
  present_keys.reserve(kKeyCount);
  std::vector<Slice> present_slices;
  present_slices.reserve(kKeyCount);
  for (int i = 0; i < kKeyCount; ++i) {
    present_keys.push_back("present-key-" + std::to_string(i));
  }
  for (const std::string& key : present_keys) {
    present_slices.emplace_back(key);
  }

  std::string filter;
  ASSERT_TRUE(
      BloomFilter::Build(present_slices, &filter).ok());
  ASSERT_GE(filter.size(), kBloomFilterHeaderSize);
  EXPECT_EQ(DecodeFixed32(filter.data()), kBloomFilterMagic);
  EXPECT_EQ(DecodeFixed32(filter.data() + 4),
            static_cast<uint32_t>(kKeyCount) *
                kBloomFilterBitsPerKey);

  for (const std::string& key : present_keys) {
    ASSERT_TRUE(BloomFilter::KeyMayMatch(Slice(key), Slice(filter)))
        << key;
  }

  int false_positives = 0;
  std::vector<std::string> absent_keys;
  absent_keys.reserve(kKeyCount);
  for (int i = 0; i < kKeyCount; ++i) {
    absent_keys.push_back("absent-key-" + std::to_string(i));
  }
  for (const std::string& key : absent_keys) {
    if (BloomFilter::KeyMayMatch(Slice(key), Slice(filter))) {
      ++false_positives;
    }
  }

  const double false_positive_rate =
      static_cast<double>(false_positives) / kKeyCount;
  EXPECT_LT(false_positive_rate, 0.02)
      << "false positives: " << false_positives;
}

TEST(BloomFilterTest, HandlesEmptyDuplicateAndBinaryKeys) {
  std::string empty_filter = "old";
  ASSERT_TRUE(
      BloomFilter::Build({}, &empty_filter).ok());
  ASSERT_EQ(empty_filter.size(), kBloomFilterHeaderSize);
  EXPECT_EQ(DecodeFixed32(empty_filter.data() + 4), 0U);
  EXPECT_FALSE(BloomFilter::KeyMayMatch(
      Slice("anything"), Slice(empty_filter)));

  const std::string binary("a\0b", 3);
  const std::vector<std::string> keys = {
      "", "duplicate", "duplicate", binary};
  std::vector<Slice> slices;
  for (const std::string& key : keys) {
    slices.emplace_back(key);
  }

  std::string filter;
  ASSERT_TRUE(BloomFilter::Build(slices, &filter).ok());
  for (const std::string& key : keys) {
    EXPECT_TRUE(
        BloomFilter::KeyMayMatch(Slice(key), Slice(filter)));
  }
  EXPECT_EQ(BloomFilter::Build(slices, nullptr).code(),
            Code::kInvalidArgument);
}

TEST(BloomFilterTest, InvalidFiltersFailOpen) {
  std::vector<std::string> keys = {"alpha", "beta"};
  std::vector<Slice> slices;
  for (const std::string& key : keys) {
    slices.emplace_back(key);
  }

  std::string filter;
  ASSERT_TRUE(BloomFilter::Build(slices, &filter).ok());

  std::string truncated = filter.substr(0, filter.size() - 1);
  EXPECT_TRUE(BloomFilter::KeyMayMatch(
      Slice("alpha"), Slice(truncated)));

  std::string wrong_magic = filter;
  wrong_magic[0] = static_cast<char>(wrong_magic[0] ^ 0x01);
  EXPECT_TRUE(BloomFilter::KeyMayMatch(
      Slice("alpha"), Slice(wrong_magic)));

  std::string partial_header = filter.substr(0, 4);
  EXPECT_TRUE(BloomFilter::KeyMayMatch(
      Slice("alpha"), Slice(partial_header)));
}

TEST(LRUCacheTest, EvictsLeastRecentlyUsedAndRefreshesLookup) {
  const std::string key_a = "a";
  const std::string key_b = "b";
  const std::string key_c = "c";
  const std::string key_d = "d";
  const auto value_a = MakeValue("1");
  const auto value_b = MakeValue("2");
  const auto value_c = MakeValue("3");
  const auto value_d = MakeValue("4");
  const size_t item_charge = Charge(key_a, value_a);

  LRUCache cache(item_charge * 3);
  ASSERT_TRUE(cache.Insert(key_a, value_a).ok());
  ASSERT_TRUE(cache.Insert(key_b, value_b).ok());
  ASSERT_TRUE(cache.Insert(key_c, value_c).ok());
  EXPECT_EQ(cache.Size(), 3U);
  EXPECT_EQ(cache.Usage(), item_charge * 3);

  EXPECT_EQ(cache.Lookup(key_a), value_a);
  ASSERT_TRUE(cache.Insert(key_d, value_d).ok());

  EXPECT_EQ(cache.Lookup(key_b), nullptr);
  EXPECT_EQ(cache.Lookup(key_a), value_a);
  EXPECT_EQ(cache.Lookup(key_c), value_c);
  EXPECT_EQ(cache.Lookup(key_d), value_d);
  EXPECT_EQ(cache.Size(), 3U);
  EXPECT_EQ(cache.Usage(), item_charge * 3);
}

TEST(LRUCacheTest, UpdatesErasesAndClearsEntries) {
  const std::string key_a = "a";
  const std::string key_b = "b";
  const auto first_a = MakeValue("1");
  const auto updated_a = MakeValue("9");
  const auto value_b = MakeValue("2");
  const size_t item_charge = Charge(key_a, first_a);

  LRUCache cache(item_charge * 2);
  ASSERT_TRUE(cache.Insert(key_a, first_a).ok());
  ASSERT_TRUE(cache.Insert(key_b, value_b).ok());
  ASSERT_TRUE(cache.Insert(key_a, updated_a).ok());
  EXPECT_EQ(cache.Size(), 2U);
  EXPECT_EQ(cache.Usage(), item_charge * 2);
  EXPECT_EQ(cache.Lookup(key_a), updated_a);

  EXPECT_TRUE(cache.Erase(key_a));
  EXPECT_FALSE(cache.Erase(key_a));
  EXPECT_EQ(cache.Size(), 1U);
  EXPECT_EQ(cache.Usage(), item_charge);

  cache.Clear();
  EXPECT_EQ(cache.Size(), 0U);
  EXPECT_EQ(cache.Usage(), 0U);
  EXPECT_EQ(cache.Capacity(), item_charge * 2);
}

TEST(LRUCacheTest, SharedValueAndFailedInsertPreserveState) {
  const std::string key_a = "a";
  const std::string key_b = "b";
  const auto value_a = MakeValue("1");
  const auto value_b = MakeValue("2");
  const size_t item_charge = Charge(key_a, value_a);

  LRUCache cache(item_charge);
  ASSERT_TRUE(cache.Insert(key_a, value_a).ok());
  std::shared_ptr<const std::string> retained = cache.Lookup(key_a);
  ASSERT_TRUE(cache.Insert(key_b, value_b).ok());
  EXPECT_EQ(cache.Lookup(key_a), nullptr);
  ASSERT_NE(retained, nullptr);
  EXPECT_EQ(*retained, "1");

  const size_t usage = cache.Usage();
  EXPECT_EQ(cache.Insert(key_b, nullptr).code(),
            Code::kInvalidArgument);
  const auto oversized =
      MakeValue(std::string(item_charge, 'x'));
  EXPECT_EQ(cache.Insert("large", oversized).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(cache.Usage(), usage);
  EXPECT_EQ(cache.Size(), 1U);
  EXPECT_EQ(cache.Lookup(key_b), value_b);

  LRUCache zero_capacity(0);
  EXPECT_EQ(zero_capacity.Insert(key_a, value_a).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(zero_capacity.Size(), 0U);
}

TEST(LRUCacheTest, SupportsConcurrentAccess) {
  constexpr int kKeyCount = 128;
  constexpr int kThreadCount = 4;
  constexpr int kIterations = 5000;

  const auto value = MakeValue("value");
  const size_t item_charge = Charge("key-0", value);
  LRUCache cache(item_charge * kKeyCount);
  std::atomic<int> failures{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int thread_index = 0; thread_index < kThreadCount;
       ++thread_index) {
    threads.emplace_back([&, thread_index] {
      for (int i = 0; i < kIterations; ++i) {
        const int key_number =
            (thread_index * 997 + i * 37) % kKeyCount;
        const std::string key =
            "key-" + std::to_string(key_number);
        const int operation = i % 3;
        if (operation == 0) {
          if (!cache.Insert(key, value).ok()) {
            ++failures;
          }
        } else if (operation == 1) {
          const auto found = cache.Lookup(key);
          if (found != nullptr && *found != "value") {
            ++failures;
          }
        } else {
          cache.Erase(key);
        }
      }
    });
  }

  for (std::thread& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(failures.load(), 0);
  EXPECT_LE(cache.Usage(), cache.Capacity());
  EXPECT_LE(cache.Size(), static_cast<size_t>(kKeyCount));
  cache.Clear();
  EXPECT_EQ(cache.Usage(), 0U);
  EXPECT_EQ(cache.Size(), 0U);
}

}  // namespace
}  // namespace raftkv
