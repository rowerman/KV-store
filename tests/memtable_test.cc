#include "raftkv/storage/memtable.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "raftkv/storage/internal_key.h"
#include "raftkv/storage/skiplist.h"

namespace raftkv {
namespace {

struct InternalKeyLess {
  bool operator()(const InternalKey& lhs,
                  const InternalKey& rhs) const {
    return CompareInternalKeys(lhs, rhs) < 0;
  }
};

TEST(InternalKeyTest, CompareOrdersUserSequenceAndType) {
  EXPECT_LT(CompareInternalKeys(
                InternalKey{"a", 1, ValueType::kValue},
                InternalKey{"b", 3, ValueType::kDeletion}),
            0);
  EXPECT_LT(CompareInternalKeys(
                InternalKey{"key", 8, ValueType::kValue},
                InternalKey{"key", 7, ValueType::kDeletion}),
            0);
  EXPECT_LT(CompareInternalKeys(
                InternalKey{"key", 8, ValueType::kValue},
                InternalKey{"key", 8, ValueType::kDeletion}),
            0);
  EXPECT_EQ(CompareInternalKeys(
                InternalKey{"key", 8, ValueType::kValue},
                InternalKey{"key", 8, ValueType::kValue}),
            0);
  EXPECT_TRUE(IsValidValueType(ValueType::kDeletion));
  EXPECT_TRUE(IsValidValueType(ValueType::kValue));
  EXPECT_FALSE(IsValidValueType(
      static_cast<ValueType>(42)));
}

TEST(SkipListTest, IteratorMaintainsOrderAndSeekResults) {
  SkipList list(12345);
  EXPECT_GT(list.Add(InternalKey{"b", 3, ValueType::kDeletion},
                     std::string()),
            0U);
  EXPECT_GT(list.Add(InternalKey{"a", 1, ValueType::kValue}, "a1"),
            0U);
  EXPECT_GT(list.Add(InternalKey{"c", 4, ValueType::kValue}, "c4"),
            0U);
  EXPECT_GT(list.Add(InternalKey{"a", 2, ValueType::kValue}, "a2"),
            0U);

  auto iterator = list.NewIterator();
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().user_key, "a");
  EXPECT_EQ(iterator->key().sequence, 2U);
  EXPECT_EQ(iterator->value().ToString(), "a2");

  iterator->Next();
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().sequence, 1U);
  EXPECT_EQ(iterator->value().ToString(), "a1");

  iterator->Seek(Slice("b"));
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().user_key, "b");
  EXPECT_EQ(iterator->key().type, ValueType::kDeletion);
  EXPECT_TRUE(iterator->value().empty());

  iterator->Seek(Slice("bb"));
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().user_key, "c");

  iterator->Seek(InternalKey{"a", 2, ValueType::kValue});
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().sequence, 2U);

  iterator->Seek(Slice("z"));
  EXPECT_FALSE(iterator->Valid());
  iterator->Next();
  EXPECT_FALSE(iterator->Valid());
}

TEST(SkipListTest, EmptySeekAndDeterministicSeedAreSafe) {
  SkipList first(7);
  SkipList second(7);
  auto first_iterator = first.NewIterator();
  auto second_iterator = second.NewIterator();
  first_iterator->SeekToFirst();
  second_iterator->Seek(Slice("missing"));
  EXPECT_FALSE(first_iterator->Valid());
  EXPECT_FALSE(second_iterator->Valid());

  for (int i = 0; i < 1000; ++i) {
    const std::string key =
        "key-" + std::to_string((i * 37) % 1000);
    first.Add(InternalKey{key, static_cast<uint64_t>(i + 1),
                          ValueType::kValue},
              "value-" + std::to_string(i));
    second.Add(InternalKey{key, static_cast<uint64_t>(i + 1),
                           ValueType::kValue},
               "value-" + std::to_string(i));
  }

  first_iterator->SeekToFirst();
  second_iterator->SeekToFirst();
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(first_iterator->Valid());
    ASSERT_TRUE(second_iterator->Valid());
    EXPECT_EQ(CompareInternalKeys(first_iterator->key(),
                                  second_iterator->key()),
              0);
    EXPECT_EQ(first_iterator->value().ToString(),
              second_iterator->value().ToString());
    first_iterator->Next();
    second_iterator->Next();
  }
  EXPECT_FALSE(first_iterator->Valid());
  EXPECT_FALSE(second_iterator->Valid());
}

TEST(MemTableTest, MatchesReferenceModelWithHundredThousandWrites) {
  constexpr int kUserKeyCount = 50000;
  constexpr int kWriteCount = 100000;

  struct ExpectedValue {
    bool deleted = false;
    std::string value;
  };

  MemTable table;
  std::map<std::string, ExpectedValue> latest;
  std::multimap<InternalKey, std::string, InternalKeyLess> all_versions;
  std::mt19937 random(0x524B3101U);
  std::uniform_int_distribution<int> key_distribution(
      0, kUserKeyCount - 1);
  std::uniform_int_distribution<int> operation_distribution(0, 9);

  EXPECT_EQ(table.ApproximateMemoryUsage(), 0U);
  for (int i = 0; i < kWriteCount; ++i) {
    const int key_number = key_distribution(random);
    const std::string key =
        "key-" + std::to_string(key_number);
    const uint64_t sequence = static_cast<uint64_t>(i + 1);
    const bool deleted = operation_distribution(random) < 2;
    const std::string value =
        deleted ? std::string()
                : "value-" + std::to_string(sequence);
    const ValueType type =
        deleted ? ValueType::kDeletion : ValueType::kValue;

    ASSERT_TRUE(table.Add(sequence, type, Slice(key), Slice(value)).ok());
    all_versions.emplace(
        InternalKey{key, sequence, type}, value);
    latest[key] = ExpectedValue{deleted, value};
  }

  EXPECT_EQ(table.LastSequence(),
            static_cast<uint64_t>(kWriteCount));
  EXPECT_GT(table.ApproximateMemoryUsage(), 0U);

  int point_mismatches = 0;
  for (int key_number = 0; key_number < kUserKeyCount;
       ++key_number) {
    const std::string key =
        "key-" + std::to_string(key_number);
    std::string value = "unchanged";
    bool is_deleted = true;
    const Status status =
        table.Get(Slice(key), &value, &is_deleted);
    const auto expected = latest.find(key);
    if (expected == latest.end()) {
      if (status.code() != Code::kNotFound ||
          value != "unchanged" || !is_deleted) {
        ++point_mismatches;
      }
      continue;
    }

    if (!status.ok() ||
        value != expected->second.value ||
        is_deleted != expected->second.deleted) {
      ++point_mismatches;
    }
  }
  EXPECT_EQ(point_mismatches, 0);

  auto iterator = table.NewIterator();
  iterator->SeekToFirst();
  int entry_mismatches = 0;
  size_t entries = 0;
  auto expected = all_versions.begin();
  while (iterator->Valid() &&
         expected != all_versions.end()) {
    if (CompareInternalKeys(iterator->key(), expected->first) != 0 ||
        iterator->value().ToString() != expected->second) {
      ++entry_mismatches;
    }
    ++entries;
    iterator->Next();
    ++expected;
  }
  if (iterator->Valid() || expected != all_versions.end()) {
    ++entry_mismatches;
  }

  EXPECT_EQ(entries, static_cast<size_t>(kWriteCount));
  EXPECT_EQ(entry_mismatches, 0);
}

TEST(MemTableTest, ValidatesInputAndCommitsOutputsOnlyOnSuccess) {
  MemTable table;
  ASSERT_TRUE(table.Add(1, ValueType::kValue, Slice("key"),
                        Slice("value-1"))
                  .ok());
  const uint64_t last_sequence = table.LastSequence();
  const size_t memory_usage = table.ApproximateMemoryUsage();

  EXPECT_EQ(table.Add(2, static_cast<ValueType>(9), Slice("bad"),
                      Slice("value"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(table.Add(kMaxSequenceNumber, ValueType::kValue,
                      Slice("max"), Slice("value"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(table.Add(1, ValueType::kValue, Slice("duplicate"),
                      Slice("value"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(table.LastSequence(), last_sequence);
  EXPECT_EQ(table.ApproximateMemoryUsage(), memory_usage);

  std::string value = "unchanged";
  bool is_deleted = true;
  EXPECT_EQ(table.Get(Slice("missing"), nullptr, &is_deleted).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(table.Get(Slice("missing"), &value, nullptr).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(table.Get(Slice("missing"), &value, &is_deleted).code(),
            Code::kNotFound);
  EXPECT_EQ(value, "unchanged");
  EXPECT_TRUE(is_deleted);

  ASSERT_TRUE(table.Add(2, ValueType::kDeletion, Slice("key"),
                        Slice("ignored"))
                  .ok());
  ASSERT_TRUE(table.Get(Slice("key"), &value, &is_deleted).ok());
  EXPECT_TRUE(is_deleted);
  EXPECT_TRUE(value.empty());

  ASSERT_TRUE(table.Add(3, ValueType::kValue, Slice("key"),
                        Slice("value-3"))
                  .ok());
  ASSERT_TRUE(table.Get(Slice("key"), &value, &is_deleted).ok());
  EXPECT_FALSE(is_deleted);
  EXPECT_EQ(value, "value-3");
}

TEST(MemTableTest, SupportsConcurrentReadersAndSingleWriter) {
  constexpr int kUserKeyCount = 64;
  constexpr int kWriteCount = 20000;
  constexpr int kReaderCount = 3;

  MemTable table;
  std::atomic<bool> done{false};
  std::atomic<int> failures{0};

  std::vector<std::thread> readers;
  for (int reader = 0; reader < kReaderCount; ++reader) {
    readers.emplace_back([&, reader] {
      std::mt19937 random(0x310000U + reader);
      std::uniform_int_distribution<int> key_distribution(
          0, kUserKeyCount - 1);
      while (!done.load(std::memory_order_acquire)) {
        for (int i = 0; i < 16; ++i) {
          const std::string key =
              "key-" + std::to_string(key_distribution(random));
          std::string value;
          bool is_deleted = false;
          const Status status =
              table.Get(Slice(key), &value, &is_deleted);
          if (!status.ok() &&
              status.code() != Code::kNotFound) {
            ++failures;
          }
          if (status.ok() &&
              (is_deleted || value.rfind("value-", 0) != 0)) {
            ++failures;
          }
        }

        auto iterator = table.NewIterator();
        iterator->SeekToFirst();
        InternalKey previous;
        bool has_previous = false;
        while (iterator->Valid()) {
          const InternalKey current = iterator->key();
          if (has_previous &&
              CompareInternalKeys(previous, current) >= 0) {
            ++failures;
            break;
          }
          previous = current;
          has_previous = true;
          iterator->Next();
        }
      }
    });
  }

  std::thread writer([&] {
    for (int i = 0; i < kWriteCount; ++i) {
      const int key_number = i % kUserKeyCount;
      const std::string key =
          "key-" + std::to_string(key_number);
      const std::string value =
          "value-" + std::to_string(i + 1);
      if (!table.Add(static_cast<uint64_t>(i + 1),
                     ValueType::kValue, Slice(key), Slice(value))
               .ok()) {
        ++failures;
      }
      if (i % 64 == 0) {
        std::this_thread::yield();
      }
    }
    done.store(true, std::memory_order_release);
  });

  writer.join();
  for (std::thread& reader : readers) {
    reader.join();
  }

  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(table.LastSequence(),
            static_cast<uint64_t>(kWriteCount));
  EXPECT_GT(table.ApproximateMemoryUsage(), 0U);
}

}  // namespace
}  // namespace raftkv
