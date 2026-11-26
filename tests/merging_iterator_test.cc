#include "raftkv/storage/merging_iterator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "raftkv/storage/memtable.h"
#include "raftkv/storage/sstable.h"

namespace raftkv {
namespace {

struct IteratorEntry {
  InternalKey key;
  std::string value;
};

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-merging-XXXXXX";
    const char* created = ::mkdtemp(pattern);
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed");
    }
    path_ = created;
  }

  ~TempDir() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  std::string Path(const std::string& name) const {
    return path_ + "/" + name;
  }

 private:
  std::string path_;
};

class TestIterator : public StorageIterator {
 public:
  explicit TestIterator(std::vector<IteratorEntry> entries)
      : entries_(std::move(entries)) {}

  bool Valid() const override {
    return position_ < entries_.size();
  }

  void SeekToFirst() override {
    status_ = Status::OK();
    if (fail_seek_once_) {
      fail_seek_once_ = false;
      position_ = entries_.size();
      status_ = Status::Corruption("injected seek failure");
      return;
    }
    position_ = 0;
  }

  void Seek(Slice user_key) override {
    InternalKey target;
    target.user_key = user_key.ToString();
    target.sequence = kMaxSequenceNumber;
    target.type = ValueType::kValue;
    Seek(target);
  }

  void Seek(const InternalKey& target) override {
    status_ = Status::OK();
    if (fail_seek_once_) {
      fail_seek_once_ = false;
      position_ = entries_.size();
      status_ = Status::Corruption("injected seek failure");
      return;
    }

    const auto found = std::lower_bound(
        entries_.begin(), entries_.end(), target,
        [](const IteratorEntry& entry,
           const InternalKey& wanted) {
          return CompareInternalKeys(entry.key, wanted) < 0;
        });
    position_ = static_cast<size_t>(found - entries_.begin());
  }

  void Next() override {
    if (!Valid()) {
      return;
    }
    ++position_;
    if (fail_next_once_) {
      fail_next_once_ = false;
      position_ = entries_.size();
      status_ = Status::Corruption("injected next failure");
    }
  }

  const InternalKey& key() const override {
    return entries_[position_].key;
  }

  Slice value() const override {
    return Slice(entries_[position_].value);
  }

  const Status& status() const override {
    return status_;
  }

  void FailNextOnce() {
    fail_next_once_ = true;
  }

  void FailSeekOnce() {
    fail_seek_once_ = true;
  }

 private:
  std::vector<IteratorEntry> entries_;
  size_t position_ = 0;
  Status status_;
  bool fail_next_once_ = false;
  bool fail_seek_once_ = false;
};

std::string KeyFor(int value) {
  const std::string digits = std::to_string(value);
  return "key-" + std::string(8 - digits.size(), '0') + digits;
}

std::vector<std::pair<std::string, std::string>>
CollectVisible(MergingIterator* iterator) {
  std::vector<std::pair<std::string, std::string>> visible;
  while (iterator->Valid()) {
    visible.emplace_back(iterator->key().user_key,
                         iterator->value().ToString());
    iterator->Next();
  }
  return visible;
}

std::vector<std::unique_ptr<StorageIterator>> NewChildren(
    const std::vector<std::unique_ptr<MemTable>>& tables) {
  std::vector<std::unique_ptr<StorageIterator>> children;
  children.reserve(tables.size());
  for (const std::unique_ptr<MemTable>& table : tables) {
    children.push_back(table->NewIterator());
  }
  return children;
}

TEST(MergingIteratorTest,
     MergesFourSourcesAndMatchesReferenceModel) {
  constexpr int kSourceCount = 4;
  constexpr int kUserKeyCount = 1000;
  constexpr int kWriteCount = 5000;

  std::vector<std::unique_ptr<MemTable>> tables;
  for (int i = 0; i < kSourceCount; ++i) {
    tables.push_back(std::make_unique<MemTable>());
  }

  std::map<std::string,
           std::pair<InternalKey, std::string>>
      latest;
  std::mt19937 random(0x4D455247U);
  std::uniform_int_distribution<int> source_distribution(
      0, kSourceCount - 1);
  std::uniform_int_distribution<int> key_distribution(
      0, kUserKeyCount - 1);
  std::uniform_int_distribution<int> operation_distribution(0, 4);

  for (int i = 0; i < kWriteCount; ++i) {
    const int source = source_distribution(random);
    const std::string key = KeyFor(key_distribution(random));
    const uint64_t sequence = static_cast<uint64_t>(i + 1);
    const bool deleted =
        operation_distribution(random) == 0;
    const ValueType type =
        deleted ? ValueType::kDeletion : ValueType::kValue;
    const std::string stored_value =
        deleted ? std::string()
                : "value-" + std::to_string(sequence);

    ASSERT_TRUE(tables[source]
                    ->Add(sequence, type, Slice(key),
                          Slice(stored_value))
                    .ok());
    latest[key] =
        std::make_pair(InternalKey{key, sequence, type},
                       stored_value);
  }

  MergingIterator iterator(NewChildren(tables));
  iterator.SeekToFirst();
  ASSERT_TRUE(iterator.status().ok());

  auto expected = latest.begin();
  while (expected != latest.end()) {
    if (expected->second.first.type == ValueType::kDeletion) {
      ++expected;
      continue;
    }

    ASSERT_TRUE(iterator.Valid());
    EXPECT_EQ(iterator.key().user_key, expected->first);
    EXPECT_EQ(CompareInternalKeys(iterator.key(),
                                  expected->second.first),
              0);
    EXPECT_EQ(iterator.value().ToString(),
              expected->second.second);
    iterator.Next();
    ++expected;
  }

  EXPECT_FALSE(iterator.Valid());
  EXPECT_EQ(iterator.status(), Status::OK());
}

TEST(MergingIteratorTest,
     AppliesNewestValueAndTombstoneAcrossSources) {
  std::vector<std::unique_ptr<MemTable>> tables;
  for (int i = 0; i < 4; ++i) {
    tables.push_back(std::make_unique<MemTable>());
  }

  ASSERT_TRUE(tables[0]
                  ->Add(1, ValueType::kValue, Slice("dup"),
                        Slice("dup-value"))
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(2, ValueType::kValue,
                        Slice("delete-latest"), Slice("old"))
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(3, ValueType::kDeletion,
                        Slice("value-latest"), Slice())
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(4, ValueType::kValue, Slice("multi"),
                        Slice("multi-1"))
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(5, ValueType::kValue,
                        Slice("same-source"), Slice("same-5"))
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(6, ValueType::kValue,
                        Slice("same-source"), Slice("same-6"))
                  .ok());

  ASSERT_TRUE(tables[1]
                  ->Add(1, ValueType::kValue, Slice("dup"),
                        Slice("dup-value"))
                  .ok());
  ASSERT_TRUE(tables[1]
                  ->Add(50, ValueType::kDeletion,
                        Slice("delete-latest"), Slice())
                  .ok());
  ASSERT_TRUE(tables[1]
                  ->Add(60, ValueType::kValue, Slice("multi"),
                        Slice("multi-60"))
                  .ok());
  ASSERT_TRUE(tables[1]
                  ->Add(70, ValueType::kValue,
                        Slice("value-latest"), Slice("value-70"))
                  .ok());

  ASSERT_TRUE(tables[2]
                  ->Add(1, ValueType::kDeletion,
                        Slice("old-delete"), Slice())
                  .ok());
  ASSERT_TRUE(tables[2]
                  ->Add(90, ValueType::kValue, Slice("multi"),
                        Slice("multi-90"))
                  .ok());
  ASSERT_TRUE(tables[3]
                  ->Add(80, ValueType::kValue,
                        Slice("old-delete"), Slice("visible-80"))
                  .ok());

  MergingIterator iterator(NewChildren(tables));
  iterator.SeekToFirst();
  ASSERT_TRUE(iterator.status().ok());

  const std::vector<std::pair<std::string, std::string>>
      expected = {
          {"dup", "dup-value"},
          {"multi", "multi-90"},
          {"old-delete", "visible-80"},
          {"same-source", "same-6"},
          {"value-latest", "value-70"},
      };
  EXPECT_EQ(CollectVisible(&iterator), expected);
  EXPECT_EQ(iterator.status(), Status::OK());
}

TEST(MergingIteratorTest, CanExposeLatestTombstoneForCompaction) {
  std::vector<std::unique_ptr<MemTable>> tables;
  tables.push_back(std::make_unique<MemTable>());
  ASSERT_TRUE(tables[0]
                  ->Add(1, ValueType::kValue, Slice("a"),
                        Slice("old"))
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(2, ValueType::kDeletion, Slice("a"),
                        Slice())
                  .ok());
  ASSERT_TRUE(tables[0]
                  ->Add(3, ValueType::kValue, Slice("b"),
                        Slice("value-b"))
                  .ok());

  MergingIterator hidden(NewChildren(tables));
  hidden.SeekToFirst();
  ASSERT_TRUE(hidden.Valid());
  EXPECT_EQ(hidden.key().user_key, "b");

  MergingIterator kept(
      NewChildren(tables),
      DeletionOutputPolicy::kKeep);
  kept.SeekToFirst();
  ASSERT_TRUE(kept.Valid());
  EXPECT_EQ(kept.key().user_key, "a");
  EXPECT_EQ(kept.key().type, ValueType::kDeletion);
  EXPECT_TRUE(kept.value().empty());
  kept.Next();
  ASSERT_TRUE(kept.Valid());
  EXPECT_EQ(kept.key().user_key, "b");
}

TEST(MergingIteratorTest, SupportsUserAndInternalKeySeek) {
  std::vector<std::unique_ptr<MemTable>> tables;
  tables.push_back(std::make_unique<MemTable>());
  tables.push_back(std::make_unique<MemTable>());

  ASSERT_TRUE(tables[0]->Add(1, ValueType::kValue, Slice("a"),
                             Slice("a-1"))
                  .ok());
  ASSERT_TRUE(tables[0]->Add(3, ValueType::kValue, Slice("c"),
                             Slice("c-3"))
                  .ok());
  ASSERT_TRUE(tables[0]->Add(4, ValueType::kValue, Slice("d"),
                             Slice("d-4"))
                  .ok());

  ASSERT_TRUE(tables[1]->Add(1, ValueType::kValue, Slice("b"),
                             Slice("b-old"))
                  .ok());
  ASSERT_TRUE(tables[1]->Add(2, ValueType::kDeletion, Slice("b"),
                             Slice())
                  .ok());
  ASSERT_TRUE(tables[1]->Add(5, ValueType::kValue, Slice("e"),
                             Slice("e-5"))
                  .ok());
  ASSERT_TRUE(tables[1]->Add(30, ValueType::kValue, Slice("d"),
                             Slice("d-30"))
                  .ok());
  ASSERT_TRUE(tables[1]->Add(50, ValueType::kValue, Slice("f"),
                             Slice("f-50"))
                  .ok());

  MergingIterator iterator(NewChildren(tables));

  iterator.Seek(Slice("b"));
  ASSERT_TRUE(iterator.Valid());
  EXPECT_EQ(iterator.key().user_key, "c");

  iterator.Seek(Slice("dd"));
  ASSERT_TRUE(iterator.Valid());
  EXPECT_EQ(iterator.key().user_key, "e");
  EXPECT_EQ(iterator.value().ToString(), "e-5");

  iterator.Seek(Slice("g"));
  EXPECT_FALSE(iterator.Valid());
  iterator.Next();
  EXPECT_FALSE(iterator.Valid());

  iterator.Seek(InternalKey{"c", 3, ValueType::kValue});
  ASSERT_TRUE(iterator.Valid());
  EXPECT_EQ(iterator.key().user_key, "c");
  EXPECT_EQ(iterator.key().sequence, 3U);

  iterator.Seek(InternalKey{"d", 29, ValueType::kValue});
  ASSERT_TRUE(iterator.Valid());
  EXPECT_EQ(iterator.key().user_key, "d");
  EXPECT_EQ(iterator.key().sequence, 4U);
  EXPECT_EQ(iterator.value().ToString(), "d-4");

  // 序列号按降序参与内部键排序，d@31 之前的 d@30 不会被跳过。
  iterator.Seek(InternalKey{"d", 31, ValueType::kValue});
  ASSERT_TRUE(iterator.Valid());
  EXPECT_EQ(iterator.key().user_key, "d");
  EXPECT_EQ(iterator.key().sequence, 30U);
  EXPECT_EQ(iterator.value().ToString(), "d-30");
  EXPECT_EQ(iterator.status(), Status::OK());
}

TEST(MergingIteratorTest, HandlesEmptyInputsAndRepeatedSeek) {
  MergingIterator empty(
      std::vector<std::unique_ptr<StorageIterator>>{});
  empty.SeekToFirst();
  EXPECT_FALSE(empty.Valid());
  EXPECT_EQ(empty.status(), Status::OK());

  empty.Seek(Slice("missing"));
  EXPECT_FALSE(empty.Valid());
  empty.Next();
  EXPECT_FALSE(empty.Valid());
  EXPECT_EQ(empty.status(), Status::OK());

  std::vector<std::unique_ptr<StorageIterator>> children;
  MemTable table;
  children.push_back(table.NewIterator());
  MergingIterator iterator(std::move(children));
  iterator.SeekToFirst();
  EXPECT_FALSE(iterator.Valid());
  iterator.Next();
  EXPECT_FALSE(iterator.Valid());
  EXPECT_EQ(iterator.status(), Status::OK());
}

TEST(MergingIteratorTest, PropagatesChildErrorsAndRecovers) {
  auto failing = std::make_unique<TestIterator>(
      std::vector<IteratorEntry>{
          {InternalKey{"a", 2, ValueType::kValue}, "a"},
          {InternalKey{"d", 4, ValueType::kValue}, "d"},
      });
  auto healthy = std::make_unique<TestIterator>(
      std::vector<IteratorEntry>{
          {InternalKey{"b", 3, ValueType::kValue}, "b"},
      });
  TestIterator* failing_ptr = failing.get();

  std::vector<std::unique_ptr<StorageIterator>> children;
  children.push_back(std::move(failing));
  children.push_back(std::move(healthy));
  MergingIterator iterator(std::move(children));

  failing_ptr->FailNextOnce();
  iterator.SeekToFirst();
  EXPECT_FALSE(iterator.Valid());
  EXPECT_EQ(iterator.status().code(), Code::kCorruption);

  iterator.SeekToFirst();
  ASSERT_TRUE(iterator.Valid());
  EXPECT_EQ(iterator.key().user_key, "a");
  EXPECT_EQ(iterator.status(), Status::OK());

  failing_ptr->FailSeekOnce();
  iterator.Seek(Slice("b"));
  EXPECT_FALSE(iterator.Valid());
  EXPECT_EQ(iterator.status().code(), Code::kCorruption);

  iterator.SeekToFirst();
  ASSERT_TRUE(iterator.Valid());
  const std::vector<std::pair<std::string, std::string>>
      expected = {
          {"a", "a"},
          {"b", "b"},
          {"d", "d"},
      };
  EXPECT_EQ(CollectVisible(&iterator), expected);
  EXPECT_EQ(iterator.status(), Status::OK());
}

TEST(MergingIteratorTest,
     SupportsMemTableAndSSTableMixedChildren) {
  TempDir directory;
  const std::string path = directory.Path("mixed.sst");
  MemTable table;
  ASSERT_TRUE(table.Add(5, ValueType::kValue, Slice("a"),
                        Slice("mem-a"))
                  .ok());
  ASSERT_TRUE(table.Add(15, ValueType::kValue, Slice("c"),
                        Slice("mem-c"))
                  .ok());

  SSTableBuilder builder(path);
  ASSERT_TRUE(builder
                  .Add(InternalKey{"a", 1, ValueType::kValue},
                       Slice("sst-a"))
                  .ok());
  ASSERT_TRUE(builder
                  .Add(InternalKey{"b", 10, ValueType::kValue},
                       Slice("sst-b"))
                  .ok());
  ASSERT_TRUE(builder
                  .Add(InternalKey{"c", 12, ValueType::kValue},
                       Slice("sst-c"))
                  .ok());
  ASSERT_TRUE(builder.Finish().ok());

  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(SSTableReader::Open(path, nullptr, &reader).ok());

  std::vector<std::unique_ptr<StorageIterator>> children;
  children.push_back(table.NewIterator());
  children.push_back(reader->NewIterator());
  MergingIterator iterator(std::move(children));
  iterator.SeekToFirst();
  ASSERT_TRUE(iterator.status().ok());

  const std::vector<std::pair<std::string, std::string>>
      expected = {
          {"a", "mem-a"},
          {"b", "sst-b"},
          {"c", "mem-c"},
      };
  EXPECT_EQ(CollectVisible(&iterator), expected);
  EXPECT_EQ(iterator.status(), Status::OK());
}

TEST(MergingIteratorTest,
     MatchesReferenceWithHundredThousandVersions) {
  constexpr int kSourceCount = 4;
  constexpr int kUserKeyCount = 10000;
  constexpr int kWriteCount = 100000;

  std::vector<std::unique_ptr<MemTable>> tables;
  for (int i = 0; i < kSourceCount; ++i) {
    tables.push_back(std::make_unique<MemTable>());
  }

  std::map<std::string, std::string> latest_value;
  std::map<std::string, bool> latest_deleted;
  std::mt19937 random(0x4D455253U);
  std::uniform_int_distribution<int> key_distribution(
      0, kUserKeyCount - 1);
  std::uniform_int_distribution<int> operation_distribution(0, 9);

  for (int i = 0; i < kWriteCount; ++i) {
    const int source = i % kSourceCount;
    const std::string key = KeyFor(key_distribution(random));
    const uint64_t sequence = static_cast<uint64_t>(i + 1);
    const bool deleted =
        operation_distribution(random) < 2;
    const ValueType type =
        deleted ? ValueType::kDeletion : ValueType::kValue;
    const std::string value =
        deleted ? std::string()
                : "value-" + std::to_string(sequence);

    ASSERT_TRUE(tables[source]
                    ->Add(sequence, type, Slice(key), Slice(value))
                    .ok());
    latest_deleted[key] = deleted;
    latest_value[key] = value;
  }

  MergingIterator iterator(NewChildren(tables));
  iterator.SeekToFirst();
  ASSERT_TRUE(iterator.status().ok());

  std::vector<std::pair<std::string, std::string>> expected;
  expected.reserve(latest_deleted.size());
  for (const auto& entry : latest_deleted) {
    if (!entry.second) {
      expected.emplace_back(entry.first,
                            latest_value[entry.first]);
    }
  }

  EXPECT_EQ(CollectVisible(&iterator), expected);
  EXPECT_EQ(iterator.status(), Status::OK());
}

}  // namespace
}  // namespace raftkv
