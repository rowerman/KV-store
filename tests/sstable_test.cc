#include "raftkv/storage/sstable.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/storage/lru_cache.h"

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-sstable-XXXXXX";
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

struct StoredHandle {
  uint64_t offset = 0;
  uint32_t size = 0;
};

std::string ReadFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

bool MutateByte(const std::string& path, uint64_t offset,
                unsigned char mask) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  unsigned char value = 0;
  bool ok = ::pread(fd, &value, sizeof(value),
                    static_cast<off_t>(offset)) ==
            static_cast<ssize_t>(sizeof(value));
  value ^= mask;
  ok = ok &&
       ::pwrite(fd, &value, sizeof(value),
                static_cast<off_t>(offset)) ==
           static_cast<ssize_t>(sizeof(value));
  ::close(fd);
  return ok;
}

bool ReadStoredBlock(const std::string& file, uint64_t offset,
                     uint64_t size, std::string* payload) {
  if (offset > file.size() || size > file.size() - offset ||
      size < kSSTableBlockTrailerSize) {
    return false;
  }
  const size_t block_size = static_cast<size_t>(size);
  const size_t payload_size = block_size - kSSTableBlockTrailerSize;
  const char* block = file.data() + offset;
  if (DecodeFixed32(block + payload_size) != payload_size) {
    return false;
  }
  if (DecodeFixed32(block + payload_size + sizeof(uint32_t)) !=
      Crc32(block, payload_size)) {
    return false;
  }
  payload->assign(block, payload_size);
  return true;
}

bool DecodeStoredIndex(const std::string& file,
                       std::vector<StoredHandle>* handles) {
  if (file.size() < kSSTableFooterSize) {
    return false;
  }
  const size_t footer_offset = file.size() - kSSTableFooterSize;
  const uint64_t index_offset =
      DecodeFixed64(file.data() + footer_offset + 16);
  const uint64_t index_size =
      DecodeFixed64(file.data() + footer_offset + 24);

  std::string payload;
  if (!ReadStoredBlock(file, index_offset, index_size, &payload)) {
    return false;
  }

  handles->clear();
  size_t position = 0;
  constexpr size_t kFixedSize = 4 + 8 + 1 + 8 + 4;
  while (position < payload.size()) {
    if (payload.size() - position < kFixedSize) {
      return false;
    }
    const uint32_t user_key_size =
        DecodeFixed32(payload.data() + position);
    if (payload.size() - position - kFixedSize < user_key_size) {
      return false;
    }
    const size_t handle_position =
        position + 4 + user_key_size + 8 + 1;
    StoredHandle handle;
    handle.offset =
        DecodeFixed64(payload.data() + handle_position);
    handle.size = DecodeFixed32(
        payload.data() + handle_position + sizeof(uint64_t));
    handles->push_back(handle);
    position += kFixedSize + user_key_size;
  }
  return true;
}

std::string KeyFor(int i) {
  std::string digits = std::to_string(i);
  return "key-" + std::string(10 - digits.size(), '0') + digits;
}

std::string ValueFor(int i) {
  return "value-" + std::to_string(i);
}

TEST(SSTableTest, WritesReadsAndIteratesFiftyThousandEntries) {
  constexpr int kEntryCount = 50000;
  TempDir directory;
  const std::string path = directory.Path("large.sst");

  SSTableBuilder builder(path);
  for (int i = 0; i < kEntryCount; ++i) {
    InternalKey key{KeyFor(i), static_cast<uint64_t>(i + 1),
                    ValueType::kValue};
    const Status status = builder.Add(key, Slice(ValueFor(i)));
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  ASSERT_TRUE(builder.Finish().ok());

  auto cache = std::make_shared<LRUCache>(4U * 1024U * 1024U);
  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(
      SSTableReader::Open(path, cache, &reader).ok());
  ASSERT_NE(reader, nullptr);
  EXPECT_EQ(reader->EntryCount(),
            static_cast<uint64_t>(kEntryCount));

  std::mt19937 random(0x53445434U);
  std::uniform_int_distribution<int> index_distribution(
      0, kEntryCount - 1);
  for (int i = 0; i < 5000; ++i) {
    const int index = index_distribution(random);
    const std::string key = KeyFor(index);
    SSTableLookupResult result;
    const Status get_status = reader->Get(Slice(key), &result);
    ASSERT_TRUE(get_status.ok()) << key << ": "
                                 << get_status.ToString();
    EXPECT_EQ(result.key.user_key, key);
    EXPECT_EQ(result.key.sequence,
              static_cast<uint64_t>(index + 1));
    EXPECT_EQ(result.key.type, ValueType::kValue);
    EXPECT_EQ(result.value, ValueFor(index));
  }
  EXPECT_GT(cache->Size(), 0U);

  auto iterator = reader->NewIterator();
  iterator->SeekToFirst();
  for (int i = 0; i < kEntryCount; ++i) {
    ASSERT_TRUE(iterator->Valid()) << i;
    EXPECT_EQ(iterator->key().user_key, KeyFor(i));
    EXPECT_EQ(iterator->value().ToString(), ValueFor(i));
    iterator->Next();
  }
  EXPECT_FALSE(iterator->Valid());
  EXPECT_TRUE(iterator->status().ok());

  iterator->Seek(Slice(KeyFor(25000)));
  ASSERT_TRUE(iterator->Valid());
  EXPECT_EQ(iterator->key().user_key, KeyFor(25000));
  iterator->Seek(Slice("key-999999"));
  EXPECT_FALSE(iterator->Valid());
  EXPECT_TRUE(iterator->status().ok());
}

TEST(SSTableTest, HandlesVersionsTombstonesBinaryKeysAndEmptyTable) {
  TempDir directory;
  const std::string path = directory.Path("versions.sst");
  const std::string binary_key("bin\0key", 7);

  SSTableBuilder builder(path, 128);
  ASSERT_TRUE(builder.Add(
      InternalKey{"a", 5, ValueType::kValue}, Slice("new")).ok());
  ASSERT_TRUE(builder.Add(
      InternalKey{"a", 4, ValueType::kDeletion}, Slice("ignored")).ok());
  ASSERT_TRUE(builder.Add(
      InternalKey{"a", 3, ValueType::kValue}, Slice("old")).ok());
  ASSERT_TRUE(builder.Add(
      InternalKey{"b", 2, ValueType::kDeletion}, Slice("ignored")).ok());
  ASSERT_TRUE(builder.Add(
      InternalKey{binary_key, 1, ValueType::kValue},
      Slice("bin\0value", 9)).ok());
  ASSERT_TRUE(builder.Finish().ok());

  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(SSTableReader::Open(path, nullptr, &reader).ok());

  SSTableLookupResult result;
  const Status get_status = reader->Get(Slice("a"), &result);
  ASSERT_TRUE(get_status.ok()) << get_status.ToString();
  EXPECT_EQ(result.key.sequence, 5U);
  EXPECT_EQ(result.value, "new");

  ASSERT_TRUE(reader->Get(Slice("b"), &result).ok());
  EXPECT_EQ(result.key.type, ValueType::kDeletion);
  EXPECT_TRUE(result.value.empty());

  ASSERT_TRUE(reader->Get(Slice(binary_key), &result).ok());
  EXPECT_EQ(result.value, std::string("bin\0value", 9));
  EXPECT_EQ(reader->Get(Slice("missing"), &result).code(),
            Code::kNotFound);

  const std::string empty_path = directory.Path("empty.sst");
  SSTableBuilder empty_builder(empty_path);
  ASSERT_TRUE(empty_builder.Finish().ok());
  std::unique_ptr<SSTableReader> empty_reader;
  ASSERT_TRUE(SSTableReader::Open(empty_path, nullptr,
                                  &empty_reader).ok());
  EXPECT_EQ(empty_reader->EntryCount(), 0U);
  EXPECT_EQ(empty_reader->Get(Slice("a"), &result).code(),
            Code::kNotFound);
  auto empty_iterator = empty_reader->NewIterator();
  empty_iterator->SeekToFirst();
  EXPECT_FALSE(empty_iterator->Valid());
  EXPECT_TRUE(empty_iterator->status().ok());
}

TEST(SSTableTest, ValidatesBuilderInputAndLifecycle) {
  TempDir directory;
  const std::string invalid_path = directory.Path("invalid.sst");
  SSTableBuilder invalid_builder(invalid_path);
  EXPECT_EQ(invalid_builder.Add(
                InternalKey{"a", 1, static_cast<ValueType>(9)},
                Slice("value"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_FALSE(std::filesystem::exists(invalid_path));

  const std::string path = directory.Path("ordered.sst");
  SSTableBuilder builder(path);
  ASSERT_TRUE(builder.Add(
      InternalKey{"b", 1, ValueType::kValue}, Slice("b")).ok());
  EXPECT_EQ(builder.Add(
                InternalKey{"a", 1, ValueType::kValue}, Slice("a"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(builder.Add(
                InternalKey{"c", kMaxSequenceNumber,
                            ValueType::kValue},
                Slice("c"))
                .code(),
            Code::kInvalidArgument);
  ASSERT_TRUE(builder.Finish().ok());
  EXPECT_EQ(builder.Add(
                InternalKey{"c", 2, ValueType::kValue}, Slice("c"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(builder.Finish().code(), Code::kInvalidArgument);

  SSTableBuilder zero_block(directory.Path("zero.sst"), 0);
  EXPECT_EQ(zero_block.Finish().code(), Code::kInvalidArgument);
}

TEST(SSTableTest, DetectsFooterFilterIndexAndDataCorruption) {
  TempDir directory;
  const std::string original = directory.Path("original.sst");
  SSTableBuilder builder(original, 128);
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(builder.Add(
        InternalKey{KeyFor(i), static_cast<uint64_t>(i + 1),
                    ValueType::kValue},
        Slice(ValueFor(i))).ok());
  }
  ASSERT_TRUE(builder.Finish().ok());

  const std::string file = ReadFile(original);
  ASSERT_GE(file.size(), kSSTableFooterSize);
  const size_t footer_offset = file.size() - kSSTableFooterSize;
  const uint64_t filter_offset =
      DecodeFixed64(file.data() + footer_offset);
  const uint64_t filter_size =
      DecodeFixed64(file.data() + footer_offset + 8);
  const uint64_t index_offset =
      DecodeFixed64(file.data() + footer_offset + 16);
  const uint64_t index_size =
      DecodeFixed64(file.data() + footer_offset + 24);
  std::vector<StoredHandle> handles;
  ASSERT_TRUE(DecodeStoredIndex(file, &handles));
  ASSERT_FALSE(handles.empty());

  const std::vector<std::pair<std::string, uint64_t>>
      open_corruptions = {
          {"footer", file.size() - 1},
          {"filter", filter_offset + filter_size - 1},
          {"index", index_offset + index_size - 1},
      };
  for (const auto& corruption : open_corruptions) {
    const std::string damaged =
        directory.Path(corruption.first + ".sst");
    std::filesystem::copy_file(
        original, damaged,
        std::filesystem::copy_options::overwrite_existing);
    ASSERT_TRUE(MutateByte(damaged, corruption.second, 0x01));
    std::unique_ptr<SSTableReader> reader;
    EXPECT_EQ(SSTableReader::Open(damaged, nullptr, &reader).code(),
              Code::kCorruption);
  }

  const std::string data_damaged = directory.Path("data.sst");
  std::filesystem::copy_file(
      original, data_damaged,
      std::filesystem::copy_options::overwrite_existing);
  ASSERT_TRUE(MutateByte(
      data_damaged, handles[0].offset + handles[0].size - 1, 0x01));
  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(
      SSTableReader::Open(data_damaged, nullptr, &reader).ok());
  SSTableLookupResult result;
  EXPECT_EQ(reader->Get(Slice(KeyFor(0)), &result).code(),
            Code::kCorruption);
}

TEST(SSTableTest, IteratorReportsLaterBlockCorruption) {
  TempDir directory;
  const std::string original = directory.Path("iterator.sst");
  SSTableBuilder builder(original, 96);
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(builder.Add(
        InternalKey{KeyFor(i), static_cast<uint64_t>(i + 1),
                    ValueType::kValue},
        Slice(ValueFor(i))).ok());
  }
  ASSERT_TRUE(builder.Finish().ok());

  const std::string file = ReadFile(original);
  std::vector<StoredHandle> handles;
  ASSERT_TRUE(DecodeStoredIndex(file, &handles));
  ASSERT_GE(handles.size(), 2U);

  const std::string damaged = directory.Path("later-block.sst");
  std::filesystem::copy_file(
      original, damaged,
      std::filesystem::copy_options::overwrite_existing);
  ASSERT_TRUE(MutateByte(
      damaged, handles[1].offset + handles[1].size - 1, 0x01));

  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(SSTableReader::Open(damaged, nullptr, &reader).ok());
  auto iterator = reader->NewIterator();
  iterator->SeekToFirst();
  while (iterator->Valid()) {
    iterator->Next();
  }
  EXPECT_EQ(iterator->status().code(), Code::kCorruption);
}

TEST(SSTableTest, SupportsConcurrentReadsWithSharedCache) {
  constexpr int kEntryCount = 2000;
  constexpr int kThreadCount = 4;
  TempDir directory;
  const std::string path = directory.Path("concurrent.sst");
  SSTableBuilder builder(path);
  for (int i = 0; i < kEntryCount; ++i) {
    ASSERT_TRUE(builder.Add(
        InternalKey{KeyFor(i), static_cast<uint64_t>(i + 1),
                    ValueType::kValue},
        Slice(ValueFor(i))).ok());
  }
  ASSERT_TRUE(builder.Finish().ok());

  auto cache = std::make_shared<LRUCache>(1024U * 1024U);
  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(SSTableReader::Open(path, cache, &reader).ok());

  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int thread_index = 0; thread_index < kThreadCount;
       ++thread_index) {
    threads.emplace_back([&, thread_index] {
      for (int i = 0; i < 1000; ++i) {
        const int index =
            (thread_index * 503 + i * 37) % kEntryCount;
        SSTableLookupResult result;
        const Status status =
            reader->Get(Slice(KeyFor(index)), &result);
        if (!status.ok() || result.value != ValueFor(index)) {
          ++failures;
        }
      }
      auto iterator = reader->NewIterator();
      iterator->Seek(Slice(KeyFor(thread_index * 100)));
      if (!iterator->Valid()) {
        ++failures;
      }
    });
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(failures.load(), 0);
}

}  // namespace
}  // namespace raftkv
