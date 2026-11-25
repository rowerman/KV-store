#include "raftkv/storage/wal.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-wal-XXXXXX";
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

struct WalRecord {
  uint64_t sequence = 0;
  ValueType type = ValueType::kValue;
  std::string key;
  std::string value;
};

std::vector<WalRecord> MakeRecords(int count) {
  std::vector<WalRecord> records;
  records.reserve(static_cast<size_t>(count));
  for (int i = 0; i < count; ++i) {
    WalRecord record;
    record.sequence = static_cast<uint64_t>(i + 1);
    record.type =
        i % 5 == 0 ? ValueType::kDeletion : ValueType::kValue;
    record.key = "key-" + std::to_string(i);
    if (i % 13 == 0) {
      record.key.push_back('\0');
      record.key.push_back(static_cast<char>(i % 256));
    }
    record.value =
        "value-" + std::to_string(record.sequence);
    records.push_back(std::move(record));
  }
  return records;
}

Status WriteRecords(const std::string& path,
                    const std::vector<WalRecord>& records,
                    bool append = false) {
  WalWriter writer(path);
  Status status = writer.Open(append);
  if (!status.ok()) {
    return status;
  }
  for (const WalRecord& record : records) {
    status = writer.Append(record.sequence, record.type,
                           Slice(record.key), Slice(record.value));
    if (!status.ok()) {
      return status;
    }
  }
  status = writer.Sync();
  if (!status.ok()) {
    return status;
  }
  return writer.Close();
}

std::vector<WalRecord> ReplayRecords(const std::string& path,
                                     Status* status,
                                     uint64_t* valid_bytes = nullptr) {
  std::vector<WalRecord> records;
  WalReader reader(path);
  *status = reader.Replay(
      [&](uint64_t sequence, ValueType type, Slice key,
          Slice value) {
        records.push_back(WalRecord{
            sequence, type, key.ToString(), value.ToString()});
      },
      valid_bytes);
  return records;
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

bool WriteLength(const std::string& path, uint64_t offset,
                 uint32_t value) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }

  char encoded[sizeof(uint32_t)] = {};
  EncodeFixed32(encoded, value);
  const bool ok =
      ::pwrite(fd, encoded, sizeof(encoded),
               static_cast<off_t>(offset)) ==
      static_cast<ssize_t>(sizeof(encoded));
  ::close(fd);
  return ok;
}

uint64_t RecordSize(const WalRecord& record) {
  const uint64_t value_size =
      record.type == ValueType::kValue ? record.value.size() : 0;
  return kWalHeaderSize + record.key.size() +
         kWalValueLengthSize + value_size;
}

TEST(WalTest, WritesAndReplaysThousandRecords) {
  TempDir directory;
  const std::string path = directory.Path("basic.wal");
  const std::vector<WalRecord> expected = MakeRecords(1000);

  ASSERT_TRUE(WriteRecords(path, expected).ok());
  ASSERT_EQ(std::filesystem::file_size(path),
            [&] {
              uint64_t size = 0;
              for (const WalRecord& record : expected) {
                size += RecordSize(record);
              }
              return size;
            }());

  Status status;
  uint64_t valid_bytes = 0;
  const std::vector<WalRecord> actual =
      ReplayRecords(path, &status, &valid_bytes);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(actual.size(), expected.size());
  EXPECT_EQ(valid_bytes, std::filesystem::file_size(path));

  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i].sequence, expected[i].sequence);
    EXPECT_EQ(actual[i].type, expected[i].type);
    EXPECT_EQ(actual[i].key, expected[i].key);
    const std::string expected_value =
        expected[i].type == ValueType::kValue
            ? expected[i].value
            : std::string();
    EXPECT_EQ(actual[i].value, expected_value);
  }
}

TEST(WalTest, RecoversRecordsBeforeTruncatedTail) {
  TempDir directory;
  const std::string path = directory.Path("truncated.wal");
  const std::vector<WalRecord> expected = MakeRecords(1000);
  ASSERT_TRUE(WriteRecords(path, expected).ok());

  const uint64_t original_size =
      std::filesystem::file_size(path);
  ASSERT_GE(original_size, 5U);
  std::filesystem::resize_file(path, original_size - 5);

  uint64_t expected_prefix = 0;
  for (size_t i = 0; i + 1 < expected.size(); ++i) {
    expected_prefix += RecordSize(expected[i]);
  }

  Status status;
  uint64_t valid_bytes = 0;
  const std::vector<WalRecord> actual =
      ReplayRecords(path, &status, &valid_bytes);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(actual.size(), 999U);
  EXPECT_EQ(valid_bytes, expected_prefix);
  EXPECT_LT(valid_bytes, original_size - 5);

  const std::string empty_path = directory.Path("empty.wal");
  ASSERT_TRUE(WriteRecords(empty_path, {}).ok());
  const std::vector<WalRecord> empty = ReplayRecords(
      empty_path, &status, &valid_bytes);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(empty.empty());
  EXPECT_EQ(valid_bytes, 0U);

  const std::string partial_header = directory.Path("partial.wal");
  {
    std::ofstream output(partial_header,
                         std::ios::binary | std::ios::trunc);
    output.write("abcde", 5);
  }
  const std::vector<WalRecord> partial = ReplayRecords(
      partial_header, &status, &valid_bytes);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(partial.empty());
  EXPECT_EQ(valid_bytes, 0U);
}

TEST(WalTest, RejectsCorruptionAndOversizedLengths) {
  TempDir directory;
  const std::string original = directory.Path("original.wal");
  const std::vector<WalRecord> records = MakeRecords(2);
  ASSERT_TRUE(WriteRecords(original, records).ok());

  const uint64_t second_offset = RecordSize(records[0]);
  const uint64_t key_offset = second_offset + kWalHeaderSize;
  const uint64_t value_length_offset =
      key_offset + records[1].key.size();
  const uint64_t value_offset =
      value_length_offset + kWalValueLengthSize;

  const std::vector<std::pair<uint64_t, unsigned char>> mutations = {
      {second_offset, 0x01},
      {second_offset + 12, 0x7F},
      {key_offset, 0x01},
      {value_offset, 0x01},
  };

  for (size_t i = 0; i < mutations.size(); ++i) {
    SCOPED_TRACE(i);
    const std::string damaged =
        directory.Path("damaged-" + std::to_string(i) + ".wal");
    std::filesystem::copy_file(
        original, damaged,
        std::filesystem::copy_options::overwrite_existing);
    ASSERT_TRUE(MutateByte(damaged, mutations[i].first,
                           mutations[i].second));

    Status status;
    uint64_t valid_bytes = 0;
    const std::vector<WalRecord> actual =
        ReplayRecords(damaged, &status, &valid_bytes);
    EXPECT_EQ(status.code(), Code::kCorruption);
    EXPECT_EQ(actual.size(), 1U);
    EXPECT_EQ(valid_bytes, second_offset);
  }

  const std::string oversized = directory.Path("oversized.wal");
  std::filesystem::copy_file(
      original, oversized,
      std::filesystem::copy_options::overwrite_existing);
  ASSERT_TRUE(WriteLength(
      oversized, second_offset + 13,
      static_cast<uint32_t>(kWalMaxRecordSize)));

  Status status;
  uint64_t valid_bytes = 0;
  const std::vector<WalRecord> actual =
      ReplayRecords(oversized, &status, &valid_bytes);
  EXPECT_EQ(status.code(), Code::kCorruption);
  EXPECT_EQ(actual.size(), 1U);
  EXPECT_EQ(valid_bytes, second_offset);
}

TEST(WalTest, SupportsAppendAndLifecycleValidation) {
  TempDir directory;
  const std::string path = directory.Path("append.wal");
  const std::vector<WalRecord> first = MakeRecords(2);
  std::vector<WalRecord> second = MakeRecords(5);
  second.erase(second.begin(), second.begin() + 2);
  ASSERT_TRUE(WriteRecords(path, first).ok());
  ASSERT_TRUE(WriteRecords(path, second, true).ok());

  Status status;
  uint64_t valid_bytes = 0;
  const std::vector<WalRecord> actual =
      ReplayRecords(path, &status, &valid_bytes);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(actual.size(), 5U);
  EXPECT_EQ(valid_bytes, std::filesystem::file_size(path));

  WalWriter writer(directory.Path("lifecycle.wal"));
  EXPECT_FALSE(writer.IsOpen());
  EXPECT_EQ(writer.Append(1, ValueType::kValue, Slice("k"),
                          Slice("v"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(writer.Sync().code(), Code::kInvalidArgument);
  EXPECT_EQ(writer.Close().code(), Code::kOk);
  ASSERT_TRUE(writer.Open().ok());
  EXPECT_TRUE(writer.IsOpen());
  EXPECT_EQ(writer.Open().code(), Code::kInvalidArgument);
  EXPECT_TRUE(writer.Close().ok());
  EXPECT_TRUE(writer.Close().ok());
  EXPECT_FALSE(writer.IsOpen());
}

TEST(WalTest, RejectsInvalidArgumentsAndMissingFiles) {
  TempDir directory;
  WalWriter writer(directory.Path("invalid.wal"));
  ASSERT_TRUE(writer.Open().ok());

  EXPECT_EQ(writer.Append(1, static_cast<ValueType>(9),
                          Slice("key"), Slice("value"))
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(writer.Append(kMaxSequenceNumber, ValueType::kValue,
                          Slice("key"), Slice("value"))
                .code(),
            Code::kInvalidArgument);

  const std::string path = directory.Path("invalid.wal");
  ASSERT_TRUE(writer.Close().ok());

  WalReader reader(path);
  EXPECT_EQ(reader.Replay(WalReplayCallback{}).code(),
            Code::kInvalidArgument);

  WalReader missing(directory.Path("missing.wal"));
  EXPECT_EQ(missing.Replay(
                    [](uint64_t, ValueType, Slice, Slice) {})
                .code(),
            Code::kIOError);
  EXPECT_EQ(WalWriter("").Open().code(),
            Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
