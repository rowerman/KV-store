#include "raftkv/raft/raft_log.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-raft-log-XXXXXX";
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

proto::LogEntry MakeEntry(uint64_t index, uint64_t term,
                          proto::EntryType type,
                          std::string payload = std::string()) {
  proto::LogEntry entry;
  entry.set_index(index);
  entry.set_term(term);
  entry.set_entry_type(type);
  entry.set_payload(std::move(payload));
  return entry;
}

std::vector<proto::LogEntry> MakeEntries(
    uint64_t first_index, size_t count, uint64_t term,
    size_t payload_size = 8) {
  std::vector<proto::LogEntry> entries;
  entries.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    std::string payload(payload_size,
                        static_cast<char>('a' + i % 26));
    entries.push_back(MakeEntry(
        first_index + i, term, proto::ENTRY_TYPE_NORMAL,
        std::move(payload)));
  }
  return entries;
}

void ExpectEntry(const RaftLog& log, const proto::LogEntry& expected) {
  proto::LogEntry actual;
  const Status status = log.EntryAt(expected.index(), &actual);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(actual.index(), expected.index());
  EXPECT_EQ(actual.term(), expected.term());
  EXPECT_EQ(actual.entry_type(), expected.entry_type());
  EXPECT_EQ(actual.payload(), expected.payload());
}

std::vector<std::filesystem::path> SegmentPaths(
    const std::string& directory) {
  std::vector<std::filesystem::path> paths;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(directory)) {
    const std::string name = entry.path().filename().string();
    if (name.rfind("RAFT-LOG-", 0) == 0) {
      paths.push_back(entry.path());
    }
  }
  std::sort(paths.begin(), paths.end());
  return paths;
}

bool FlipByte(const std::string& path, uint64_t offset) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  unsigned char value = 0;
  bool ok = ::pread(fd, &value, sizeof(value),
                    static_cast<off_t>(offset)) ==
            static_cast<ssize_t>(sizeof(value));
  value ^= 0x5AU;
  ok = ok &&
       ::pwrite(fd, &value, sizeof(value),
                static_cast<off_t>(offset)) ==
           static_cast<ssize_t>(sizeof(value));
  ok = ok && ::fsync(fd) == 0;
  ::close(fd);
  return ok;
}

uint64_t RecordSize(size_t payload_size) {
  return 8 + 8 + 8 + 1 + 4 + payload_size;
}

TEST(RaftLogTest, AppendsReadsAndRestoresHardState) {
  TempDir directory;
  const std::string path = directory.Path("raft-log");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());

  EXPECT_EQ(log->FirstIndex(), 1U);
  EXPECT_EQ(log->LastIndex(), 0U);
  uint64_t term = 99;
  ASSERT_TRUE(log->TermAt(0, &term).ok());
  EXPECT_EQ(term, 0U);

  ASSERT_TRUE(log->SaveHardState(3, 2).ok());
  EXPECT_EQ(log->hard_state().current_term, 3U);
  ASSERT_TRUE(log->hard_state().voted_for.has_value());
  EXPECT_EQ(log->hard_state().voted_for.value(), 2U);

  std::vector<proto::LogEntry> entries;
  entries.push_back(MakeEntry(
      1, 1, proto::ENTRY_TYPE_NORMAL,
      std::string("\0abc\xff", 5)));
  entries.push_back(MakeEntry(
      2, 1, proto::ENTRY_TYPE_CONF_CHANGE, "conf"));
  entries.push_back(MakeEntry(
      3, 2, proto::ENTRY_TYPE_NO_OP));
  ASSERT_TRUE(log->Append(std::move(entries)).ok());
  EXPECT_EQ(log->FirstIndex(), 1U);
  EXPECT_EQ(log->LastIndex(), 3U);

  proto::LogEntry actual =
      MakeEntry(77, 77, proto::ENTRY_TYPE_NO_OP);
  EXPECT_EQ(log->EntryAt(4, &actual).code(), Code::kNotFound);
  EXPECT_EQ(actual.index(), 77U);

  ExpectEntry(*log, MakeEntry(
      1, 1, proto::ENTRY_TYPE_NORMAL,
      std::string("\0abc\xff", 5)));
  ExpectEntry(*log, MakeEntry(
      2, 1, proto::ENTRY_TYPE_CONF_CHANGE, "conf"));
  ExpectEntry(*log, MakeEntry(
      3, 2, proto::ENTRY_TYPE_NO_OP));

  log.reset();
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  EXPECT_EQ(log->hard_state().current_term, 3U);
  ASSERT_TRUE(log->hard_state().voted_for.has_value());
  EXPECT_EQ(log->hard_state().voted_for.value(), 2U);
  EXPECT_EQ(log->LastIndex(), 3U);
  ExpectEntry(*log, MakeEntry(
      2, 1, proto::ENTRY_TYPE_CONF_CHANGE, "conf"));

  ASSERT_TRUE(log->SaveHardState(4, std::nullopt).ok());
  EXPECT_FALSE(log->hard_state().voted_for.has_value());
  EXPECT_EQ(log->SaveHardState(3, 1).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->hard_state().current_term, 4U);
}

TEST(RaftLogTest, ValidatesAppendBeforeChangingLog) {
  TempDir directory;
  RaftLogOptions options;
  options.max_entry_payload_size = 4;
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(
      directory.Path("raft-log"), options, &log).ok());
  ASSERT_TRUE(log->SaveHardState(2, std::nullopt).ok());
  EXPECT_TRUE(log->Append({}).ok());

  EXPECT_EQ(log->Append(
                {MakeEntry(
                    2, 1, proto::ENTRY_TYPE_NORMAL, "x")})
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->LastIndex(), 0U);

  ASSERT_TRUE(log->Append(
      {MakeEntry(1, 1, proto::ENTRY_TYPE_NORMAL, "ok")})
                  .ok());
  EXPECT_EQ(log->Append(
                {MakeEntry(
                    3, 1, proto::ENTRY_TYPE_NORMAL, "bad")})
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->LastIndex(), 1U);

  EXPECT_EQ(log->Append(
                {MakeEntry(
                    2, 0, proto::ENTRY_TYPE_NORMAL, "x")})
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->Append(
                {MakeEntry(
                    2, 3, proto::ENTRY_TYPE_NORMAL, "x")})
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->Append(
                {MakeEntry(
                    2, 1, static_cast<proto::EntryType>(9), "x")})
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->Append(
                {MakeEntry(
                    2, 1, proto::ENTRY_TYPE_NORMAL, "12345")})
                .code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->LastIndex(), 1U);
  ExpectEntry(*log, MakeEntry(
      1, 1, proto::ENTRY_TYPE_NORMAL, "ok"));
}

TEST(RaftLogTest, RotatesSegmentsAndRecoversAllEntries) {
  TempDir directory;
  const std::string path = directory.Path("raft-log");
  RaftLogOptions options;
  options.segment_size = 64;
  options.max_entry_payload_size = 1024;
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(path, options, &log).ok());
  ASSERT_TRUE(log->SaveHardState(3, std::nullopt).ok());

  const std::vector<proto::LogEntry> expected =
      MakeEntries(1, 12, 1, 40);
  ASSERT_TRUE(log->Append(expected).ok());
  EXPECT_GE(SegmentPaths(path).size(), 3U);
  log.reset();

  ASSERT_TRUE(RaftLog::Open(path, options, &log).ok());
  EXPECT_EQ(log->FirstIndex(), 1U);
  EXPECT_EQ(log->LastIndex(), 12U);
  for (const proto::LogEntry& entry : expected) {
    ExpectEntry(*log, entry);
  }
}

TEST(RaftLogTest, TruncatesSuffixAndAppendsReplacement) {
  TempDir directory;
  const std::string path = directory.Path("raft-log");
  RaftLogOptions options;
  options.segment_size = 96;
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(path, options, &log).ok());
  ASSERT_TRUE(log->SaveHardState(4, std::nullopt).ok());

  const std::vector<proto::LogEntry> original =
      MakeEntries(1, 10, 1, 30);
  ASSERT_TRUE(log->Append(original).ok());
  ASSERT_TRUE(log->TruncateSuffix(6).ok());
  EXPECT_EQ(log->LastIndex(), 5U);

  const proto::LogEntry replacement =
      MakeEntry(6, 2, proto::ENTRY_TYPE_NORMAL, "replacement");
  ASSERT_TRUE(log->Append({replacement}).ok());
  EXPECT_EQ(log->LastIndex(), 6U);
  EXPECT_TRUE(log->TruncateSuffix(7).ok());
  EXPECT_EQ(log->TruncateSuffix(0).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->TruncateSuffix(8).code(),
            Code::kInvalidArgument);

  log.reset();
  ASSERT_TRUE(RaftLog::Open(path, options, &log).ok());
  ExpectEntry(*log, original[4]);
  ExpectEntry(*log, replacement);
  EXPECT_EQ(log->EntryAt(10, nullptr).code(),
            Code::kInvalidArgument);

  ASSERT_TRUE(log->TruncateSuffix(1).ok());
  EXPECT_EQ(log->LastIndex(), 0U);
  log.reset();
  ASSERT_TRUE(RaftLog::Open(path, options, &log).ok());
  EXPECT_EQ(log->FirstIndex(), 1U);
  EXPECT_EQ(log->LastIndex(), 0U);
}

TEST(RaftLogTest, CompactsPrefixAndContinuesAfterRestart) {
  TempDir directory;
  const std::string path = directory.Path("raft-log");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  ASSERT_TRUE(log->SaveHardState(5, std::nullopt).ok());
  ASSERT_TRUE(log->Append(MakeEntries(1, 12, 2, 12)).ok());

  ASSERT_TRUE(log->CompactPrefix(5, 2).ok());
  EXPECT_EQ(log->snapshot_marker().last_included_index, 5U);
  EXPECT_EQ(log->snapshot_marker().last_included_term, 2U);
  EXPECT_EQ(log->FirstIndex(), 6U);
  EXPECT_EQ(log->LastIndex(), 12U);
  uint64_t term = 99;
  ASSERT_TRUE(log->TermAt(5, &term).ok());
  EXPECT_EQ(term, 2U);
  EXPECT_EQ(log->TermAt(4, &term).code(), Code::kNotFound);
  EXPECT_EQ(log->EntryAt(5, nullptr).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->CompactPrefix(6, 1).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->snapshot_marker().last_included_index, 5U);

  log.reset();
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  EXPECT_EQ(log->FirstIndex(), 6U);
  EXPECT_EQ(log->LastIndex(), 12U);
  ExpectEntry(*log, MakeEntry(
      12, 2, proto::ENTRY_TYPE_NORMAL,
      std::string(12, 'l')));

  ASSERT_TRUE(log->SaveHardState(6, std::nullopt).ok());
  const proto::LogEntry next =
      MakeEntry(13, 3, proto::ENTRY_TYPE_NORMAL, "next");
  ASSERT_TRUE(log->Append({next}).ok());
  ASSERT_TRUE(log->CompactPrefix(13, 3).ok());
  EXPECT_EQ(log->FirstIndex(), 14U);
  EXPECT_EQ(log->LastIndex(), 13U);
  log.reset();
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  EXPECT_EQ(log->snapshot_marker().last_included_index, 13U);
  EXPECT_EQ(log->snapshot_marker().last_included_term, 3U);
}

TEST(RaftLogTest, RecoversTruncatedFinalRecordTail) {
  TempDir directory;
  const std::string path = directory.Path("raft-log");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  ASSERT_TRUE(log->SaveHardState(5, std::nullopt).ok());
  const std::vector<proto::LogEntry> entries =
      MakeEntries(1, 3, 2, 17);
  ASSERT_TRUE(log->Append(entries).ok());
  log.reset();

  const std::vector<std::filesystem::path> segments =
      SegmentPaths(path);
  ASSERT_EQ(segments.size(), 1U);
  const uint64_t original_size =
      std::filesystem::file_size(segments[0]);
  ASSERT_GT(original_size, 3U);
  std::filesystem::resize_file(segments[0], original_size - 3);

  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  EXPECT_EQ(log->LastIndex(), 2U);
  ExpectEntry(*log, entries[0]);
  ExpectEntry(*log, entries[1]);
  EXPECT_EQ(std::filesystem::file_size(segments[0]),
            RecordSize(entries[0].payload().size()) +
                RecordSize(entries[1].payload().size()));
}

TEST(RaftLogTest, RejectsCorruptRecordsAndMetadata) {
  TempDir directory;
  const std::string record_path = directory.Path("record");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(record_path, &log).ok());
  ASSERT_TRUE(log->SaveHardState(3, std::nullopt).ok());
  const std::vector<proto::LogEntry> entries =
      MakeEntries(1, 3, 1, 9);
  ASSERT_TRUE(log->Append(entries).ok());
  log.reset();

  const std::vector<std::filesystem::path> segments =
      SegmentPaths(record_path);
  ASSERT_EQ(segments.size(), 1U);
  ASSERT_TRUE(FlipByte(
      segments[0].string(),
      RecordSize(entries[0].payload().size()) + 3));
  EXPECT_EQ(RaftLog::Open(record_path, &log).code(),
            Code::kCorruption);

  const std::string metadata_path = directory.Path("metadata");
  ASSERT_TRUE(RaftLog::Open(metadata_path, &log).ok());
  ASSERT_TRUE(log->SaveHardState(2, 1).ok());
  log.reset();
  ASSERT_TRUE(FlipByte(
      metadata_path + "/raft_meta.bin", 0));
  EXPECT_EQ(RaftLog::Open(metadata_path, &log).code(),
            Code::kCorruption);

  const std::string missing_metadata = directory.Path("missing");
  ASSERT_TRUE(std::filesystem::create_directories(missing_metadata));
  {
    std::ofstream segment(
        missing_metadata + "/RAFT-LOG-000001.wal",
        std::ios::binary);
  }
  EXPECT_EQ(RaftLog::Open(missing_metadata, &log).code(),
            Code::kCorruption);
}

TEST(RaftLogTest, ValidatesDirectoryAndOptionsWithoutReplacingOutput) {
  TempDir directory;
  const std::string valid_path = directory.Path("valid");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(valid_path, &log).ok());
  ASSERT_TRUE(log->SaveHardState(2, std::nullopt).ok());
  ASSERT_TRUE(log->Append(
      {MakeEntry(1, 1, proto::ENTRY_TYPE_NORMAL, "value")})
                  .ok());

  const std::string file_path = directory.Path("not-a-directory");
  {
    std::ofstream output(file_path);
    output << "file";
  }
  EXPECT_EQ(RaftLog::Open(file_path, &log).code(),
            Code::kIOError);
  ASSERT_NE(log, nullptr);
  EXPECT_EQ(log->LastIndex(), 1U);

  RaftLogOptions invalid_options;
  invalid_options.segment_size = 0;
  EXPECT_EQ(RaftLog::Open(
                directory.Path("invalid-size"),
                invalid_options, &log)
                .code(),
            Code::kInvalidArgument);

  EXPECT_EQ(RaftLog::Open(
                directory.Path("null-output"), nullptr)
                .code(),
            Code::kInvalidArgument);
}

TEST(RaftLogTest, ResetsToInstalledSnapshotAndContinues) {
  TempDir directory;
  const std::string path = directory.Path("raft-log");
  std::unique_ptr<RaftLog> log;
  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  ASSERT_TRUE(log->SaveHardState(5, std::nullopt).ok());
  ASSERT_TRUE(log->Append(MakeEntries(1, 3, 2, 7)).ok());

  ASSERT_TRUE(log->ResetToSnapshot(10, 3).ok());
  EXPECT_EQ(log->FirstIndex(), 11U);
  EXPECT_EQ(log->LastIndex(), 10U);
  EXPECT_EQ(log->snapshot_marker().last_included_index, 10U);
  EXPECT_EQ(log->snapshot_marker().last_included_term, 3U);
  uint64_t term = 0;
  ASSERT_TRUE(log->TermAt(10, &term).ok());
  EXPECT_EQ(term, 3U);
  EXPECT_EQ(log->EntryAt(1, nullptr).code(),
            Code::kInvalidArgument);
  EXPECT_TRUE(log->ResetToSnapshot(10, 3).ok());
  EXPECT_EQ(log->ResetToSnapshot(9, 2).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(log->ResetToSnapshot(11, 6).code(),
            Code::kInvalidArgument);

  const proto::LogEntry next =
      MakeEntry(11, 5, proto::ENTRY_TYPE_NORMAL, "after-snapshot");
  ASSERT_TRUE(log->Append({next}).ok());
  EXPECT_EQ(log->LastIndex(), 11U);
  log.reset();

  ASSERT_TRUE(RaftLog::Open(path, &log).ok());
  EXPECT_EQ(log->FirstIndex(), 11U);
  EXPECT_EQ(log->LastIndex(), 11U);
  ExpectEntry(*log, next);
}

}  // namespace
}  // namespace raftkv
