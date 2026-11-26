#include "raftkv/storage/version_set.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "raftkv/storage/memtable.h"
#include "raftkv/storage/merging_iterator.h"
#include "raftkv/storage/sstable.h"

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-version-set-XXXXXX";
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

  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

FileMetaData BuildTable(VersionSet* versions,
                        const std::vector<InternalKey>& keys,
                        uint64_t* file_number_out = nullptr) {
  uint64_t file_number = 0;
  EXPECT_TRUE(versions->NewFileNumber(&file_number).ok());
  const std::string path = versions->TableFileName(file_number);
  SSTableBuilder builder(path);
  for (const InternalKey& key : keys) {
    EXPECT_TRUE(
        builder
            .Add(key, Slice("value-" + std::to_string(key.sequence)))
            .ok());
  }
  EXPECT_TRUE(builder.Finish().ok());

  FileMetaData metadata;
  metadata.file_number = file_number;
  metadata.file_size = std::filesystem::file_size(path);
  metadata.smallest_key = keys.front().user_key;
  metadata.largest_key = keys.back().user_key;
  if (file_number_out != nullptr) {
    *file_number_out = file_number;
  }
  return metadata;
}

std::shared_ptr<MemTable> MakeMemTable(
    const std::vector<InternalKey>& keys) {
  auto table = std::make_shared<MemTable>();
  for (const InternalKey& key : keys) {
    EXPECT_TRUE(
        table
            ->Add(key.sequence, key.type, Slice(key.user_key),
                  Slice(key.type == ValueType::kValue
                            ? "value-" +
                                  std::to_string(key.sequence)
                            : std::string()))
            .ok());
  }
  return table;
}

void Flush(VersionSet* versions,
           const std::vector<InternalKey>& keys,
           const std::string& wal_path = std::string()) {
  auto table = MakeMemTable(keys);
  ASSERT_TRUE(
      versions
          ->MaybeScheduleFlush(FlushRequest{table, wal_path})
          .ok());
  ASSERT_TRUE(versions->WaitForBackgroundWork().ok());
}

TEST(VersionSetTest, ValidatesAndAppliesVersionEdits) {
  Version base;
  VersionEdit first;
  first.added_files.push_back(
      FileAddition{0, FileMetaData{1, 10, "a", "b"}});
  first.added_files.push_back(
      FileAddition{0, FileMetaData{2, 10, "c", "d"}});

  Version updated;
  ASSERT_TRUE(ApplyVersionEdit(base, first, &updated).ok());
  ASSERT_EQ(updated.levels[0].size(), 2U);
  EXPECT_EQ(updated.levels[0][0].file_number, 2U);
  EXPECT_EQ(updated.levels[0][1].file_number, 1U);

  VersionEdit second;
  second.deleted_files.push_back(FileDeletion{0, 2});
  second.added_files.push_back(
      FileAddition{1, FileMetaData{3, 20, "a", "d"}});
  Version with_l1;
  ASSERT_TRUE(ApplyVersionEdit(updated, second, &with_l1).ok());
  ASSERT_EQ(with_l1.levels[1].size(), 1U);
  EXPECT_EQ(with_l1.levels[1][0].file_number, 3U);

  Version unchanged = with_l1;
  VersionEdit invalid;
  invalid.added_files.push_back(
      FileAddition{1, FileMetaData{4, 5, "b", "c"}});
  EXPECT_EQ(ApplyVersionEdit(with_l1, invalid, &unchanged).code(),
            Code::kInvalidArgument);
  ASSERT_EQ(unchanged.levels[1].size(), 1U);
  EXPECT_EQ(unchanged.levels[1][0].file_number, 3U);

  Version reverse;
  reverse.levels[0].push_back(
      FileMetaData{1, 1, "a", "a"});
  reverse.levels[0].push_back(
      FileMetaData{2, 1, "b", "b"});
  EXPECT_EQ(ValidateVersion(reverse).code(),
            Code::kInvalidArgument);
}

TEST(VersionSetTest, PersistsEditsAndTruncatesManifestTail) {
  TempDir directory;
  std::unique_ptr<VersionSet> versions;
  ASSERT_TRUE(
      VersionSet::Open(directory.path(), &versions).ok());

  const FileMetaData metadata = BuildTable(
      versions.get(),
      {{"a", 1, ValueType::kValue},
       {"b", 2, ValueType::kValue}});
  VersionEdit edit;
  edit.added_files.push_back(FileAddition{0, metadata});
  edit.has_last_sequence = true;
  edit.last_sequence = 2;
  ASSERT_TRUE(versions->LogAndApply(std::move(edit)).ok());
  EXPECT_EQ(versions->LastSequence(), 2U);

  std::vector<std::string> live_files;
  ASSERT_TRUE(versions->GetLiveFiles(&live_files).ok());
  std::string manifest_name;
  for (const std::string& name : live_files) {
    if (name.rfind("MANIFEST-", 0) == 0) {
      manifest_name = name;
      break;
    }
  }
  ASSERT_FALSE(manifest_name.empty());
  const std::string manifest_path =
      directory.path() + "/" + manifest_name;
  versions.reset();

  {
    std::ofstream output(
        manifest_path, std::ios::binary | std::ios::app);
    output << "tail";
  }
  const uint64_t damaged_size =
      std::filesystem::file_size(manifest_path);

  ASSERT_TRUE(
      VersionSet::Open(directory.path(), &versions).ok());
  const VersionPtr recovered = versions->CurrentVersion();
  ASSERT_NE(recovered, nullptr);
  ASSERT_EQ(recovered->levels[0].size(), 1U);
  EXPECT_EQ(recovered->levels[0][0].file_number,
            metadata.file_number);
  EXPECT_EQ(versions->LastSequence(), 2U);
  EXPECT_LT(std::filesystem::file_size(manifest_path),
            damaged_size);
}

TEST(VersionSetTest, FlushesMemTableAndDeletesWalAfterCommit) {
  TempDir directory;
  const std::string wal_path =
      directory.path() + "/000000.wal";
  {
    std::ofstream wal(wal_path, std::ios::binary);
    wal << "wal";
  }

  std::unique_ptr<VersionSet> versions;
  ASSERT_TRUE(
      VersionSet::Open(directory.path(), &versions).ok());
  Flush(
      versions.get(),
      {{"a", 1, ValueType::kValue},
       {"b", 2, ValueType::kDeletion},
       {"c", 3, ValueType::kValue}},
      wal_path);

  EXPECT_FALSE(std::filesystem::exists(wal_path));
  const VersionPtr current = versions->CurrentVersion();
  ASSERT_EQ(current->levels[0].size(), 1U);
  const FileMetaData metadata = current->levels[0][0];
  EXPECT_EQ(metadata.smallest_key, "a");
  EXPECT_EQ(metadata.largest_key, "c");

  std::unique_ptr<SSTableReader> reader;
  ASSERT_TRUE(
      SSTableReader::Open(
          versions->TableFileName(metadata.file_number),
          nullptr, &reader)
          .ok());
  SSTableLookupResult result;
  ASSERT_TRUE(reader->Get(Slice("a"), &result).ok());
  EXPECT_EQ(result.value, "value-1");
  ASSERT_TRUE(reader->Get(Slice("b"), &result).ok());
  EXPECT_EQ(result.key.type, ValueType::kDeletion);

  versions.reset();
  ASSERT_TRUE(
      VersionSet::Open(directory.path(), &versions).ok());
  EXPECT_EQ(versions->CurrentVersion()->levels[0].size(), 1U);
}

TEST(VersionSetTest, CompactsFourL0FilesAndKeepsOldVersionFiles) {
  TempDir directory;
  std::unique_ptr<VersionSet> versions;
  ASSERT_TRUE(
      VersionSet::Open(directory.path(), &versions).ok());

  Flush(versions.get(),
        {{"b", 1, ValueType::kValue},
         {"d", 2, ValueType::kValue}});
  Flush(versions.get(),
        {{"b", 3, ValueType::kDeletion},
         {"c", 4, ValueType::kValue}});
  Flush(versions.get(),
        {{"a", 5, ValueType::kValue},
         {"d", 6, ValueType::kDeletion}});

  VersionPtr old_version = versions->CurrentVersion();
  ASSERT_EQ(old_version->levels[0].size(), 3U);
  std::vector<std::string> old_paths;
  for (const FileMetaData& metadata : old_version->levels[0]) {
    old_paths.push_back(
        versions->TableFileName(metadata.file_number));
  }

  Flush(versions.get(),
        {{"c", 7, ValueType::kValue},
         {"e", 8, ValueType::kValue}});

  const VersionPtr compacted = versions->CurrentVersion();
  EXPECT_TRUE(compacted->levels[0].empty());
  ASSERT_FALSE(compacted->levels[1].empty());
  for (const std::string& path : old_paths) {
    EXPECT_TRUE(std::filesystem::exists(path));
  }

  bool found_tombstone = false;
  for (const FileMetaData& metadata : compacted->levels[1]) {
    std::unique_ptr<SSTableReader> reader;
    ASSERT_TRUE(
        SSTableReader::Open(
            versions->TableFileName(metadata.file_number),
            nullptr, &reader)
            .ok());
    SSTableLookupResult result;
    if (reader->Get(Slice("b"), &result).ok() &&
        result.key.type == ValueType::kDeletion) {
      found_tombstone = true;
    }
  }
  EXPECT_TRUE(found_tombstone);

  ASSERT_TRUE(versions->WaitForBackgroundWork().ok());
  for (const std::string& path : old_paths) {
    EXPECT_TRUE(std::filesystem::exists(path));
  }
  old_version.reset();
  ASSERT_TRUE(versions->WaitForBackgroundWork().ok());
  for (const std::string& path : old_paths) {
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

TEST(VersionSetTest, CompactsL1ToL2AfterTenMegabytes) {
  TempDir directory;
  std::unique_ptr<VersionSet> versions;
  ASSERT_TRUE(
      VersionSet::Open(directory.path(), &versions).ok());

  const std::string value(700U * 1024U, 'x');
  uint64_t sequence = 1;
  for (int file = 0; file < 4; ++file) {
    auto table = std::make_shared<MemTable>();
    for (int entry = 0; entry < 4; ++entry) {
      const std::string key =
          "key-" + std::to_string(file * 4 + entry);
      ASSERT_TRUE(
          table->Add(sequence++, ValueType::kValue,
                     Slice(key), Slice(value))
              .ok());
    }
    ASSERT_TRUE(
        versions
            ->MaybeScheduleFlush(FlushRequest{table, ""})
            .ok());
    ASSERT_TRUE(versions->WaitForBackgroundWork().ok());
  }

  const VersionPtr current = versions->CurrentVersion();
  EXPECT_TRUE(current->levels[0].empty());
  EXPECT_FALSE(current->levels[1].empty());
  EXPECT_FALSE(current->levels[2].empty());
}

}  // namespace
}  // namespace raftkv
