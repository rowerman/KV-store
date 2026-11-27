#include "raftkv/storage/lsm_tree.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-lsm-XXXXXX";
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

std::string KeyFor(int value) {
  const std::string digits = std::to_string(value);
  return "key-" + std::string(4 - digits.size(), '0') + digits;
}

TEST(LSMTreeTest, SupportsPutGetDeleteAndOutputContracts) {
  TempDir directory;
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(LSMTree::Open(directory.Path("db"), &tree).ok());

  ASSERT_TRUE(tree->Put("a", "value-a").ok());
  ASSERT_TRUE(tree->Put("b", "").ok());
  std::string value;
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "value-a");
  ASSERT_TRUE(tree->Get("b", &value).ok());
  EXPECT_TRUE(value.empty());

  value = "unchanged";
  EXPECT_EQ(tree->Get("missing", &value).code(),
            Code::kNotFound);
  EXPECT_EQ(value, "unchanged");
  EXPECT_EQ(tree->Get("a", nullptr).code(),
            Code::kInvalidArgument);

  ASSERT_TRUE(tree->Delete("a").ok());
  value = "unchanged";
  EXPECT_EQ(tree->Get("a", &value).code(),
            Code::kNotFound);
  EXPECT_EQ(value, "unchanged");

  const std::string binary_key("bin\0key", 7);
  const std::string binary_value("bin\0value", 9);
  ASSERT_TRUE(tree->Put(binary_key, binary_value).ok());
  ASSERT_TRUE(tree->Get(binary_key, &value).ok());
  EXPECT_EQ(value, binary_value);
}

TEST(LSMTreeTest, RecoversUnflushedWritesFromWal) {
  TempDir directory;
  const std::string path = directory.Path("db");
  {
    std::unique_ptr<LSMTree> tree;
    ASSERT_TRUE(LSMTree::Open(path, &tree).ok());
    ASSERT_TRUE(tree->Put("a", "value-a").ok());
    ASSERT_TRUE(tree->Put("b", "value-b").ok());
    ASSERT_TRUE(tree->Delete("b").ok());
  }

  std::unique_ptr<LSMTree> reopened;
  ASSERT_TRUE(LSMTree::Open(path, &reopened).ok());
  std::string value;
  ASSERT_TRUE(reopened->Get("a", &value).ok());
  EXPECT_EQ(value, "value-a");
  EXPECT_EQ(reopened->Get("b", &value).code(),
            Code::kNotFound);
  ASSERT_TRUE(reopened->Put("c", "value-c").ok());
  ASSERT_TRUE(reopened->Get("c", &value).ok());
  EXPECT_EQ(value, "value-c");
}

TEST(LSMTreeTest, ScansAcrossMemTablesAndCompactedFiles) {
  TempDir directory;
  LSMTOptions options;
  options.memtable_size_limit = 128;
  options.max_immutable_memtables = 2;

  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), options, &tree).ok());
  const std::string padding(128, 'x');
  for (int i = 0; i < 24; ++i) {
    ASSERT_TRUE(
        tree->Put(KeyFor(i), padding + std::to_string(i)).ok());
  }
  for (int i = 3; i < 24; i += 4) {
    ASSERT_TRUE(tree->Delete(KeyFor(i)).ok());
  }

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(
      tree->Scan(KeyFor(2), KeyFor(16), 5, &rows).ok());
  ASSERT_EQ(rows.size(), 5U);
  EXPECT_EQ(rows[0].first, KeyFor(2));
  EXPECT_EQ(rows[1].first, KeyFor(4));
  EXPECT_EQ(rows[2].first, KeyFor(5));
  EXPECT_EQ(rows[3].first, KeyFor(6));
  EXPECT_EQ(rows[4].first, KeyFor(8));

  ASSERT_TRUE(
      tree->Scan(KeyFor(0), KeyFor(100), 100, &rows).ok());
  int expected_visible = 0;
  for (int i = 0; i < 24; ++i) {
    if (i < 3 || (i - 3) % 4 != 0) {
      ++expected_visible;
    }
  }
  EXPECT_EQ(rows.size(), static_cast<size_t>(expected_visible));

  std::vector<std::pair<std::string, std::string>> unchanged =
      {{"unchanged", "unchanged"}};
  EXPECT_EQ(
      tree->Scan(KeyFor(5), KeyFor(5), 10, &unchanged).code(),
      Code::kInvalidArgument);
  ASSERT_EQ(unchanged.size(), 1U);
  EXPECT_EQ(unchanged[0].first, "unchanged");

  ASSERT_TRUE(
      tree->Scan(KeyFor(0), KeyFor(10), 0, &rows).ok());
  EXPECT_TRUE(rows.empty());
}

TEST(LSMTreeTest, CheckpointUsesHardLinksAndRestoreRollsBackData) {
  TempDir directory;
  const std::string db_path = directory.Path("db");
  const std::string checkpoint_path =
      directory.Path("nested/checkpoint");
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(LSMTree::Open(db_path, &tree).ok());
  ASSERT_TRUE(tree->Put("a", "old-a").ok());
  ASSERT_TRUE(tree->Put("b", "value-b").ok());
  ASSERT_TRUE(tree->CreateCheckpoint(checkpoint_path).ok());

  struct stat source_stat {};
  struct stat checkpoint_stat {};
  ASSERT_EQ(::stat(
                (db_path + "/CURRENT").c_str(), &source_stat),
            0);
  ASSERT_EQ(::stat(
                (checkpoint_path + "/CURRENT").c_str(),
                &checkpoint_stat),
            0);
  EXPECT_EQ(source_stat.st_ino, checkpoint_stat.st_ino);

  ASSERT_TRUE(tree->Put("a", "new-a").ok());
  ASSERT_TRUE(tree->Put("c", "value-c").ok());
  ASSERT_TRUE(tree->RestoreFromCheckpoint(checkpoint_path).ok());
  EXPECT_FALSE(std::filesystem::exists(checkpoint_path));

  std::string value;
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "old-a");
  ASSERT_TRUE(tree->Get("b", &value).ok());
  EXPECT_EQ(value, "value-b");
  EXPECT_EQ(tree->Get("c", &value).code(),
            Code::kNotFound);

  ASSERT_TRUE(tree->Put("d", "value-d").ok());
  ASSERT_TRUE(tree->Get("d", &value).ok());
  EXPECT_EQ(value, "value-d");
}

TEST(LSMTreeTest, RejectsInvalidCheckpointAndRestorePaths) {
  TempDir directory;
  const std::string db_path = directory.Path("db");
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(LSMTree::Open(db_path, &tree).ok());
  ASSERT_TRUE(tree->Put("a", "value-a").ok());

  const std::string existing = directory.Path("existing");
  ASSERT_TRUE(std::filesystem::create_directory(existing));
  EXPECT_EQ(tree->CreateCheckpoint(existing).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(
      tree->RestoreFromCheckpoint(
          directory.Path("missing")).code(),
      Code::kInvalidArgument);

  std::string value;
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "value-a");
}

TEST(LSMTreeTest, SupportsConcurrentReadersAndWriters) {
  TempDir directory;
  LSMTOptions options;
  options.memtable_size_limit = 512;
  options.max_immutable_memtables = 2;
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), options, &tree).ok());

  constexpr int kThreads = 4;
  constexpr int kKeysPerThread = 100;
  std::atomic<int> failures{0};
  std::vector<std::thread> workers;
  for (int thread = 0; thread < kThreads; ++thread) {
    workers.emplace_back([&, thread] {
      for (int i = 0; i < kKeysPerThread; ++i) {
        const std::string key =
            "thread-" + std::to_string(thread) + "-" +
            std::to_string(i);
        if (!tree->Put(key, std::string(80, 'v')).ok()) {
          ++failures;
        }
        std::string value;
        const Status status = tree->Get(key, &value);
        if (!status.ok() || value.size() != 80) {
          ++failures;
        }
      }
    });
  }
  for (std::thread& worker : workers) {
    worker.join();
  }
  workers.clear();
  EXPECT_EQ(failures.load(), 0);

  std::vector<std::pair<std::string, std::string>> rows;
  ASSERT_TRUE(tree->Scan("thread-", "thread~", 1000, &rows).ok());
  EXPECT_EQ(rows.size(), kThreads * kKeysPerThread);
  for (int i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i] {
      for (int j = 0; j < kKeysPerThread; ++j) {
        const std::string key =
            "reader-" + std::to_string(i) + "-" +
            std::to_string(j);
        if (!tree->Put(key, "reader-value").ok()) {
          ++failures;
        }
        std::string value;
        if (!tree->Get(key, &value).ok() ||
            value != "reader-value") {
          ++failures;
        }
      }
    });
  }
  for (std::thread& worker : workers) {
    worker.join();
  }
  EXPECT_EQ(failures.load(), 0);
}

}  // namespace
}  // namespace raftkv
