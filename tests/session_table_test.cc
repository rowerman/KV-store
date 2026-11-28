#include "raftkv/server/session_table.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-session-table-XXXXXX";
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

std::string ReadFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("open file for read failed");
  }
  return std::string(
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>());
}

void WriteFile(const std::string& path, const std::string& data) {
  std::ofstream output(
      path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("open file for write failed");
  }
  output.write(data.data(), static_cast<std::streamsize>(data.size()));
  if (!output.good()) {
    throw std::runtime_error("write file failed");
  }
}

TEST(SessionTableTest, TracksIndependentClientWatermarks) {
  SessionTable sessions;
  EXPECT_TRUE(sessions.ShouldApply(1, 1));
  EXPECT_TRUE(sessions.ShouldApply(2, 1));

  sessions.RecordApplied(1, 5);
  EXPECT_FALSE(sessions.ShouldApply(1, 5));
  EXPECT_FALSE(sessions.ShouldApply(1, 3));
  EXPECT_TRUE(sessions.ShouldApply(1, 6));
  EXPECT_TRUE(sessions.ShouldApply(2, 1));
  EXPECT_TRUE(sessions.ShouldApply(3, 1));

  sessions.RecordApplied(1, 4);
  EXPECT_FALSE(sessions.ShouldApply(1, 5));
  sessions.RecordApplied(1, 7);
  EXPECT_FALSE(sessions.ShouldApply(1, 7));
  EXPECT_TRUE(sessions.ShouldApply(1, 8));
}

TEST(SessionTableTest, SavesDeterministicallyAndLoads) {
  TempDir directory;
  SessionTable first;
  first.RecordApplied(8, 4);
  first.RecordApplied(2, 7);
  ASSERT_TRUE(first.Save(directory.Path("first.bin")).ok());

  SessionTable second;
  second.RecordApplied(2, 7);
  second.RecordApplied(8, 4);
  ASSERT_TRUE(second.Save(directory.Path("second.bin")).ok());
  EXPECT_EQ(
      ReadFile(directory.Path("first.bin")),
      ReadFile(directory.Path("second.bin")));

  SessionTable loaded;
  ASSERT_TRUE(loaded.Load(directory.Path("first.bin")).ok());
  EXPECT_FALSE(loaded.ShouldApply(8, 4));
  EXPECT_TRUE(loaded.ShouldApply(8, 5));
  EXPECT_FALSE(loaded.ShouldApply(2, 7));
  EXPECT_TRUE(loaded.ShouldApply(2, 8));
  EXPECT_TRUE(loaded.ShouldApply(9, 1));
}

TEST(SessionTableTest, CorruptLoadKeepsExistingState) {
  TempDir directory;
  SessionTable source;
  source.RecordApplied(1, 3);
  ASSERT_TRUE(source.Save(directory.Path("sessions.bin")).ok());

  std::string encoded =
      ReadFile(directory.Path("sessions.bin"));
  ASSERT_FALSE(encoded.empty());
  encoded.back() ^= 0x01;
  WriteFile(directory.Path("sessions.bin"), encoded);

  SessionTable loaded;
  loaded.RecordApplied(9, 7);
  const Status status =
      loaded.Load(directory.Path("sessions.bin"));
  EXPECT_EQ(status.code(), Code::kCorruption);
  EXPECT_FALSE(loaded.ShouldApply(9, 7));
  EXPECT_TRUE(loaded.ShouldApply(9, 8));
  EXPECT_TRUE(loaded.ShouldApply(1, 1));
}

TEST(SessionTableTest, RejectsInvalidPathsAndSymlinkSnapshot) {
  TempDir directory;
  SessionTable sessions;
  EXPECT_EQ(sessions.Save("").code(), Code::kInvalidArgument);
  EXPECT_EQ(sessions.Load("").code(), Code::kInvalidArgument);

  const std::string target = directory.Path("target.bin");
  ASSERT_TRUE(sessions.Save(target).ok());
  const std::string link = directory.Path("link.bin");
  std::error_code error;
  std::filesystem::create_symlink(target, link, error);
  ASSERT_FALSE(error);
  EXPECT_EQ(sessions.Load(link).code(), Code::kIOError);
}

}  // namespace
}  // namespace raftkv
