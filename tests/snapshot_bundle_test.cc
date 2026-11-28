#include "raftkv/raft/snapshot_bundle.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "raftkv/common/coding.h"

namespace raftkv {
namespace {

namespace fs = std::filesystem;

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-snapshot-bundle-XXXXXX";
    const char* created = ::mkdtemp(pattern);
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed");
    }
    path_ = created;
  }

  ~TempDir() {
    std::error_code error;
    fs::remove_all(path_, error);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  std::string Path(const std::string& name) const {
    return path_ + "/" + name;
  }

 private:
  std::string path_;
};

void WriteFile(const fs::path& path, const std::string& data) {
  fs::create_directories(path.parent_path());
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.is_open());
  output.write(data.data(), static_cast<std::streamsize>(data.size()));
  ASSERT_TRUE(output.good());
}

std::string ReadFile(const fs::path& path) {
  std::ifstream input(path, std::ios::binary);
  EXPECT_TRUE(input.is_open());
  return std::string(
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>());
}

void AppendUint32(std::string* output, uint32_t value) {
  const size_t offset = output->size();
  output->resize(offset + sizeof(value));
  EncodeFixed32(output->data() + offset, value);
}

void AppendUint64(std::string* output, uint64_t value) {
  const size_t offset = output->size();
  output->resize(offset + sizeof(value));
  EncodeFixed64(output->data() + offset, value);
}

TEST(SnapshotBundleTest, RoundTripsNestedBinaryFiles) {
  TempDir directory;
  const fs::path source = directory.Path("source");
  fs::create_directories(source / "nested" / "deeper");
  WriteFile(source / "empty", "");
  WriteFile(source / "binary.bin", std::string("\0a\xffz", 4));
  WriteFile(
      source / "nested" / "deeper" / "data.txt",
      "snapshot-data");

  const std::string bundle = directory.Path("snapshot.bundle");
  ASSERT_TRUE(PackSnapshotDirectory(source.string(), bundle).ok());
  uint64_t size = 0;
  ASSERT_TRUE(ValidateSnapshotBundle(bundle, &size).ok());
  EXPECT_EQ(size, fs::file_size(bundle));

  const fs::path destination = directory.Path("restored");
  ASSERT_TRUE(
      UnpackSnapshotDirectory(bundle, destination.string()).ok());
  EXPECT_EQ(ReadFile(destination / "empty"), "");
  EXPECT_EQ(
      ReadFile(destination / "binary.bin"),
      std::string("\0a\xffz", 4));
  EXPECT_EQ(
      ReadFile(destination / "nested" / "deeper" / "data.txt"),
      "snapshot-data");
}

TEST(SnapshotBundleTest, ProducesDeterministicOrdering) {
  TempDir directory;
  const fs::path source = directory.Path("source");
  WriteFile(source / "z", "last");
  WriteFile(source / "a", "first");
  WriteFile(source / "m", "middle");

  const std::string first = directory.Path("first.bundle");
  const std::string second = directory.Path("second.bundle");
  ASSERT_TRUE(PackSnapshotDirectory(source.string(), first).ok());
  ASSERT_TRUE(PackSnapshotDirectory(source.string(), second).ok());
  EXPECT_EQ(ReadFile(first), ReadFile(second));
}

TEST(SnapshotBundleTest, EmptyDirectoryRoundTrips) {
  TempDir directory;
  const fs::path source = directory.Path("source");
  fs::create_directories(source);
  const std::string bundle = directory.Path("snapshot.bundle");
  ASSERT_TRUE(PackSnapshotDirectory(source.string(), bundle).ok());

  const fs::path destination = directory.Path("restored");
  ASSERT_TRUE(
      UnpackSnapshotDirectory(bundle, destination.string()).ok());
  EXPECT_TRUE(fs::is_empty(destination));
}

TEST(SnapshotBundleTest, DetectsTruncationAndChecksumCorruption) {
  TempDir directory;
  const fs::path source = directory.Path("source");
  WriteFile(source / "state", "checksum-me");
  const std::string original = directory.Path("original.bundle");
  ASSERT_TRUE(PackSnapshotDirectory(source.string(), original).ok());

  const std::string truncated = directory.Path("truncated.bundle");
  std::string bytes = ReadFile(original);
  ASSERT_FALSE(bytes.empty());
  bytes.pop_back();
  WriteFile(truncated, bytes);
  uint64_t size = 0;
  EXPECT_EQ(
      ValidateSnapshotBundle(truncated, &size).code(),
      Code::kCorruption);

  const std::string corrupted = directory.Path("corrupted.bundle");
  bytes = ReadFile(original);
  ASSERT_GT(bytes.size(), 40U);
  bytes[40] ^= 0x5A;
  WriteFile(corrupted, bytes);
  EXPECT_EQ(
      ValidateSnapshotBundle(corrupted, &size).code(),
      Code::kCorruption);

  const fs::path destination = directory.Path("failed-restore");
  EXPECT_FALSE(UnpackSnapshotDirectory(
                   corrupted, destination.string())
                   .ok());
  EXPECT_FALSE(fs::exists(destination));
}

TEST(SnapshotBundleTest, RejectsUnsafeRelativePath) {
  TempDir directory;
  std::string bundle;
  bundle.append("RFTSNP01", 8);
  AppendUint32(&bundle, 1);
  AppendUint32(&bundle, 1);
  const std::string path = "../escape";
  AppendUint32(&bundle, static_cast<uint32_t>(path.size()));
  bundle.append(path);
  AppendUint64(&bundle, 0);
  AppendUint32(&bundle, 0);
  bundle.append("RFTSEND1", 8);

  const std::string bundle_path = directory.Path("unsafe.bundle");
  WriteFile(bundle_path, bundle);
  uint64_t size = 0;
  EXPECT_EQ(
      ValidateSnapshotBundle(bundle_path, &size).code(),
      Code::kCorruption);
}

TEST(SnapshotBundleTest, RejectsSymbolicLinks) {
  TempDir directory;
  const fs::path source = directory.Path("source");
  fs::create_directories(source);
  WriteFile(directory.Path("outside"), "secret");
  std::error_code error;
  fs::create_symlink(
      directory.Path("outside"), source / "link", error);
  ASSERT_FALSE(error);

  const std::string bundle = directory.Path("snapshot.bundle");
  EXPECT_EQ(
      PackSnapshotDirectory(source.string(), bundle).code(),
      Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
