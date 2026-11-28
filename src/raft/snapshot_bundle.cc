#include "raftkv/raft/snapshot_bundle.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <set>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"

namespace raftkv {
namespace {

namespace fs = std::filesystem;

inline constexpr char kBundleMagic[] = "RFTSNP01";
inline constexpr char kBundleFooter[] = "RFTSEND1";
inline constexpr uint32_t kBundleVersion = 1;
inline constexpr size_t kBundleMagicSize = sizeof(kBundleMagic) - 1;
inline constexpr size_t kBundleFooterSize = sizeof(kBundleFooter) - 1;
inline constexpr uint32_t kMaxSnapshotPathSize = 4096;
inline constexpr uint32_t kMaxSnapshotFileCount = 1000000;
inline constexpr size_t kCrcChunkSize = 64 * 1024;

Status MakeIoError(const char* operation) {
  const int saved_errno = errno;
  return Status::IOError(
      std::string(operation) + ": " +
      std::strerror(saved_errno));
}

std::string ParentDirectory(const std::string& path) {
  const fs::path parent = fs::path(path).parent_path();
  return parent.empty() ? "." : parent.string();
}

Status SyncFd(int fd, const char* operation) {
  while (::fsync(fd) < 0) {
    if (errno == EINTR) {
      continue;
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

Status SyncDirectory(const std::string& directory) {
  const int fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open snapshot directory");
  }
  Status status = SyncFd(fd, "fsync snapshot directory");
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    return MakeIoError("close snapshot directory");
  }
  return status;
}

Status WriteAll(int fd, const char* data, size_t size,
                const char* operation) {
  size_t written = 0;
  while (written < size) {
    const ssize_t result =
        ::write(fd, data + written, size - written);
    if (result > 0) {
      written += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

Status ReadAtExact(int fd, char* data, size_t size,
                   uint64_t offset, const char* operation) {
  size_t read_size = 0;
  while (read_size < size) {
    const ssize_t result = ::pread(
        fd, data + read_size, size - read_size,
        static_cast<off_t>(offset + read_size));
    if (result > 0) {
      read_size += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result == 0) {
      return Status::Corruption(
          std::string(operation) + ": unexpected end of file");
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

std::string EncodeUint32(uint32_t value) {
  std::string encoded(sizeof(value), '\0');
  EncodeFixed32(encoded.data(), value);
  return encoded;
}

std::string EncodeUint64(uint64_t value) {
  std::string encoded(sizeof(value), '\0');
  EncodeFixed64(encoded.data(), value);
  return encoded;
}

bool IsSafeRelativePath(const std::string& path) {
  if (path.empty() || path.size() > kMaxSnapshotPathSize ||
      path.front() == '/' ||
      path.find('\0') != std::string::npos ||
      path.find('\\') != std::string::npos) {
    return false;
  }
  size_t begin = 0;
  while (begin <= path.size()) {
    const size_t end = path.find('/', begin);
    const size_t length =
        (end == std::string::npos ? path.size() : end) - begin;
    if (length == 0) {
      return false;
    }
    const std::string component = path.substr(begin, length);
    if (component == "." || component == "..") {
      return false;
    }
    if (end == std::string::npos) {
      break;
    }
    begin = end + 1;
  }
  return true;
}

Status ReadSection(int fd, uint64_t* offset, uint64_t file_size,
                   char* output, size_t size,
                   const char* operation) {
  if (*offset > file_size ||
      static_cast<uint64_t>(size) > file_size - *offset) {
    return Status::Corruption(
        std::string(operation) + ": truncated snapshot bundle");
  }
  Status status =
      ReadAtExact(fd, output, size, *offset, operation);
  if (status.ok()) {
    *offset += static_cast<uint64_t>(size);
  }
  return status;
}

Status ReadUint32FromBundle(int fd, uint64_t* offset,
                            uint64_t file_size, uint32_t* value) {
  char encoded[sizeof(uint32_t)];
  Status status = ReadSection(
      fd, offset, file_size, encoded, sizeof(encoded),
      "read snapshot bundle integer");
  if (status.ok()) {
    *value = DecodeFixed32(encoded);
  }
  return status;
}

Status ReadUint64FromBundle(int fd, uint64_t* offset,
                            uint64_t file_size, uint64_t* value) {
  char encoded[sizeof(uint64_t)];
  Status status = ReadSection(
      fd, offset, file_size, encoded, sizeof(encoded),
      "read snapshot bundle integer");
  if (status.ok()) {
    *value = DecodeFixed64(encoded);
  }
  return status;
}

Status ComputeFileCrcInMemory(int fd, uint64_t offset, uint64_t size,
                              uint32_t* crc) {
  if (crc == nullptr) {
    return Status::InvalidArgument("snapshot CRC output is null");
  }
  if (size > static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
    return Status::Corruption("snapshot file is too large");
  }
  std::string data(static_cast<size_t>(size), '\0');
  Status status = data.empty()
                      ? Status::OK()
                      : ReadAtExact(
                            fd, data.data(), data.size(), offset,
                            "read snapshot bundle data");
  if (status.ok()) {
    *crc = Crc32(data.data(), data.size());
  }
  return status;
}

Status WriteValue(int fd, const std::string& value) {
  return WriteAll(
      fd, value.data(), value.size(),
      "write snapshot bundle");
}

}  // namespace

Status PackSnapshotDirectory(
    const std::string& source_directory,
    const std::string& bundle_path) {
  if (source_directory.empty() || bundle_path.empty()) {
    return Status::InvalidArgument(
        "snapshot source or bundle path is empty");
  }
  std::error_code error;
  if (!fs::is_directory(source_directory, error) || error) {
    return Status::InvalidArgument(
        "snapshot source is not a directory");
  }

  std::vector<fs::path> files;
  // 固定按相对路径排序，保证相同目录生成完全一致的 bundle。
  for (fs::recursive_directory_iterator iterator(
           source_directory, error), end;
       !error && iterator != end; iterator.increment(error)) {
    if (iterator->is_symlink()) {
      return Status::InvalidArgument(
          "snapshot contains a symbolic link");
    }
    if (iterator->is_directory()) {
      continue;
    }
    if (!iterator->is_regular_file()) {
      return Status::InvalidArgument(
          "snapshot contains a non-regular file");
    }
    files.push_back(iterator->path());
  }
  if (error) {
    return Status::IOError(
        "scan snapshot directory: " + error.message());
  }
  std::sort(files.begin(), files.end());
  if (files.size() > kMaxSnapshotFileCount) {
    return Status::InvalidArgument(
        "snapshot contains too many files");
  }

  const std::string temporary = bundle_path + ".tmp";
  const int target_fd = ::open(
      temporary.c_str(),
      O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (target_fd < 0) {
    return MakeIoError("create snapshot bundle");
  }
  Status status = WriteAll(
      target_fd, kBundleMagic, kBundleMagicSize,
      "write snapshot bundle magic");
  if (status.ok()) {
    status = WriteValue(target_fd, EncodeUint32(kBundleVersion));
  }
  if (status.ok()) {
    status = WriteValue(
        target_fd, EncodeUint32(static_cast<uint32_t>(files.size())));
  }

  for (const fs::path& file : files) {
    if (!status.ok()) {
      break;
    }
    const fs::path relative_path =
        fs::relative(file, source_directory, error);
    if (error) {
      status = Status::IOError(
          "make snapshot relative path: " + error.message());
      break;
    }
    const std::string relative = relative_path.generic_string();
    if (!IsSafeRelativePath(relative)) {
      status = Status::Corruption("snapshot path is unsafe");
      break;
    }
    const int source_fd =
        ::open(file.c_str(), O_RDONLY | O_CLOEXEC);
    if (source_fd < 0) {
      status = MakeIoError("open snapshot state file");
      break;
    }
    struct stat file_stat {};
    if (::fstat(source_fd, &file_stat) < 0 || file_stat.st_size < 0) {
      status = MakeIoError("stat snapshot state file");
      ::close(source_fd);
      break;
    }
    const uint64_t file_size =
        static_cast<uint64_t>(file_stat.st_size);
    uint32_t checksum = 0;
    status =
        ComputeFileCrcInMemory(source_fd, 0, file_size, &checksum);
    if (status.ok()) {
      status = WriteValue(
          target_fd, EncodeUint32(
                         static_cast<uint32_t>(relative.size())));
    }
    if (status.ok()) {
      status = WriteAll(
          target_fd, relative.data(), relative.size(),
          "write snapshot path");
    }
    if (status.ok()) {
      status = WriteValue(target_fd, EncodeUint64(file_size));
    }
    if (status.ok()) {
      status = WriteValue(target_fd, EncodeUint32(checksum));
    }
    if (status.ok()) {
      std::string buffer(kCrcChunkSize, '\0');
      uint64_t remaining = file_size;
      uint64_t offset = 0;
      while (remaining != 0) {
        const size_t chunk = static_cast<size_t>(
            std::min<uint64_t>(remaining, buffer.size()));
        status = ReadAtExact(
            source_fd, buffer.data(), chunk, offset,
            "read snapshot state file");
        if (!status.ok()) {
          break;
        }
        status = WriteAll(
            target_fd, buffer.data(), chunk,
            "write snapshot bundle data");
        if (!status.ok()) {
          break;
        }
        offset += chunk;
        remaining -= chunk;
      }
    }
    const int close_result = ::close(source_fd);
    if (status.ok() && close_result < 0) {
      status = MakeIoError("close snapshot state file");
    }
  }

  if (status.ok()) {
    status = WriteAll(
        target_fd, kBundleFooter, kBundleFooterSize,
        "write snapshot bundle footer");
  }
  if (status.ok()) {
    status = SyncFd(target_fd, "fsync snapshot bundle");
  }
  const int close_result = ::close(target_fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close snapshot bundle");
  }
  if (status.ok() &&
      ::rename(temporary.c_str(), bundle_path.c_str()) < 0) {
    status = MakeIoError("rename snapshot bundle");
  }
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  return SyncDirectory(ParentDirectory(bundle_path));
}

Status ValidateSnapshotBundle(
    const std::string& bundle_path,
    uint64_t* serialized_size) {
  if (bundle_path.empty() || serialized_size == nullptr) {
    return Status::InvalidArgument(
        "snapshot bundle input is invalid");
  }
  const int fd = ::open(bundle_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open snapshot bundle");
  }
  struct stat file_stat {};
  if (::fstat(fd, &file_stat) < 0 || file_stat.st_size < 0) {
    const Status status = MakeIoError("stat snapshot bundle");
    ::close(fd);
    return status;
  }
  const uint64_t file_size =
      static_cast<uint64_t>(file_stat.st_size);
  uint64_t offset = 0;
  char magic[kBundleMagicSize];
  Status status = ReadSection(
      fd, &offset, file_size, magic, sizeof(magic),
      "read snapshot bundle magic");
  if (!status.ok() ||
      std::memcmp(magic, kBundleMagic, sizeof(magic)) != 0) {
    ::close(fd);
    return status.ok()
               ? Status::Corruption("invalid snapshot bundle magic")
               : status;
  }
  uint32_t version = 0;
  uint32_t file_count = 0;
  status = ReadUint32FromBundle(fd, &offset, file_size, &version);
  if (status.ok()) {
    status = ReadUint32FromBundle(
        fd, &offset, file_size, &file_count);
  }
  if (!status.ok()) {
    ::close(fd);
    return status;
  }
  if (version != kBundleVersion ||
      file_count > kMaxSnapshotFileCount) {
    ::close(fd);
    return Status::Corruption("invalid snapshot bundle header");
  }

  std::set<std::string> paths;
  // 先完整校验再解包，避免把损坏或路径穿越数据写入目标目录。
  for (uint32_t index = 0; index < file_count; ++index) {
    uint32_t path_size = 0;
    uint64_t data_size = 0;
    uint32_t checksum = 0;
    status =
        ReadUint32FromBundle(fd, &offset, file_size, &path_size);
    if (!status.ok() || path_size == 0 ||
        path_size > kMaxSnapshotPathSize ||
        static_cast<uint64_t>(path_size) >
            file_size - std::min(file_size, offset)) {
      if (!status.ok()) {
        break;
      }
      status = Status::Corruption("invalid snapshot path size");
      break;
    }
    std::string path(path_size, '\0');
    status = ReadSection(
        fd, &offset, file_size, path.data(), path.size(),
        "read snapshot path");
    if (!status.ok()) {
      break;
    }
    if (!IsSafeRelativePath(path) ||
        !paths.insert(path).second) {
      status = Status::Corruption("invalid snapshot path");
      break;
    }
    status =
        ReadUint64FromBundle(fd, &offset, file_size, &data_size);
    if (status.ok()) {
      status =
          ReadUint32FromBundle(fd, &offset, file_size, &checksum);
    }
    if (!status.ok()) {
      break;
    }
    if (data_size > file_size - std::min(file_size, offset)) {
      status = Status::Corruption("truncated snapshot file data");
      break;
    }
    uint32_t actual_checksum = 0;
    status = ComputeFileCrcInMemory(
        fd, offset, data_size, &actual_checksum);
    if (!status.ok()) {
      break;
    }
    if (actual_checksum != checksum) {
      status = Status::Corruption("snapshot file checksum mismatch");
      break;
    }
    offset += data_size;
  }

  if (status.ok()) {
    char footer[kBundleFooterSize];
    status = ReadSection(
        fd, &offset, file_size, footer, sizeof(footer),
        "read snapshot bundle footer");
    if (status.ok() &&
        std::memcmp(footer, kBundleFooter, sizeof(footer)) != 0) {
      status = Status::Corruption("invalid snapshot bundle footer");
    }
  }
  if (status.ok() && offset != file_size) {
    status = Status::Corruption("trailing snapshot bundle data");
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close snapshot bundle");
  }
  if (status.ok()) {
    *serialized_size = file_size;
  }
  return status;
}

Status UnpackSnapshotDirectory(
    const std::string& bundle_path,
    const std::string& destination_directory) {
  if (bundle_path.empty() || destination_directory.empty()) {
    return Status::InvalidArgument(
        "snapshot bundle or destination is empty");
  }
  uint64_t bundle_size = 0;
  Status status =
      ValidateSnapshotBundle(bundle_path, &bundle_size);
  if (!status.ok()) {
    return status;
  }

  std::error_code error;
  if (fs::exists(destination_directory, error)) {
    if (error || !fs::is_directory(destination_directory, error) ||
        !fs::is_empty(destination_directory, error) || error) {
      return Status::InvalidArgument(
          "snapshot destination is not empty");
    }
  } else {
    if (!fs::create_directories(destination_directory, error) ||
        error) {
      return Status::IOError(
          "create snapshot destination: " + error.message());
    }
  }

  const auto cleanup = [&]() {
    std::error_code ignored;
    fs::remove_all(destination_directory, ignored);
  };
  const int fd = ::open(bundle_path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    cleanup();
    return MakeIoError("open snapshot bundle");
  }
  uint64_t offset = 0;
  char magic[kBundleMagicSize];
  uint32_t version = 0;
  uint32_t file_count = 0;
  status = ReadSection(
      fd, &offset, bundle_size, magic, sizeof(magic),
      "read snapshot bundle magic");
  if (status.ok()) {
    status = ReadUint32FromBundle(
        fd, &offset, bundle_size, &version);
  }
  if (status.ok()) {
    status = ReadUint32FromBundle(
        fd, &offset, bundle_size, &file_count);
  }
  if (status.ok() &&
      (std::memcmp(magic, kBundleMagic, sizeof(magic)) != 0 ||
       version != kBundleVersion)) {
    status = Status::Corruption("invalid snapshot bundle header");
  }

  for (uint32_t index = 0; status.ok() && index < file_count;
       ++index) {
    uint32_t path_size = 0;
    uint64_t data_size = 0;
    uint32_t ignored_checksum = 0;
    status = ReadUint32FromBundle(
        fd, &offset, bundle_size, &path_size);
    if (!status.ok()) {
      break;
    }
    if (path_size == 0 || path_size > kMaxSnapshotPathSize) {
      status = Status::Corruption("invalid snapshot path size");
      break;
    }
    std::string relative(path_size, '\0');
    status = ReadSection(
        fd, &offset, bundle_size, relative.data(), relative.size(),
        "read snapshot path");
    if (status.ok()) {
      status = ReadUint64FromBundle(
          fd, &offset, bundle_size, &data_size);
    }
    if (status.ok()) {
      status = ReadUint32FromBundle(
          fd, &offset, bundle_size, &ignored_checksum);
    }
    if (!status.ok() || !IsSafeRelativePath(relative) ||
        data_size > bundle_size - std::min(bundle_size, offset)) {
      if (status.ok()) {
        status = Status::Corruption("invalid snapshot file entry");
      }
      break;
    }

    const fs::path target =
        fs::path(destination_directory) / relative;
    fs::create_directories(target.parent_path(), error);
    if (error) {
      status = Status::IOError(
          "create snapshot file directory: " + error.message());
      break;
    }
    const std::string temporary = target.string() + ".tmp";
    const int output_fd = ::open(
        temporary.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (output_fd < 0) {
      status = MakeIoError("create snapshot output file");
      break;
    }
    std::string buffer(kCrcChunkSize, '\0');
    uint64_t remaining = data_size;
    uint64_t data_offset = offset;
    while (status.ok() && remaining != 0) {
      const size_t chunk = static_cast<size_t>(
          std::min<uint64_t>(remaining, buffer.size()));
      status = ReadAtExact(
          fd, buffer.data(), chunk, data_offset,
          "read snapshot data");
      if (status.ok()) {
        status = WriteAll(
            output_fd, buffer.data(), chunk,
            "write snapshot output file");
      }
      data_offset += chunk;
      remaining -= chunk;
    }
    if (status.ok()) {
      status = SyncFd(output_fd, "fsync snapshot output file");
    }
    const int close_result = ::close(output_fd);
    if (status.ok() && close_result < 0) {
      status = MakeIoError("close snapshot output file");
    }
    if (status.ok() &&
        ::rename(temporary.c_str(), target.c_str()) < 0) {
      status = MakeIoError("rename snapshot output file");
    }
    if (!status.ok()) {
      ::unlink(temporary.c_str());
      break;
    }
    offset += data_size;
  }

  if (status.ok()) {
    fs::create_directories(destination_directory, error);
    if (error) {
      status = Status::IOError(
          "sync snapshot destination: " + error.message());
    }
  }
  if (status.ok()) {
    status = SyncDirectory(destination_directory);
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close snapshot bundle");
  }
  if (!status.ok()) {
    cleanup();
  }
  return status;
}

}  // namespace raftkv
