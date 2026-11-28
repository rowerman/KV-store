#include "raftkv/server/session_table.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"

namespace raftkv {
namespace {

inline constexpr char kSessionMagic[] = "RFTSESS1";
inline constexpr uint32_t kSessionVersion = 1;
inline constexpr size_t kSessionMagicSize =
    sizeof(kSessionMagic) - 1;
inline constexpr size_t kSessionHeaderSize =
    kSessionMagicSize + sizeof(uint32_t) + sizeof(uint64_t);
inline constexpr size_t kSessionRecordSize =
    sizeof(uint64_t) + sizeof(uint64_t);
inline constexpr size_t kSessionChecksumSize = sizeof(uint32_t);

Status MakeIoError(const char* operation) {
  const int saved_errno = errno;
  return Status::IOError(
      std::string(operation) + ": " +
      std::strerror(saved_errno));
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
    return MakeIoError("open session directory");
  }
  const Status status = SyncFd(fd, "fsync session directory");
  if (::close(fd) < 0 && status.ok()) {
    return MakeIoError("close session directory");
  }
  return status;
}

Status WriteAll(int fd, const char* data, size_t size) {
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
    return MakeIoError("write session table");
  }
  return Status::OK();
}

Status ReadFileNoFollow(const std::string& path, std::string* output) {
  output->clear();
  const int fd =
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) {
    return MakeIoError("open session table");
  }

  struct stat info {};
  if (::fstat(fd, &info) < 0) {
    const Status status = MakeIoError("stat session table");
    ::close(fd);
    return status;
  }
  if (!S_ISREG(info.st_mode) || info.st_size < 0 ||
      static_cast<uint64_t>(info.st_size) > output->max_size()) {
    ::close(fd);
    return Status::Corruption("invalid session table file type or size");
  }

  std::string data(static_cast<size_t>(info.st_size), '\0');
  size_t read_size = 0;
  while (read_size < data.size()) {
    const ssize_t result =
        ::read(fd, data.data() + read_size, data.size() - read_size);
    if (result > 0) {
      read_size += static_cast<size_t>(result);
      continue;
    }
    if (result == 0) {
      ::close(fd);
      return Status::Corruption("truncated session table");
    }
    if (errno == EINTR) {
      continue;
    }
    const Status status = MakeIoError("read session table");
    ::close(fd);
    return status;
  }
  if (::close(fd) < 0) {
    return MakeIoError("close session table");
  }
  output->swap(data);
  return Status::OK();
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

std::string ParentDirectory(const std::string& path) {
  const std::filesystem::path parent =
      std::filesystem::path(path).parent_path();
  return parent.empty() ? "." : parent.string();
}

}  // namespace

bool SessionTable::ShouldApply(
    uint64_t client_id, uint64_t seq_num) const {
  const auto iterator = last_seq_by_client_.find(client_id);
  return iterator == last_seq_by_client_.end() ||
         seq_num > iterator->second;
}

void SessionTable::RecordApplied(
    uint64_t client_id, uint64_t seq_num) {
  auto result =
      last_seq_by_client_.emplace(client_id, seq_num);
  if (!result.second && seq_num > result.first->second) {
    result.first->second = seq_num;
  }
}

Status SessionTable::Save(const std::string& path) const {
  if (path.empty()) {
    return Status::InvalidArgument("session table path is empty");
  }

  std::vector<std::pair<uint64_t, uint64_t>> records{
      last_seq_by_client_.begin(), last_seq_by_client_.end()};
  std::sort(records.begin(), records.end(),
            [](const auto& lhs, const auto& rhs) {
              return lhs.first < rhs.first;
            });

  std::string encoded;
  if (records.size() >
      (encoded.max_size() - kSessionHeaderSize -
       kSessionChecksumSize) /
          kSessionRecordSize) {
    return Status::InvalidArgument("session table is too large");
  }
  encoded.reserve(kSessionHeaderSize +
                  records.size() * kSessionRecordSize +
                  kSessionChecksumSize);
  encoded.append(kSessionMagic, kSessionMagicSize);
  AppendUint32(&encoded, kSessionVersion);
  AppendUint64(&encoded, static_cast<uint64_t>(records.size()));
  for (const auto& record : records) {
    AppendUint64(&encoded, record.first);
    AppendUint64(&encoded, record.second);
  }
  AppendUint32(
      &encoded, Crc32(encoded.data(), encoded.size()));

  std::string temporary = path + ".tmp-XXXXXX";
  int fd = ::mkstemp(temporary.data());
  if (fd < 0) {
    return MakeIoError("create session table temporary file");
  }
  const auto fail = [&](Status status) {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
    ::unlink(temporary.c_str());
    return status;
  };

  const int descriptor_flags = ::fcntl(fd, F_GETFD);
  if (descriptor_flags < 0 ||
      ::fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) < 0) {
    return fail(MakeIoError("set session table close-on-exec"));
  }
  Status status = WriteAll(fd, encoded.data(), encoded.size());
  if (status.ok()) {
    status = SyncFd(fd, "fsync session table");
  }
  if (::close(fd) < 0 && status.ok()) {
    status = MakeIoError("close session table");
  }
  fd = -1;
  if (!status.ok()) {
    return fail(status);
  }

  if (::rename(temporary.c_str(), path.c_str()) < 0) {
    return fail(MakeIoError("rename session table"));
  }
  temporary.clear();
  return SyncDirectory(ParentDirectory(path));
}

Status SessionTable::Load(const std::string& path) {
  if (path.empty()) {
    return Status::InvalidArgument("session table path is empty");
  }

  std::string encoded;
  Status status = ReadFileNoFollow(path, &encoded);
  if (!status.ok()) {
    return status;
  }
  if (encoded.size() <
      kSessionHeaderSize + kSessionChecksumSize) {
    return Status::Corruption("session table is truncated");
  }

  const size_t checksum_offset =
      encoded.size() - kSessionChecksumSize;
  const uint32_t stored_checksum =
      DecodeFixed32(encoded.data() + checksum_offset);
  if (Crc32(encoded.data(), checksum_offset) != stored_checksum) {
    return Status::Corruption("session table checksum mismatch");
  }
  if (encoded.compare(
          0, kSessionMagicSize,
          kSessionMagic, kSessionMagicSize) != 0) {
    return Status::Corruption("session table magic mismatch");
  }

  size_t offset = kSessionMagicSize;
  const uint32_t version = DecodeFixed32(encoded.data() + offset);
  offset += sizeof(uint32_t);
  if (version != kSessionVersion) {
    return Status::Corruption("unsupported session table version");
  }
  const uint64_t record_count =
      DecodeFixed64(encoded.data() + offset);
  offset += sizeof(uint64_t);

  const size_t payload_size = checksum_offset - offset;
  if (payload_size % kSessionRecordSize != 0 ||
      record_count !=
          static_cast<uint64_t>(payload_size / kSessionRecordSize)) {
    return Status::Corruption("session table record count mismatch");
  }

  const size_t count = static_cast<size_t>(record_count);
  std::unordered_map<uint64_t, uint64_t> loaded;
  if (count > loaded.max_size()) {
    return Status::Corruption("session table has too many records");
  }
  loaded.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    const uint64_t client_id =
        DecodeFixed64(encoded.data() + offset);
    offset += sizeof(uint64_t);
    const uint64_t seq_num =
        DecodeFixed64(encoded.data() + offset);
    offset += sizeof(uint64_t);
    if (client_id == 0 || seq_num == 0 ||
        !loaded.emplace(client_id, seq_num).second) {
      return Status::Corruption("invalid session table record");
    }
  }
  if (offset != checksum_offset) {
    return Status::Corruption("session table has trailing data");
  }

  last_seq_by_client_.swap(loaded);
  return Status::OK();
}

}  // namespace raftkv
