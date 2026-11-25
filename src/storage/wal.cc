#include "raftkv/storage/wal.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"

namespace raftkv {
namespace {

Status MakeIoError(const char* operation) {
  return Status::IOError(
      std::string(operation) + ": " + std::strerror(errno));
}

}  // namespace

WalWriter::WalWriter(std::string path)
    : path_(std::move(path)) {}

WalWriter::~WalWriter() {
  Close();
}

Status WalWriter::Open(bool append) {
  if (fd_ >= 0) {
    return Status::InvalidArgument("wal writer is already open");
  }
  if (path_.empty()) {
    return Status::InvalidArgument("wal path is empty");
  }

  int flags = O_WRONLY | O_CREAT | O_CLOEXEC;
  flags |= append ? O_APPEND : O_TRUNC;
  fd_ = ::open(path_.c_str(), flags, 0644);
  if (fd_ < 0) {
    return MakeIoError("open wal");
  }
  return Status::OK();
}

Status WalWriter::Append(uint64_t sequence, ValueType type,
                         Slice key, Slice value) {
  if (fd_ < 0) {
    return Status::InvalidArgument("wal writer is not open");
  }
  if (!IsValidValueType(type)) {
    return Status::InvalidArgument("invalid wal value type");
  }
  if (sequence == kMaxSequenceNumber) {
    return Status::InvalidArgument(
        "wal sequence number is reserved");
  }
  if (key.size() >
      std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("wal key is too large");
  }

  const size_t value_size =
      type == ValueType::kValue ? value.size() : 0;
  if (value_size >
      std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("wal value is too large");
  }

  constexpr size_t kFixedRecordSize =
      kWalChecksumSize + 8 + 1 + 4 + kWalValueLengthSize;
  if (key.size() > kWalMaxRecordSize - kFixedRecordSize ||
      value_size > kWalMaxRecordSize - kFixedRecordSize -
                       key.size()) {
    return Status::InvalidArgument("wal record is too large");
  }

  const size_t record_size =
      kFixedRecordSize + key.size() + value_size;
  std::string record(record_size, '\0');
  char* current = record.data();

  current += kWalChecksumSize;
  EncodeFixed64(current, sequence);
  current += sizeof(uint64_t);
  *current++ = static_cast<char>(type);
  EncodeFixed32(current, static_cast<uint32_t>(key.size()));
  current += sizeof(uint32_t);
  if (!key.empty()) {
    std::memcpy(current, key.data(), key.size());
    current += key.size();
  }
  EncodeFixed32(current, static_cast<uint32_t>(value_size));
  current += sizeof(uint32_t);
  if (value_size != 0) {
    std::memcpy(current, value.data(), value_size);
  }

  const uint32_t checksum =
      Crc32(record.data() + kWalChecksumSize,
            record.size() - kWalChecksumSize);
  EncodeFixed32(record.data(), checksum);
  return WriteAll(record.data(), record.size());
}

Status WalWriter::Sync() {
  if (fd_ < 0) {
    return Status::InvalidArgument("wal writer is not open");
  }
  while (::fsync(fd_) < 0) {
    if (errno == EINTR) {
      continue;
    }
    return MakeIoError("fsync wal");
  }
  return Status::OK();
}

Status WalWriter::Close() {
  if (fd_ < 0) {
    return Status::OK();
  }

  const int fd = fd_;
  fd_ = -1;
  if (::close(fd) < 0) {
    return MakeIoError("close wal");
  }
  return Status::OK();
}

Status WalWriter::WriteAll(const char* data, size_t size) {
  size_t written = 0;
  while (written < size) {
    const ssize_t result =
        ::write(fd_, data + written, size - written);
    if (result > 0) {
      written += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return MakeIoError("write wal");
  }
  return Status::OK();
}

namespace {

enum class WalReadResult {
  kOk,
  kEndOfFile,
  kTruncated,
  kError,
};

// 顺序读满 size 字节，并区分正常 EOF 与记录中途截断。
WalReadResult ReadExact(int fd, char* data, size_t size,
                        size_t* bytes_read, Status* status) {
  *bytes_read = 0;
  while (*bytes_read < size) {
    const ssize_t result =
        ::read(fd, data + *bytes_read, size - *bytes_read);
    if (result > 0) {
      *bytes_read += static_cast<size_t>(result);
      continue;
    }
    if (result == 0) {
      return *bytes_read == 0 ? WalReadResult::kEndOfFile
                              : WalReadResult::kTruncated;
    }
    if (errno == EINTR) {
      continue;
    }
    *status = MakeIoError("read wal");
    return WalReadResult::kError;
  }
  return WalReadResult::kOk;
}

class ScopedFd {
 public:
  explicit ScopedFd(int fd) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  int get() const { return fd_; }

 private:
  int fd_;
};

}  // namespace

WalReader::WalReader(std::string path)
    : path_(std::move(path)) {}

Status WalReader::Replay(const WalReplayCallback& callback,
                         uint64_t* valid_bytes) const {
  if (callback == nullptr) {
    return Status::InvalidArgument("wal replay callback is null");
  }
  if (path_.empty()) {
    return Status::InvalidArgument("wal path is empty");
  }
  if (valid_bytes != nullptr) {
    *valid_bytes = 0;
  }

  const int raw_fd =
      ::open(path_.c_str(), O_RDONLY | O_CLOEXEC);
  if (raw_fd < 0) {
    return MakeIoError("open wal for replay");
  }
  ScopedFd fd(raw_fd);

  uint64_t offset = 0;
  while (true) {
    char header[kWalHeaderSize] = {};
    size_t bytes_read = 0;
    Status status;
    const WalReadResult header_result =
        ReadExact(fd.get(), header, sizeof(header),
                  &bytes_read, &status);
    if (header_result == WalReadResult::kEndOfFile) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::OK();
    }
    if (header_result == WalReadResult::kTruncated) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::OK();
    }
    if (header_result == WalReadResult::kError) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return status;
    }

    const uint32_t expected_checksum =
        DecodeFixed32(header);
    const uint64_t sequence = DecodeFixed64(header + 4);
    const ValueType type = static_cast<ValueType>(
        static_cast<unsigned char>(header[12]));
    const uint32_t key_size = DecodeFixed32(header + 13);

    if (!IsValidValueType(type) ||
        sequence == kMaxSequenceNumber) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::Corruption("invalid wal record header");
    }

    constexpr size_t kFixedWithoutPayload =
        kWalHeaderSize + kWalValueLengthSize;
    if (key_size >
        kWalMaxRecordSize - kFixedWithoutPayload) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::Corruption("wal key length is too large");
    }

    std::string key(key_size, '\0');
    const WalReadResult key_result =
        ReadExact(fd.get(), key.data(), key.size(),
                  &bytes_read, &status);
    if (key_result == WalReadResult::kEndOfFile ||
        key_result == WalReadResult::kTruncated) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::OK();
    }
    if (key_result == WalReadResult::kError) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return status;
    }

    char value_length_data[kWalValueLengthSize] = {};
    const WalReadResult value_length_result =
        ReadExact(fd.get(), value_length_data,
                  sizeof(value_length_data), &bytes_read, &status);
    if (value_length_result == WalReadResult::kEndOfFile ||
        value_length_result == WalReadResult::kTruncated) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::OK();
    }
    if (value_length_result == WalReadResult::kError) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return status;
    }

    const uint32_t value_size =
        DecodeFixed32(value_length_data);
    if (value_size >
        kWalMaxRecordSize - kFixedWithoutPayload - key.size()) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::Corruption(
          "wal value length is too large");
    }

    std::string value(value_size, '\0');
    const WalReadResult value_result =
        ReadExact(fd.get(), value.data(), value.size(),
                  &bytes_read, &status);
    if (value_result == WalReadResult::kEndOfFile ||
        value_result == WalReadResult::kTruncated) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::OK();
    }
    if (value_result == WalReadResult::kError) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return status;
    }

    std::string checksum_data;
    checksum_data.reserve(
        sizeof(header) - kWalChecksumSize + key.size() +
        sizeof(value_length_data) + value.size());
    checksum_data.append(header + kWalChecksumSize,
                         sizeof(header) - kWalChecksumSize);
    checksum_data.append(key);
    checksum_data.append(value_length_data,
                         sizeof(value_length_data));
    checksum_data.append(value);

    const uint32_t actual_checksum =
        Crc32(checksum_data.data(), checksum_data.size());
    if (actual_checksum != expected_checksum) {
      if (valid_bytes != nullptr) {
        *valid_bytes = offset;
      }
      return Status::Corruption("wal checksum mismatch");
    }

    callback(sequence, type, Slice(key), Slice(value));
    offset += kFixedWithoutPayload + static_cast<uint64_t>(key.size()) +
              static_cast<uint64_t>(value.size());
    if (valid_bytes != nullptr) {
      *valid_bytes = offset;
    }
  }
}

}  // namespace raftkv
