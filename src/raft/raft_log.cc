#include "raftkv/raft/raft_log.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <memory>
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

inline constexpr char kMetadataMagic[] = "RAFTMETA";
inline constexpr uint32_t kMetadataFormatVersion = 1;
inline constexpr size_t kMetadataEncodedSize =
    sizeof(kMetadataMagic) - 1 + sizeof(uint32_t) +
    sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t) +
    sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);

inline constexpr size_t kLogRecordHeaderSize =
    sizeof(uint32_t) + sizeof(uint32_t);
inline constexpr size_t kLogPayloadFixedSize =
    sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint8_t) +
    sizeof(uint32_t);

Status MakeIoError(const char* operation) {
  const int saved_errno = errno;
  return Status::IOError(
      std::string(operation) + ": " +
      std::strerror(saved_errno));
}

std::string JoinPath(const std::string& directory,
                     const std::string& name) {
  if (!directory.empty() && directory.back() == '/') {
    return directory + name;
  }
  return directory + "/" + name;
}

void AppendUint8(std::string* output, uint8_t value) {
  output->push_back(static_cast<char>(value));
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

bool ReadUint8(Slice* input, uint8_t* value) {
  if (input->size() < sizeof(uint8_t)) {
    return false;
  }
  *value = static_cast<uint8_t>(input->data()[0]);
  *input = Slice(input->data() + 1, input->size() - 1);
  return true;
}

bool ReadUint32(Slice* input, uint32_t* value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  *input = Slice(input->data() + sizeof(uint32_t),
                 input->size() - sizeof(uint32_t));
  return true;
}

bool ReadUint64(Slice* input, uint64_t* value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  *input = Slice(input->data() + sizeof(uint64_t),
                 input->size() - sizeof(uint64_t));
  return true;
}

Status ValidateSnapshotMarker(const RaftHardState& hard_state,
                              const RaftSnapshotMarker& marker) {
  if ((marker.last_included_index == 0) !=
      (marker.last_included_term == 0)) {
    return Status::Corruption("invalid snapshot marker");
  }
  if (hard_state.current_term < marker.last_included_term) {
    return Status::Corruption(
        "hard state term is behind snapshot term");
  }
  if (marker.last_included_index ==
      std::numeric_limits<uint64_t>::max()) {
    return Status::Corruption("snapshot index is exhausted");
  }
  return Status::OK();
}

Status EncodeMetadata(const RaftHardState& hard_state,
                      const RaftSnapshotMarker& marker,
                      std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("metadata output is null");
  }
  Status status = ValidateSnapshotMarker(hard_state, marker);
  if (!status.ok()) {
    return status;
  }

  std::string encoded;
  encoded.reserve(kMetadataEncodedSize);
  encoded.append(kMetadataMagic, sizeof(kMetadataMagic) - 1);
  AppendUint32(&encoded, kMetadataFormatVersion);
  AppendUint64(&encoded, hard_state.current_term);
  AppendUint8(&encoded, hard_state.voted_for.has_value() ? 1U : 0U);
  AppendUint32(&encoded, hard_state.voted_for.value_or(0));
  AppendUint64(&encoded, marker.last_included_index);
  AppendUint64(&encoded, marker.last_included_term);
  AppendUint32(&encoded,
               Crc32(encoded.data(), encoded.size()));
  output->swap(encoded);
  return Status::OK();
}

Status DecodeMetadata(Slice input, RaftHardState* hard_state,
                      RaftSnapshotMarker* marker) {
  if (hard_state == nullptr || marker == nullptr) {
    return Status::InvalidArgument("metadata output is null");
  }
  if (input.size() != kMetadataEncodedSize) {
    return Status::Corruption("invalid raft metadata size");
  }

  const size_t body_size = input.size() - sizeof(uint32_t);
  const uint32_t expected_checksum =
      DecodeFixed32(input.data() + body_size);
  if (Crc32(input.data(), body_size) != expected_checksum) {
    return Status::Corruption("raft metadata checksum mismatch");
  }

  Slice cursor(input.data(), body_size);
  if (cursor.size() < sizeof(kMetadataMagic) - 1 ||
      std::memcmp(cursor.data(), kMetadataMagic,
                  sizeof(kMetadataMagic) - 1) != 0) {
    return Status::Corruption("invalid raft metadata magic");
  }
  cursor = Slice(cursor.data() + sizeof(kMetadataMagic) - 1,
                 cursor.size() - (sizeof(kMetadataMagic) - 1));

  uint32_t format_version = 0;
  uint64_t current_term = 0;
  uint8_t has_voted_for = 0;
  uint32_t voted_for = 0;
  uint64_t snapshot_index = 0;
  uint64_t snapshot_term = 0;
  if (!ReadUint32(&cursor, &format_version) ||
      !ReadUint64(&cursor, &current_term) ||
      !ReadUint8(&cursor, &has_voted_for) ||
      !ReadUint32(&cursor, &voted_for) ||
      !ReadUint64(&cursor, &snapshot_index) ||
      !ReadUint64(&cursor, &snapshot_term) ||
      !cursor.empty()) {
    return Status::Corruption("truncated raft metadata");
  }
  if (format_version != kMetadataFormatVersion) {
    return Status::Corruption("unsupported raft metadata version");
  }
  if (has_voted_for > 1) {
    return Status::Corruption("invalid raft vote marker");
  }

  RaftHardState decoded_hard_state;
  decoded_hard_state.current_term = current_term;
  if (has_voted_for != 0) {
    decoded_hard_state.voted_for = voted_for;
  }
  const RaftSnapshotMarker decoded_marker{
      snapshot_index, snapshot_term};
  Status status =
      ValidateSnapshotMarker(decoded_hard_state, decoded_marker);
  if (!status.ok()) {
    return status;
  }

  *hard_state = decoded_hard_state;
  *marker = decoded_marker;
  return Status::OK();
}

bool IsValidEntryType(proto::EntryType type) {
  switch (type) {
    case proto::ENTRY_TYPE_NORMAL:
    case proto::ENTRY_TYPE_CONF_CHANGE:
    case proto::ENTRY_TYPE_NO_OP:
      return true;
    default:
      return false;
  }
}

Status EncodeLogRecord(const proto::LogEntry& entry,
                       uint64_t max_payload_size,
                       std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("log record output is null");
  }
  if (entry.index() == 0) {
    return Status::InvalidArgument("raft log index is zero");
  }
  if (entry.term() == 0) {
    return Status::InvalidArgument("raft log term is zero");
  }
  if (!IsValidEntryType(entry.entry_type())) {
    return Status::InvalidArgument("invalid raft log entry type");
  }
  if (entry.payload().size() > max_payload_size ||
      entry.payload().size() >
          std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("raft log payload is too large");
  }
  if (kLogPayloadFixedSize >
      std::numeric_limits<uint32_t>::max() -
          entry.payload().size()) {
    return Status::InvalidArgument("raft log record is too large");
  }

  std::string payload;
  payload.reserve(kLogPayloadFixedSize + entry.payload().size());
  AppendUint64(&payload, entry.term());
  AppendUint64(&payload, entry.index());
  AppendUint8(&payload, static_cast<uint8_t>(entry.entry_type()));
  AppendUint32(&payload,
               static_cast<uint32_t>(entry.payload().size()));
  payload.append(entry.payload());

  std::string record(kLogRecordHeaderSize + payload.size(), '\0');
  EncodeFixed32(record.data(),
                static_cast<uint32_t>(payload.size()));
  EncodeFixed32(record.data() + sizeof(uint32_t),
                Crc32(payload.data(), payload.size()));
  std::memcpy(record.data() + kLogRecordHeaderSize,
              payload.data(), payload.size());
  output->swap(record);
  return Status::OK();
}

Status DecodeLogRecord(Slice record, uint64_t max_payload_size,
                       proto::LogEntry* entry,
                       size_t* consumed) {
  if (entry == nullptr || consumed == nullptr) {
    return Status::InvalidArgument("log record output is null");
  }
  if (record.size() < kLogRecordHeaderSize) {
    return Status::Corruption("truncated raft log record header");
  }

  const uint32_t payload_size = DecodeFixed32(record.data());
  if (payload_size < kLogPayloadFixedSize) {
    return Status::Corruption("raft log payload is too short");
  }
  if (payload_size - kLogPayloadFixedSize > max_payload_size ||
      payload_size > record.size() - kLogRecordHeaderSize) {
    return Status::Corruption("invalid raft log payload size");
  }

  const uint32_t expected_checksum =
      DecodeFixed32(record.data() + sizeof(uint32_t));
  const char* payload_data =
      record.data() + kLogRecordHeaderSize;
  if (Crc32(payload_data, payload_size) != expected_checksum) {
    return Status::Corruption("raft log record checksum mismatch");
  }

  Slice payload(payload_data, payload_size);
  uint64_t term = 0;
  uint64_t index = 0;
  uint8_t raw_type = 0;
  uint32_t data_size = 0;
  if (!ReadUint64(&payload, &term) ||
      !ReadUint64(&payload, &index) ||
      !ReadUint8(&payload, &raw_type) ||
      !ReadUint32(&payload, &data_size) ||
      payload.size() != data_size) {
    return Status::Corruption("invalid raft log payload");
  }
  if (term == 0 || index == 0) {
    return Status::Corruption("invalid raft log identity");
  }
  const auto type =
      static_cast<proto::EntryType>(raw_type);
  if (!IsValidEntryType(type)) {
    return Status::Corruption("invalid raft log entry type");
  }

  proto::LogEntry decoded;
  decoded.set_term(term);
  decoded.set_index(index);
  decoded.set_entry_type(type);
  decoded.set_payload(payload.data(), payload.size());
  *entry = std::move(decoded);
  *consumed = kLogRecordHeaderSize + payload_size;
  return Status::OK();
}

Status WriteAllFd(int fd, const char* data, size_t size,
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
    return MakeIoError("open raft directory");
  }
  Status status = SyncFd(fd, "fsync raft directory");
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    return MakeIoError("close raft directory");
  }
  return status;
}

Status PersistMetadata(const std::string& directory,
                       const RaftHardState& hard_state,
                       const RaftSnapshotMarker& marker) {
  std::string encoded;
  Status status = EncodeMetadata(hard_state, marker, &encoded);
  if (!status.ok()) {
    return status;
  }

  const std::string target =
      JoinPath(directory, "raft_meta.bin");
  const std::string temporary = target + ".tmp";
  const int fd = ::open(temporary.c_str(),
                        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                        0644);
  if (fd < 0) {
    return MakeIoError("open raft metadata temporary file");
  }

  status = WriteAllFd(fd, encoded.data(), encoded.size(),
                      "write raft metadata");
  if (status.ok()) {
    status = SyncFd(fd, "fsync raft metadata");
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    status = MakeIoError("close raft metadata");
  }
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }

  if (::rename(temporary.c_str(), target.c_str()) < 0) {
    status = MakeIoError("rename raft metadata");
    ::unlink(temporary.c_str());
    return status;
  }
  return SyncDirectory(directory);
}

inline constexpr char kSegmentPrefix[] = "RAFT-LOG-";
inline constexpr char kSegmentSuffix[] = ".wal";
inline constexpr size_t kSegmentNumberWidth = 6;

struct SegmentFile {
  uint64_t number = 0;
  std::string path;
  uint64_t size = 0;
  uint64_t first_index = 0;
  uint64_t last_index = 0;
};

struct LogEntryRef {
  uint64_t term = 0;
  size_t segment_slot = 0;
  uint64_t offset = 0;
  uint64_t record_size = 0;
};

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

bool HasSegmentPrefix(const std::string& name) {
  const size_t prefix_size = sizeof(kSegmentPrefix) - 1;
  return name.size() >= prefix_size &&
         name.compare(0, prefix_size, kSegmentPrefix) == 0;
}

bool ParseSegmentName(const std::string& name,
                      uint64_t* number) {
  if (number == nullptr) {
    return false;
  }
  const size_t prefix_size = sizeof(kSegmentPrefix) - 1;
  const size_t suffix_size = sizeof(kSegmentSuffix) - 1;
  if (name.size() !=
      prefix_size + kSegmentNumberWidth + suffix_size) {
    return false;
  }
  if (name.compare(0, prefix_size, kSegmentPrefix) != 0 ||
      name.compare(name.size() - suffix_size, suffix_size,
                   kSegmentSuffix) != 0) {
    return false;
  }

  uint64_t decoded = 0;
  for (size_t i = 0; i < kSegmentNumberWidth; ++i) {
    const char digit = name[prefix_size + i];
    if (digit < '0' || digit > '9') {
      return false;
    }
    decoded = decoded * 10 +
              static_cast<uint64_t>(digit - '0');
  }
  if (decoded == 0) {
    return false;
  }
  *number = decoded;
  return true;
}

std::string SegmentFileName(uint64_t number) {
  std::string digits(kSegmentNumberWidth, '0');
  for (size_t i = 0; i < kSegmentNumberWidth; ++i) {
    digits[kSegmentNumberWidth - 1 - i] =
        static_cast<char>('0' + number % 10);
    number /= 10;
  }
  return std::string(kSegmentPrefix) + digits +
         kSegmentSuffix;
}

Status PrepareDirectory(const std::string& input,
                        std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("directory output is null");
  }
  if (input.empty()) {
    return Status::InvalidArgument("raft log directory is empty");
  }

  std::filesystem::path path(input);
  path = path.lexically_normal();
  std::error_code error;
  const bool exists = std::filesystem::exists(path, error);
  if (error) {
    return Status::IOError(
        "inspect raft log directory: " + error.message());
  }
  if (!exists) {
    std::filesystem::create_directories(path, error);
    if (error) {
      return Status::IOError(
          "create raft log directory: " + error.message());
    }
  }
  const bool is_directory =
      std::filesystem::is_directory(path, error);
  if (error) {
    return Status::IOError(
        "inspect raft log directory type: " + error.message());
  }
  if (!is_directory) {
    return Status::IOError("raft log path is not a directory");
  }
  *output = path.string();
  return Status::OK();
}

Status ScanSegmentFiles(const std::string& directory,
                        std::vector<SegmentFile>* segments) {
  if (segments == nullptr) {
    return Status::InvalidArgument("segment output is null");
  }

  std::vector<SegmentFile> scanned;
  std::error_code error;
  std::filesystem::directory_iterator iterator(
      directory, error);
  if (error) {
    return Status::IOError(
        "open raft log directory: " + error.message());
  }
  const std::filesystem::directory_iterator end;
  while (iterator != end) {
    const std::filesystem::directory_entry entry = *iterator;
    const std::string name =
        entry.path().filename().string();
    if (HasSegmentPrefix(name)) {
      uint64_t number = 0;
      if (!ParseSegmentName(name, &number)) {
        return Status::Corruption(
            "invalid raft log segment name");
      }
      std::error_code type_error;
      const bool regular = entry.is_regular_file(type_error);
      if (type_error) {
        return Status::IOError(
            "inspect raft log segment: " +
            type_error.message());
      }
      if (!regular) {
        return Status::Corruption(
            "raft log segment is not a regular file");
      }
      std::error_code size_error;
      const std::uintmax_t size =
          entry.file_size(size_error);
      if (size_error ||
          size > std::numeric_limits<uint64_t>::max()) {
        return Status::IOError(
            "read raft log segment size: " +
            (size_error ? size_error.message()
                        : std::string("size overflow")));
      }
      scanned.push_back(SegmentFile{
          number, entry.path().string(),
          static_cast<uint64_t>(size), 0, 0});
    }
    iterator.increment(error);
    if (error) {
      return Status::IOError(
          "scan raft log directory: " + error.message());
    }
  }

  std::sort(
      scanned.begin(), scanned.end(),
      [](const SegmentFile& lhs, const SegmentFile& rhs) {
        return lhs.number < rhs.number;
      });
  for (size_t i = 1; i < scanned.size(); ++i) {
    if (scanned[i - 1].number == scanned[i].number) {
      return Status::Corruption(
          "duplicate raft log segment number");
    }
  }
  segments->swap(scanned);
  return Status::OK();
}

Status LoadMetadata(const std::string& directory,
                    bool* exists,
                    RaftHardState* hard_state,
                    RaftSnapshotMarker* marker) {
  if (exists == nullptr || hard_state == nullptr ||
      marker == nullptr) {
    return Status::InvalidArgument("metadata output is null");
  }
  *exists = false;

  const std::string path =
      JoinPath(directory, "raft_meta.bin");
  const int raw_fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (raw_fd < 0) {
    if (errno == ENOENT) {
      return Status::OK();
    }
    return MakeIoError("open raft metadata");
  }
  ScopedFd fd(raw_fd);

  struct stat file_stat {};
  if (::fstat(fd.get(), &file_stat) < 0) {
    return MakeIoError("stat raft metadata");
  }
  if (file_stat.st_size < 0 ||
      static_cast<uint64_t>(file_stat.st_size) !=
          kMetadataEncodedSize) {
    return Status::Corruption("invalid raft metadata size");
  }

  std::string encoded(kMetadataEncodedSize, '\0');
  size_t read_size = 0;
  while (read_size < encoded.size()) {
    const ssize_t result =
        ::pread(fd.get(), encoded.data() + read_size,
                encoded.size() - read_size,
                static_cast<off_t>(read_size));
    if (result > 0) {
      read_size += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result == 0) {
      return Status::Corruption("truncated raft metadata");
    }
    return MakeIoError("read raft metadata");
  }

  RaftHardState decoded_hard_state;
  RaftSnapshotMarker decoded_marker;
  Status status = DecodeMetadata(
      Slice(encoded), &decoded_hard_state, &decoded_marker);
  if (!status.ok()) {
    return status;
  }
  *hard_state = decoded_hard_state;
  *marker = decoded_marker;
  *exists = true;
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
      return Status::IOError(
          std::string(operation) + ": unexpected end of file");
    }
    return MakeIoError(operation);
  }
  return Status::OK();
}

Status TruncateFile(const std::string& path, uint64_t size) {
  if (size > static_cast<uint64_t>(
                 std::numeric_limits<off_t>::max())) {
    return Status::IOError("raft log offset is too large");
  }
  const int fd =
      ::open(path.c_str(), O_WRONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open raft log segment for truncate");
  }
  Status status;
  if (::ftruncate(fd, static_cast<off_t>(size)) < 0) {
    status = MakeIoError("truncate raft log segment");
  } else {
    status = SyncFd(fd, "fsync raft log segment");
  }
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    return MakeIoError("close truncated raft log segment");
  }
  return status;
}

Status RecoverEntries(const std::vector<SegmentFile>& input_segments,
                      const RaftHardState& hard_state,
                      const RaftSnapshotMarker& marker,
                      const RaftLogOptions& options,
                      std::vector<SegmentFile>* segments,
                      std::vector<LogEntryRef>* entries) {
  if (segments == nullptr || entries == nullptr) {
    return Status::InvalidArgument("recovery output is null");
  }
  std::vector<SegmentFile> recovered_segments = input_segments;
  std::vector<LogEntryRef> recovered_entries;

  bool saw_entry = false;
  uint64_t previous_index = 0;
  uint64_t previous_term = 0;
  for (size_t slot = 0; slot < recovered_segments.size();
       ++slot) {
    SegmentFile& segment = recovered_segments[slot];
    const int raw_fd =
        ::open(segment.path.c_str(), O_RDONLY | O_CLOEXEC);
    if (raw_fd < 0) {
      return MakeIoError("open raft log segment");
    }
    ScopedFd fd(raw_fd);

    struct stat file_stat {};
    if (::fstat(fd.get(), &file_stat) < 0) {
      return MakeIoError("stat raft log segment");
    }
    if (file_stat.st_size < 0) {
      return Status::Corruption(
          "invalid raft log segment size");
    }
    uint64_t segment_size =
        static_cast<uint64_t>(file_stat.st_size);
    segment.size = segment_size;
    if (segment_size == 0 &&
        slot + 1 < recovered_segments.size()) {
      return Status::Corruption(
          "empty raft log segment is not last");
    }

    uint64_t offset = 0;
    while (offset < segment_size) {
      const uint64_t remaining = segment_size - offset;
      if (remaining < kLogRecordHeaderSize) {
        if (slot + 1 != recovered_segments.size()) {
          return Status::Corruption(
              "truncated non-final raft log segment");
        }
        Status status = TruncateFile(segment.path, offset);
        if (!status.ok()) {
          return status;
        }
        segment.size = offset;
        break;
      }

      char header[kLogRecordHeaderSize] = {};
      Status status = ReadAtExact(
          fd.get(), header, sizeof(header), offset,
          "read raft log record header");
      if (!status.ok()) {
        return status;
      }
      const uint32_t payload_size =
          DecodeFixed32(header);
      if (payload_size < kLogPayloadFixedSize ||
          payload_size - kLogPayloadFixedSize >
              options.max_entry_payload_size) {
        return Status::Corruption(
            "invalid raft log record size");
      }
      const uint64_t record_size =
          kLogRecordHeaderSize + payload_size;
      if (record_size > remaining) {
        if (slot + 1 != recovered_segments.size()) {
          return Status::Corruption(
              "truncated non-final raft log record");
        }
        status = TruncateFile(segment.path, offset);
        if (!status.ok()) {
          return status;
        }
        segment.size = offset;
        break;
      }

      std::string record(
          static_cast<size_t>(record_size), '\0');
      status = ReadAtExact(
          fd.get(), record.data(), record.size(), offset,
          "read raft log record");
      if (!status.ok()) {
        return status;
      }
      proto::LogEntry entry;
      size_t consumed = 0;
      status = DecodeLogRecord(
          Slice(record), options.max_entry_payload_size,
          &entry, &consumed);
      if (!status.ok()) {
        return status;
      }
      if (consumed != record.size()) {
        return Status::Corruption(
            "raft log record size mismatch");
      }
      if (entry.term() > hard_state.current_term) {
        return Status::Corruption(
            "raft log term exceeds current term");
      }
      if (saw_entry) {
        if (entry.index() != previous_index + 1) {
          return Status::Corruption(
              "raft log index gap");
        }
        if (entry.term() < previous_term) {
          return Status::Corruption(
              "raft log term regressed");
        }
      } else {
        saw_entry = true;
        if (entry.index() >
            marker.last_included_index + 1) {
          return Status::Corruption(
              "raft log starts after snapshot gap");
        }
      }
      if (entry.index() == marker.last_included_index &&
          entry.term() != marker.last_included_term) {
        return Status::Corruption(
            "snapshot term does not match log");
      }
      if (entry.index() > marker.last_included_index) {
        if (recovered_entries.empty() &&
            entry.index() !=
                marker.last_included_index + 1) {
          return Status::Corruption(
              "raft log does not continue snapshot");
        }
        recovered_entries.push_back(LogEntryRef{
            entry.term(), slot, offset, record_size});
      }
      if (segment.first_index == 0) {
        segment.first_index = entry.index();
      }
      segment.last_index = entry.index();
      previous_index = entry.index();
      previous_term = entry.term();
      offset += record_size;
    }
  }

  segments->swap(recovered_segments);
  entries->swap(recovered_entries);
  return Status::OK();
}

}  // namespace

struct RaftLog::Impl {
  inline static constexpr uint64_t kMaxSegmentNumber = 999999;

  std::string directory;
  RaftLogOptions options;
  RaftHardState hard_state;
  RaftSnapshotMarker snapshot_marker;
  std::vector<SegmentFile> segments;
  std::vector<LogEntryRef> entries;
  int active_fd = -1;
  uint64_t active_segment_number = 0;
  bool unusable = false;

  ~Impl() {
    if (active_fd >= 0) {
      ::close(active_fd);
    }
  }

  Status CloseActive() {
    if (active_fd < 0) {
      return Status::OK();
    }
    const int fd = active_fd;
    active_fd = -1;
    active_segment_number = 0;
    if (::close(fd) < 0) {
      return MakeIoError("close active raft log segment");
    }
    return Status::OK();
  }

  Status CreateSegment(uint64_t number) {
    if (number == 0 || number > kMaxSegmentNumber) {
      return Status::InvalidArgument(
          "raft log segment number is invalid");
    }
    const std::string path =
        JoinPath(directory, SegmentFileName(number));
    const int fd = ::open(
        path.c_str(),
        O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC,
        0644);
    if (fd < 0) {
      return MakeIoError("create raft log segment");
    }
    Status status = SyncDirectory(directory);
    if (!status.ok()) {
      ::close(fd);
      ::unlink(path.c_str());
      return status;
    }
    segments.push_back(
        SegmentFile{number, path, 0, 0, 0});
    active_fd = fd;
    active_segment_number = number;
    return Status::OK();
  }

  Status EnsureActive() {
    if (active_fd >= 0) {
      return Status::OK();
    }
    if (segments.empty()) {
      return CreateSegment(1);
    }

    SegmentFile& last = segments.back();
    const int fd =
        ::open(last.path.c_str(),
               O_WRONLY | O_APPEND | O_CLOEXEC);
    if (fd < 0) {
      return MakeIoError("open active raft log segment");
    }
    struct stat file_stat {};
    if (::fstat(fd, &file_stat) < 0) {
      const Status status = MakeIoError("stat active raft log segment");
      ::close(fd);
      return status;
    }
    if (file_stat.st_size < 0) {
      ::close(fd);
      return Status::Corruption(
          "invalid active raft log segment size");
    }
    last.size = static_cast<uint64_t>(file_stat.st_size);
    active_fd = fd;
    active_segment_number = last.number;
    return Status::OK();
  }
};

RaftLog::RaftLog(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

RaftLog::~RaftLog() = default;

Status RaftLog::Open(
    std::string directory,
    std::unique_ptr<RaftLog>* log) {
  return Open(std::move(directory), RaftLogOptions(), log);
}

Status RaftLog::Open(
    std::string directory,
    RaftLogOptions options,
    std::unique_ptr<RaftLog>* log) {
  if (log == nullptr) {
    return Status::InvalidArgument("raft log output is null");
  }
  if (directory.empty()) {
    return Status::InvalidArgument("raft log directory is empty");
  }
  if (options.segment_size == 0) {
    return Status::InvalidArgument("raft log segment size is zero");
  }
  if (options.max_entry_payload_size >
      std::numeric_limits<uint32_t>::max() -
          kLogPayloadFixedSize) {
    return Status::InvalidArgument(
        "raft log payload limit is too large");
  }

  std::string normalized_directory;
  Status status =
      PrepareDirectory(directory, &normalized_directory);
  if (!status.ok()) {
    return status;
  }

  std::vector<SegmentFile> scanned_segments;
  status = ScanSegmentFiles(
      normalized_directory, &scanned_segments);
  if (!status.ok()) {
    return status;
  }

  auto impl = std::make_unique<Impl>();
  impl->directory = std::move(normalized_directory);
  impl->options = options;
  bool metadata_exists = false;
  status = LoadMetadata(
      impl->directory, &metadata_exists, &impl->hard_state,
      &impl->snapshot_marker);
  if (!status.ok()) {
    return status;
  }
  if (!metadata_exists) {
    if (!scanned_segments.empty()) {
      return Status::Corruption(
          "raft log segments exist without metadata");
    }
    status = PersistMetadata(
        impl->directory, impl->hard_state,
        impl->snapshot_marker);
    if (!status.ok()) {
      return status;
    }
  }

  status = RecoverEntries(
      scanned_segments, impl->hard_state,
      impl->snapshot_marker, impl->options,
      &impl->segments, &impl->entries);
  if (!status.ok()) {
    return status;
  }

  *log = std::unique_ptr<RaftLog>(
      new RaftLog(std::move(impl)));
  return Status::OK();
}

uint64_t RaftLog::FirstIndex() const {
  return impl_->snapshot_marker.last_included_index + 1;
}

uint64_t RaftLog::LastIndex() const {
  return impl_->snapshot_marker.last_included_index +
         static_cast<uint64_t>(impl_->entries.size());
}

const RaftHardState& RaftLog::hard_state() const {
  return impl_->hard_state;
}

const RaftSnapshotMarker& RaftLog::snapshot_marker() const {
  return impl_->snapshot_marker;
}

const std::string& RaftLog::directory() const {
  return impl_->directory;
}

Status RaftLog::Append(std::vector<proto::LogEntry> entries) {
  if (impl_->unusable) {
    return Status::IOError("raft log is unusable; reopen required");
  }
  if (entries.empty()) {
    return Status::OK();
  }
  if (LastIndex() == std::numeric_limits<uint64_t>::max()) {
    return Status::InvalidArgument("raft log index is exhausted");
  }

  const uint64_t first_index = LastIndex() + 1;
  uint64_t expected_index = first_index;
  uint64_t previous_term =
      impl_->entries.empty()
          ? impl_->snapshot_marker.last_included_term
          : impl_->entries.back().term;
  std::vector<std::string> records;
  records.reserve(entries.size());
  for (const proto::LogEntry& entry : entries) {
    if (entry.index() != expected_index) {
      return Status::InvalidArgument(
          "raft log append indexes are not contiguous");
    }
    if (entry.term() == 0 ||
        entry.term() > impl_->hard_state.current_term) {
      return Status::InvalidArgument(
          "raft log append term is invalid");
    }
    if (entry.term() < previous_term) {
      return Status::InvalidArgument(
          "raft log append term regressed");
    }
    std::string record;
    Status status = EncodeLogRecord(
        entry, impl_->options.max_entry_payload_size, &record);
    if (!status.ok()) {
      return status;
    }
    records.push_back(std::move(record));
    previous_term = entry.term();
    if (expected_index ==
        std::numeric_limits<uint64_t>::max()) {
      if (&entry != &entries.back()) {
        return Status::InvalidArgument(
            "raft log index is exhausted");
      }
    } else {
      ++expected_index;
    }
  }

  const size_t pre_append_segment_count =
      impl_->segments.size();
  Status status = impl_->EnsureActive();
  if (!status.ok()) {
    return status;
  }

  struct SegmentChange {
    size_t slot = 0;
    uint64_t original_size = 0;
    uint64_t final_size = 0;
    bool created = false;
  };
  std::vector<SegmentChange> changes;
  std::vector<LogEntryRef> new_refs;
  new_refs.reserve(entries.size());
  size_t current_slot = impl_->segments.size() - 1;
  bool current_created =
      current_slot >= pre_append_segment_count;
  uint64_t current_size = impl_->segments[current_slot].size;

  auto note_current = [&]() -> size_t {
    for (size_t i = 0; i < changes.size(); ++i) {
      SegmentChange& change = changes[i];
      if (change.slot == current_slot) {
        change.final_size = current_size;
        return i;
      }
    }
    changes.push_back(SegmentChange{
        current_slot, current_size, current_size,
        current_created});
    return changes.size() - 1;
  };

  auto rollback = [&]() -> Status {
    Status rollback_status = impl_->CloseActive();
    for (auto iterator = changes.rbegin();
         iterator != changes.rend(); ++iterator) {
      Status current_status;
      if (iterator->created) {
        if (::unlink(
                impl_->segments[iterator->slot].path.c_str()) < 0 &&
            errno != ENOENT) {
          current_status = MakeIoError(
              "remove rolled-back raft log segment");
        }
      } else {
        current_status = TruncateFile(
            impl_->segments[iterator->slot].path,
            iterator->original_size);
      }
      if (rollback_status.ok() && !current_status.ok()) {
        rollback_status = current_status;
      }
    }
    if (impl_->segments.size() >
        pre_append_segment_count) {
      impl_->segments.resize(
          pre_append_segment_count);
      const Status sync_status = SyncDirectory(impl_->directory);
      if (rollback_status.ok() && !sync_status.ok()) {
        rollback_status = sync_status;
      }
    }
    impl_->active_fd = -1;
    impl_->active_segment_number = 0;
    if (!rollback_status.ok()) {
      impl_->unusable = true;
    }
    return rollback_status;
  };

  for (size_t i = 0; i < records.size(); ++i) {
    const std::string& record = records[i];
    if (current_size != 0 &&
        current_size + record.size() >
            impl_->options.segment_size) {
      status = SyncFd(impl_->active_fd,
                      "fsync raft log segment before rollover");
      if (status.ok()) {
        status = impl_->CloseActive();
      }
      if (status.ok()) {
        const uint64_t next_number =
            impl_->segments.back().number + 1;
        status = impl_->CreateSegment(next_number);
      }
      if (!status.ok()) {
        const Status rollback_status = rollback();
        return rollback_status.ok() ? status : rollback_status;
      }
      current_slot = impl_->segments.size() - 1;
      current_created = true;
      current_size = 0;
    }

    const size_t change_slot = note_current();
    const uint64_t record_offset = current_size;
    status = WriteAllFd(
        impl_->active_fd, record.data(), record.size(),
        "write raft log record");
    if (status.ok()) {
      current_size += record.size();
      changes[change_slot].final_size = current_size;
      new_refs.push_back(LogEntryRef{
          entries[i].term(), current_slot, record_offset,
          record.size()});
    } else {
      const Status rollback_status = rollback();
      return rollback_status.ok() ? status : rollback_status;
    }
  }

  status = SyncFd(impl_->active_fd, "fsync raft log segment");
  if (!status.ok()) {
    const Status rollback_status = rollback();
    return rollback_status.ok() ? status : rollback_status;
  }

  for (const SegmentChange& change : changes) {
    impl_->segments[change.slot].size = change.final_size;
  }
  for (size_t i = 0; i < new_refs.size(); ++i) {
    SegmentFile& segment =
        impl_->segments[new_refs[i].segment_slot];
    if (segment.first_index == 0) {
      segment.first_index = entries[i].index();
    }
    segment.last_index = entries[i].index();
  }
  impl_->entries.insert(
      impl_->entries.end(), new_refs.begin(), new_refs.end());
  return Status::OK();
}

Status RaftLog::EntryAt(uint64_t index,
                        proto::LogEntry* entry) const {
  if (entry == nullptr) {
    return Status::InvalidArgument("raft log entry output is null");
  }
  if (index <= impl_->snapshot_marker.last_included_index ||
      index > LastIndex()) {
    return Status::NotFound("raft log entry not found");
  }

  const uint64_t position = index - FirstIndex();
  if (position >= impl_->entries.size()) {
    return Status::NotFound("raft log entry not found");
  }
  const LogEntryRef& reference =
      impl_->entries[static_cast<size_t>(position)];
  if (reference.segment_slot >= impl_->segments.size() ||
      reference.record_size >
          std::numeric_limits<size_t>::max()) {
    return Status::Corruption("invalid raft log index");
  }
  const SegmentFile& segment =
      impl_->segments[reference.segment_slot];
  const int fd =
      ::open(segment.path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open raft log segment for read");
  }

  std::string record(
      static_cast<size_t>(reference.record_size), '\0');
  Status status = ReadAtExact(
      fd, record.data(), record.size(), reference.offset,
      "read raft log record");
  const int close_result = ::close(fd);
  if (status.ok() && close_result < 0) {
    return MakeIoError("close raft log segment after read");
  }
  if (!status.ok()) {
    return status;
  }

  proto::LogEntry decoded;
  size_t consumed = 0;
  status = DecodeLogRecord(
      Slice(record), impl_->options.max_entry_payload_size,
      &decoded, &consumed);
  if (!status.ok()) {
    return status;
  }
  if (consumed != record.size() ||
      decoded.index() != index ||
      decoded.term() != reference.term) {
    return Status::Corruption("raft log index mismatch");
  }
  *entry = std::move(decoded);
  return Status::OK();
}

Status RaftLog::TermAt(uint64_t index, uint64_t* term) const {
  if (term == nullptr) {
    return Status::InvalidArgument("raft log term output is null");
  }
  if (index == 0 &&
      impl_->snapshot_marker.last_included_index == 0) {
    *term = 0;
    return Status::OK();
  }
  if (index ==
      impl_->snapshot_marker.last_included_index) {
    *term = impl_->snapshot_marker.last_included_term;
    return Status::OK();
  }
  if (index < impl_->snapshot_marker.last_included_index ||
      index > LastIndex()) {
    return Status::NotFound("raft log term not found");
  }

  const uint64_t position = index - FirstIndex();
  if (position >= impl_->entries.size()) {
    return Status::NotFound("raft log term not found");
  }
  *term = impl_->entries[static_cast<size_t>(position)].term;
  return Status::OK();
}

Status RaftLog::LastTerm(uint64_t* term) const {
  if (term == nullptr) {
    return Status::InvalidArgument("raft log term output is null");
  }
  if (impl_->entries.empty()) {
    *term = impl_->snapshot_marker.last_included_term;
  } else {
    *term = impl_->entries.back().term;
  }
  return Status::OK();
}

Status RaftLog::TruncateSuffix(uint64_t from_index) {
  if (impl_->unusable) {
    return Status::IOError("raft log is unusable; reopen required");
  }
  const uint64_t last_index = LastIndex();
  if (last_index != std::numeric_limits<uint64_t>::max() &&
      from_index == last_index + 1) {
    return Status::OK();
  }
  if (from_index <=
          impl_->snapshot_marker.last_included_index ||
      from_index > last_index) {
    return Status::InvalidArgument("invalid raft log truncate index");
  }

  const uint64_t keep_count64 = from_index - FirstIndex();
  if (keep_count64 > impl_->entries.size()) {
    return Status::Corruption("invalid raft log truncate offset");
  }
  const size_t keep_count =
      static_cast<size_t>(keep_count64);
  const LogEntryRef& first_removed = impl_->entries[keep_count];
  const size_t boundary_slot = first_removed.segment_slot;
  if (boundary_slot >= impl_->segments.size()) {
    return Status::Corruption("invalid raft log segment index");
  }

  Status status = impl_->CloseActive();
  if (!status.ok()) {
    impl_->unusable = true;
    return status;
  }

  bool removed_segments = false;
  for (size_t slot = boundary_slot + 1;
       slot < impl_->segments.size(); ++slot) {
    if (::unlink(impl_->segments[slot].path.c_str()) < 0) {
      impl_->unusable = true;
      return MakeIoError("remove truncated raft log segment");
    }
    removed_segments = true;
  }
  if (removed_segments) {
    status = SyncDirectory(impl_->directory);
    if (!status.ok()) {
      impl_->unusable = true;
      return status;
    }
    impl_->segments.resize(boundary_slot + 1);
  }

  SegmentFile& boundary = impl_->segments[boundary_slot];
  status = TruncateFile(boundary.path, first_removed.offset);
  if (!status.ok()) {
    impl_->unusable = true;
    return status;
  }
  boundary.size = first_removed.offset;
  boundary.last_index =
      keep_count == 0
          ? impl_->snapshot_marker.last_included_index
          : from_index - 1;
  impl_->entries.resize(keep_count);
  return Status::OK();
}

Status RaftLog::CompactPrefix(uint64_t up_to_index,
                              uint64_t last_term) {
  if (impl_->unusable) {
    return Status::IOError("raft log is unusable; reopen required");
  }
  if (up_to_index <=
          impl_->snapshot_marker.last_included_index ||
      up_to_index > LastIndex()) {
    return Status::InvalidArgument(
        "invalid raft log compact index");
  }
  uint64_t actual_term = 0;
  Status status = TermAt(up_to_index, &actual_term);
  if (!status.ok()) {
    return status;
  }
  if (actual_term != last_term) {
    return Status::InvalidArgument(
        "raft log compact term mismatch");
  }

  const RaftSnapshotMarker new_marker{
      up_to_index, last_term};
  status = PersistMetadata(
      impl_->directory, impl_->hard_state, new_marker);
  if (!status.ok()) {
    return status;
  }

  const uint64_t remove_count64 =
      up_to_index -
      impl_->snapshot_marker.last_included_index;
  if (remove_count64 > impl_->entries.size()) {
    impl_->unusable = true;
    return Status::Corruption(
        "invalid raft log compact offset");
  }
  impl_->snapshot_marker = new_marker;
  impl_->entries.erase(
      impl_->entries.begin(),
      impl_->entries.begin() +
          static_cast<size_t>(remove_count64));

  status = impl_->CloseActive();
  if (!status.ok()) {
    impl_->unusable = true;
    return status;
  }

  size_t delete_count = 0;
  while (delete_count + 1 < impl_->segments.size() &&
         impl_->segments[delete_count].last_index != 0 &&
         impl_->segments[delete_count].last_index <=
             up_to_index) {
    ++delete_count;
  }
  for (size_t i = 0; i < delete_count; ++i) {
    if (::unlink(impl_->segments[i].path.c_str()) < 0) {
      impl_->unusable = true;
      return MakeIoError("remove compacted raft log segment");
    }
  }
  if (delete_count != 0) {
    status = SyncDirectory(impl_->directory);
    if (!status.ok()) {
      impl_->unusable = true;
      return status;
    }
    impl_->segments.erase(
        impl_->segments.begin(),
        impl_->segments.begin() +
            static_cast<size_t>(delete_count));
    for (LogEntryRef& reference : impl_->entries) {
      if (reference.segment_slot < delete_count) {
        impl_->unusable = true;
        return Status::Corruption(
            "compact removed referenced raft segment");
      }
      reference.segment_slot -= delete_count;
    }
  }
  return Status::OK();
}

Status RaftLog::ResetToSnapshot(
    uint64_t last_included_index,
    uint64_t last_included_term) {
  if (impl_->unusable) {
    return Status::IOError("raft log is unusable; reopen required");
  }
  if (last_included_index ==
          impl_->snapshot_marker.last_included_index &&
      last_included_term ==
          impl_->snapshot_marker.last_included_term) {
    return Status::OK();
  }
  if (last_included_index == 0 ||
      last_included_index ==
          std::numeric_limits<uint64_t>::max() ||
      last_included_index <=
          impl_->snapshot_marker.last_included_index ||
      last_included_term == 0 ||
      last_included_term > impl_->hard_state.current_term) {
    return Status::InvalidArgument(
        "invalid remote raft snapshot marker");
  }

  Status status = impl_->CloseActive();
  if (!status.ok()) {
    impl_->unusable = true;
    return status;
  }
  for (const SegmentFile& segment : impl_->segments) {
    if (::unlink(segment.path.c_str()) < 0 && errno != ENOENT) {
      impl_->unusable = true;
      return MakeIoError("remove raft log before snapshot reset");
    }
  }
  status = SyncDirectory(impl_->directory);
  if (!status.ok()) {
    impl_->unusable = true;
    return status;
  }

  const RaftSnapshotMarker marker{
      last_included_index, last_included_term};
  status = PersistMetadata(
      impl_->directory, impl_->hard_state, marker);
  if (!status.ok()) {
    impl_->unusable = true;
    return status;
  }
  impl_->segments.clear();
  impl_->entries.clear();
  impl_->snapshot_marker = marker;
  return Status::OK();
}

Status RaftLog::SaveHardState(
    uint64_t current_term,
    std::optional<uint32_t> voted_for) {
  if (impl_->unusable) {
    return Status::IOError("raft log is unusable; reopen required");
  }
  if (current_term < impl_->hard_state.current_term) {
    return Status::InvalidArgument(
        "raft current term cannot regress");
  }
  uint64_t last_term = 0;
  Status status = LastTerm(&last_term);
  if (!status.ok()) {
    return status;
  }
  if (current_term < last_term ||
      current_term <
          impl_->snapshot_marker.last_included_term) {
    return Status::InvalidArgument(
        "raft current term is behind persisted state");
  }
  if (current_term == 0 && voted_for.has_value()) {
    return Status::InvalidArgument(
        "raft zero term cannot contain a vote");
  }

  RaftHardState updated = impl_->hard_state;
  updated.current_term = current_term;
  updated.voted_for = voted_for;
  status = PersistMetadata(
      impl_->directory, updated, impl_->snapshot_marker);
  if (!status.ok()) {
    return status;
  }
  impl_->hard_state = updated;
  return Status::OK();
}

}  // namespace raftkv
