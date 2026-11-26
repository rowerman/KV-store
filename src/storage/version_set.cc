#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <vector>

#include "compaction_internal.h"
#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/storage/internal_key.h"
#include "raftkv/storage/memtable.h"
#include "raftkv/storage/sstable.h"
#include "raftkv/storage/version_set.h"
#include "raftkv/storage/memtable.h"


namespace raftkv {

namespace {

Status MakeIoError(const char* operation) {
  return Status::IOError(
      std::string(operation) + ": " + std::strerror(errno));
}

bool IsValidLevel(uint32_t level) {
  return level < kNumLevels;
}

inline constexpr uint32_t kManifestMagic = 0x314E414D;
inline constexpr uint32_t kManifestFormatVersion = 1;
inline constexpr uint8_t kManifestSnapshotRecord = 1;
inline constexpr uint8_t kManifestEditRecord = 2;
inline constexpr size_t kManifestFrameHeaderSize =
    sizeof(uint32_t) + sizeof(uint32_t);
inline constexpr size_t kManifestMaxRecordSize =
    64U * 1024U * 1024U;
inline constexpr uint64_t kManifestMaxFileSize =
    256ULL * 1024ULL * 1024ULL;
inline constexpr size_t kCurrentMaxFileSize = 128;

struct ManifestState {
  Version version;
  uint64_t next_file_number = 0;
  uint64_t last_sequence = 0;
  size_t valid_bytes = 0;
  bool snapshot_seen = false;
};

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

Status AppendLengthPrefixedString(std::string* output,
                                  const std::string& value) {
  return PutLengthPrefixedSlice(output, Slice(value));
}

Status ReadLengthPrefixedString(Slice* input,
                                std::string* value) {
  Slice decoded;
  const Status status = GetLengthPrefixedSlice(input, &decoded);
  if (!status.ok()) {
    return status;
  }
  value->assign(decoded.data(), decoded.size());
  return Status::OK();
}

Status AppendFileMetadata(std::string* output,
                          const FileMetaData& metadata) {
  AppendUint64(output, metadata.file_number);
  AppendUint64(output, metadata.file_size);
  Status status =
      AppendLengthPrefixedString(output, metadata.smallest_key);
  if (!status.ok()) {
    return status;
  }
  return AppendLengthPrefixedString(output, metadata.largest_key);
}

Status DecodeFileMetadata(Slice* input,
                          FileMetaData* metadata) {
  FileMetaData decoded;
  if (!ReadUint64(input, &decoded.file_number) ||
      !ReadUint64(input, &decoded.file_size)) {
    return Status::Corruption(
        "truncated manifest file metadata");
  }
  Status status =
      ReadLengthPrefixedString(input, &decoded.smallest_key);
  if (!status.ok()) {
    return status;
  }
  status =
      ReadLengthPrefixedString(input, &decoded.largest_key);
  if (!status.ok()) {
    return status;
  }
  status = ValidateFileMetaData(decoded);
  if (!status.ok()) {
    return Status::Corruption(
        "invalid manifest file metadata");
  }
  *metadata = std::move(decoded);
  return Status::OK();
}

Status EncodeSnapshotPayload(const ManifestState& state,
                             std::string* payload) {
  if (payload == nullptr) {
    return Status::InvalidArgument(
        "manifest snapshot output is null");
  }

  std::string encoded;
  AppendUint8(&encoded, kManifestSnapshotRecord);
  AppendUint32(&encoded, kManifestMagic);
  AppendUint32(&encoded, kManifestFormatVersion);
  AppendUint64(&encoded, state.next_file_number);
  AppendUint64(&encoded, state.last_sequence);
  AppendUint32(&encoded,
               static_cast<uint32_t>(kNumLevels));
  for (uint32_t level = 0; level < kNumLevels; ++level) {
    AppendUint32(
        &encoded,
        static_cast<uint32_t>(
            state.version.levels[level].size()));
    for (const FileMetaData& metadata :
         state.version.levels[level]) {
      const Status status =
          AppendFileMetadata(&encoded, metadata);
      if (!status.ok()) {
        return status;
      }
    }
  }

  payload->swap(encoded);
  return Status::OK();
}

Status EncodeEditPayload(uint64_t next_file_number,
                         const VersionEdit& edit,
                         std::string* payload) {
  if (payload == nullptr) {
    return Status::InvalidArgument(
        "manifest edit output is null");
  }

  std::string encoded;
  AppendUint8(&encoded, kManifestEditRecord);
  AppendUint64(&encoded, next_file_number);
  AppendUint8(&encoded,
              edit.has_last_sequence ? 1U : 0U);
  if (edit.has_last_sequence) {
    AppendUint64(&encoded, edit.last_sequence);
  }

  for (uint32_t level = 0; level < kNumLevels; ++level) {
    std::vector<FileMetaData> additions;
    std::vector<uint64_t> deletions;
    for (const FileAddition& addition : edit.added_files) {
      if (addition.level == level) {
        additions.push_back(addition.metadata);
      }
    }
    for (const FileDeletion& deletion : edit.deleted_files) {
      if (deletion.level == level) {
        deletions.push_back(deletion.file_number);
      }
    }

    AppendUint32(
        &encoded,
        static_cast<uint32_t>(additions.size()));
    for (const FileMetaData& metadata : additions) {
      const Status status =
          AppendFileMetadata(&encoded, metadata);
      if (!status.ok()) {
        return status;
      }
    }
    AppendUint32(
        &encoded,
        static_cast<uint32_t>(deletions.size()));
    for (uint64_t file_number : deletions) {
      AppendUint64(&encoded, file_number);
    }
  }

  payload->swap(encoded);
  return Status::OK();
}

Status EncodeManifestFrame(Slice payload,
                           std::string* frame) {
  if (frame == nullptr) {
    return Status::InvalidArgument(
        "manifest frame output is null");
  }
  if (payload.size() > kManifestMaxRecordSize ||
      payload.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument(
        "manifest record is too large");
  }

  std::string encoded(
      kManifestFrameHeaderSize + payload.size(), '\0');
  EncodeFixed32(encoded.data(),
                static_cast<uint32_t>(payload.size()));
  EncodeFixed32(encoded.data() + sizeof(uint32_t),
                Crc32(payload.data(), payload.size()));
  if (!payload.empty()) {
    std::memcpy(encoded.data() + kManifestFrameHeaderSize,
                payload.data(), payload.size());
  }
  frame->swap(encoded);
  return Status::OK();
}

Status DecodeSnapshotPayload(Slice* input,
                             ManifestState* state) {
  if (state->snapshot_seen) {
    return Status::Corruption(
        "manifest contains multiple snapshots");
  }

  uint32_t magic = 0;
  uint32_t format_version = 0;
  uint32_t level_count = 0;
  ManifestState decoded;
  if (!ReadUint32(input, &magic) ||
      !ReadUint32(input, &format_version) ||
      !ReadUint64(input, &decoded.next_file_number) ||
      !ReadUint64(input, &decoded.last_sequence) ||
      !ReadUint32(input, &level_count)) {
    return Status::Corruption(
        "truncated manifest snapshot");
  }
  if (magic != kManifestMagic ||
      format_version != kManifestFormatVersion ||
      level_count != kNumLevels ||
      decoded.next_file_number == 0 ||
      decoded.last_sequence == kMaxSequenceNumber) {
    return Status::Corruption(
        "invalid manifest snapshot header");
  }

  for (uint32_t level = 0; level < kNumLevels; ++level) {
    uint32_t file_count = 0;
    if (!ReadUint32(input, &file_count) ||
        file_count > input->size()) {
      return Status::Corruption(
          "invalid manifest snapshot file count");
    }
    for (uint32_t i = 0; i < file_count; ++i) {
      FileMetaData metadata;
      const Status status =
          DecodeFileMetadata(input, &metadata);
      if (!status.ok()) {
        return status;
      }
      if (metadata.file_number >=
          decoded.next_file_number) {
        return Status::Corruption(
            "snapshot file number exceeds next number");
      }
      decoded.version.levels[level].push_back(
          std::move(metadata));
    }
  }
  if (!input->empty()) {
    return Status::Corruption(
        "trailing data in manifest snapshot");
  }

  const Status status = ValidateVersion(decoded.version);
  if (!status.ok()) {
    return Status::Corruption(
        "invalid version in manifest snapshot");
  }
  decoded.snapshot_seen = true;
  *state = std::move(decoded);
  return Status::OK();
}

Status DecodeEditPayload(Slice* input,
                         ManifestState* state) {
  if (!state->snapshot_seen) {
    return Status::Corruption(
        "manifest must start with a snapshot");
  }

  uint64_t next_file_number = 0;
  uint8_t has_last_sequence = 0;
  VersionEdit edit;
  if (!ReadUint64(input, &next_file_number) ||
      !ReadUint8(input, &has_last_sequence)) {
    return Status::Corruption(
        "truncated manifest edit");
  }
  if (has_last_sequence > 1) {
    return Status::Corruption(
        "invalid manifest sequence flag");
  }
  if (has_last_sequence != 0) {
    edit.has_last_sequence = true;
    if (!ReadUint64(input, &edit.last_sequence)) {
      return Status::Corruption(
          "truncated manifest last sequence");
    }
  }
  if (next_file_number < state->next_file_number) {
    return Status::Corruption(
        "manifest next file number moved backwards");
  }

  for (uint32_t level = 0; level < kNumLevels; ++level) {
    uint32_t addition_count = 0;
    if (!ReadUint32(input, &addition_count) ||
        addition_count > input->size()) {
      return Status::Corruption(
          "invalid manifest addition count");
    }
    for (uint32_t i = 0; i < addition_count; ++i) {
      FileMetaData metadata;
      const Status status =
          DecodeFileMetadata(input, &metadata);
      if (!status.ok()) {
        return status;
      }
      if (metadata.file_number >= next_file_number) {
        return Status::Corruption(
            "manifest addition exceeds next file number");
      }
      edit.added_files.push_back(
          FileAddition{level, std::move(metadata)});
    }

    uint32_t deletion_count = 0;
    if (!ReadUint32(input, &deletion_count) ||
        deletion_count > input->size() /
                          sizeof(uint64_t)) {
      return Status::Corruption(
          "invalid manifest deletion count");
    }
    for (uint32_t i = 0; i < deletion_count; ++i) {
      uint64_t file_number = 0;
      if (!ReadUint64(input, &file_number)) {
        return Status::Corruption(
            "truncated manifest deletion");
      }
      edit.deleted_files.push_back(
          FileDeletion{level, file_number});
    }
  }
  if (!input->empty()) {
    return Status::Corruption(
        "trailing data in manifest edit");
  }
  if (edit.has_last_sequence &&
      (edit.last_sequence == kMaxSequenceNumber ||
       edit.last_sequence < state->last_sequence)) {
    return Status::Corruption(
        "manifest last sequence moved backwards");
  }

  Version updated;
  const Status apply_status =
      ApplyVersionEdit(state->version, edit, &updated);
  if (!apply_status.ok()) {
    return Status::Corruption(
        "invalid manifest version edit");
  }
  state->version = std::move(updated);
  state->next_file_number = next_file_number;
  if (edit.has_last_sequence) {
    state->last_sequence = edit.last_sequence;
  }
  return Status::OK();
}

Status DecodeManifestPayload(Slice payload,
                             ManifestState* state) {
  if (state == nullptr) {
    return Status::InvalidArgument(
        "manifest state is null");
  }

  uint8_t record_type = 0;
  if (!ReadUint8(&payload, &record_type)) {
    return Status::Corruption(
        "empty manifest payload");
  }
  if (record_type == kManifestSnapshotRecord) {
    return DecodeSnapshotPayload(&payload, state);
  }
  if (record_type == kManifestEditRecord) {
    return DecodeEditPayload(&payload, state);
  }
  return Status::Corruption(
      "unknown manifest record type");
}

Status ParseManifest(Slice data, ManifestState* state,
                     bool* truncated_tail) {
  if (state == nullptr || truncated_tail == nullptr) {
    return Status::InvalidArgument(
        "manifest parse output is null");
  }

  ManifestState parsed;
  *truncated_tail = false;
  size_t offset = 0;
  while (offset < data.size()) {
    const size_t remaining = data.size() - offset;
    if (remaining < kManifestFrameHeaderSize) {
      *truncated_tail = true;
      break;
    }

    const char* frame = data.data() + offset;
    const uint32_t payload_size =
        DecodeFixed32(frame);
    if (payload_size == 0 ||
        payload_size > kManifestMaxRecordSize) {
      return Status::Corruption(
          "invalid manifest record size");
    }
    const uint64_t frame_size =
        static_cast<uint64_t>(kManifestFrameHeaderSize) +
        payload_size;
    if (remaining < frame_size) {
      *truncated_tail = true;
      break;
    }

    const char* payload =
        frame + kManifestFrameHeaderSize;
    const uint32_t expected_crc =
        DecodeFixed32(frame + sizeof(uint32_t));
    if (Crc32(payload, payload_size) != expected_crc) {
      return Status::Corruption(
          "manifest record checksum mismatch");
    }
    const Status status =
        DecodeManifestPayload(
            Slice(payload, payload_size), &parsed);
    if (!status.ok()) {
      return status;
    }
    offset += static_cast<size_t>(frame_size);
    parsed.valid_bytes = offset;
  }

  if (!parsed.snapshot_seen) {
    return Status::Corruption(
        "manifest has no complete snapshot");
  }
  *state = std::move(parsed);
  return Status::OK();
}

std::string JoinPath(const std::string& directory,
                     const std::string& name) {
  if (directory.empty() || directory.back() == '/') {
    return directory + name;
  }
  return directory + "/" + name;
}

std::string NumberedFileName(const char* prefix,
                             uint64_t number,
                             const char* suffix) {
  char number_buffer[32] = {};
  std::snprintf(number_buffer, sizeof(number_buffer),
                "%06llu",
                static_cast<unsigned long long>(number));
  return std::string(prefix) + number_buffer + suffix;
}

bool ParseNumberedFileName(const std::string& name,
                           const std::string& prefix,
                           const std::string& suffix,
                           uint64_t* number) {
  if (number == nullptr ||
      name.size() <= prefix.size() + suffix.size() ||
      name.compare(0, prefix.size(), prefix) != 0 ||
      name.compare(name.size() - suffix.size(),
                   suffix.size(), suffix) != 0) {
    return false;
  }

  const char* begin =
      name.data() + prefix.size();
  const char* end =
      name.data() + name.size() - suffix.size();
  uint64_t parsed = 0;
  const auto result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc() || result.ptr != end ||
      parsed == 0 ||
      name != NumberedFileName(prefix.c_str(), parsed,
                               suffix.c_str())) {
    return false;
  }
  *number = parsed;
  return true;
}

std::string TableFileNameForNumber(uint64_t file_number) {
  return NumberedFileName("", file_number, ".sst");
}

std::string ManifestFileNameForNumber(uint64_t file_number) {
  return NumberedFileName("MANIFEST-", file_number, "");
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
  const int fd = ::open(
      directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open database directory");
  }
  const Status status = SyncFd(fd, "fsync database directory");
  ::close(fd);
  return status;
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

Status ReadWholeFile(const std::string& path,
                     uint64_t max_size,
                     std::string* contents) {
  if (contents == nullptr) {
    return Status::InvalidArgument(
        "file contents output is null");
  }

  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open manifest or current file");
  }

  struct stat file_stat {};
  if (::fstat(fd, &file_stat) < 0) {
    const Status status = MakeIoError("fstat manifest");
    ::close(fd);
    return status;
  }
  if (file_stat.st_size < 0 ||
      static_cast<uint64_t>(file_stat.st_size) > max_size) {
    ::close(fd);
    return Status::Corruption("metadata file is too large");
  }

  std::string data(
      static_cast<size_t>(file_stat.st_size), '\0');
  size_t read_bytes = 0;
  while (read_bytes < data.size()) {
    const ssize_t result =
        ::read(fd, data.data() + read_bytes,
               data.size() - read_bytes);
    if (result > 0) {
      read_bytes += static_cast<size_t>(result);
      continue;
    }
    if (result == 0) {
      ::close(fd);
      return Status::Corruption(
          "metadata file was truncated while reading");
    }
    if (errno == EINTR) {
      continue;
    }
    const Status status = MakeIoError("read metadata file");
    ::close(fd);
    return status;
  }
  if (::close(fd) < 0) {
    return MakeIoError("close metadata file");
  }
  contents->swap(data);
  return Status::OK();
}

Status AppendManifestRecord(int fd,
                            Slice payload) {
  std::string frame;
  Status status = EncodeManifestFrame(payload, &frame);
  if (!status.ok()) {
    return status;
  }
  status = WriteAllFd(fd, frame.data(), frame.size(),
                      "append manifest");
  if (!status.ok()) {
    return status;
  }
  return SyncFd(fd, "fsync manifest");
}

Status WriteCurrentFile(const std::string& directory,
                        uint64_t manifest_number) {
  const std::string current_path =
      JoinPath(directory, "CURRENT");
  const std::string temporary_path =
      JoinPath(directory, "CURRENT.tmp");
  std::string contents =
      ManifestFileNameForNumber(manifest_number) + "\n";

  const int fd = ::open(
      temporary_path.c_str(),
      O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return MakeIoError("open CURRENT temp file");
  }
  Status status =
      WriteAllFd(fd, contents.data(), contents.size(),
                 "write CURRENT temp file");
  if (status.ok()) {
    status = SyncFd(fd, "fsync CURRENT temp file");
  }
  if (::close(fd) < 0 && status.ok()) {
    status = MakeIoError("close CURRENT temp file");
  }
  if (!status.ok()) {
    ::unlink(temporary_path.c_str());
    return status;
  }

  if (::rename(temporary_path.c_str(),
               current_path.c_str()) < 0) {
    status = MakeIoError("rename CURRENT");
    ::unlink(temporary_path.c_str());
    return status;
  }
  return SyncDirectory(directory);
}

Status ReadCurrentManifestNumber(
    const std::string& directory, uint64_t* manifest_number) {
  if (manifest_number == nullptr) {
    return Status::InvalidArgument(
        "current manifest output is null");
  }

  const std::string current_path =
      JoinPath(directory, "CURRENT");
  if (::access(current_path.c_str(), F_OK) < 0) {
    if (errno == ENOENT) {
      return Status::NotFound("CURRENT does not exist");
    }
    return MakeIoError("check CURRENT");
  }

  std::string contents;
  const Status status = ReadWholeFile(
      current_path, kCurrentMaxFileSize, &contents);
  if (!status.ok()) {
    return status;
  }
  if (contents.empty() || contents.back() != '\n') {
    return Status::Corruption("CURRENT has no newline");
  }
  contents.pop_back();
  if (contents.empty() ||
      contents.find('\n') != std::string::npos ||
      contents.find('\r') != std::string::npos) {
    return Status::Corruption("CURRENT content is invalid");
  }

  uint64_t parsed = 0;
  if (!ParseNumberedFileName(
          contents, "MANIFEST-", "", &parsed)) {
    return Status::Corruption(
        "CURRENT does not name a manifest");
  }
  *manifest_number = parsed;
  return Status::OK();
}

Status ScanMaxNumberedFile(const std::string& directory,
                           uint64_t* max_number) {
  if (max_number == nullptr) {
    return Status::InvalidArgument(
        "maximum file number output is null");
  }

  *max_number = 0;
  std::error_code error;
  std::filesystem::directory_iterator iterator(directory, error);
  if (error) {
    return Status::IOError(
        "iterate database directory: " + error.message());
  }
  for (const std::filesystem::directory_entry& entry :
       iterator) {
    if (!entry.is_regular_file(error)) {
      error.clear();
      continue;
    }
    const std::string name =
        entry.path().filename().string();
    uint64_t number = 0;
    if (ParseNumberedFileName(name, "", ".sst", &number) ||
        ParseNumberedFileName(name, "MANIFEST-", "", &number)) {
      *max_number = std::max(*max_number, number);
    }
  }
  return Status::OK();
}

Status ValidateLiveTableFiles(const Version& version,
                              const std::string& directory) {
  for (uint32_t level = 0; level < kNumLevels; ++level) {
    for (const FileMetaData& metadata :
         version.levels[level]) {
      const std::string path = JoinPath(
          directory, TableFileNameForNumber(
                         metadata.file_number));
      struct stat file_stat {};
      if (::stat(path.c_str(), &file_stat) < 0) {
        return MakeIoError("stat live sstable");
      }
      if (!S_ISREG(file_stat.st_mode) ||
          file_stat.st_size < 0 ||
          static_cast<uint64_t>(file_stat.st_size) !=
              metadata.file_size) {
        return Status::Corruption(
            "live sstable size does not match manifest");
      }
    }
  }
  return Status::OK();
}

Status EnsureDirectory(const std::string& directory) {
  std::error_code error;
  std::filesystem::create_directories(directory, error);
  if (error) {
    return Status::IOError(
        "create database directory: " + error.message());
  }
  if (!std::filesystem::is_directory(directory, error) ||
      error) {
    return Status::InvalidArgument(
        "database path is not a directory");
  }
  return Status::OK();
}

void RetireVersion(std::mutex* mutex,
                   std::vector<VersionPtr>* retired,
                   VersionPtr version) {
  if (version == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(*mutex);
  retired->push_back(std::move(version));
}

uint64_t LevelMaxBytes(uint32_t level) {
  switch (level) {
    case 1:
      return kL1MaxBytes;
    case 2:
      return kL2MaxBytes;
    case 3:
      return kL3MaxBytes;
    default:
      return std::numeric_limits<uint64_t>::max();
  }
}

void AddOverlappingFiles(
    const std::vector<FileMetaData>& source_files,
    const std::vector<FileMetaData>& target_files,
    std::vector<FileMetaData>* overlapping) {
  if (source_files.empty() || overlapping == nullptr) {
    return;
  }

  std::string smallest = source_files.front().smallest_key;
  std::string largest = source_files.front().largest_key;
  for (const FileMetaData& source : source_files) {
    smallest = std::min(smallest, source.smallest_key);
    largest = std::max(largest, source.largest_key);
  }

  for (const FileMetaData& target : target_files) {
    if (target.smallest_key <= largest &&
        smallest <= target.largest_key) {
      overlapping->push_back(target);
    }
  }
}

bool PickCompaction(
    const Version& version,
    std::array<uint32_t, kNumLevels>* compaction_pointer,
    CompactionInput* input) {
  if (compaction_pointer == nullptr || input == nullptr) {
    return false;
  }

  CompactionInput selected;
  if (version.levels[0].size() >= kL0CompactionTrigger) {
    selected.source_level = 0;
    selected.target_level = 1;
    selected.source_files = version.levels[0];
    AddOverlappingFiles(
        selected.source_files, version.levels[1],
        &selected.target_files);
    *input = std::move(selected);
    return true;
  }

  for (uint32_t level = 1; level + 1 < kNumLevels; ++level) {
    if (version.TotalBytes(level) <= LevelMaxBytes(level) ||
        version.levels[level].empty()) {
      continue;
    }

    const size_t file_index =
        (*compaction_pointer)[level] % version.levels[level].size();
    (*compaction_pointer)[level] = static_cast<uint32_t>(
        (file_index + 1) % version.levels[level].size());

    selected.source_level = level;
    selected.target_level = level + 1;
    selected.source_files = {version.levels[level][file_index]};
    AddOverlappingFiles(
        selected.source_files, version.levels[level + 1],
        &selected.target_files);
    *input = std::move(selected);
    return true;
  }
  return false;
}

}  // namespace

struct VersionSet::Impl {
  explicit Impl(std::string path)
      : directory(std::move(path)) {
    compaction_pointer.fill(0);
  }

  ~Impl() {
    StopBackgroundWorker();
    if (manifest_fd >= 0) {
      ::close(manifest_fd);
      manifest_fd = -1;
    }
  }

  Status Create();
  Status Recover();
  Status FlushMemTable(const FlushRequest& request);
  Status RunCompactions();
  Status CollectObsoleteFiles();
  void StartBackgroundWorker();
  void StopBackgroundWorker();
  void RunBackgroundWorker();

  std::string directory;
  VersionSet* owner = nullptr;
  int manifest_fd = -1;
  uint64_t manifest_number = 0;
  std::atomic<bool> manifest_poisoned{false};
  std::atomic<uint64_t> next_file_number{2};
  std::atomic<uint64_t> last_sequence{0};
  VersionPtr current;

  std::mutex apply_mutex;
  std::mutex retired_mutex;
  std::vector<VersionPtr> retired_versions;
  std::array<uint32_t, kNumLevels> compaction_pointer {};

  std::mutex background_mutex;
  std::condition_variable background_cv;
  std::condition_variable idle_cv;
  std::deque<FlushRequest> flush_queue;
  bool compaction_requested = false;
  bool pending_work = false;
  bool shutting_down = false;
  Status background_status;
  std::thread background_thread;
};

Status VersionSet::Impl::Create() {
  uint64_t max_file_number = 0;
  Status status = ScanMaxNumberedFile(directory, &max_file_number);
  if (!status.ok()) {
    return status;
  }

  ManifestState state;
  state.next_file_number = max_file_number + 1;
  if (state.next_file_number == 0 ||
      state.next_file_number ==
          std::numeric_limits<uint64_t>::max()) {
    return Status::InvalidArgument(
        "database file numbers are exhausted");
  }
  const std::string manifest_path = JoinPath(
      directory,
      ManifestFileNameForNumber(state.next_file_number));

  manifest_fd = ::open(
      manifest_path.c_str(),
      O_RDWR | O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC, 0644);
  if (manifest_fd < 0) {
    return MakeIoError("create manifest");
  }

  std::string payload;
  status = EncodeSnapshotPayload(state, &payload);
  if (status.ok()) {
    status = AppendManifestRecord(manifest_fd, Slice(payload));
  }
  if (status.ok()) {
    status = WriteCurrentFile(directory, state.next_file_number);
  }
  if (!status.ok()) {
    ::close(manifest_fd);
    manifest_fd = -1;
    return status;
  }

  manifest_number = state.next_file_number;
  ++state.next_file_number;
  next_file_number.store(
      state.next_file_number, std::memory_order_release);
  last_sequence.store(0, std::memory_order_release);
  current = std::make_shared<Version>();
  return Status::OK();
}

Status VersionSet::Impl::Recover() {
  uint64_t active_manifest = 0;
  Status status =
      ReadCurrentManifestNumber(directory, &active_manifest);
  if (!status.ok()) {
    return status;
  }

  const std::string manifest_path = JoinPath(
      directory, ManifestFileNameForNumber(active_manifest));
  std::string manifest_data;
  status = ReadWholeFile(
      manifest_path, kManifestMaxFileSize, &manifest_data);
  if (!status.ok()) {
    return status;
  }

  ManifestState state;
  bool truncated_tail = false;
  status = ParseManifest(
      Slice(manifest_data), &state, &truncated_tail);
  if (!status.ok()) {
    return status;
  }

  manifest_fd = ::open(
      manifest_path.c_str(), O_RDWR | O_APPEND | O_CLOEXEC);
  if (manifest_fd < 0) {
    return MakeIoError("open manifest for append");
  }
  if (truncated_tail) {
    if (::ftruncate(
            manifest_fd,
            static_cast<off_t>(state.valid_bytes)) < 0) {
      status = MakeIoError("truncate manifest tail");
    } else {
      status = SyncFd(manifest_fd, "fsync truncated manifest");
    }
    if (!status.ok()) {
      ::close(manifest_fd);
      manifest_fd = -1;
      return status;
    }
  }

  uint64_t max_file_number = 0;
  status = ScanMaxNumberedFile(directory, &max_file_number);
  if (status.ok()) {
    status = ValidateLiveTableFiles(state.version, directory);
  }
  if (!status.ok()) {
    ::close(manifest_fd);
    manifest_fd = -1;
    return status;
  }

  manifest_number = active_manifest;
  next_file_number.store(
      std::max(state.next_file_number, max_file_number + 1),
      std::memory_order_release);
  last_sequence.store(
      state.last_sequence, std::memory_order_release);
  current = std::make_shared<Version>(std::move(state.version));
  return Status::OK();
}

uint64_t Version::TotalBytes(uint32_t level) const {
  if (!IsValidLevel(level)) {
    return 0;
  }

  uint64_t total = 0;
  for (const FileMetaData& metadata : levels[level]) {
    if (metadata.file_size >
        std::numeric_limits<uint64_t>::max() - total) {
      return std::numeric_limits<uint64_t>::max();
    }
    total += metadata.file_size;
  }
  return total;
}

Status ValidateFileMetaData(const FileMetaData& metadata) {
  if (metadata.file_number == 0) {
    return Status::InvalidArgument(
        "sstable file number must be non-zero");
  }
  if (metadata.file_size == 0) {
    return Status::InvalidArgument(
        "sstable file size must be non-zero");
  }
  if (metadata.smallest_key > metadata.largest_key) {
    return Status::InvalidArgument(
        "sstable user key range is reversed");
  }
  return Status::OK();
}

Status ValidateVersion(const Version& version) {
  std::unordered_set<uint64_t> file_numbers;

  for (uint32_t level = 0; level < kNumLevels; ++level) {
    const std::vector<FileMetaData>& files =
        version.levels[level];
    for (const FileMetaData& metadata : files) {
      const Status status = ValidateFileMetaData(metadata);
      if (!status.ok()) {
        return status;
      }
      if (!file_numbers.insert(metadata.file_number).second) {
        return Status::InvalidArgument(
            "duplicate sstable file number");
      }
    }

    if (level == 0) {
      for (size_t i = 1; i < files.size(); ++i) {
        if (files[i - 1].file_number <
            files[i].file_number) {
          return Status::InvalidArgument(
              "L0 files must be ordered newest first");
        }
      }
      continue;
    }

    for (size_t i = 1; i < files.size(); ++i) {
      if (files[i - 1].smallest_key >=
          files[i].smallest_key) {
        return Status::InvalidArgument(
            "L1+ files must be ordered by smallest key");
      }
      if (files[i - 1].largest_key >=
          files[i].smallest_key) {
        return Status::InvalidArgument(
            "L1+ sstable key ranges overlap");
      }
    }
  }

  return Status::OK();
}

Status ApplyVersionEdit(const Version& base,
                        const VersionEdit& edit,
                        Version* output) {
  if (output == nullptr) {
    return Status::InvalidArgument(
        "version edit output is null");
  }

  std::unordered_set<uint64_t> added_numbers;
  std::unordered_set<uint64_t> deleted_numbers;
  for (const FileAddition& addition : edit.added_files) {
    if (!IsValidLevel(addition.level)) {
      return Status::InvalidArgument(
          "version edit addition level is invalid");
    }
    const Status status =
        ValidateFileMetaData(addition.metadata);
    if (!status.ok()) {
      return status;
    }
    if (!added_numbers.insert(
            addition.metadata.file_number).second) {
      return Status::InvalidArgument(
          "version edit adds a file more than once");
    }
  }
  for (const FileDeletion& deletion : edit.deleted_files) {
    if (!IsValidLevel(deletion.level)) {
      return Status::InvalidArgument(
          "version edit deletion level is invalid");
    }
    if (deletion.file_number == 0) {
      return Status::InvalidArgument(
          "version edit deletes file number zero");
    }
    if (!deleted_numbers.insert(
            deletion.file_number).second) {
      return Status::InvalidArgument(
          "version edit deletes a file more than once");
    }
  }
  for (uint64_t file_number : added_numbers) {
    if (deleted_numbers.count(file_number) != 0) {
      return Status::InvalidArgument(
          "version edit both adds and deletes a file");
    }
  }

  Version next_version = base;
  for (const FileDeletion& deletion : edit.deleted_files) {
    std::vector<FileMetaData>& files =
        next_version.levels[deletion.level];
    const auto found = std::find_if(
        files.begin(), files.end(),
        [&](const FileMetaData& metadata) {
          return metadata.file_number ==
                 deletion.file_number;
        });
    if (found == files.end()) {
      return Status::InvalidArgument(
          "version edit deletes a missing file");
    }
    files.erase(found);
  }

  for (uint32_t level = 0; level < kNumLevels; ++level) {
    for (const FileMetaData& metadata :
         next_version.levels[level]) {
      if (added_numbers.count(metadata.file_number) != 0) {
        return Status::InvalidArgument(
            "version edit reuses an existing file number");
      }
    }
  }

  for (const FileAddition& addition : edit.added_files) {
    next_version.levels[addition.level].push_back(
        addition.metadata);
  }

  std::sort(
      next_version.levels[0].begin(),
      next_version.levels[0].end(),
      [](const FileMetaData& lhs,
         const FileMetaData& rhs) {
        return lhs.file_number > rhs.file_number;
      });
  for (uint32_t level = 1; level < kNumLevels; ++level) {
    std::sort(
        next_version.levels[level].begin(),
        next_version.levels[level].end(),
        [](const FileMetaData& lhs,
           const FileMetaData& rhs) {
          if (lhs.smallest_key != rhs.smallest_key) {
            return lhs.smallest_key < rhs.smallest_key;
          }
          return lhs.file_number < rhs.file_number;
        });
  }

  const Status status = ValidateVersion(next_version);
  if (!status.ok()) {
    return status;
  }

  *output = std::move(next_version);
  return Status::OK();
}

Status VersionSet::Open(
    std::string directory,
    std::unique_ptr<VersionSet>* version_set) {
  if (version_set == nullptr || directory.empty()) {
    return Status::InvalidArgument(
        "invalid VersionSet open arguments");
  }
  version_set->reset();

  Status status = EnsureDirectory(directory);
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<Impl> impl =
      std::make_unique<Impl>(std::move(directory));
  const std::string current_path =
      JoinPath(impl->directory, "CURRENT");
  if (::access(current_path.c_str(), F_OK) == 0) {
    status = impl->Recover();
  } else if (errno == ENOENT) {
    status = impl->Create();
  } else {
    status = MakeIoError("check CURRENT");
  }
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<VersionSet> opened(
      new VersionSet(std::move(impl)));
  opened->impl_->owner = opened.get();
  status = opened->impl_->CollectObsoleteFiles();
  if (!status.ok()) {
    return status;
  }
  opened->impl_->StartBackgroundWorker();
  *version_set = std::move(opened);
  return Status::OK();
}

VersionSet::VersionSet(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

VersionSet::~VersionSet() = default;

VersionPtr VersionSet::CurrentVersion() const {
  return std::atomic_load_explicit(
      &impl_->current, std::memory_order_acquire);
}

uint64_t VersionSet::LastSequence() const {
  return impl_->last_sequence.load(std::memory_order_acquire);
}

uint64_t VersionSet::NextFileNumber() const {
  return impl_->next_file_number.load(std::memory_order_acquire);
}

Status VersionSet::NewFileNumber(uint64_t* file_number) {
  if (file_number == nullptr) {
    return Status::InvalidArgument(
        "new file number output is null");
  }
  if (impl_->manifest_poisoned.load(std::memory_order_acquire)) {
    return Status::IOError(
        "VersionSet manifest is not writable");
  }

  uint64_t current =
      impl_->next_file_number.load(std::memory_order_acquire);
  for (;;) {
    if (current == 0 ||
        current == std::numeric_limits<uint64_t>::max()) {
      return Status::InvalidArgument(
          "sstable file numbers are exhausted");
    }
    if (impl_->next_file_number.compare_exchange_weak(
            current, current + 1,
            std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
  *file_number = current;
  return Status::OK();
}

Status VersionSet::LogAndApply(VersionEdit edit) {
  std::lock_guard<std::mutex> lock(impl_->apply_mutex);
  if (impl_->manifest_poisoned.load(std::memory_order_acquire)) {
    return Status::IOError(
        "VersionSet manifest is not writable");
  }

  VersionPtr base = CurrentVersion();
  if (base == nullptr) {
    return Status::Corruption(
        "VersionSet has no current version");
  }
  if (edit.has_last_sequence) {
    if (edit.last_sequence == kMaxSequenceNumber ||
        edit.last_sequence <
            impl_->last_sequence.load(std::memory_order_acquire)) {
      return Status::InvalidArgument(
          "version edit last sequence is invalid");
    }
  }

  const uint64_t next_number =
      impl_->next_file_number.load(std::memory_order_acquire);
  for (const FileAddition& addition : edit.added_files) {
    if (addition.metadata.file_number >= next_number) {
      return Status::InvalidArgument(
          "version edit uses an unallocated file number");
    }
    const std::string path = JoinPath(
        impl_->directory,
        TableFileNameForNumber(addition.metadata.file_number));
    struct stat file_stat {};
    if (::stat(path.c_str(), &file_stat) < 0) {
      return MakeIoError("stat version edit sstable");
    }
    if (!S_ISREG(file_stat.st_mode) || file_stat.st_size < 0 ||
        static_cast<uint64_t>(file_stat.st_size) !=
            addition.metadata.file_size) {
      return Status::InvalidArgument(
          "version edit sstable size mismatch");
    }
  }

  Version updated;
  Status status = ApplyVersionEdit(*base, edit, &updated);
  if (!status.ok()) {
    return status;
  }

  std::string payload;
  status = EncodeEditPayload(next_number, edit, &payload);
  if (!status.ok()) {
    return status;
  }
  status = AppendManifestRecord(impl_->manifest_fd, Slice(payload));
  if (!status.ok()) {
    impl_->manifest_poisoned.store(
        true, std::memory_order_release);
    return status;
  }

  VersionPtr next =
      std::make_shared<Version>(std::move(updated));
  VersionPtr previous = std::atomic_exchange_explicit(
      &impl_->current, next, std::memory_order_acq_rel);
  RetireVersion(
      &impl_->retired_mutex, &impl_->retired_versions,
      std::move(previous));
  if (edit.has_last_sequence) {
    impl_->last_sequence.store(
        edit.last_sequence, std::memory_order_release);
  }
  return Status::OK();
}

std::string VersionSet::TableFileName(
    uint64_t file_number) const {
  return JoinPath(
      impl_->directory, TableFileNameForNumber(file_number));
}

std::string VersionSet::ManifestFileName(
    uint64_t file_number) const {
  return JoinPath(
      impl_->directory, ManifestFileNameForNumber(file_number));
}

Status VersionSet::GetLiveFiles(
    std::vector<std::string>* file_names) const {
  if (file_names == nullptr) {
    return Status::InvalidArgument(
        "live file output is null");
  }
  VersionPtr version = CurrentVersion();
  if (version == nullptr) {
    return Status::Corruption(
        "VersionSet has no current version");
  }

  std::vector<std::string> live;
  live.push_back("CURRENT");
  live.push_back(
      ManifestFileNameForNumber(impl_->manifest_number));
  for (uint32_t level = 0; level < kNumLevels; ++level) {
    for (const FileMetaData& metadata : version->levels[level]) {
      live.push_back(TableFileNameForNumber(metadata.file_number));
    }
  }
  std::sort(live.begin(), live.end());
  live.erase(std::unique(live.begin(), live.end()), live.end());
  file_names->swap(live);
  return Status::OK();
}

const std::string& VersionSet::directory() const {
  return impl_->directory;
}

Status VersionSet::Impl::FlushMemTable(
    const FlushRequest& request) {
  if (request.memtable == nullptr) {
    return Status::InvalidArgument(
        "flush request has no memtable");
  }
  if (owner == nullptr) {
    return Status::Corruption(
        "VersionSet owner is not initialized");
  }

  auto iterator = request.memtable->NewIterator();
  iterator->SeekToFirst();
  if (!iterator->Valid()) {
    if (!request.wal_path.empty()) {
      std::error_code error;
      std::filesystem::remove(request.wal_path, error);
      if (error) {
        return Status::IOError(
            "remove empty memtable wal: " + error.message());
      }
    }
    return Status::OK();
  }

  uint64_t file_number = 0;
  Status status = owner->NewFileNumber(&file_number);
  if (!status.ok()) {
    return status;
  }
  const std::string table_path =
      owner->TableFileName(file_number);

  SSTableBuilder builder(table_path);
  std::string smallest_key;
  std::string largest_key;
  bool has_entry = false;
  while (iterator->Valid()) {
    const InternalKey& key = iterator->key();
    if (!has_entry) {
      smallest_key = key.user_key;
      has_entry = true;
    }
    largest_key = key.user_key;
    status = builder.Add(key, iterator->value());
    if (!status.ok()) {
      break;
    }
    iterator->Next();
  }
  if (status.ok()) {
    status = builder.Finish();
  }
  if (!status.ok()) {
    std::error_code ignored;
    std::filesystem::remove(table_path, ignored);
    return status;
  }

  std::error_code size_error;
  const uint64_t file_size =
      std::filesystem::file_size(table_path, size_error);
  if (size_error || file_size == 0) {
    std::error_code ignored;
    std::filesystem::remove(table_path, ignored);
    return Status::IOError("read flushed sstable size");
  }

  FileMetaData metadata;
  metadata.file_number = file_number;
  metadata.file_size = file_size;
  metadata.smallest_key = std::move(smallest_key);
  metadata.largest_key = std::move(largest_key);

  VersionEdit edit;
  edit.added_files.push_back(
      FileAddition{0, std::move(metadata)});
  const uint64_t memtable_sequence =
      request.memtable->LastSequence();
  if (memtable_sequence > owner->LastSequence()) {
    edit.has_last_sequence = true;
    edit.last_sequence = memtable_sequence;
  }

  status = owner->LogAndApply(std::move(edit));
  if (!status.ok()) {
    std::error_code ignored;
    std::filesystem::remove(table_path, ignored);
    return status;
  }

  if (!request.wal_path.empty()) {
    std::error_code error;
    std::filesystem::remove(request.wal_path, error);
    if (error) {
      return Status::IOError(
          "remove flushed memtable wal: " + error.message());
    }
  }
  return Status::OK();
}

Status VersionSet::Impl::RunCompactions() {
  if (owner == nullptr) {
    return Status::Corruption(
        "VersionSet owner is not initialized");
  }

  for (;;) {
    VersionPtr input_version = owner->CurrentVersion();
    if (input_version == nullptr) {
      return Status::Corruption(
          "VersionSet has no current version");
    }

    CompactionInput input;
    if (!PickCompaction(
            *input_version, &compaction_pointer, &input)) {
      return Status::OK();
    }

    const Status status =
        RunCompaction(directory, input_version, input, owner);
    if (!status.ok()) {
      return status;
    }
    const Status cleanup_status = CollectObsoleteFiles();
    if (!cleanup_status.ok()) {
      return cleanup_status;
    }
  }
}

Status VersionSet::Impl::CollectObsoleteFiles() {
  VersionPtr live_version = std::atomic_load_explicit(
      &current, std::memory_order_acquire);
  if (live_version == nullptr) {
    return Status::Corruption(
        "VersionSet has no current version");
  }

  std::unordered_set<uint64_t> live_numbers;
  for (uint32_t level = 0; level < kNumLevels; ++level) {
    for (const FileMetaData& metadata :
         live_version->levels[level]) {
      live_numbers.insert(metadata.file_number);
    }
  }

  {
    std::lock_guard<std::mutex> lock(retired_mutex);
    retired_versions.erase(
        std::remove_if(
            retired_versions.begin(), retired_versions.end(),
            [](const VersionPtr& version) {
              return version.use_count() == 1;
            }),
        retired_versions.end());
    for (const VersionPtr& version : retired_versions) {
      for (uint32_t level = 0; level < kNumLevels; ++level) {
        for (const FileMetaData& metadata :
             version->levels[level]) {
          live_numbers.insert(metadata.file_number);
        }
      }
    }
  }

  std::error_code error;
  std::filesystem::directory_iterator iterator(directory, error);
  if (error) {
    return Status::IOError(
        "iterate database directory: " + error.message());
  }
  for (const std::filesystem::directory_entry& entry : iterator) {
    if (!entry.is_regular_file(error)) {
      error.clear();
      continue;
    }
    const std::string name = entry.path().filename().string();
    uint64_t file_number = 0;
    if (!ParseNumberedFileName(name, "", ".sst", &file_number) ||
        live_numbers.count(file_number) != 0) {
      continue;
    }
    std::filesystem::remove(entry.path(), error);
    if (error) {
      return Status::IOError(
          "remove obsolete sstable: " + error.message());
    }
  }
  return Status::OK();
}

void VersionSet::Impl::StartBackgroundWorker() {
  std::lock_guard<std::mutex> lock(background_mutex);
  if (background_thread.joinable()) {
    return;
  }
  shutting_down = false;
  background_thread = std::thread(&Impl::RunBackgroundWorker, this);
}

void VersionSet::Impl::StopBackgroundWorker() {
  {
    std::lock_guard<std::mutex> lock(background_mutex);
    shutting_down = true;
    pending_work = false;
    background_cv.notify_all();
    idle_cv.notify_all();
  }
  if (background_thread.joinable()) {
    background_thread.join();
  }
}

void VersionSet::Impl::RunBackgroundWorker() {
  for (;;) {
    FlushRequest flush;
    bool has_flush = false;
    bool run_compaction = false;
    {
      std::unique_lock<std::mutex> lock(background_mutex);
      background_cv.wait(lock, [&] {
        return shutting_down || pending_work;
      });
      if (shutting_down) {
        pending_work = false;
        idle_cv.notify_all();
        return;
      }
      if (!background_status.ok()) {
        pending_work = false;
        idle_cv.notify_all();
        return;
      }
      if (!flush_queue.empty()) {
        flush = std::move(flush_queue.front());
        flush_queue.pop_front();
        has_flush = true;
      } else if (compaction_requested) {
        compaction_requested = false;
        run_compaction = true;
      } else {
        pending_work = false;
        idle_cv.notify_all();
        continue;
      }
    }

    Status status = Status::OK();
    if (has_flush) {
      status = FlushMemTable(flush);
    }
    if (status.ok() && (has_flush || run_compaction)) {
      status = RunCompactions();
    }
    if (status.ok()) {
      status = CollectObsoleteFiles();
    }

    {
      std::lock_guard<std::mutex> lock(background_mutex);
      if (!status.ok()) {
        background_status = status;
        flush_queue.clear();
        compaction_requested = false;
        pending_work = false;
        idle_cv.notify_all();
        return;
      }
      if (flush_queue.empty() && !compaction_requested) {
        pending_work = false;
        idle_cv.notify_all();
      }
    }
  }
}

Status VersionSet::MaybeScheduleFlush(FlushRequest request) {
  if (request.memtable == nullptr) {
    return Status::InvalidArgument(
        "flush request has no memtable");
  }

  std::lock_guard<std::mutex> lock(impl_->background_mutex);
  if (impl_->shutting_down) {
    return Status::InvalidArgument("VersionSet is shutting down");
  }
  if (!impl_->background_status.ok()) {
    return impl_->background_status;
  }
  impl_->flush_queue.push_back(std::move(request));
  impl_->pending_work = true;
  impl_->background_cv.notify_one();
  return Status::OK();
}

Status VersionSet::MaybeScheduleCompaction() {
  std::lock_guard<std::mutex> lock(impl_->background_mutex);
  if (impl_->shutting_down) {
    return Status::InvalidArgument("VersionSet is shutting down");
  }
  if (!impl_->background_status.ok()) {
    return impl_->background_status;
  }
  impl_->compaction_requested = true;
  impl_->pending_work = true;
  impl_->background_cv.notify_one();
  return Status::OK();
}

Status VersionSet::WaitForBackgroundWork() {
  Status status;
  {
    std::unique_lock<std::mutex> lock(impl_->background_mutex);
    impl_->idle_cv.wait(lock, [&] {
      return !impl_->pending_work;
    });
    status = impl_->background_status;
  }
  if (!status.ok()) {
    return status;
  }
  return impl_->CollectObsoleteFiles();
}

Status VersionSet::BackgroundStatus() const {
  std::lock_guard<std::mutex> lock(impl_->background_mutex);
  return impl_->background_status;
}

}  // namespace raftkv
