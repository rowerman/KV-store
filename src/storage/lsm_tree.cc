#include "raftkv/storage/lsm_tree.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "raftkv/storage/lru_cache.h"
#include "raftkv/storage/memtable.h"
#include "raftkv/storage/merging_iterator.h"
#include "raftkv/storage/sstable.h"
#include "raftkv/storage/version_set.h"
#include "raftkv/storage/wal.h"

namespace raftkv {

namespace {

Status MakeIoError(const char* operation) {
  return Status::IOError(
      std::string(operation) + ": " + std::strerror(errno));
}

std::string JoinPath(const std::string& directory,
                     const std::string& name) {
  if (directory.empty() || directory.back() == '/') {
    return directory + name;
  }
  return directory + "/" + name;
}

std::string WalFileName(uint64_t number) {
  char digits[32] = {};
  std::snprintf(digits, sizeof(digits), "%06llu",
                static_cast<unsigned long long>(number));
  return std::string("WAL-") + digits + ".log";
}

bool ParseWalFileName(const std::string& name,
                      uint64_t* number) {
  constexpr const char* prefix = "WAL-";
  constexpr const char* suffix = ".log";
  const size_t prefix_size = std::strlen(prefix);
  const size_t suffix_size = std::strlen(suffix);
  if (number == nullptr ||
      name.size() <= prefix_size + suffix_size ||
      name.compare(0, prefix_size, prefix) != 0 ||
      name.compare(name.size() - suffix_size,
                   suffix_size, suffix) != 0) {
    return false;
  }

  const char* begin = name.data() + prefix_size;
  const char* end = name.data() + name.size() - suffix_size;
  uint64_t parsed = 0;
  const auto result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc() || result.ptr != end ||
      parsed == 0 ||
      name != WalFileName(parsed)) {
    return false;
  }
  *number = parsed;
  return true;
}

Status SyncDirectory(const std::string& directory) {
  const int fd = ::open(
      directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return MakeIoError("open LSM directory");
  }
  Status status;
  while (::fsync(fd) < 0) {
    if (errno == EINTR) {
      continue;
    }
    status = MakeIoError("fsync LSM directory");
    break;
  }
  if (::close(fd) < 0 && status.ok()) {
    status = MakeIoError("close LSM directory");
  }
  return status;
}

Status ScanWalFiles(
    const std::string& directory,
    std::vector<std::pair<uint64_t, std::string>>* files) {
  if (files == nullptr) {
    return Status::InvalidArgument(
        "WAL file output is null");
  }

  files->clear();
  std::error_code error;
  std::filesystem::directory_iterator iterator(directory, error);
  if (error) {
    return Status::IOError(
        "iterate LSM directory: " + error.message());
  }
  for (const std::filesystem::directory_entry& entry : iterator) {
    if (!entry.is_regular_file(error)) {
      error.clear();
      continue;
    }
    const std::string name = entry.path().filename().string();
    uint64_t number = 0;
    if (ParseWalFileName(name, &number)) {
      files->emplace_back(number, entry.path().string());
    }
  }
  std::sort(
      files->begin(), files->end(),
      [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
      });
  return Status::OK();
}

Status RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    return Status::IOError(
        "remove file: " + error.message());
  }
  return Status::OK();
}

std::string ParentDirectory(const std::string& path) {
  const std::filesystem::path parent =
      std::filesystem::path(path).parent_path();
  return parent.empty() ? std::string(".") : parent.string();
}

std::string UniqueSiblingPath(const std::string& target,
                              const char* tag) {
  static std::atomic<uint64_t> sequence{0};
  const uint64_t stamp =
      static_cast<uint64_t>(
          std::chrono::steady_clock::now()
              .time_since_epoch()
              .count());
  for (uint64_t attempt = 0; attempt < 1000; ++attempt) {
    const std::string candidate =
        target + "." + tag + "." +
        std::to_string(static_cast<long long>(::getpid())) + "." +
        std::to_string(stamp) + "." +
        std::to_string(sequence.fetch_add(1));
    if (!std::filesystem::exists(candidate)) {
      return candidate;
    }
  }
  return std::string();
}

Status RenamePath(const std::string& source,
                  const std::string& destination) {
  std::error_code error;
  std::filesystem::rename(source, destination, error);
  if (error) {
    return Status::IOError(
        "rename " + source + " to " + destination +
        ": " + error.message());
  }
  return Status::OK();
}

}  // namespace

struct ImmutableMemTable {
  std::shared_ptr<const MemTable> table;
  std::string wal_path;
};

struct LSMTree::Impl {
  Impl(std::string path, LSMTOptions config)
      : directory(std::move(path)), options(std::move(config)) {}

  Status RecoverWalFiles();
  Status CreateNextWal(std::unique_ptr<WalWriter>* writer,
                       std::string* path);
  Status OpenActiveWal();
  Status WriteRecord(ValueType type,
                     Slice key, Slice value);
  Status RotateMemTable(bool force);
  Status PruneFlushedMemtables();
  Status LoadReader(
      uint64_t file_number,
      std::shared_ptr<SSTableReader>* reader);
  Status GetLocked(const std::string& key,
                   std::string* value);
  Status ScanLocked(
      const std::string& start_key,
      const std::string& end_key,
      size_t limit,
      std::vector<std::pair<std::string, std::string>>* out);
  Status FlushActiveLocked();
  Status CreateCheckpointLocked(
      const std::string& checkpoint_dir);
  Status OpenStateLocked();
  void CloseStateLocked();
  Status RestoreLocked(const std::string& checkpoint_dir);

  std::string directory;
  LSMTOptions options;
  mutable std::shared_mutex lifecycle_mutex;
  std::unique_ptr<VersionSet> versions;
  std::shared_ptr<LRUCache> block_cache;
  std::shared_ptr<MemTable> active_memtable;
  std::deque<ImmutableMemTable> immutables;
  std::unique_ptr<WalWriter> wal_writer;
  std::string active_wal_path;
  uint64_t next_wal_number = 1;
  uint64_t last_sequence = 0;
  Status fatal_status;

  std::mutex reader_mutex;
  std::unordered_map<uint64_t,
                     std::shared_ptr<SSTableReader>>
      readers;
};

Status LSMTree::Impl::CreateNextWal(
    std::unique_ptr<WalWriter>* writer,
    std::string* path) {
  if (writer == nullptr || path == nullptr) {
    return Status::InvalidArgument(
        "WAL creation output is null");
  }
  if (next_wal_number == 0 ||
      next_wal_number ==
          std::numeric_limits<uint64_t>::max()) {
    return Status::InvalidArgument(
        "WAL file numbers are exhausted");
  }

  const std::string candidate_path =
      JoinPath(directory, WalFileName(next_wal_number));
  auto candidate =
      std::make_unique<WalWriter>(candidate_path);
  Status status = candidate->Open(false);
  if (!status.ok()) {
    return status;
  }
  status = SyncDirectory(directory);
  if (!status.ok()) {
    candidate->Close();
    return status;
  }

  ++next_wal_number;
  *writer = std::move(candidate);
  *path = candidate_path;
  return Status::OK();
}

Status LSMTree::Impl::OpenActiveWal() {
  std::unique_ptr<WalWriter> writer;
  std::string path;
  Status status = CreateNextWal(&writer, &path);
  if (!status.ok()) {
    return status;
  }

  active_wal_path = std::move(path);
  wal_writer = std::move(writer);
  active_memtable = std::make_shared<MemTable>();
  return Status::OK();
}

Status LSMTree::Impl::RecoverWalFiles() {
  std::vector<std::pair<uint64_t, std::string>> wal_files;
  Status status =
      ScanWalFiles(directory, &wal_files);
  if (!status.ok()) {
    return status;
  }

  uint64_t max_wal_number = 0;
  uint64_t manifest_sequence = versions->LastSequence();
  last_sequence = manifest_sequence;
  uint64_t observed_sequence = manifest_sequence;
  for (const auto& wal_file : wal_files) {
    max_wal_number =
        std::max(max_wal_number, wal_file.first);
    auto recovered = std::make_shared<MemTable>();
    Status callback_status;
    WalReader reader(wal_file.second);
    status = reader.Replay(
        [&](uint64_t sequence, ValueType type,
            Slice key, Slice value) {
          if (!callback_status.ok()) {
            return;
          }
          if (sequence <= manifest_sequence) {
            return;
          }
          if (sequence <= observed_sequence) {
            callback_status = Status::Corruption(
                "WAL sequences are not strictly increasing");
            return;
          }
          const Status add_status = recovered->Add(
              sequence, type, key, value);
          if (!add_status.ok()) {
            callback_status = add_status;
            return;
          }
          observed_sequence = sequence;
          last_sequence =
              std::max(last_sequence, sequence);
        });
    if (!status.ok()) {
      return status;
    }
    if (!callback_status.ok()) {
      return callback_status;
    }

    if (recovered->ApproximateMemoryUsage() == 0) {
      status = RemoveFileIfExists(wal_file.second);
      if (!status.ok()) {
        return status;
      }
      continue;
    }

    ImmutableMemTable immutable;
    immutable.table = std::move(recovered);
    immutable.wal_path = wal_file.second;
    immutables.push_back(immutable);
    status = versions->MaybeScheduleFlush(
        FlushRequest{immutable.table, immutable.wal_path});
    if (!status.ok()) {
      return status;
    }
  }

  if (max_wal_number ==
      std::numeric_limits<uint64_t>::max()) {
    return Status::InvalidArgument(
        "WAL file numbers are exhausted");
  }
  next_wal_number = max_wal_number + 1;
  return OpenActiveWal();
}

Status LSMTree::Impl::PruneFlushedMemtables() {
  const uint64_t durable_sequence = versions->LastSequence();
  for (auto iterator = immutables.begin();
       iterator != immutables.end();) {
    std::error_code error;
    const bool wal_exists =
        std::filesystem::exists(iterator->wal_path, error);
    if (error) {
      return Status::IOError(
          "check immutable WAL: " + error.message());
    }

    if (durable_sequence >=
            iterator->table->LastSequence() &&
        !wal_exists) {
      iterator = immutables.erase(iterator);
    } else {
      ++iterator;
    }
  }
  return Status::OK();
}

Status LSMTree::Impl::RotateMemTable(bool force) {
  if (active_memtable == nullptr || wal_writer == nullptr) {
    return Status::Corruption(
        "LSMTree active memtable is not initialized");
  }
  if (!force &&
      active_memtable->ApproximateMemoryUsage() <
          options.memtable_size_limit) {
    return Status::OK();
  }
  if (active_memtable->ApproximateMemoryUsage() == 0) {
    return Status::OK();
  }

  std::unique_ptr<WalWriter> next_writer;
  std::string next_wal_path;
  Status status =
      CreateNextWal(&next_writer, &next_wal_path);
  if (!status.ok()) {
    return status;
  }

  status = wal_writer->Sync();
  if (status.ok()) {
    status = wal_writer->Close();
  }
  if (!status.ok()) {
    fatal_status = status;
    return status;
  }

  ImmutableMemTable immutable;
  immutable.table = active_memtable;
  immutable.wal_path = active_wal_path;
  immutables.push_back(immutable);

  status = versions->MaybeScheduleFlush(
      FlushRequest{immutable.table, immutable.wal_path});
  if (!status.ok()) {
    fatal_status = status;
    return status;
  }

  active_memtable = std::make_shared<MemTable>();
  wal_writer = std::move(next_writer);
  active_wal_path = std::move(next_wal_path);

  if (immutables.size() >=
      options.max_immutable_memtables) {
    status = versions->WaitForBackgroundWork();
    if (status.ok()) {
      status = PruneFlushedMemtables();
    }
    if (!status.ok()) {
      fatal_status = status;
      return status;
    }
  }
  return Status::OK();
}

Status LSMTree::Impl::WriteRecord(
    ValueType type, Slice key, Slice value) {
  if (!fatal_status.ok()) {
    return fatal_status;
  }
  const Status background_status =
      versions->BackgroundStatus();
  if (!background_status.ok()) {
    fatal_status = background_status;
    return fatal_status;
  }

  Status status = PruneFlushedMemtables();
  if (!status.ok()) {
    fatal_status = status;
    return status;
  }
  if (last_sequence == kMaxSequenceNumber - 1) {
    return Status::InvalidArgument(
        "LSMTree sequence numbers are exhausted");
  }

  const uint64_t sequence = last_sequence + 1;
  status = wal_writer->Append(sequence, type, key, value);
  if (status.ok()) {
    status = wal_writer->Sync();
  }
  if (!status.ok()) {
    fatal_status = status;
    return status;
  }

  status = active_memtable->Add(
      sequence, type, key, value);
  if (!status.ok()) {
    fatal_status = status;
    return status;
  }
  last_sequence = sequence;

  status = RotateMemTable(false);
  if (!status.ok()) {
    fatal_status = status;
    return status;
  }
  return Status::OK();
}

Status LSMTree::Impl::LoadReader(
    uint64_t file_number,
    std::shared_ptr<SSTableReader>* reader) {
  if (reader == nullptr) {
    return Status::InvalidArgument(
        "sstable reader output is null");
  }

  std::lock_guard<std::mutex> lock(reader_mutex);
  const auto cached = readers.find(file_number);
  if (cached != readers.end()) {
    *reader = cached->second;
    return Status::OK();
  }

  std::unique_ptr<SSTableReader> opened;
  Status status = SSTableReader::Open(
      versions->TableFileName(file_number),
      block_cache, &opened);
  if (!status.ok()) {
    return status;
  }
  std::shared_ptr<SSTableReader> shared(opened.release());
  readers.emplace(file_number, shared);
  *reader = std::move(shared);
  return Status::OK();
}

Status LSMTree::Impl::GetLocked(
    const std::string& key, std::string* value) {
  std::string found;
  bool deleted = false;
  Status status =
      active_memtable->Get(Slice(key), &found, &deleted);
  if (status.ok()) {
    if (deleted) {
      return Status::NotFound("key is deleted");
    }
    *value = std::move(found);
    return Status::OK();
  }
  if (status.code() != Code::kNotFound) {
    return status;
  }

  for (auto iterator = immutables.rbegin();
       iterator != immutables.rend(); ++iterator) {
    status = iterator->table->Get(
        Slice(key), &found, &deleted);
    if (status.ok()) {
      if (deleted) {
        return Status::NotFound("key is deleted");
      }
      *value = std::move(found);
      return Status::OK();
    }
    if (status.code() != Code::kNotFound) {
      return status;
    }
  }

  const VersionPtr version = versions->CurrentVersion();
  if (version == nullptr) {
    return Status::Corruption(
        "VersionSet has no current version");
  }
  for (uint32_t level = 0; level < kNumLevels; ++level) {
    for (const FileMetaData& metadata :
         version->levels[level]) {
      std::shared_ptr<SSTableReader> reader;
      status = LoadReader(metadata.file_number, &reader);
      if (!status.ok()) {
        return status;
      }

      SSTableLookupResult result;
      status = reader->Get(Slice(key), &result);
      if (status.ok()) {
        if (result.key.type == ValueType::kDeletion) {
          return Status::NotFound("key is deleted");
        }
        *value = std::move(result.value);
        return Status::OK();
      }
      if (status.code() != Code::kNotFound) {
        return status;
      }
    }
  }
  return Status::NotFound("key not found");
}

Status LSMTree::Impl::ScanLocked(
    const std::string& start_key,
    const std::string& end_key,
    size_t limit,
    std::vector<std::pair<std::string, std::string>>* out) {
  if (start_key >= end_key) {
    return Status::InvalidArgument(
        "scan range is empty or reversed");
  }

  std::vector<std::pair<std::string, std::string>> rows;
  if (limit == 0) {
    out->swap(rows);
    return Status::OK();
  }

  std::vector<std::shared_ptr<SSTableReader>> scan_readers;
  std::vector<std::unique_ptr<StorageIterator>> children;
  children.push_back(active_memtable->NewIterator());
  for (const ImmutableMemTable& immutable : immutables) {
    children.push_back(immutable.table->NewIterator());
  }

  const VersionPtr version = versions->CurrentVersion();
  if (version == nullptr) {
    return Status::Corruption(
        "VersionSet has no current version");
  }
  for (uint32_t level = 0; level < kNumLevels; ++level) {
    for (const FileMetaData& metadata :
         version->levels[level]) {
      std::shared_ptr<SSTableReader> reader;
      Status status =
          LoadReader(metadata.file_number, &reader);
      if (!status.ok()) {
        return status;
      }
      children.push_back(reader->NewIterator());
      scan_readers.push_back(std::move(reader));
    }
  }

  MergingIterator iterator(std::move(children));
  iterator.Seek(Slice(start_key));
  if (!iterator.status().ok()) {
    return iterator.status();
  }
  while (iterator.Valid() && rows.size() < limit) {
    if (iterator.key().user_key >= end_key) {
      break;
    }
    rows.emplace_back(
        iterator.key().user_key,
        iterator.value().ToString());
    iterator.Next();
  }
  if (!iterator.status().ok()) {
    return iterator.status();
  }

  out->swap(rows);
  return Status::OK();
}

Status LSMTree::Impl::FlushActiveLocked() {
  Status status = RotateMemTable(true);
  if (!status.ok()) {
    return status;
  }
  status = versions->WaitForBackgroundWork();
  if (!status.ok()) {
    return status;
  }
  return PruneFlushedMemtables();
}

Status LSMTree::Impl::CreateCheckpointLocked(
    const std::string& checkpoint_dir) {
  if (checkpoint_dir.empty()) {
    return Status::InvalidArgument(
        "checkpoint directory is empty");
  }

  const std::filesystem::path target =
      std::filesystem::absolute(checkpoint_dir)
          .lexically_normal();
  std::error_code error;
  if (std::filesystem::exists(target, error)) {
    return Status::InvalidArgument(
        "checkpoint directory already exists");
  }
  if (error) {
    return Status::IOError(
        "check checkpoint path: " + error.message());
  }

  Status status = FlushActiveLocked();
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string> live_files;
  status = versions->GetLiveFiles(&live_files);
  if (!status.ok()) {
    return status;
  }

  std::filesystem::create_directories(target.parent_path(), error);
  if (error) {
    return Status::IOError(
        "create checkpoint parent: " + error.message());
  }
  const std::string temporary =
      UniqueSiblingPath(target.string(), "checkpoint-tmp");
  if (temporary.empty()) {
    return Status::IOError(
        "cannot allocate checkpoint temporary path");
  }
  if (!std::filesystem::create_directory(temporary, error) || error) {
    return Status::IOError(
        "create checkpoint directory: " + error.message());
  }

  for (const std::string& name : live_files) {
    const std::string source = JoinPath(directory, name);
    const std::string destination = JoinPath(temporary, name);
    if (::link(source.c_str(), destination.c_str()) < 0) {
      status = MakeIoError("hard link checkpoint file");
      break;
    }
  }
  if (status.ok()) {
    status = SyncDirectory(temporary);
  }
  if (!status.ok()) {
    std::filesystem::remove_all(temporary, error);
    return status;
  }

  status = RenamePath(temporary, target.string());
  if (!status.ok()) {
    std::filesystem::remove_all(temporary, error);
    return status;
  }
  return SyncDirectory(ParentDirectory(target.string()));
}

void LSMTree::Impl::CloseStateLocked() {
  if (versions != nullptr) {
    versions->WaitForBackgroundWork();
    versions.reset();
  }
  wal_writer.reset();
  active_memtable.reset();
  immutables.clear();
  {
    std::lock_guard<std::mutex> lock(reader_mutex);
    readers.clear();
  }
  block_cache.reset();
  active_wal_path.clear();
  next_wal_number = 1;
  last_sequence = 0;
  fatal_status = Status::OK();
}

Status LSMTree::Impl::OpenStateLocked() {
  Status status = VersionSet::Open(directory, &versions);
  if (!status.ok()) {
    CloseStateLocked();
    return status;
  }
  block_cache = std::make_shared<LRUCache>(
      options.block_cache_capacity);
  status = RecoverWalFiles();
  if (!status.ok()) {
    CloseStateLocked();
    return status;
  }
  return Status::OK();
}

Status LSMTree::Impl::RestoreLocked(
    const std::string& checkpoint_dir) {
  if (checkpoint_dir.empty()) {
    return Status::InvalidArgument(
        "checkpoint directory is empty");
  }

  const std::filesystem::path source =
      std::filesystem::absolute(checkpoint_dir)
          .lexically_normal();
  const std::filesystem::path current =
      std::filesystem::absolute(directory)
          .lexically_normal();
  if (source == current ||
      source.string().rfind(current.string() + "/", 0) == 0) {
    return Status::InvalidArgument(
        "checkpoint directory is inside the data directory");
  }

  std::error_code error;
  if (!std::filesystem::is_directory(source, error) || error ||
      !std::filesystem::exists(
          source / "CURRENT", error) ||
      error) {
    return Status::InvalidArgument(
        "checkpoint directory is invalid");
  }

  std::unique_ptr<VersionSet> validation;
  Status status = VersionSet::Open(source.string(), &validation);
  if (!status.ok()) {
    return status;
  }
  validation.reset();

  status = versions->WaitForBackgroundWork();
  if (!status.ok()) {
    return status;
  }
  CloseStateLocked();

  const std::string backup =
      UniqueSiblingPath(current.string(), "restore-old");
  if (backup.empty()) {
    status = Status::IOError(
        "cannot allocate restore backup path");
  } else {
    status = RenamePath(current.string(), backup);
  }
  if (!status.ok()) {
    return OpenStateLocked();
  }

  status = RenamePath(source.string(), current.string());
  if (!status.ok()) {
    const Status rollback = RenamePath(backup, current.string());
    const Status reopen = OpenStateLocked();
    if (!rollback.ok()) {
      return rollback;
    }
    if (!reopen.ok()) {
      return reopen;
    }
    return status;
  }

  status = SyncDirectory(ParentDirectory(current.string()));
  if (status.ok()) {
    status = OpenStateLocked();
  }
  if (status.ok()) {
    std::filesystem::remove_all(backup, error);
    if (error) {
      return Status::IOError(
          "remove restore backup: " + error.message());
    }
    return Status::OK();
  }

  const Status restore_error = status;
  CloseStateLocked();
  status = RenamePath(current.string(), source.string());
  if (!status.ok()) {
    return status;
  }
  status = RenamePath(backup, current.string());
  if (!status.ok()) {
    return status;
  }
  status = OpenStateLocked();
  if (!status.ok()) {
    return status;
  }
  return restore_error;
}

Status LSMTree::Open(
    std::string directory,
    std::unique_ptr<LSMTree>* tree) {
  return Open(std::move(directory), LSMTOptions{}, tree);
}

Status LSMTree::Open(
    std::string directory,
    LSMTOptions options,
    std::unique_ptr<LSMTree>* tree) {
  if (tree == nullptr || directory.empty() ||
      options.memtable_size_limit == 0 ||
      options.block_cache_capacity == 0 ||
      options.max_immutable_memtables == 0) {
    return Status::InvalidArgument(
        "invalid LSMTree open arguments");
  }
  tree->reset();

  std::error_code path_error;
  const std::string absolute_directory =
      std::filesystem::absolute(directory, path_error)
          .lexically_normal()
          .string();
  if (path_error) {
    return Status::IOError(
        "resolve LSMTree directory: " + path_error.message());
  }
  directory = absolute_directory;
  auto impl =
      std::make_unique<Impl>(std::move(directory), options);
  Status status = impl->OpenStateLocked();
  if (!status.ok()) {
    return status;
  }

  *tree = std::unique_ptr<LSMTree>(
      new LSMTree(std::move(impl)));
  return Status::OK();
}

LSMTree::LSMTree(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

LSMTree::~LSMTree() = default;

Status LSMTree::Put(const std::string& key,
                    const std::string& value) {
  std::unique_lock<std::shared_mutex> lock(
      impl_->lifecycle_mutex);
  return impl_->WriteRecord(
      ValueType::kValue, Slice(key), Slice(value));
}

Status LSMTree::Delete(const std::string& key) {
  std::unique_lock<std::shared_mutex> lock(
      impl_->lifecycle_mutex);
  return impl_->WriteRecord(
      ValueType::kDeletion, Slice(key), Slice());
}

Status LSMTree::Get(const std::string& key,
                    std::string* value) {
  if (value == nullptr) {
    return Status::InvalidArgument("get output is null");
  }
  std::shared_lock<std::shared_mutex> lock(
      impl_->lifecycle_mutex);
  return impl_->GetLocked(key, value);
}

Status LSMTree::Scan(
    const std::string& start_key,
    const std::string& end_key,
    size_t limit,
    std::vector<std::pair<std::string, std::string>>* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("scan output is null");
  }
  std::shared_lock<std::shared_mutex> lock(
      impl_->lifecycle_mutex);
  return impl_->ScanLocked(
      start_key, end_key, limit, out);
}

Status LSMTree::CreateCheckpoint(
    const std::string& checkpoint_dir) {
  std::unique_lock<std::shared_mutex> lock(
      impl_->lifecycle_mutex);
  return impl_->CreateCheckpointLocked(checkpoint_dir);
}

Status LSMTree::RestoreFromCheckpoint(
    const std::string& checkpoint_dir) {
  std::unique_lock<std::shared_mutex> lock(
      impl_->lifecycle_mutex);
  return impl_->RestoreLocked(checkpoint_dir);
}

const std::string& LSMTree::directory() const {
  return impl_->directory;
}

}  // namespace raftkv
