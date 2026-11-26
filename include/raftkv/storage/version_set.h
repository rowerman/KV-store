#ifndef RAFTKV_STORAGE_VERSION_SET_H_
#define RAFTKV_STORAGE_VERSION_SET_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "raftkv/common/status.h"

namespace raftkv {

class MemTable;

inline constexpr uint32_t kNumLevels = 4;
inline constexpr size_t kL0CompactionTrigger = 4;
inline constexpr uint64_t kL1MaxBytes = 10ULL * 1024ULL * 1024ULL;
inline constexpr uint64_t kL2MaxBytes = 100ULL * 1024ULL * 1024ULL;
inline constexpr uint64_t kL3MaxBytes = 1024ULL * 1024ULL * 1024ULL;
inline constexpr uint64_t kTargetSSTableFileSize =
    2ULL * 1024ULL * 1024ULL;

// 单个不可变 SSTable 的用户键范围与磁盘占用。
struct FileMetaData {
  uint64_t file_number = 0;
  uint64_t file_size = 0;
  std::string smallest_key;
  std::string largest_key;
};

// 发布后不可再修改；读取方通过 shared_ptr<const Version> 延长生命周期。
struct Version {
  std::array<std::vector<FileMetaData>, kNumLevels> levels;

  uint64_t TotalBytes(uint32_t level) const;
};

using VersionPtr = std::shared_ptr<const Version>;

struct FileAddition {
  uint32_t level = 0;
  FileMetaData metadata;
};

struct FileDeletion {
  uint32_t level = 0;
  uint64_t file_number = 0;
};

// 一次 Flush 或 Compaction 对 Version 的原子修改。
struct VersionEdit {
  std::vector<FileAddition> added_files;
  std::vector<FileDeletion> deleted_files;
  uint64_t last_sequence = 0;
  bool has_last_sequence = false;
};

// 校验单个文件的编号、大小、层范围和用户键范围。
Status ValidateFileMetaData(const FileMetaData& metadata);

// 校验 L0 新到旧、L1-L3 有序不重叠、文件编号全局唯一。
Status ValidateVersion(const Version& version);

// 在 base 上应用 edit；失败时不修改 output。
Status ApplyVersionEdit(const Version& base,
                        const VersionEdit& edit,
                        Version* output);

// Immutable MemTable 的后台 Flush 请求。
struct FlushRequest {
  std::shared_ptr<const MemTable> memtable;
  std::string wal_path;
};

// 管理不可变 Version、MANIFEST、CURRENT 和文件编号。
class VersionSet {
 public:
  // 目录为空时创建，已有 CURRENT 时恢复。
  static Status Open(std::string directory,
                     std::unique_ptr<VersionSet>* version_set);
  ~VersionSet();

  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  VersionPtr CurrentVersion() const;
  uint64_t LastSequence() const;
  uint64_t NextFileNumber() const;

  Status NewFileNumber(uint64_t* file_number);
  Status LogAndApply(VersionEdit edit);

  Status MaybeScheduleFlush(FlushRequest request);
  Status MaybeScheduleCompaction();
  Status WaitForBackgroundWork();
  Status BackgroundStatus() const;


  std::string TableFileName(uint64_t file_number) const;
  std::string ManifestFileName(uint64_t file_number) const;
  Status GetLiveFiles(std::vector<std::string>* file_names) const;

  const std::string& directory() const;

 private:
  struct Impl;
  explicit VersionSet(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_VERSION_SET_H_
