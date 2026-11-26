#include "compaction_internal.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "raftkv/storage/iterator.h"
#include "raftkv/storage/merging_iterator.h"
#include "raftkv/storage/sstable.h"

namespace raftkv {
namespace {

bool KeyInFile(const std::string& key,
               const FileMetaData& metadata) {
  return metadata.smallest_key <= key &&
         key <= metadata.largest_key;
}

bool CanDropDeletion(const Version& version,
                     const CompactionInput& input,
                     const std::string& key) {
  if (input.target_level + 1 != kNumLevels) {
    return false;
  }
  for (uint32_t level = 0; level < input.source_level; ++level) {
    for (const FileMetaData& metadata : version.levels[level]) {
      if (KeyInFile(key, metadata)) {
        return false;
      }
    }
  }
  return true;
}

void RemoveOutputFiles(
    const std::vector<std::string>& output_paths) {
  for (const std::string& path : output_paths) {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
  }
}

}  // namespace

Status RunCompaction(const std::string& directory,
                     const VersionPtr& input_version,
                     const CompactionInput& input,
                     VersionSet* version_set) {
  if (input_version == nullptr || version_set == nullptr ||
      directory.empty() ||
      input.source_level >= input.target_level ||
      input.target_level >= kNumLevels ||
      input.source_files.empty()) {
    return Status::InvalidArgument("invalid compaction input");
  }

  std::vector<std::unique_ptr<SSTableReader>> readers;
  std::vector<std::unique_ptr<StorageIterator>> children;
  std::vector<FileMetaData> all_inputs = input.source_files;
  all_inputs.insert(all_inputs.end(),
                    input.target_files.begin(),
                    input.target_files.end());
  readers.reserve(all_inputs.size());
  children.reserve(all_inputs.size());
  for (const FileMetaData& metadata : all_inputs) {
    std::unique_ptr<SSTableReader> reader;
    const Status status = SSTableReader::Open(
        version_set->TableFileName(metadata.file_number),
        nullptr, &reader);
    if (!status.ok()) {
      return status;
    }
    children.push_back(reader->NewIterator());
    readers.push_back(std::move(reader));
  }

  MergingIterator merger(
      std::move(children), DeletionOutputPolicy::kKeep);
  merger.SeekToFirst();
  if (!merger.status().ok()) {
    return merger.status();
  }

  VersionEdit edit;
  for (const FileMetaData& metadata : input.source_files) {
    edit.deleted_files.push_back(
        FileDeletion{input.source_level, metadata.file_number});
  }
  for (const FileMetaData& metadata : input.target_files) {
    edit.deleted_files.push_back(
        FileDeletion{input.target_level, metadata.file_number});
  }

  std::vector<std::string> output_paths;
  std::unique_ptr<SSTableBuilder> builder;
  std::string current_path;
  std::string smallest_key;
  std::string largest_key;
  uint64_t current_file_number = 0;
  uint64_t estimated_size = 0;
  bool has_output_entry = false;

  auto finish_output = [&]() -> Status {
    if (builder == nullptr) {
      return Status::OK();
    }
    Status status = builder->Finish();
    builder.reset();
    if (!status.ok()) {
      return status;
    }

    std::error_code size_error;
    const uint64_t file_size =
        std::filesystem::file_size(current_path, size_error);
    if (size_error || file_size == 0) {
      return Status::IOError("read compacted sstable size");
    }

    FileMetaData metadata;
    metadata.file_number = current_file_number;
    metadata.file_size = file_size;
    metadata.smallest_key = smallest_key;
    metadata.largest_key = largest_key;
    status = ValidateFileMetaData(metadata);
    if (!status.ok()) {
      return status;
    }
    edit.added_files.push_back(
        FileAddition{input.target_level, std::move(metadata)});
    smallest_key.clear();
    largest_key.clear();
    current_file_number = 0;
    current_path.clear();
    estimated_size = 0;
    has_output_entry = false;
    return Status::OK();
  };

  Status status = Status::OK();
  while (merger.Valid()) {
    const InternalKey key = merger.key();
    std::string value = merger.value().ToString();
    const std::string user_key = key.user_key;
    const bool deletion = key.type == ValueType::kDeletion;
    const bool drop =
        deletion &&
        CanDropDeletion(*input_version, input, user_key);
    if (!drop) {
      if (builder != nullptr &&
          estimated_size >= kTargetSSTableFileSize) {
        status = finish_output();
        if (!status.ok()) {
          break;
        }
      }

      if (builder == nullptr) {
        status = version_set->NewFileNumber(&current_file_number);
        if (!status.ok()) {
          break;
        }
        current_path =
            version_set->TableFileName(current_file_number);
        builder = std::make_unique<SSTableBuilder>(current_path);
        output_paths.push_back(current_path);
      }

      status = builder->Add(key, Slice(value));
      if (!status.ok()) {
        break;
      }
      if (!has_output_entry) {
        smallest_key = user_key;
        has_output_entry = true;
      }
      largest_key = user_key;
      estimated_size += user_key.size() + value.size() + 32;
    }
    merger.Next();
  }

  if (status.ok() && !merger.status().ok()) {
    status = merger.status();
  }
  if (status.ok()) {
    status = finish_output();
  }
  if (!status.ok()) {
    RemoveOutputFiles(output_paths);
    return status;
  }

  status = version_set->LogAndApply(std::move(edit));
  if (!status.ok()) {
    RemoveOutputFiles(output_paths);
    return status;
  }
  return Status::OK();
}

}  // namespace raftkv
