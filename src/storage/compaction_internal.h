#ifndef RAFTKV_SRC_STORAGE_COMPACTION_INTERNAL_H_
#define RAFTKV_SRC_STORAGE_COMPACTION_INTERNAL_H_

#include <cstdint>
#include <string>
#include <vector>

#include "raftkv/storage/version_set.h"

namespace raftkv {

struct CompactionInput {
  uint32_t source_level = 0;
  uint32_t target_level = 0;
  std::vector<FileMetaData> source_files;
  std::vector<FileMetaData> target_files;
};

Status RunCompaction(const std::string& directory,
                     const VersionPtr& input_version,
                     const CompactionInput& input,
                     VersionSet* version_set);

}  // namespace raftkv

#endif  // RAFTKV_SRC_STORAGE_COMPACTION_INTERNAL_H_

