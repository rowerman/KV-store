#ifndef RAFTKV_RAFT_SNAPSHOT_BUNDLE_H_
#define RAFTKV_RAFT_SNAPSHOT_BUNDLE_H_

#include <cstdint>
#include <string>

#include "raftkv/common/status.h"

namespace raftkv {

inline constexpr uint64_t kMaxSnapshotChunkSize =
    16ULL * 1024ULL * 1024ULL;

// 把目录中的普通文件按稳定顺序打包为单个 bundle。
Status PackSnapshotDirectory(
    const std::string& source_directory,
    const std::string& bundle_path);

// 把 bundle 解包到空目录；失败时清理目标目录。
Status UnpackSnapshotDirectory(
    const std::string& bundle_path,
    const std::string& destination_directory);

// 校验完整 bundle，并返回文件大小供分块发送。
Status ValidateSnapshotBundle(
    const std::string& bundle_path,
    uint64_t* serialized_size);

}  // namespace raftkv

#endif  // RAFTKV_RAFT_SNAPSHOT_BUNDLE_H_
