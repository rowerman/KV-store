#ifndef RAFTKV_RAFT_RAFT_LOG_H_
#define RAFTKV_RAFT_RAFT_LOG_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "raftkv/common/status.h"
#include "rpc_raft.pb.h"

namespace raftkv {

// 分段日志参数；segment_size 是滚动阈值。
struct RaftLogOptions {
  uint64_t segment_size = 64ULL * 1024ULL * 1024ULL;
  uint64_t max_entry_payload_size = 64ULL * 1024ULL * 1024ULL;
};

// current_term 和本轮任期内的投票对象。
struct RaftHardState {
  uint64_t current_term = 0;
  std::optional<uint32_t> voted_for;
};

// 已被快照覆盖的最后一条日志位置。
struct RaftSnapshotMarker {
  uint64_t last_included_index = 0;
  uint64_t last_included_term = 0;
};

// 单线程使用的 Raft 日志；不创建内部线程。
class RaftLog {
 public:
  // 目录为空时初始化，已有文件时恢复。
  static Status Open(
      std::string directory,
      std::unique_ptr<RaftLog>* log);
  static Status Open(
      std::string directory,
      RaftLogOptions options,
      std::unique_ptr<RaftLog>* log);

  ~RaftLog();

  RaftLog(const RaftLog&) = delete;
  RaftLog& operator=(const RaftLog&) = delete;

  // 连续追加 Index 紧随现有日志的条目，返回前完成 fsync。
  Status Append(std::vector<proto::LogEntry> entries);

  // 按 Index 读取完整日志条目。
  Status EntryAt(uint64_t index, proto::LogEntry* entry) const;

  // 读取指定 Index 的任期；快照边界也可查询。
  Status TermAt(uint64_t index, uint64_t* term) const;

  uint64_t FirstIndex() const;
  uint64_t LastIndex() const;

  // 读取最后一条日志的任期；空日志返回快照任期。
  Status LastTerm(uint64_t* term) const;

  // 删除从 from_index 开始的后缀；成功后才更新内存索引。
  Status TruncateSuffix(uint64_t from_index);

  // 把 up_to_index 及其之前日志标记为快照覆盖范围。
  Status CompactPrefix(uint64_t up_to_index, uint64_t last_term);

  // 原子保存任期和投票对象。
  Status SaveHardState(uint64_t current_term,
                       std::optional<uint32_t> voted_for);

  const RaftHardState& hard_state() const;
  const RaftSnapshotMarker& snapshot_marker() const;
  const std::string& directory() const;

 private:
  struct Impl;
  explicit RaftLog(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_RAFT_RAFT_LOG_H_
