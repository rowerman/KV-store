#ifndef RAFTKV_SERVER_SESSION_TABLE_H_
#define RAFTKV_SERVER_SESSION_TABLE_H_

#include <cstdint>
#include <string>
#include <unordered_map>

#include "raftkv/common/status.h"

namespace raftkv {

// 记录每个客户端已成功执行的最大写序号。
class SessionTable final {
 public:
  // 首次请求或序号更大时需要执行；已执行及更小序号视为重复。
  bool ShouldApply(uint64_t client_id, uint64_t seq_num) const;

  // 仅在对应存储写入成功后调用。
  void RecordApplied(uint64_t client_id, uint64_t seq_num);

  // 原子写入完整快照文件。
  Status Save(const std::string& path) const;

  // 完整校验后替换当前状态；失败时保持原状态。
  Status Load(const std::string& path);

 private:
  std::unordered_map<uint64_t, uint64_t> last_seq_by_client_;
};

}  // namespace raftkv

#endif  // RAFTKV_SERVER_SESSION_TABLE_H_
