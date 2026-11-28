#ifndef RAFTKV_SERVER_KV_STATE_MACHINE_H_
#define RAFTKV_SERVER_KV_STATE_MACHINE_H_

#include <cstdint>
#include <string>

#include "raftkv/raft/state_machine.h"
#include "raftkv/server/session_table.h"
#include "raftkv/storage/lsm_tree.h"

namespace raftkv {

// 将已提交的 KV 命令幂等应用到存储引擎。
class KVStateMachine final : public StateMachine {
 public:
  // storage 必须非空，且生命周期长于本对象。
  explicit KVStateMachine(StorageEngine* storage);

  Status Apply(
      uint64_t log_index, const std::string& command) override;

  Status TakeSnapshot(
      uint64_t last_index, const std::string& directory) override;

  Status LoadSnapshot(const std::string& directory) override;

 private:
  StorageEngine* storage_;
  SessionTable session_table_;
};

}  // namespace raftkv

#endif  // RAFTKV_SERVER_KV_STATE_MACHINE_H_
