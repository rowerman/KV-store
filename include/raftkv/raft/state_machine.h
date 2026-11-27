#ifndef RAFTKV_RAFT_STATE_MACHINE_H_
#define RAFTKV_RAFT_STATE_MACHINE_H_

#include <cstdint>
#include <string>

#include "raftkv/common/status.h"

namespace raftkv {

// Raft 只负责按提交顺序调用；业务语义由实现方负责。
class StateMachine {
 public:
  virtual ~StateMachine() = default;

  // command 是 Normal 日志的 payload。
  virtual Status Apply(
      uint64_t log_index, const std::string& command) = 0;
};

}  // namespace raftkv

#endif  // RAFTKV_RAFT_STATE_MACHINE_H_
