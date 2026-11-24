#ifndef RAFTKV_COMMON_STATUS_H_
#define RAFTKV_COMMON_STATUS_H_

#include <string>
#include <utility>

namespace raftkv {

// 各模块共用的错误分类。
enum class Code {
  kOk,
  kNotFound,
  kCorruption,
  kIOError,
  kNotLeader,
  kTimeout,
  kInvalidArgument,
};

// 返回稳定的错误分类名称。
inline const char* CodeName(Code code) {
  switch (code) {
    case Code::kOk:
      return "OK";
    case Code::kNotFound:
      return "NotFound";
    case Code::kCorruption:
      return "Corruption";
    case Code::kIOError:
      return "IOError";
    case Code::kNotLeader:
      return "NotLeader";
    case Code::kTimeout:
      return "Timeout";
    case Code::kInvalidArgument:
      return "InvalidArgument";
  }
  return "Unknown";
}

// 不可变的操作结果，包含错误码和可选详情。
class Status {
 public:
  // 创建成功状态。
  Status() = default;

  // 创建不带详情的成功状态。
  static Status OK() { return Status(); }

  // 创建未找到错误。
  static Status NotFound(std::string msg) {
    return Status(Code::kNotFound, std::move(msg));
  }

  // 创建数据损坏错误。
  static Status Corruption(std::string msg) {
    return Status(Code::kCorruption, std::move(msg));
  }

  // 创建 I/O 错误。
  static Status IOError(std::string msg) {
    return Status(Code::kIOError, std::move(msg));
  }

  // 创建非 Leader 错误。
  static Status NotLeader(std::string msg) {
    return Status(Code::kNotLeader, std::move(msg));
  }

  // 创建超时错误。
  static Status Timeout(std::string msg) {
    return Status(Code::kTimeout, std::move(msg));
  }

  // 创建参数错误。
  static Status InvalidArgument(std::string msg) {
    return Status(Code::kInvalidArgument, std::move(msg));
  }

  // 操作成功时返回 true。
  bool ok() const { return code_ == Code::kOk; }

  // 返回错误分类。
  Code code() const { return code_; }

  // 返回错误详情；成功状态的消息始终为空。
  const std::string& message() const { return msg_; }

  // 格式化为 "OK" 或 "CodeName: message"。
  std::string ToString() const {
    if (ok()) {
      return CodeName(code_);
    }
    return std::string(CodeName(code_)) + ": " + msg_;
  }

  // 同时比较错误分类和详情。
  friend bool operator==(const Status& lhs, const Status& rhs) {
    return lhs.code_ == rhs.code_ && lhs.msg_ == rhs.msg_;
  }

  // 错误分类或详情不同时返回 true。
  friend bool operator!=(const Status& lhs, const Status& rhs) {
    return !(lhs == rhs);
  }

 private:
  // 仅通过工厂创建状态，避免成功状态携带错误消息。
  Status(Code code, std::string msg) : code_(code), msg_(std::move(msg)) {}

  Code code_ = Code::kOk;
  std::string msg_;
};

}  // namespace raftkv

#endif  // RAFTKV_COMMON_STATUS_H_
