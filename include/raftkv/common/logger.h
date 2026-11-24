#ifndef RAFTKV_COMMON_LOGGER_H_
#define RAFTKV_COMMON_LOGGER_H_

#include <mutex>
#include <ostream>
#include <string>
#include <string_view>

namespace raftkv {

// 日志级别，数值越大越严重。
enum class LogLevel {
  kDebug,
  kInfo,
  kWarn,
  kError,
};

// 返回稳定的日志级别名称。
const char* LogLevelName(LogLevel level);

// 线程安全的同步日志器。
class Logger {
 public:
  // 返回进程级日志器。
  static Logger& Instance();

  // 设置最低输出级别。
  void SetMinLevel(LogLevel level);

  // 设置日志中的节点标识。
  void SetNodeId(std::string node_id);

  // 设置输出流；nullptr 恢复为标准错误流。
  void SetOutput(std::ostream* output);

  // 写入一条完整日志。
  void Log(LogLevel level, const char* file, int line,
           std::string_view message);

  // 刷新当前输出流。
  void Flush();

 private:
  Logger() = default;

  std::mutex mutex_;
  LogLevel min_level_ = LogLevel::kInfo;
  std::string node_id_ = "-";
  std::ostream* output_ = nullptr;
};

}  // namespace raftkv

#define RAFTKV_LOG_DEBUG(message)                                      \
  ::raftkv::Logger::Instance().Log(::raftkv::LogLevel::kDebug, __FILE__, \
                                   __LINE__, (message))

#define RAFTKV_LOG_INFO(message)                                      \
  ::raftkv::Logger::Instance().Log(::raftkv::LogLevel::kInfo, __FILE__, \
                                   __LINE__, (message))

#define RAFTKV_LOG_WARN(message)                                      \
  ::raftkv::Logger::Instance().Log(::raftkv::LogLevel::kWarn, __FILE__, \
                                   __LINE__, (message))

#define RAFTKV_LOG_ERROR(message)                                      \
  ::raftkv::Logger::Instance().Log(::raftkv::LogLevel::kError, __FILE__, \
                                   __LINE__, (message))

#endif  // RAFTKV_COMMON_LOGGER_H_
