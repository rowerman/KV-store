#include "raftkv/common/logger.h"

#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>
#include <utility>

namespace raftkv {
namespace {

const char* BaseName(const char* path) {
  if (path == nullptr) {
    return "<unknown>";
  }
  const char* slash = std::strrchr(path, '/');
  return slash == nullptr ? path : slash + 1;
}

void AppendTimestamp(std::ostringstream* line) {
  const auto now = std::chrono::system_clock::now();
  const auto milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          now.time_since_epoch()) %
      1000;
  const std::time_t seconds = std::chrono::system_clock::to_time_t(now);
  std::tm local_time{};
  localtime_r(&seconds, &local_time);

  *line << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S") << '.'
        << std::setfill('0') << std::setw(3) << milliseconds.count();
}

}  // namespace

const char* LogLevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
  }
  return "UNKNOWN";
}

Logger& Logger::Instance() {
  static Logger logger;
  return logger;
}

void Logger::SetMinLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(mutex_);
  min_level_ = level;
}

void Logger::SetNodeId(std::string node_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  node_id_ = std::move(node_id);
}

void Logger::SetOutput(std::ostream* output) {
  std::lock_guard<std::mutex> lock(mutex_);
  output_ = output == nullptr ? &std::cerr : output;
}

void Logger::Log(LogLevel level, const char* file, int line,
                 std::string_view message) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (level < min_level_) {
    return;
  }

  std::ostream* output = output_ == nullptr ? &std::cerr : output_;
  std::ostringstream formatted;
  formatted << '[';
  AppendTimestamp(&formatted);
  formatted << "][" << LogLevelName(level) << "][node=" << node_id_
            << "][tid=" << std::this_thread::get_id() << "]["
            << BaseName(file) << ':' << line << "] " << message << '\n';
  *output << formatted.str();
}

void Logger::Flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ostream* output = output_ == nullptr ? &std::cerr : output_;
  output->flush();
}

}  // namespace raftkv
