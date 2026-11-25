#ifndef RAFTKV_STORAGE_WAL_H_
#define RAFTKV_STORAGE_WAL_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"
#include "raftkv/storage/internal_key.h"

namespace raftkv {

// WAL 每条记录固定头和长度字段大小。
inline constexpr size_t kWalChecksumSize = 4;
inline constexpr size_t kWalHeaderSize = 17;
inline constexpr size_t kWalValueLengthSize = 4;
// 限制单条记录，防止损坏长度触发超大内存分配。
inline constexpr size_t kWalMaxRecordSize = 64U * 1024U * 1024U;

// 回调中的 Slice 只在当前回调期间有效。
using WalReplayCallback = std::function<void(
    uint64_t sequence, ValueType type, Slice key, Slice value)>;

// 单线程顺序追加 WAL；Sync 成功后才满足崩溃持久性。
class WalWriter {
 public:
  explicit WalWriter(std::string path);
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  // append=false 截断重建；append=true 保留已有记录。
  Status Open(bool append = false);
  Status Append(uint64_t sequence, ValueType type,
                Slice key, Slice value);
  Status Sync();
  Status Close();

  bool IsOpen() const { return fd_ >= 0; }

 private:
  Status WriteAll(const char* data, size_t size);

  std::string path_;
  int fd_ = -1;
};

// 只读 WAL 回放器；每次 Replay 使用独立文件描述符。
class WalReader {
 public:
  explicit WalReader(std::string path);

  WalReader(const WalReader&) = delete;
  WalReader& operator=(const WalReader&) = delete;

  // valid_bytes 返回 CRC 完整的有效前缀长度。
  Status Replay(const WalReplayCallback& callback,
                uint64_t* valid_bytes = nullptr) const;

 private:
  std::string path_;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_WAL_H_
