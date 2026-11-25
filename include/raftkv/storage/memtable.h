#ifndef RAFTKV_STORAGE_MEMTABLE_H_
#define RAFTKV_STORAGE_MEMTABLE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"
#include "raftkv/storage/internal_key.h"
#include "raftkv/storage/skiplist.h"

namespace raftkv {

// LSM-Tree 的可写内存表；单写多读，保留同一用户键的全部版本。
class MemTable {
 public:
  MemTable();
  ~MemTable();

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // 序列号必须严格递增；墓碑的 value 不写入内存。
  Status Add(uint64_t seq, ValueType type, Slice key,
             Slice value);

  // 返回指定用户键的最新版本；墓碑通过 is_deleted 标记。
  Status Get(Slice key, std::string* value,
             bool* is_deleted) const;

  // 返回按 InternalKey 全序排列的全部版本。
  std::unique_ptr<SkipList::Iterator> NewIterator() const;

  uint64_t LastSequence() const;
  size_t ApproximateMemoryUsage() const;

 private:
  std::unique_ptr<SkipList> table_;
  std::atomic<uint64_t> last_sequence_{0};
  std::atomic<bool> has_writes_{false};
  std::atomic<size_t> approximate_memory_usage_{0};
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_MEMTABLE_H_
