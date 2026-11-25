#ifndef RAFTKV_STORAGE_LRU_CACHE_H_
#define RAFTKV_STORAGE_LRU_CACHE_H_

#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "raftkv/common/status.h"

namespace raftkv {

// 每个缓存项的固定近似开销。
inline constexpr size_t kLruEntryOverhead = 64;

// 按近似字节容量淘汰最久未访问项的线程安全缓存。
class LRUCache {
 public:
  using Key = std::string;
  using Value = std::shared_ptr<const std::string>;

  explicit LRUCache(size_t capacity_bytes);
  ~LRUCache();

  LRUCache(const LRUCache&) = delete;
  LRUCache& operator=(const LRUCache&) = delete;

  // 插入或更新；失败时保留原缓存状态。
  Status Insert(Key key, Value value);

  // 命中时刷新为最近使用；未命中返回空指针。
  Value Lookup(const Key& key);

  bool Erase(const Key& key);
  void Clear();

  size_t Capacity() const;
  size_t Usage() const;
  size_t Size() const;

 private:
  struct Entry {
    Key key;
    Value value;
    size_t charge = 0;
  };

  using EntryList = std::list<Entry>;
  using EntryMap = std::unordered_map<Key, EntryList::iterator>;

  static size_t EntryCharge(const Key& key, const Value& value);
  // 调用方必须持有 mutex_，且链表非空。
  void EvictLru();

  const size_t capacity_;
  mutable std::mutex mutex_;
  size_t usage_bytes_ = 0;
  EntryList entries_;
  EntryMap index_;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_LRU_CACHE_H_
