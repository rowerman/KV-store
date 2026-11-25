#include "raftkv/storage/lru_cache.h"

#include <cstddef>
#include <limits>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

namespace raftkv {

LRUCache::LRUCache(size_t capacity_bytes)
    : capacity_(capacity_bytes) {}

LRUCache::~LRUCache() = default;

size_t LRUCache::EntryCharge(const Key& key,
                             const Value& value) {
  const size_t max_size = std::numeric_limits<size_t>::max();
  if (key.size() > max_size - kLruEntryOverhead) {
    return max_size;
  }
  const size_t fixed = kLruEntryOverhead + key.size();
  if (value->size() > max_size - fixed) {
    return max_size;
  }
  return fixed + value->size();
}

Status LRUCache::Insert(Key key, Value value) {
  if (value == nullptr) {
    return Status::InvalidArgument("lru value is null");
  }
  const size_t charge = EntryCharge(key, value);
  if (charge > capacity_) {
    return Status::InvalidArgument("lru entry exceeds capacity");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = index_.find(key);
  if (found != index_.end()) {
    Entry& entry = *found->second;
    usage_bytes_ -= entry.charge;

    // 更新可能增大占用；只驱逐目标项之外的 LRU 节点。
    while (usage_bytes_ > capacity_ - charge &&
           !entries_.empty() &&
           std::prev(entries_.end()) != found->second) {
      EvictLru();
    }

    entry.value = std::move(value);
    entry.charge = charge;
    usage_bytes_ += charge;
    entries_.splice(entries_.begin(), entries_,
                    found->second);
    return Status::OK();
  }

  while (usage_bytes_ > capacity_ - charge) {
    EvictLru();
  }

  entries_.emplace_front(
      Entry{std::move(key), std::move(value), charge});
  try {
    index_.emplace(entries_.front().key, entries_.begin());
  } catch (...) {
    entries_.pop_front();
    throw;
  }
  usage_bytes_ += charge;
  return Status::OK();
}

LRUCache::Value LRUCache::Lookup(const Key& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = index_.find(key);
  if (found == index_.end()) {
    return nullptr;
  }

  entries_.splice(entries_.begin(), entries_, found->second);
  return found->second->value;
}

bool LRUCache::Erase(const Key& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = index_.find(key);
  if (found == index_.end()) {
    return false;
  }

  usage_bytes_ -= found->second->charge;
  entries_.erase(found->second);
  index_.erase(found);
  return true;
}

void LRUCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  usage_bytes_ = 0;
  entries_.clear();
  index_.clear();
}

size_t LRUCache::Capacity() const {
  return capacity_;
}

size_t LRUCache::Usage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return usage_bytes_;
}

size_t LRUCache::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

void LRUCache::EvictLru() {
  auto victim = std::prev(entries_.end());
  usage_bytes_ -= victim->charge;
  index_.erase(victim->key);
  entries_.pop_back();
}

}  // namespace raftkv
