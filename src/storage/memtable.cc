#include "raftkv/storage/memtable.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

namespace raftkv {

MemTable::MemTable()
    : table_(std::make_unique<SkipList>()) {}

MemTable::~MemTable() = default;

Status MemTable::Add(uint64_t seq, ValueType type, Slice key,
                     Slice value) {
  if (!IsValidValueType(type)) {
    return Status::InvalidArgument("invalid memtable value type");
  }
  if (seq == kMaxSequenceNumber) {
    return Status::InvalidArgument(
        "memtable sequence number is reserved");
  }
  if (has_writes_.load(std::memory_order_relaxed) &&
      seq <= last_sequence_.load(std::memory_order_relaxed)) {
    return Status::InvalidArgument(
        "memtable sequence must increase strictly");
  }

  InternalKey internal_key;
  internal_key.user_key = key.ToString();
  internal_key.sequence = seq;
  internal_key.type = type;

  std::string stored_value;
  if (type == ValueType::kValue) {
    stored_value = value.ToString();
  }

  const size_t added_bytes = table_->Add(
      std::move(internal_key), std::move(stored_value));
  approximate_memory_usage_.fetch_add(
      added_bytes, std::memory_order_relaxed);
  last_sequence_.store(seq, std::memory_order_release);
  has_writes_.store(true, std::memory_order_release);
  return Status::OK();
}

Status MemTable::Get(Slice key, std::string* value,
                     bool* is_deleted) const {
  if (value == nullptr || is_deleted == nullptr) {
    return Status::InvalidArgument(
        "memtable get output is null");
  }

  const std::string user_key = key.ToString();
  auto iterator = table_->NewIterator();
  iterator->Seek(key);
  if (!iterator->Valid() ||
      iterator->key().user_key != user_key) {
    return Status::NotFound("memtable key not found");
  }

  const bool deleted =
      iterator->key().type == ValueType::kDeletion;
  const std::string result =
      deleted ? std::string() : iterator->value().ToString();
  // 所有检查成功后一次性提交输出，避免失败路径留下部分结果。
  *value = result;
  *is_deleted = deleted;
  return Status::OK();
}

std::unique_ptr<SkipList::Iterator> MemTable::NewIterator() const {
  return table_->NewIterator();
}

uint64_t MemTable::LastSequence() const {
  return last_sequence_.load(std::memory_order_acquire);
}

size_t MemTable::ApproximateMemoryUsage() const {
  return approximate_memory_usage_.load(
      std::memory_order_acquire);
}

}  // namespace raftkv
