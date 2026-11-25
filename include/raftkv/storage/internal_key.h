#ifndef RAFTKV_STORAGE_INTERNAL_KEY_H_
#define RAFTKV_STORAGE_INTERNAL_KEY_H_

#include <cstdint>
#include <limits>
#include <string>

namespace raftkv {

// 点查使用的最大序列号，正常写入不能占用该值。
inline constexpr uint64_t kMaxSequenceNumber =
    std::numeric_limits<uint64_t>::max();

// LSM 内部记录类型；数值 0 保留给删除墓碑。
enum class ValueType : uint8_t {
  kDeletion = 0,
  kValue = 1,
};

// 只接受当前定义的两个取值。
inline bool IsValidValueType(ValueType type) {
  switch (type) {
    case ValueType::kDeletion:
    case ValueType::kValue:
      return true;
  }
  return false;
}

// MemTable 和后续 SSTable 共用的内部键。
struct InternalKey {
  std::string user_key;
  uint64_t sequence = 0;
  ValueType type = ValueType::kValue;
};

// 用户键升序；同键按序列号和类型降序，使新版本排在前面。
inline int CompareInternalKeys(const InternalKey& lhs,
                               const InternalKey& rhs) {
  const int user_key_order =
      lhs.user_key.compare(rhs.user_key);
  if (user_key_order != 0) {
    return user_key_order;
  }
  if (lhs.sequence != rhs.sequence) {
    return lhs.sequence > rhs.sequence ? -1 : 1;
  }
  const uint8_t lhs_type = static_cast<uint8_t>(lhs.type);
  const uint8_t rhs_type = static_cast<uint8_t>(rhs.type);
  if (lhs_type == rhs_type) {
    return 0;
  }
  return lhs_type > rhs_type ? -1 : 1;
}

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_INTERNAL_KEY_H_
