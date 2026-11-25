#ifndef RAFTKV_STORAGE_SKIPLIST_H_
#define RAFTKV_STORAGE_SKIPLIST_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "raftkv/common/slice.h"
#include "raftkv/storage/internal_key.h"

namespace raftkv {

// 跳表最大层数与提升概率 1/4 对应的分支因子。
inline constexpr int kSkipListMaxHeight = 12;
inline constexpr int kSkipListBranchingFactor = 4;
inline constexpr uint32_t kDefaultSkipListSeed = 0x9E3779B9U;

// 单写多读的内存有序索引。
class SkipList {
 public:
  class Iterator;

  explicit SkipList(
      uint32_t seed = kDefaultSkipListSeed);
  ~SkipList();

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // 只允许一个写线程调用；返回新节点近似占用字节数。
  size_t Add(InternalKey key, std::string value);

  // 创建从第 0 层顺序遍历的只读迭代器。
  std::unique_ptr<Iterator> NewIterator() const;

 private:
  struct Node {
    InternalKey key;
    std::string value;
    std::array<std::atomic<Node*>, kSkipListMaxHeight> next;

    Node(InternalKey node_key, std::string node_value)
        : key(std::move(node_key)),
          value(std::move(node_value)) {
      for (std::atomic<Node*>& link : next) {
        link.store(nullptr, std::memory_order_relaxed);
      }
    }
  };

  // 返回第一个大于等于 target 的节点。
  Node* FindGreaterOrEqual(const InternalKey& target) const;
  // 生成 1 到 kSkipListMaxHeight 的随机层高。
  int RandomHeight();
  // 生成下一次 xorshift32 随机值。
  uint32_t NextRandom();

  Node* head_;
  std::atomic<int> max_height_{1};
  uint32_t random_state_;
};

// 读取已发布节点；SkipList 必须比迭代器存活更久。
class SkipList::Iterator {
 public:
  explicit Iterator(const SkipList* list);

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  bool Valid() const { return node_ != nullptr; }
  void SeekToFirst();
  void Seek(Slice user_key);
  void Seek(const InternalKey& target);
  void Next();

  const InternalKey& key() const { return node_->key; }
  Slice value() const { return Slice(node_->value); }

 private:
  const SkipList* list_;
  const Node* node_ = nullptr;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_SKIPLIST_H_
