#include "raftkv/storage/skiplist.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace raftkv {

SkipList::SkipList(uint32_t seed)
    : head_(new Node(InternalKey{}, std::string())),
      random_state_(seed == 0 ? kDefaultSkipListSeed : seed) {}

SkipList::~SkipList() {
  const Node* node =
      head_->next[0].load(std::memory_order_relaxed);
  while (node != nullptr) {
    const Node* next =
        node->next[0].load(std::memory_order_relaxed);
    delete node;
    node = next;
  }
  delete head_;
}

size_t SkipList::Add(InternalKey key, std::string value) {
  const int node_height = RandomHeight();
  std::array<Node*, kSkipListMaxHeight> previous {};
  previous.fill(head_);

  Node* current = head_;
  const int current_max_height =
      max_height_.load(std::memory_order_acquire);
  for (int level = current_max_height - 1; level >= 0; --level) {
    Node* next =
        current->next[level].load(std::memory_order_acquire);
    while (next != nullptr &&
           CompareInternalKeys(next->key, key) < 0) {
      current = next;
      next = current->next[level].load(
          std::memory_order_acquire);
    }
    previous[level] = current;
  }

  Node* node =
      new Node(std::move(key), std::move(value));
  for (int level = 0; level < node_height; ++level) {
    Node* successor = previous[level]->next[level].load(
        std::memory_order_relaxed);
    node->next[level].store(
        successor, std::memory_order_relaxed);
    previous[level]->next[level].store(
        node, std::memory_order_release);
  }

  // 高层链表全部接好后，读者才能看到新的最高层。
  if (node_height > current_max_height) {
    max_height_.store(node_height, std::memory_order_release);
  }
  return sizeof(Node) + node->key.user_key.capacity() +
         node->value.capacity();
}

std::unique_ptr<SkipList::Iterator> SkipList::NewIterator() const {
  return std::make_unique<Iterator>(this);
}

SkipList::Node* SkipList::FindGreaterOrEqual(
    const InternalKey& target) const {
  const Node* current = head_;
  for (int level =
           max_height_.load(std::memory_order_acquire) - 1;
       level >= 0; --level) {
    const Node* next =
        current->next[level].load(std::memory_order_acquire);
    while (next != nullptr &&
           CompareInternalKeys(next->key, target) < 0) {
      current = next;
      next = current->next[level].load(
          std::memory_order_acquire);
    }
    if (level == 0) {
      return const_cast<Node*>(next);
    }
  }
  return nullptr;
}

int SkipList::RandomHeight() {
  int height = 1;
  while (height < kSkipListMaxHeight &&
         (NextRandom() % kSkipListBranchingFactor) == 0) {
    ++height;
  }
  return height;
}

uint32_t SkipList::NextRandom() {
  uint32_t value = random_state_;
  value ^= value << 13;
  value ^= value >> 17;
  value ^= value << 5;
  random_state_ = value;
  return value;
}

SkipList::Iterator::Iterator(const SkipList* list)
    : list_(list) {}

void SkipList::Iterator::SeekToFirst() {
  node_ = list_->head_->next[0].load(
      std::memory_order_acquire);
}

void SkipList::Iterator::Seek(Slice user_key) {
  InternalKey target;
  target.user_key = user_key.ToString();
  target.sequence = kMaxSequenceNumber;
  target.type = ValueType::kValue;
  node_ = list_->FindGreaterOrEqual(target);
}

void SkipList::Iterator::Seek(const InternalKey& target) {
  node_ = list_->FindGreaterOrEqual(target);
}

void SkipList::Iterator::Next() {
  if (node_ == nullptr) {
    return;
  }
  node_ = node_->next[0].load(std::memory_order_acquire);
}

}  // namespace raftkv
