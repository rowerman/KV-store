#include "raftkv/storage/merging_iterator.h"

#include <algorithm>
#include <string>
#include <utility>

namespace raftkv {

MergingIterator::MergingIterator(
    std::vector<std::unique_ptr<StorageIterator>> children,
    DeletionOutputPolicy deletion_policy)
    : children_(std::move(children)),
      deletion_policy_(deletion_policy) {
  heap_.reserve(children_.size());
}

MergingIterator::~MergingIterator() = default;

bool MergingIterator::Valid() const {
  return has_current_;
}

const InternalKey& MergingIterator::key() const {
  return current_key_;
}

Slice MergingIterator::value() const {
  return Slice(current_value_);
}

const Status& MergingIterator::status() const {
  return status_;
}

bool MergingIterator::HeapGreater(
    const StorageIterator* lhs,
    const StorageIterator* rhs) {
  return CompareInternalKeys(lhs->key(), rhs->key()) > 0;
}

void MergingIterator::Reset() {
  heap_.clear();
  current_key_ = InternalKey{};
  current_value_.clear();
  status_ = Status::OK();
  has_current_ = false;
}

void MergingIterator::HeapPush(StorageIterator* child) {
  if (child == nullptr || !child->Valid()) {
    return;
  }
  heap_.push_back(child);
  std::push_heap(heap_.begin(), heap_.end(), HeapGreater);
}

StorageIterator* MergingIterator::HeapPop() {
  std::pop_heap(heap_.begin(), heap_.end(), HeapGreater);
  StorageIterator* child = heap_.back();
  heap_.pop_back();
  return child;
}

bool MergingIterator::CaptureStatus(
    const StorageIterator* child) {
  if (child->status().ok()) {
    return true;
  }

  status_ = child->status();
  heap_.clear();
  current_key_ = InternalKey{};
  current_value_.clear();
  has_current_ = false;
  return false;
}

void MergingIterator::SeekToFirst() {
  Reset();
  for (std::unique_ptr<StorageIterator>& child : children_) {
    child->SeekToFirst();
    if (!CaptureStatus(child.get())) {
      return;
    }
    HeapPush(child.get());
  }
  Advance();
}

void MergingIterator::Seek(Slice user_key) {
  Reset();
  for (std::unique_ptr<StorageIterator>& child : children_) {
    child->Seek(user_key);
    if (!CaptureStatus(child.get())) {
      return;
    }
    HeapPush(child.get());
  }
  Advance();
}

void MergingIterator::Seek(const InternalKey& target) {
  Reset();
  for (std::unique_ptr<StorageIterator>& child : children_) {
    child->Seek(target);
    if (!CaptureStatus(child.get())) {
      return;
    }
    HeapPush(child.get());
  }
  Advance();
}

void MergingIterator::Next() {
  if (!has_current_) {
    return;
  }
  Advance();
}

void MergingIterator::Advance() {
  current_key_ = InternalKey{};
  current_value_.clear();
  has_current_ = false;

  while (!heap_.empty()) {
    InternalKey candidate_key = heap_.front()->key();
    std::string candidate_value =
        heap_.front()->value().ToString();
    const std::string user_key = candidate_key.user_key;

    while (!heap_.empty() &&
           heap_.front()->key().user_key == user_key) {
      StorageIterator* child = HeapPop();
      child->Next();
      if (!CaptureStatus(child)) {
        return;
      }
      HeapPush(child);
    }

    if (candidate_key.type == ValueType::kDeletion &&
        deletion_policy_ == DeletionOutputPolicy::kHide) {
      continue;
    }

    current_key_ = std::move(candidate_key);
    current_value_ = std::move(candidate_value);
    has_current_ = true;
    return;
  }
}

}  // namespace raftkv
