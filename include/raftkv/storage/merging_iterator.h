#ifndef RAFTKV_STORAGE_MERGING_ITERATOR_H_
#define RAFTKV_STORAGE_MERGING_ITERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "raftkv/storage/iterator.h"

namespace raftkv {

enum class DeletionOutputPolicy {
  kHide,
  kKeep,
};

// 多路归并有序子迭代器，只输出每个用户键的最新有效版本。
class MergingIterator : public StorageIterator {
 public:
  explicit MergingIterator(
      std::vector<std::unique_ptr<StorageIterator>> children,
      DeletionOutputPolicy deletion_policy =
          DeletionOutputPolicy::kHide);
  ~MergingIterator() override;

  MergingIterator(const MergingIterator&) = delete;
  MergingIterator& operator=(const MergingIterator&) = delete;

  bool Valid() const override;
  void SeekToFirst() override;
  void Seek(Slice user_key) override;
  void Seek(const InternalKey& target) override;
  void Next() override;

  const InternalKey& key() const override;
  Slice value() const override;
  const Status& status() const override;

 private:
  // std::push_heap 使用反向比较，使堆顶为 InternalKey 最小项。
  static bool HeapGreater(const StorageIterator* lhs,
                          const StorageIterator* rhs);

  void Reset();
  void HeapPush(StorageIterator* child);
  StorageIterator* HeapPop();
  bool CaptureStatus(const StorageIterator* child);
  void Advance();

  std::vector<std::unique_ptr<StorageIterator>> children_;
  std::vector<StorageIterator*> heap_;
  InternalKey current_key_;
  std::string current_value_;
  Status status_;
  bool has_current_ = false;
  DeletionOutputPolicy deletion_policy_;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_MERGING_ITERATOR_H_
