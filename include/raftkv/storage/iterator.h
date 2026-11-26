#ifndef RAFTKV_STORAGE_ITERATOR_H_
#define RAFTKV_STORAGE_ITERATOR_H_

#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"
#include "raftkv/storage/internal_key.h"

namespace raftkv {

// 存储层只向前读取的有序 InternalKey 迭代器。
class StorageIterator {
 public:
  virtual ~StorageIterator() = default;

  StorageIterator(const StorageIterator&) = delete;
  StorageIterator& operator=(const StorageIterator&) = delete;

  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void Seek(Slice user_key) = 0;
  virtual void Seek(const InternalKey& target) = 0;
  virtual void Next() = 0;

  virtual const InternalKey& key() const = 0;
  virtual Slice value() const = 0;
  virtual const Status& status() const = 0;

 protected:
  StorageIterator() = default;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_ITERATOR_H_
