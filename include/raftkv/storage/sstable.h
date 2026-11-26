#ifndef RAFTKV_STORAGE_SSTABLE_H_
#define RAFTKV_STORAGE_SSTABLE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"
#include "raftkv/storage/internal_key.h"
#include "raftkv/storage/iterator.h"

namespace raftkv {

class LRUCache;

inline constexpr size_t kSSTableBlockSize = 4U * 1024U;
inline constexpr size_t kSSTableBlockTrailerSize = 8;
inline constexpr size_t kSSTableFooterSize = 48;
inline constexpr size_t kSSTableMaxBlockSize =
    64U * 1024U * 1024U;
inline constexpr uint64_t kSSTableMagic = 0x3154535353564B52ULL;

// 单个 SSTable 内某个用户 Key 的最新内部版本。
struct SSTableLookupResult {
  InternalKey key;
  std::string value;
};

// 构建不可变 SSTable 文件，输入 InternalKey 必须严格递增。
class SSTableBuilder {
 public:
  explicit SSTableBuilder(
      std::string path,
      size_t block_size = kSSTableBlockSize);
  ~SSTableBuilder();

  SSTableBuilder(const SSTableBuilder&) = delete;
  SSTableBuilder& operator=(const SSTableBuilder&) = delete;

  Status Add(const InternalKey& key, Slice value);
  Status Finish();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// 只读 SSTable；打开后索引与 Bloom Filter 常驻内存。
class SSTableReader {
 public:
  class Iterator;

  static Status Open(
      std::string path,
      std::shared_ptr<LRUCache> block_cache,
      std::unique_ptr<SSTableReader>* reader);

  ~SSTableReader();

  SSTableReader(const SSTableReader&) = delete;
  SSTableReader& operator=(const SSTableReader&) = delete;

  Status Get(Slice user_key,
             SSTableLookupResult* result) const;
  std::unique_ptr<Iterator> NewIterator() const;

  uint64_t EntryCount() const;
  const std::string& path() const;

 private:
  struct Impl;
  explicit SSTableReader(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

class SSTableReader::Iterator : public StorageIterator {
 public:
  ~Iterator() override;

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  bool Valid() const override;
  void SeekToFirst() override;
  void Seek(Slice user_key) override;
  void Seek(const InternalKey& target) override;
  void Next() override;

  const InternalKey& key() const override;
  Slice value() const override;
  const Status& status() const override;

 private:
  explicit Iterator(const SSTableReader* reader);

  struct Impl;
  std::unique_ptr<Impl> impl_;

  friend class SSTableReader;
};


}  // namespace raftkv

#endif  // RAFTKV_STORAGE_SSTABLE_H_
