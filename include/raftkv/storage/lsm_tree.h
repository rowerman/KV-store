#ifndef RAFTKV_STORAGE_LSM_TREE_H_
#define RAFTKV_STORAGE_LSM_TREE_H_

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "raftkv/common/status.h"

namespace raftkv {

// Raft 状态机使用的最小存储接口。
class StorageEngine {
 public:
  virtual ~StorageEngine() = default;

  virtual Status Put(const std::string& key,
                     const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;
  virtual Status Get(const std::string& key,
                     std::string* value) = 0;
  virtual Status Scan(
      const std::string& start_key,
      const std::string& end_key,
      size_t limit,
      std::vector<std::pair<std::string, std::string>>* out) = 0;
  virtual Status CreateCheckpoint(
      const std::string& checkpoint_dir) = 0;
  virtual Status RestoreFromCheckpoint(
      const std::string& checkpoint_dir) = 0;
};

struct LSMTOptions {
  size_t memtable_size_limit = 64U * 1024U;
  size_t block_cache_capacity = 8U * 1024U * 1024U;
  size_t max_immutable_memtables = 2;
};

class LSMTree final : public StorageEngine {
 public:
  static Status Open(
      std::string directory,
      std::unique_ptr<LSMTree>* tree);
  static Status Open(
      std::string directory,
      LSMTOptions options,
      std::unique_ptr<LSMTree>* tree);

  ~LSMTree() override;

  LSMTree(const LSMTree&) = delete;
  LSMTree& operator=(const LSMTree&) = delete;

  Status Put(const std::string& key,
             const std::string& value) override;
  Status Delete(const std::string& key) override;
  Status Get(const std::string& key,
             std::string* value) override;
  Status Scan(
      const std::string& start_key,
      const std::string& end_key,
      size_t limit,
      std::vector<std::pair<std::string, std::string>>* out)
      override;
  Status CreateCheckpoint(
      const std::string& checkpoint_dir) override;
  Status RestoreFromCheckpoint(
      const std::string& checkpoint_dir) override;

  const std::string& directory() const;

 private:
  struct Impl;
  explicit LSMTree(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_LSM_TREE_H_
