#ifndef RAFTKV_STORAGE_BLOOM_FILTER_H_
#define RAFTKV_STORAGE_BLOOM_FILTER_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"

namespace raftkv {

// 每个 Key 分配 10 bit，使用 7 个位置。
inline constexpr uint32_t kBloomFilterBitsPerKey = 10;
inline constexpr uint32_t kBloomFilterHashFunctions = 7;
inline constexpr size_t kBloomFilterHeaderSize = 8;
inline constexpr uint32_t kBloomFilterMagic = 0x42464C54U;

// 构建和查询 SSTable 使用的 Bloom Filter。
class BloomFilter {
 public:
  // 构建连续过滤器；失败时不修改 dst。
  static Status Build(const std::vector<Slice>& keys,
                      std::string* dst);

  // 返回 false 时 Key 一定不存在；返回 true 时可能不存在。
  static bool KeyMayMatch(Slice key, Slice filter);

 private:
  // 使用稳定 FNV-1a 哈希，避免依赖标准库实现。
  static uint64_t Hash(Slice value, uint64_t seed);
};

}  // namespace raftkv

#endif  // RAFTKV_STORAGE_BLOOM_FILTER_H_
