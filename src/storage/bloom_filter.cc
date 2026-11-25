#include "raftkv/storage/bloom_filter.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"

namespace raftkv {
namespace {

constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
constexpr uint64_t kFnvPrime = 1099511628211ULL;
constexpr uint64_t kSecondHashSeed = 0x9E3779B97F4A7C15ULL;

size_t BitmapSize(uint32_t bit_count) {
  return (static_cast<size_t>(bit_count) + 7U) / 8U;
}

void SetBit(std::string* bitmap, uint32_t bit) {
  const uint32_t byte_index = bit / 8U;
  const uint8_t mask = static_cast<uint8_t>(1U << (bit % 8U));
  (*bitmap)[byte_index] = static_cast<char>(
      static_cast<uint8_t>((*bitmap)[byte_index]) | mask);
}

bool GetBit(Slice bitmap, uint32_t bit) {
  const uint32_t byte_index = bit / 8U;
  const uint8_t mask = static_cast<uint8_t>(1U << (bit % 8U));
  return (static_cast<uint8_t>(bitmap.data()[byte_index]) & mask) != 0;
}

}  // namespace

uint64_t BloomFilter::Hash(Slice value, uint64_t seed) {
  uint64_t hash = kFnvOffsetBasis ^ seed;
  for (size_t i = 0; i < value.size(); ++i) {
    hash ^= static_cast<uint8_t>(value.data()[i]);
    hash *= kFnvPrime;
  }
  return hash;
}

Status BloomFilter::Build(const std::vector<Slice>& keys,
                          std::string* dst) {
  if (dst == nullptr) {
    return Status::InvalidArgument("bloom filter dst is null");
  }
  if (keys.size() >
      std::numeric_limits<uint32_t>::max() /
          kBloomFilterBitsPerKey) {
    return Status::InvalidArgument("too many bloom filter keys");
  }

  const uint32_t bit_count =
      static_cast<uint32_t>(keys.size()) *
      kBloomFilterBitsPerKey;
  const size_t bitmap_size = BitmapSize(bit_count);
  std::string built(kBloomFilterHeaderSize + bitmap_size, '\0');
  EncodeFixed32(built.data(), kBloomFilterMagic);
  EncodeFixed32(built.data() + 4, bit_count);

  if (bit_count != 0) {
    std::string bitmap(bitmap_size, '\0');
    for (const Slice& key : keys) {
      const uint64_t first = Hash(key, 0);
      const uint64_t second = Hash(key, kSecondHashSeed) | 1U;
      for (uint32_t i = 0;
           i < kBloomFilterHashFunctions; ++i) {
        const uint32_t bit = static_cast<uint32_t>(
            (first + static_cast<uint64_t>(i) * second) %
            bit_count);
        SetBit(&bitmap, bit);
      }
    }
    built.replace(kBloomFilterHeaderSize, bitmap_size, bitmap);
  }

  dst->swap(built);
  return Status::OK();
}

bool BloomFilter::KeyMayMatch(Slice key, Slice filter) {
  // 过滤器不可解析时必须返回 true，避免漏查真实存在的 Key。
  if (filter.size() < kBloomFilterHeaderSize) {
    return true;
  }
  if (DecodeFixed32(filter.data()) != kBloomFilterMagic) {
    return true;
  }

  const uint32_t bit_count =
      DecodeFixed32(filter.data() + 4);
  if (bit_count == 0) {
    return false;
  }
  const size_t expected_size =
      kBloomFilterHeaderSize + BitmapSize(bit_count);
  if (filter.size() != expected_size) {
    return true;
  }

  const Slice bitmap(
      filter.data() + kBloomFilterHeaderSize,
      filter.size() - kBloomFilterHeaderSize);
  const uint64_t first = Hash(key, 0);
  const uint64_t second = Hash(key, kSecondHashSeed) | 1U;
  for (uint32_t i = 0; i < kBloomFilterHashFunctions; ++i) {
    const uint32_t bit = static_cast<uint32_t>(
        (first + static_cast<uint64_t>(i) * second) % bit_count);
    if (!GetBit(bitmap, bit)) {
      return false;
    }
  }
  return true;
}

}  // namespace raftkv
