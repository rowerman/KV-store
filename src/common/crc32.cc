#include "raftkv/common/crc32.h"

#include <array>

namespace raftkv {
namespace {

constexpr uint32_t kCrc32Polynomial = 0xEDB88320U;

std::array<uint32_t, 256> MakeCrc32Table() {
  std::array<uint32_t, 256> table{};
  for (size_t value = 0; value < table.size(); ++value) {
    uint32_t remainder = static_cast<uint32_t>(value);
    for (int bit = 0; bit < 8; ++bit) {
      if ((remainder & 1U) != 0) {
        remainder = (remainder >> 1U) ^ kCrc32Polynomial;
      } else {
        remainder >>= 1U;
      }
    }
    table[value] = remainder;
  }
  return table;
}

const std::array<uint32_t, 256>& GetCrc32Table() {
  static const std::array<uint32_t, 256> table = MakeCrc32Table();
  return table;
}

}  // namespace

uint32_t Crc32(const char* data, size_t size) {
  uint32_t crc = 0xFFFFFFFFU;
  const auto& table = GetCrc32Table();
  for (size_t i = 0; i < size; ++i) {
    const auto byte = static_cast<unsigned char>(data[i]);
    crc = table[(crc ^ byte) & 0xFFU] ^ (crc >> 8U);
  }
  return crc ^ 0xFFFFFFFFU;
}

uint32_t Crc32(Slice data) {
  return Crc32(data.data(), data.size());
}

}  // namespace raftkv
