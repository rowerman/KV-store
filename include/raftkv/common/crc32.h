#ifndef RAFTKV_COMMON_CRC32_H_
#define RAFTKV_COMMON_CRC32_H_

#include <cstddef>
#include <cstdint>

#include "raftkv/common/slice.h"

namespace raftkv {

// 计算标准 CRC-32/ISO-HDLC 校验值。
uint32_t Crc32(const char* data, size_t size);

// 计算 Slice 引用字节的标准 CRC-32/ISO-HDLC 校验值。
uint32_t Crc32(Slice data);

}  // namespace raftkv

#endif  // RAFTKV_COMMON_CRC32_H_
