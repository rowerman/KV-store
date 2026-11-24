#ifndef RAFTKV_COMMON_CODING_H_
#define RAFTKV_COMMON_CODING_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"

namespace raftkv {

// 向目标缓冲区写入 4 字节小端整数。
void EncodeFixed32(char* dst, uint32_t value);

// 向目标缓冲区写入 8 字节小端整数。
void EncodeFixed64(char* dst, uint64_t value);

// 从 4 字节小端缓冲区读取整数。
uint32_t DecodeFixed32(const char* src);

// 从 8 字节小端缓冲区读取整数。
uint64_t DecodeFixed64(const char* src);

// 向目标缓冲区写入 2 字节大端整数。
void EncodeBigEndian16(char* dst, uint16_t value);

// 向目标缓冲区写入 4 字节大端整数。
void EncodeBigEndian32(char* dst, uint32_t value);

// 向目标缓冲区写入 8 字节大端整数。
void EncodeBigEndian64(char* dst, uint64_t value);

// 从 2 字节大端缓冲区读取整数。
uint16_t DecodeBigEndian16(const char* src);

// 从 4 字节大端缓冲区读取整数。
uint32_t DecodeBigEndian32(const char* src);

// 从 8 字节大端缓冲区读取整数。
uint64_t DecodeBigEndian64(const char* src);

// 追加 4 字节小端长度和原始字节。
Status PutLengthPrefixedSlice(std::string* dst, Slice value);

// 读取长度前缀内容并推进 input；失败时不修改参数。
Status GetLengthPrefixedSlice(Slice* input, Slice* result);

}  // namespace raftkv

#endif  // RAFTKV_COMMON_CODING_H_
