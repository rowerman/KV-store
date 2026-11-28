#ifndef RAFTKV_RPC_KV_METHODS_H_
#define RAFTKV_RPC_KV_METHODS_H_

#include <cstdint>

namespace raftkv {

// 客户端 KV RPC 的固定方法 ID。
inline constexpr uint32_t kKvPutRpcMethod = 201;
inline constexpr uint32_t kKvDeleteRpcMethod = 202;
inline constexpr uint32_t kKvGetRpcMethod = 203;
inline constexpr uint32_t kKvScanRpcMethod = 204;

}  // namespace raftkv

#endif  // RAFTKV_RPC_KV_METHODS_H_
