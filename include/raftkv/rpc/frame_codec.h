#ifndef RAFTKV_RPC_FRAME_CODEC_H_
#define RAFTKV_RPC_FRAME_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "raftkv/common/status.h"

namespace raftkv {

class Buffer;

// 帧魔数 "RK" 的大端表示。
inline constexpr uint16_t kRpcFrameMagic = 0x524B;
// 当前协议版本。
inline constexpr uint8_t kRpcFrameVersion = 0x01;
// 固定帧头长度。
inline constexpr size_t kRpcFrameHeaderSize = 20;
// 帧尾 CRC32 长度。
inline constexpr size_t kRpcFrameChecksumSize = 4;
// 单个载荷的最大长度。
inline constexpr uint32_t kRpcFrameMaxPayloadSize =
    16U * 1024U * 1024U;

// 帧承载的消息用途。
enum class RpcMessageType : uint8_t {
  kRequest = 1,
  kResponse = 2,
  kHeartbeat = 3,
  kError = 4,
};

// 已解码的一帧；payload 仍由业务层解释。
struct RpcFrame {
  // 固定魔数，用于快速识别非法连接。
  uint16_t magic = kRpcFrameMagic;
  // 协议版本。
  uint8_t version = kRpcFrameVersion;
  // Request、Response、Heartbeat 或 Error。
  uint8_t msg_type =
      static_cast<uint8_t>(RpcMessageType::kRequest);
  // 业务方法路由编号。
  uint32_t method_id = 0;
  // 用于匹配异步响应。
  uint64_t request_id = 0;
  // 未经解释的 Protobuf 序列化载荷。
  std::string payload;
};

// 流式解码结果。
enum class FrameDecodeResult {
  kOk,
  kIncomplete,
  kCorrupt,
};

// 将 RPC 帧编码到 TCP 字节流，或从字节流拆出完整帧。
class FrameCodec {
 public:
  // 追加一个完整帧；失败时不修改 out。
  static Status Encode(const RpcFrame& frame, Buffer* out);

  // 尝试解码一帧；只有 kOk 消费输入。
  static FrameDecodeResult TryDecode(Buffer* in, RpcFrame* out);
};

}  // namespace raftkv

#endif  // RAFTKV_RPC_FRAME_CODEC_H_
