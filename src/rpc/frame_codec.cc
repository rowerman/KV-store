#include "raftkv/rpc/frame_codec.h"

#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/net/buffer.h"

namespace raftkv {
namespace {

// 只接受当前协议定义的消息类型。
bool IsValidMessageType(uint8_t msg_type) {
  switch (static_cast<RpcMessageType>(msg_type)) {
    case RpcMessageType::kRequest:
    case RpcMessageType::kResponse:
    case RpcMessageType::kHeartbeat:
    case RpcMessageType::kError:
      return true;
  }
  return false;
}

}  // namespace

Status FrameCodec::Encode(const RpcFrame& frame, Buffer* out) {
  // 所有校验通过后才修改目标 Buffer。
  if (out == nullptr) {
    return Status::InvalidArgument("frame output is null");
  }
  if (frame.magic != kRpcFrameMagic) {
    return Status::InvalidArgument("invalid frame magic");
  }
  if (frame.version != kRpcFrameVersion) {
    return Status::InvalidArgument("unsupported frame version");
  }
  if (!IsValidMessageType(frame.msg_type)) {
    return Status::InvalidArgument("invalid frame message type");
  }
  if (frame.payload.size() > kRpcFrameMaxPayloadSize) {
    return Status::InvalidArgument("frame payload is too large");
  }

  const size_t payload_size = frame.payload.size();
  const size_t checksum_offset = kRpcFrameHeaderSize + payload_size;
  // Header 与 Payload 连续存放，便于一次性计算 CRC。
  std::string wire(checksum_offset + kRpcFrameChecksumSize, '\0');

  EncodeBigEndian16(wire.data(), frame.magic);
  wire[2] = static_cast<char>(frame.version);
  wire[3] = static_cast<char>(frame.msg_type);
  EncodeBigEndian32(wire.data() + 4, frame.method_id);
  EncodeBigEndian64(wire.data() + 8, frame.request_id);
  EncodeBigEndian32(
      wire.data() + 16, static_cast<uint32_t>(payload_size));
  if (payload_size != 0) {
    std::memcpy(wire.data() + kRpcFrameHeaderSize,
                frame.payload.data(), payload_size);
  }

  const uint32_t checksum =
      Crc32(wire.data(), checksum_offset);
  // CRC 字段自身不参与校验。
  EncodeBigEndian32(wire.data() + checksum_offset, checksum);
  out->Append(wire.data(), wire.size());
  return Status::OK();
}

FrameDecodeResult FrameCodec::TryDecode(Buffer* in, RpcFrame* out) {
  if (in == nullptr || out == nullptr) {
    throw std::invalid_argument("frame codec input or output is null");
  }
  if (in->ReadableBytes() < kRpcFrameHeaderSize) {
    // 头部尚不完整，保留全部输入。
    return FrameDecodeResult::kIncomplete;
  }

  const char* data = in->Peek();
  if (DecodeBigEndian16(data) != kRpcFrameMagic) {
    return FrameDecodeResult::kCorrupt;
  }

  const uint8_t version = static_cast<uint8_t>(data[2]);
  if (version != kRpcFrameVersion) {
    return FrameDecodeResult::kCorrupt;
  }

  const uint8_t msg_type = static_cast<uint8_t>(data[3]);
  if (!IsValidMessageType(msg_type)) {
    return FrameDecodeResult::kCorrupt;
  }

  const uint32_t payload_size = DecodeBigEndian32(data + 16);
  if (payload_size > kRpcFrameMaxPayloadSize) {
    // 非法长度无需等待剩余载荷，直接判定连接损坏。
    return FrameDecodeResult::kCorrupt;
  }

  const size_t frame_size =
      kRpcFrameHeaderSize + static_cast<size_t>(payload_size) +
      kRpcFrameChecksumSize;
  if (in->ReadableBytes() < frame_size) {
    // 头部合法但帧体尚未到齐。
    return FrameDecodeResult::kIncomplete;
  }

  const size_t checksum_offset = frame_size - kRpcFrameChecksumSize;
  const uint32_t expected_checksum =
      DecodeBigEndian32(data + checksum_offset);
  const uint32_t actual_checksum =
      Crc32(data, checksum_offset);
  if (actual_checksum != expected_checksum) {
    // CRC 错误不消费输入，调用方应关闭连接。
    return FrameDecodeResult::kCorrupt;
  }

  // 完整校验成功后再构造输出并消费当前帧。
  RpcFrame frame;
  frame.magic = DecodeBigEndian16(data);
  frame.version = version;
  frame.msg_type = msg_type;
  frame.method_id = DecodeBigEndian32(data + 4);
  frame.request_id = DecodeBigEndian64(data + 8);
  if (payload_size != 0) {
    frame.payload.assign(data + kRpcFrameHeaderSize, payload_size);
  }

  *out = std::move(frame);
  in->Retrieve(frame_size);
  return FrameDecodeResult::kOk;
}

}  // namespace raftkv
