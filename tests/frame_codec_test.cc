#include "raftkv/rpc/frame_codec.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rpc_kv.pb.h"
#include "rpc_raft.pb.h"
#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/net/buffer.h"

namespace raftkv {
namespace {

std::string BufferBytes(const Buffer& buffer) {
  return std::string(buffer.Peek(), buffer.ReadableBytes());
}

RpcFrame MakeFrame(uint32_t method_id, uint64_t request_id,
                   std::string payload) {
  RpcFrame frame;
  frame.msg_type =
      static_cast<uint8_t>(RpcMessageType::kRequest);
  frame.method_id = method_id;
  frame.request_id = request_id;
  frame.payload = std::move(payload);
  return frame;
}

TEST(FrameCodecTest, EncodedLayoutUsesBigEndianHeaderAndCrc) {
  const std::string payload = "abc";
  RpcFrame frame = MakeFrame(0x01020304U, 0x0102030405060708ULL,
                             payload);
  frame.msg_type =
      static_cast<uint8_t>(RpcMessageType::kResponse);

  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(frame, &encoded).ok());
  ASSERT_EQ(encoded.ReadableBytes(),
            kRpcFrameHeaderSize + payload.size() +
                kRpcFrameChecksumSize);

  const char* data = encoded.Peek();
  EXPECT_EQ(data[0], 0x52);
  EXPECT_EQ(data[1], 0x4B);
  EXPECT_EQ(static_cast<uint8_t>(data[2]), kRpcFrameVersion);
  EXPECT_EQ(static_cast<uint8_t>(data[3]),
            static_cast<uint8_t>(RpcMessageType::kResponse));
  EXPECT_EQ(DecodeBigEndian32(data + 4), 0x01020304U);
  EXPECT_EQ(DecodeBigEndian64(data + 8), 0x0102030405060708ULL);
  EXPECT_EQ(DecodeBigEndian32(data + 16), payload.size());
  EXPECT_EQ(std::string(data + kRpcFrameHeaderSize, payload.size()),
            payload);

  const size_t checksum_offset =
      kRpcFrameHeaderSize + payload.size();
  EXPECT_EQ(DecodeBigEndian32(data + checksum_offset),
            Crc32(data, checksum_offset));
}

TEST(FrameCodecTest, RoundTripsEmptyAndBinaryPayloads) {
  const std::vector<std::string> payloads = {
      "",
      std::string("a\0b\0c", 5),
      std::string(4096, '\xFF'),
  };

  for (size_t i = 0; i < payloads.size(); ++i) {
    SCOPED_TRACE(i);
    RpcFrame expected =
        MakeFrame(100 + static_cast<uint32_t>(i), 200 + i,
                  payloads[i]);
    Buffer encoded;
    ASSERT_TRUE(FrameCodec::Encode(expected, &encoded).ok());

    RpcFrame actual;
    ASSERT_EQ(FrameCodec::TryDecode(&encoded, &actual),
              FrameDecodeResult::kOk);
    EXPECT_EQ(actual.magic, expected.magic);
    EXPECT_EQ(actual.version, expected.version);
    EXPECT_EQ(actual.msg_type, expected.msg_type);
    EXPECT_EQ(actual.method_id, expected.method_id);
    EXPECT_EQ(actual.request_id, expected.request_id);
    EXPECT_EQ(actual.payload, expected.payload);
    EXPECT_EQ(encoded.ReadableBytes(), 0);
  }
}

TEST(FrameCodecTest, DecodesThreeConcatenatedFrames) {
  std::vector<RpcFrame> expected;
  Buffer encoded;
  for (uint32_t i = 0; i < 3; ++i) {
    RpcFrame frame =
        MakeFrame(10 + i, 20 + i, "payload-" + std::to_string(i));
    ASSERT_TRUE(FrameCodec::Encode(frame, &encoded).ok());
    expected.push_back(std::move(frame));
  }

  for (const RpcFrame& frame : expected) {
    RpcFrame actual;
    ASSERT_EQ(FrameCodec::TryDecode(&encoded, &actual),
              FrameDecodeResult::kOk);
    EXPECT_EQ(actual.method_id, frame.method_id);
    EXPECT_EQ(actual.request_id, frame.request_id);
    EXPECT_EQ(actual.payload, frame.payload);
  }
  EXPECT_EQ(encoded.ReadableBytes(), 0);
}

TEST(FrameCodecTest, ByteByByteInputStaysIncompleteUntilFull) {
  const RpcFrame expected = MakeFrame(7, 8, "split-payload");
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(expected, &encoded).ok());
  const std::string wire = BufferBytes(encoded);

  Buffer input;
  for (size_t i = 0; i < wire.size(); ++i) {
    input.Append(wire.data() + i, 1);
    RpcFrame actual;
    const FrameDecodeResult result =
        FrameCodec::TryDecode(&input, &actual);
    if (i + 1 < wire.size()) {
      EXPECT_EQ(result, FrameDecodeResult::kIncomplete);
      EXPECT_EQ(input.ReadableBytes(), i + 1);
    } else {
      EXPECT_EQ(result, FrameDecodeResult::kOk);
      EXPECT_EQ(actual.method_id, expected.method_id);
      EXPECT_EQ(actual.request_id, expected.request_id);
      EXPECT_EQ(actual.payload, expected.payload);
      EXPECT_EQ(input.ReadableBytes(), 0);
    }
  }
}

TEST(FrameCodecTest, RandomChunksPreserveFrameOrderAndContent) {
  std::vector<RpcFrame> expected;
  Buffer encoded;
  for (uint32_t i = 0; i < 20; ++i) {
    RpcFrame frame = MakeFrame(
        i, 1000 + i,
        std::string(static_cast<size_t>(i * 7 + 1), 'a' + i));
    ASSERT_TRUE(FrameCodec::Encode(frame, &encoded).ok());
    expected.push_back(std::move(frame));
  }
  const std::string wire = BufferBytes(encoded);

  std::mt19937 random(0x524B2601U);
  std::uniform_int_distribution<size_t> chunk_distribution(1, 17);
  Buffer input;
  std::vector<RpcFrame> actual;
  size_t offset = 0;
  while (offset < wire.size()) {
    const size_t chunk =
        std::min(chunk_distribution(random), wire.size() - offset);
    input.Append(wire.data() + offset, chunk);
    offset += chunk;

    while (true) {
      RpcFrame frame;
      const FrameDecodeResult result =
          FrameCodec::TryDecode(&input, &frame);
      if (result == FrameDecodeResult::kIncomplete) {
        break;
      }
      ASSERT_EQ(result, FrameDecodeResult::kOk);
      actual.push_back(std::move(frame));
    }
  }

  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i].method_id, expected[i].method_id);
    EXPECT_EQ(actual[i].request_id, expected[i].request_id);
    EXPECT_EQ(actual[i].payload, expected[i].payload);
  }
  EXPECT_EQ(input.ReadableBytes(), 0);
}

TEST(FrameCodecTest, PartialHeaderPayloadAndCrcDoNotConsume) {
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(
                  MakeFrame(1, 2, "partial"), &encoded)
                  .ok());
  const std::string wire = BufferBytes(encoded);
  const std::vector<size_t> partial_sizes = {
      0,
      1,
      kRpcFrameHeaderSize - 1,
      kRpcFrameHeaderSize,
      wire.size() - 1,
  };

  for (size_t partial_size : partial_sizes) {
    SCOPED_TRACE(partial_size);
    Buffer input;
    input.Append(wire.data(), partial_size);
    RpcFrame output;
    output.method_id = 0xDEADBEEFU;
    EXPECT_EQ(FrameCodec::TryDecode(&input, &output),
              FrameDecodeResult::kIncomplete);
    EXPECT_EQ(input.ReadableBytes(), partial_size);
    EXPECT_EQ(output.method_id, 0xDEADBEEFU);
  }
}

TEST(FrameCodecTest, RejectsInvalidMagicWithoutConsuming) {
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(MakeFrame(1, 2, "magic"), &encoded)
                  .ok());
  std::string wire = BufferBytes(encoded);
  wire[0] = '\0';

  Buffer input;
  input.Append(wire);
  RpcFrame output;
  output.request_id = 99;
  EXPECT_EQ(FrameCodec::TryDecode(&input, &output),
            FrameDecodeResult::kCorrupt);
  EXPECT_EQ(input.ReadableBytes(), wire.size());
  EXPECT_EQ(output.request_id, 99);
}

TEST(FrameCodecTest, RejectsInvalidVersionAndMessageType) {
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(MakeFrame(1, 2, "fields"), &encoded)
                  .ok());
  const std::string wire = BufferBytes(encoded);

  for (size_t offset : {size_t{2}, size_t{3}}) {
    SCOPED_TRACE(offset);
    std::string damaged = wire;
    damaged[offset] = '\x7F';
    Buffer input;
    input.Append(damaged);
    RpcFrame output;
    EXPECT_EQ(FrameCodec::TryDecode(&input, &output),
              FrameDecodeResult::kCorrupt);
    EXPECT_EQ(input.ReadableBytes(), damaged.size());
  }
}

TEST(FrameCodecTest, RejectsOversizedPayloadBeforeWaitingForBody) {
  char header[kRpcFrameHeaderSize] = {};
  EncodeBigEndian16(header, kRpcFrameMagic);
  header[2] = static_cast<char>(kRpcFrameVersion);
  header[3] = static_cast<char>(RpcMessageType::kRequest);
  EncodeBigEndian32(header + 16, kRpcFrameMaxPayloadSize + 1);

  Buffer input;
  input.Append(header, sizeof(header));
  RpcFrame output;
  EXPECT_EQ(FrameCodec::TryDecode(&input, &output),
            FrameDecodeResult::kCorrupt);
  EXPECT_EQ(input.ReadableBytes(), sizeof(header));
}

TEST(FrameCodecTest, RejectsCrcMutationsWithoutConsuming) {
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(MakeFrame(1, 2, "checksum"), &encoded)
                  .ok());
  const std::string wire = BufferBytes(encoded);
  const size_t payload_offset = kRpcFrameHeaderSize;
  const size_t checksum_offset = wire.size() - kRpcFrameChecksumSize;

  for (size_t offset : {size_t{4}, payload_offset, checksum_offset}) {
    SCOPED_TRACE(offset);
    std::string damaged = wire;
    damaged[offset] ^= 0x01;
    Buffer input;
    input.Append(damaged);
    RpcFrame output;
    EXPECT_EQ(FrameCodec::TryDecode(&input, &output),
              FrameDecodeResult::kCorrupt);
    EXPECT_EQ(input.ReadableBytes(), damaged.size());
  }
}

TEST(FrameCodecTest, EncodeRejectsInvalidFrameWithoutChangingOutput) {
  Buffer output;
  output.Append("prefix", 6);
  const std::string original = BufferBytes(output);

  std::vector<RpcFrame> invalid_frames;
  RpcFrame bad_magic = MakeFrame(1, 2, "x");
  bad_magic.magic = 0;
  invalid_frames.push_back(bad_magic);
  RpcFrame bad_version = MakeFrame(1, 2, "x");
  bad_version.version = 2;
  invalid_frames.push_back(bad_version);
  RpcFrame bad_type = MakeFrame(1, 2, "x");
  bad_type.msg_type = 0;
  invalid_frames.push_back(bad_type);
  invalid_frames.push_back(
      MakeFrame(1, 2,
                std::string(kRpcFrameMaxPayloadSize + 1, 'x')));

  for (const RpcFrame& frame : invalid_frames) {
    EXPECT_FALSE(FrameCodec::Encode(frame, &output).ok());
    EXPECT_EQ(BufferBytes(output), original);
  }
  EXPECT_FALSE(FrameCodec::Encode(MakeFrame(1, 2, ""), nullptr).ok());
}

TEST(FrameCodecTest, NullArgumentsAreRejected) {
  Buffer encoded;
  RpcFrame frame = MakeFrame(1, 2, "x");
  EXPECT_THROW(FrameCodec::TryDecode(nullptr, &frame),
               std::invalid_argument);
  EXPECT_THROW(FrameCodec::TryDecode(&encoded, nullptr),
               std::invalid_argument);
}

TEST(FrameCodecTest, KvPutProtobufRoundTripThroughFrame) {
  proto::KvPutRequest request;
  request.set_client_id(0x0102030405060708ULL);
  request.set_seq_num(9);
  request.set_key(std::string("k\0y", 3));
  request.set_value("value");

  std::string payload;
  ASSERT_TRUE(request.SerializeToString(&payload));
  RpcFrame frame = MakeFrame(1, 42, std::move(payload));
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(frame, &encoded).ok());

  RpcFrame decoded;
  ASSERT_EQ(FrameCodec::TryDecode(&encoded, &decoded),
            FrameDecodeResult::kOk);
  proto::KvPutRequest actual;
  ASSERT_TRUE(actual.ParseFromString(decoded.payload));
  EXPECT_EQ(actual.client_id(), request.client_id());
  EXPECT_EQ(actual.seq_num(), request.seq_num());
  EXPECT_EQ(actual.key(), request.key());
  EXPECT_EQ(actual.value(), request.value());
}

TEST(FrameCodecTest, AppendEntriesProtobufRoundTripThroughFrame) {
  proto::AppendEntriesRequest request;
  request.set_term(7);
  request.set_leader_id(2);
  request.set_prev_log_index(30);
  request.set_prev_log_term(6);
  request.set_leader_commit(29);
  request.set_read_ctx_id(99);
  for (uint32_t i = 0; i < 2; ++i) {
    proto::LogEntry* entry = request.add_entries();
    entry->set_term(7);
    entry->set_index(31 + i);
    entry->set_entry_type(proto::ENTRY_TYPE_NORMAL);
    entry->set_payload("entry-" + std::to_string(i));
  }

  std::string payload;
  ASSERT_TRUE(request.SerializeToString(&payload));
  RpcFrame frame = MakeFrame(101, 77, std::move(payload));
  Buffer encoded;
  ASSERT_TRUE(FrameCodec::Encode(frame, &encoded).ok());

  RpcFrame decoded;
  ASSERT_EQ(FrameCodec::TryDecode(&encoded, &decoded),
            FrameDecodeResult::kOk);
  proto::AppendEntriesRequest actual;
  ASSERT_TRUE(actual.ParseFromString(decoded.payload));
  EXPECT_EQ(actual.term(), request.term());
  EXPECT_EQ(actual.leader_id(), request.leader_id());
  EXPECT_EQ(actual.leader_commit(), request.leader_commit());
  ASSERT_EQ(actual.entries_size(), 2);
  EXPECT_EQ(actual.entries(1).index(), 32);
  EXPECT_EQ(actual.entries(1).entry_type(),
            proto::ENTRY_TYPE_NORMAL);
  EXPECT_EQ(actual.entries(1).payload(), "entry-1");
}

}  // namespace
}  // namespace raftkv
