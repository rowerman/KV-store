#include "raftkv/common/coding.h"

#include <limits>

namespace raftkv {
namespace {

template <typename UInt>
void EncodeLittleEndian(char* dst, UInt value) {
  for (size_t i = 0; i < sizeof(UInt); ++i) {
    dst[i] = static_cast<char>(value >> (i * 8));
  }
}

template <typename UInt>
UInt DecodeLittleEndian(const char* src) {
  UInt value = 0;
  for (size_t i = 0; i < sizeof(UInt); ++i) {
    value |= static_cast<UInt>(
                 static_cast<unsigned char>(src[i]))
             << (i * 8);
  }
  return value;
}

template <typename UInt>
void EncodeBigEndian(char* dst, UInt value) {
  for (size_t i = 0; i < sizeof(UInt); ++i) {
    dst[sizeof(UInt) - 1 - i] =
        static_cast<char>(value >> (i * 8));
  }
}

template <typename UInt>
UInt DecodeBigEndian(const char* src) {
  UInt value = 0;
  for (size_t i = 0; i < sizeof(UInt); ++i) {
    value = static_cast<UInt>(value << 8) |
            static_cast<UInt>(
                static_cast<unsigned char>(src[i]));
  }
  return value;
}

}  // namespace

void EncodeFixed32(char* dst, uint32_t value) {
  EncodeLittleEndian(dst, value);
}

void EncodeFixed64(char* dst, uint64_t value) {
  EncodeLittleEndian(dst, value);
}

uint32_t DecodeFixed32(const char* src) {
  return DecodeLittleEndian<uint32_t>(src);
}

uint64_t DecodeFixed64(const char* src) {
  return DecodeLittleEndian<uint64_t>(src);
}

void EncodeBigEndian16(char* dst, uint16_t value) {
  EncodeBigEndian(dst, value);
}

void EncodeBigEndian32(char* dst, uint32_t value) {
  EncodeBigEndian(dst, value);
}

void EncodeBigEndian64(char* dst, uint64_t value) {
  EncodeBigEndian(dst, value);
}

uint16_t DecodeBigEndian16(const char* src) {
  return DecodeBigEndian<uint16_t>(src);
}

uint32_t DecodeBigEndian32(const char* src) {
  return DecodeBigEndian<uint32_t>(src);
}

uint64_t DecodeBigEndian64(const char* src) {
  return DecodeBigEndian<uint64_t>(src);
}

Status PutLengthPrefixedSlice(std::string* dst, Slice value) {
  if (dst == nullptr) {
    return Status::InvalidArgument("destination is null");
  }
  if (value.size() >
      static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
    return Status::InvalidArgument("value is too large");
  }

  const size_t original_size = dst->size();
  dst->resize(original_size + sizeof(uint32_t));
  EncodeFixed32(dst->data() + original_size,
                static_cast<uint32_t>(value.size()));
  if (!value.empty()) {
    dst->append(value.data(), value.size());
  }
  return Status::OK();
}

Status GetLengthPrefixedSlice(Slice* input, Slice* result) {
  if (input == nullptr || result == nullptr) {
    return Status::InvalidArgument("input or result is null");
  }
  if (input->size() < sizeof(uint32_t)) {
    return Status::Corruption("truncated length prefix");
  }

  const uint32_t length = DecodeFixed32(input->data());
  const size_t payload_offset = sizeof(uint32_t);
  if (input->size() - payload_offset < length) {
    return Status::Corruption("truncated length-prefixed value");
  }

  *result = Slice(input->data() + payload_offset, length);
  *input = Slice(input->data() + payload_offset + length,
                 input->size() - payload_offset - length);
  return Status::OK();
}

}  // namespace raftkv
