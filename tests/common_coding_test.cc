#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/common/slice.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <string>

namespace raftkv {
namespace {

TEST(CodingTest, FixedLittleEndianRepresentationAndRoundTrip) {
  std::array<char, 4> encoded32{};
  std::array<char, 8> encoded64{};

  EncodeFixed32(encoded32.data(), 0x12345678U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[0]), 0x78U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[1]), 0x56U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[2]), 0x34U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[3]), 0x12U);
  EXPECT_EQ(DecodeFixed32(encoded32.data()), 0x12345678U);

  EncodeFixed64(encoded64.data(), 0x0123456789ABCDEFULL);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[0]), 0xEFU);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[1]), 0xCDU);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[2]), 0xABU);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[3]), 0x89U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[4]), 0x67U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[5]), 0x45U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[6]), 0x23U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[7]), 0x01U);
  EXPECT_EQ(DecodeFixed64(encoded64.data()), 0x0123456789ABCDEFULL);
}

TEST(CodingTest, FixedLittleEndianRandomRoundTrip) {
  std::mt19937_64 random(20261124);

  for (int i = 0; i < 1000; ++i) {
    const uint32_t value32 = static_cast<uint32_t>(random());
    std::array<char, 4> encoded32{};
    EncodeFixed32(encoded32.data(), value32);
    EXPECT_EQ(DecodeFixed32(encoded32.data()), value32);

    const uint64_t value64 = random();
    std::array<char, 8> encoded64{};
    EncodeFixed64(encoded64.data(), value64);
    EXPECT_EQ(DecodeFixed64(encoded64.data()), value64);
  }
}

TEST(CodingTest, BigEndianRepresentationAndRoundTrip) {
  std::array<char, 2> encoded16{};
  std::array<char, 4> encoded32{};
  std::array<char, 8> encoded64{};

  EncodeBigEndian16(encoded16.data(), 0x1234U);
  EXPECT_EQ(static_cast<unsigned char>(encoded16[0]), 0x12U);
  EXPECT_EQ(static_cast<unsigned char>(encoded16[1]), 0x34U);
  EXPECT_EQ(DecodeBigEndian16(encoded16.data()), 0x1234U);

  EncodeBigEndian32(encoded32.data(), 0x12345678U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[0]), 0x12U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[1]), 0x34U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[2]), 0x56U);
  EXPECT_EQ(static_cast<unsigned char>(encoded32[3]), 0x78U);
  EXPECT_EQ(DecodeBigEndian32(encoded32.data()), 0x12345678U);

  EncodeBigEndian64(encoded64.data(), 0x0123456789ABCDEFULL);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[0]), 0x01U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[1]), 0x23U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[2]), 0x45U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[3]), 0x67U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[4]), 0x89U);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[5]), 0xABU);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[6]), 0xCDU);
  EXPECT_EQ(static_cast<unsigned char>(encoded64[7]), 0xEFU);
  EXPECT_EQ(DecodeBigEndian64(encoded64.data()), 0x0123456789ABCDEFULL);
}

TEST(CodingTest, BigEndianRandomRoundTrip) {
  std::mt19937 random(20261125);

  for (int i = 0; i < 1000; ++i) {
    const uint16_t value16 = static_cast<uint16_t>(random());
    std::array<char, 2> encoded16{};
    EncodeBigEndian16(encoded16.data(), value16);
    EXPECT_EQ(DecodeBigEndian16(encoded16.data()), value16);

    const uint32_t value32 = random();
    std::array<char, 4> encoded32{};
    EncodeBigEndian32(encoded32.data(), value32);
    EXPECT_EQ(DecodeBigEndian32(encoded32.data()), value32);

    const uint64_t value64 = (static_cast<uint64_t>(random()) << 32U) |
                             static_cast<uint64_t>(random());
    std::array<char, 8> encoded64{};
    EncodeBigEndian64(encoded64.data(), value64);
    EXPECT_EQ(DecodeBigEndian64(encoded64.data()), value64);
  }
}

TEST(CodingTest, LengthPrefixedSlicesAdvanceInput) {
  const std::array<char, 5> first = {'a', '\0', 'b', 'c', 'd'};
  const std::string second = "second";
  std::string packet;

  ASSERT_TRUE(PutLengthPrefixedSlice(
                  &packet, Slice(first.data(), first.size()))
                  .ok());
  ASSERT_TRUE(PutLengthPrefixedSlice(&packet, Slice(second)).ok());
  packet.append("tail");

  Slice input(packet);
  Slice decoded;

  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &decoded).ok());
  EXPECT_EQ(decoded.ToString(), std::string(first.data(), first.size()));

  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &decoded).ok());
  EXPECT_EQ(decoded.ToString(), second);
  EXPECT_EQ(input.ToString(), "tail");
}

TEST(CodingTest, LengthPrefixedEmptyValueAdvancesFourBytes) {
  std::string packet;
  ASSERT_TRUE(PutLengthPrefixedSlice(&packet, Slice()).ok());
  packet.push_back('x');

  Slice input(packet);
  Slice decoded("unchanged");
  ASSERT_TRUE(GetLengthPrefixedSlice(&input, &decoded).ok());

  EXPECT_TRUE(decoded.empty());
  EXPECT_EQ(input.ToString(), "x");
}

TEST(CodingTest, LengthPrefixedRandomStringsRoundTrip) {
  std::mt19937 random(20261126);
  std::uniform_int_distribution<int> length_distribution(0, 128);
  std::uniform_int_distribution<int> byte_distribution(0, 255);

  for (int i = 0; i < 1000; ++i) {
    const int length = length_distribution(random);
    std::string original(static_cast<size_t>(length), '\0');
    for (char& byte : original) {
      byte = static_cast<char>(byte_distribution(random));
    }

    std::string packet;
    ASSERT_TRUE(PutLengthPrefixedSlice(&packet, Slice(original)).ok());

    Slice input(packet);
    Slice decoded;
    ASSERT_TRUE(GetLengthPrefixedSlice(&input, &decoded).ok());
    EXPECT_EQ(decoded.ToString(), original);
    EXPECT_TRUE(input.empty());
  }
}

TEST(CodingTest, LengthPrefixedRejectsInvalidArguments) {
  std::string destination;
  EXPECT_EQ(PutLengthPrefixedSlice(nullptr, Slice("value")).code(),
            Code::kInvalidArgument);
  EXPECT_TRUE(destination.empty());

  const size_t oversized = static_cast<size_t>(
                               std::numeric_limits<uint32_t>::max()) +
                           1U;
  if (oversized > std::numeric_limits<uint32_t>::max()) {
    const char byte = 'x';
    EXPECT_EQ(PutLengthPrefixedSlice(&destination, Slice(&byte, oversized))
                  .code(),
              Code::kInvalidArgument);
    EXPECT_TRUE(destination.empty());
  }

  Slice input("data");
  Slice result;
  EXPECT_EQ(GetLengthPrefixedSlice(nullptr, &result).code(),
            Code::kInvalidArgument);
  EXPECT_EQ(GetLengthPrefixedSlice(&input, nullptr).code(),
            Code::kInvalidArgument);
}

TEST(CodingTest, LengthPrefixedRejectsTruncatedLengthPrefix) {
  const std::string data = "abc";
  Slice input(data);
  Slice result("unchanged");

  const Status status = GetLengthPrefixedSlice(&input, &result);

  EXPECT_EQ(status.code(), Code::kCorruption);
  EXPECT_EQ(input.data(), data.data());
  EXPECT_EQ(input.size(), data.size());
  EXPECT_EQ(result.ToString(), "unchanged");
}

TEST(CodingTest, LengthPrefixedRejectsTruncatedPayload) {
  std::string data(4, '\0');
  EncodeFixed32(data.data(), 5);
  data.append("abc");
  Slice input(data);
  Slice result("unchanged");

  const Status status = GetLengthPrefixedSlice(&input, &result);

  EXPECT_EQ(status.code(), Code::kCorruption);
  EXPECT_EQ(input.data(), data.data());
  EXPECT_EQ(input.size(), data.size());
  EXPECT_EQ(result.ToString(), "unchanged");
}

TEST(Crc32Test, StandardVectors) {
  EXPECT_EQ(Crc32("", 0), 0x00000000U);
  EXPECT_EQ(Crc32("123456789", 9), 0xCBF43926U);
  EXPECT_EQ(Crc32(Slice("123456789")), 0xCBF43926U);
}

TEST(Crc32Test, SliceOverloadIncludesEmbeddedZeroBytes) {
  const std::array<char, 5> bytes = {'a', '\0', 'b', '\0', 'c'};
  const Slice slice(bytes.data(), bytes.size());

  EXPECT_EQ(Crc32(slice), Crc32(bytes.data(), bytes.size()));
}

TEST(Crc32Test, EverySingleByteMutationChangesChecksum) {
  std::string data = "raftkv crc32 mutation test";
  const uint32_t original = Crc32(Slice(data));

  for (char& byte : data) {
    byte ^= 0x01;
    EXPECT_NE(Crc32(Slice(data)), original);
    byte ^= 0x01;
  }
}

}  // namespace
}  // namespace raftkv
