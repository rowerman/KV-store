#include <gtest/gtest.h>
#include <google/protobuf/stubs/common.h>

TEST(Smoke, Arithmetic) {
  EXPECT_EQ(1 + 1, 2);
}

TEST(Smoke, ProtobufLinked) {
  EXPECT_GE(GOOGLE_PROTOBUF_VERSION, 3012000);
}
