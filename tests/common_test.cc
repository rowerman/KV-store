#include "raftkv/common/slice.h"
#include "raftkv/common/status.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

namespace raftkv {
namespace {

TEST(StatusTest, DefaultIsOk) {
  const Status status;

  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.code(), Code::kOk);
  EXPECT_EQ(status.message(), "");
  EXPECT_EQ(status.ToString(), "OK");
}

TEST(StatusTest, FactoriesSetCodeAndMessage) {
  const Status not_found = Status::NotFound("missing");
  const Status corruption = Status::Corruption("bad record");
  const Status io_error = Status::IOError("open failed");
  const Status not_leader = Status::NotLeader("leader is node 2");
  const Status timeout = Status::Timeout("deadline exceeded");
  const Status invalid = Status::InvalidArgument("empty key");

  EXPECT_EQ(not_found.code(), Code::kNotFound);
  EXPECT_EQ(corruption.code(), Code::kCorruption);
  EXPECT_EQ(io_error.code(), Code::kIOError);
  EXPECT_EQ(not_leader.code(), Code::kNotLeader);
  EXPECT_EQ(timeout.code(), Code::kTimeout);
  EXPECT_EQ(invalid.code(), Code::kInvalidArgument);

  EXPECT_FALSE(not_found.ok());
  EXPECT_FALSE(corruption.ok());
  EXPECT_FALSE(io_error.ok());
  EXPECT_FALSE(not_leader.ok());
  EXPECT_FALSE(timeout.ok());
  EXPECT_FALSE(invalid.ok());

  EXPECT_EQ(not_found.message(), "missing");
  EXPECT_EQ(not_found.ToString(), "NotFound: missing");
  EXPECT_EQ(not_leader.ToString(), "NotLeader: leader is node 2");
}

TEST(StatusTest, CodeNamesAreStable) {
  EXPECT_STREQ(CodeName(Code::kOk), "OK");
  EXPECT_STREQ(CodeName(Code::kNotFound), "NotFound");
  EXPECT_STREQ(CodeName(Code::kCorruption), "Corruption");
  EXPECT_STREQ(CodeName(Code::kIOError), "IOError");
  EXPECT_STREQ(CodeName(Code::kNotLeader), "NotLeader");
  EXPECT_STREQ(CodeName(Code::kTimeout), "Timeout");
  EXPECT_STREQ(CodeName(Code::kInvalidArgument), "InvalidArgument");
  EXPECT_STREQ(CodeName(static_cast<Code>(99)), "Unknown");
}

TEST(StatusTest, CopiesAndEqualityPreserveState) {
  const Status original = Status::IOError("disk full");
  const Status copy = original;
  const Status same = Status::IOError("disk full");
  const Status other_message = Status::IOError("read only");
  const Status other_code = Status::Timeout("disk full");

  EXPECT_TRUE(copy.ok() == original.ok());
  EXPECT_EQ(copy.code(), original.code());
  EXPECT_EQ(copy.message(), original.message());
  EXPECT_EQ(copy, original);
  EXPECT_EQ(original, same);
  EXPECT_NE(original, other_message);
  EXPECT_NE(original, other_code);
}

TEST(SliceTest, DefaultAndNullConstructToEmptyView) {
  const Slice empty;
  const Slice null_value(nullptr);

  EXPECT_TRUE(empty.empty());
  EXPECT_EQ(empty.size(), 0U);
  EXPECT_EQ(empty.ToString(), "");
  EXPECT_TRUE(null_value.empty());
  EXPECT_EQ(null_value.size(), 0U);
}

TEST(SliceTest, ConstructorsPreserveBytesWithoutCopying) {
  const char literal[] = "hello";
  const std::string owned = "world";
  const char raw[] = {'a', '\0', 'b', 'c'};
  const Slice from_literal(literal);
  const Slice from_owned(owned);
  const Slice from_raw(raw, sizeof(raw));
  const std::string_view view = "slice";
  const Slice from_view(view);

  EXPECT_EQ(from_literal.data(), literal);
  EXPECT_EQ(from_literal.size(), 5U);
  EXPECT_EQ(from_literal.ToString(), "hello");

  EXPECT_EQ(from_owned.data(), owned.data());
  EXPECT_EQ(from_owned.size(), owned.size());
  EXPECT_EQ(from_owned.ToString(), "world");

  EXPECT_EQ(from_raw.data(), raw);
  EXPECT_EQ(from_raw.size(), sizeof(raw));
  EXPECT_EQ(from_raw.ToString(), std::string(raw, sizeof(raw)));

  EXPECT_EQ(from_view.data(), view.data());
  EXPECT_EQ(from_view.size(), view.size());
}

TEST(SliceTest, CompareUsesLexicographicOrder) {
  const Slice empty;
  const Slice app("app");
  const Slice apple("apple");
  const Slice banana("banana");
  const Slice same_apple("apple");

  EXPECT_LT(app.compare(apple), 0);
  EXPECT_GT(banana.compare(apple), 0);
  EXPECT_EQ(apple.compare(same_apple), 0);
  EXPECT_LT(empty.compare(app), 0);
  EXPECT_GT(app.compare(empty), 0);
}

TEST(SliceTest, StartsWithHandlesPrefixAndEmptyPrefix) {
  const Slice empty;
  const Slice app("app");
  const Slice apple("apple");
  const Slice app_exact("app");
  const Slice longer("applesauce");

  EXPECT_TRUE(apple.starts_with(app));
  EXPECT_TRUE(apple.starts_with(apple));
  EXPECT_TRUE(apple.starts_with(app_exact));
  EXPECT_TRUE(apple.starts_with(empty));
  EXPECT_FALSE(app.starts_with(apple));
  EXPECT_FALSE(app.starts_with(longer));
}

TEST(SliceTest, ExplicitStringViewConversionKeepsView) {
  const std::string owner = "owned bytes";
  const Slice slice(owner);
  const std::string_view view = static_cast<std::string_view>(slice);

  EXPECT_EQ(view.data(), owner.data());
  EXPECT_EQ(view.size(), owner.size());
  EXPECT_EQ(view, std::string_view(owner));
}

}  // namespace
}  // namespace raftkv
