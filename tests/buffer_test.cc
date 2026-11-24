#include "raftkv/net/buffer.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>

namespace raftkv {
namespace {

class PipeFds {
 public:
  PipeFds() {
    if (::pipe(fds_) != 0) {
      fds_[0] = -1;
      fds_[1] = -1;
    }
  }

  ~PipeFds() {
    if (fds_[0] >= 0) {
      ::close(fds_[0]);
    }
    if (fds_[1] >= 0) {
      ::close(fds_[1]);
    }
  }

  bool valid() const { return fds_[0] >= 0 && fds_[1] >= 0; }
  int read_fd() const { return fds_[0]; }
  int write_fd() const { return fds_[1]; }

  void close_write() {
    if (fds_[1] >= 0) {
      ::close(fds_[1]);
      fds_[1] = -1;
    }
  }

 private:
  int fds_[2] = {-1, -1};
};

TEST(BufferTest, InitialStateAndAppend) {
  Buffer buffer(16);

  EXPECT_EQ(buffer.ReadableBytes(), 0U);
  EXPECT_EQ(buffer.WritableBytes(), 16U);
  EXPECT_EQ(buffer.PrependableBytes(), 8U);

  buffer.Append("hello", 5);
  buffer.Append(Slice(" world"));

  EXPECT_EQ(buffer.ReadableBytes(), 11U);
  EXPECT_EQ(buffer.WritableBytes(), 5U);
  EXPECT_EQ(std::string(buffer.Peek(), buffer.ReadableBytes()),
            "hello world");
}

TEST(BufferTest, RetrievePartAndAll) {
  Buffer buffer(32);
  buffer.Append("abcdefghij", 10);

  buffer.Retrieve(4);
  EXPECT_EQ(buffer.ReadableBytes(), 6U);
  EXPECT_EQ(std::string(buffer.Peek(), buffer.ReadableBytes()), "efghij");

  const std::string all = buffer.RetrieveAllAsString();
  EXPECT_EQ(all, "efghij");
  EXPECT_EQ(buffer.ReadableBytes(), 0U);
  EXPECT_EQ(buffer.PrependableBytes(), 8U);
  EXPECT_EQ(buffer.WritableBytes(), 32U);
  EXPECT_EQ(buffer.RetrieveAllAsString(), "");
}

TEST(BufferTest, RetrieveOverReadableBytesClearsBuffer) {
  Buffer buffer(8);
  buffer.Append("data", 4);

  buffer.Retrieve(100);

  EXPECT_EQ(buffer.ReadableBytes(), 0U);
  EXPECT_EQ(buffer.PrependableBytes(), 8U);
  EXPECT_EQ(buffer.WritableBytes(), 8U);
}

TEST(BufferTest, LargeAppendExpandsWithoutLosingData) {
  Buffer buffer(4);
  const std::string value(4096, 'x');

  buffer.Append(value.data(), value.size());

  EXPECT_EQ(buffer.ReadableBytes(), value.size());
  EXPECT_EQ(std::string(buffer.Peek(), buffer.ReadableBytes()), value);
}

TEST(BufferTest, ReusesConsumedHeadSpaceBeforeExpanding) {
  Buffer buffer(32);
  buffer.Append("0123456789ABCDEFGHIJ", 20);
  buffer.Retrieve(16);

  buffer.Append("klmnopqrstuvwxyzABCD", 20);

  EXPECT_EQ(buffer.ReadableBytes(), 24U);
  EXPECT_EQ(buffer.PrependableBytes(), 8U);
  EXPECT_EQ(std::string(buffer.Peek(), buffer.ReadableBytes()),
            "GHIJklmnopqrstuvwxyzABCD");
}

TEST(BufferTest, ReadFdUsesExistingWritableSpace) {
  PipeFds pipe;
  ASSERT_TRUE(pipe.valid());
  const std::string data = "pipe data";
  ASSERT_EQ(::write(pipe.write_fd(), data.data(), data.size()),
            static_cast<ssize_t>(data.size()));

  Buffer buffer(32);
  int saved_errno = 123;
  const ssize_t bytes_read = buffer.ReadFd(pipe.read_fd(), &saved_errno);

  EXPECT_EQ(bytes_read, static_cast<ssize_t>(data.size()));
  EXPECT_EQ(saved_errno, 123);
  EXPECT_EQ(buffer.RetrieveAllAsString(), data);
}

TEST(BufferTest, ReadFdUsesStackBufferWhenNoWritableSpace) {
  PipeFds pipe;
  ASSERT_TRUE(pipe.valid());
  const std::string data = "stack-buffer-data";
  ASSERT_EQ(::write(pipe.write_fd(), data.data(), data.size()),
            static_cast<ssize_t>(data.size()));

  Buffer buffer(0);
  ASSERT_EQ(buffer.WritableBytes(), 0U);
  int saved_errno = 0;
  const ssize_t bytes_read = buffer.ReadFd(pipe.read_fd(), &saved_errno);

  EXPECT_EQ(bytes_read, static_cast<ssize_t>(data.size()));
  EXPECT_EQ(saved_errno, 0);
  EXPECT_EQ(buffer.RetrieveAllAsString(), data);
}

TEST(BufferTest, ReadFdSavesNonBlockingErrno) {
  PipeFds pipe;
  ASSERT_TRUE(pipe.valid());
  const int flags = ::fcntl(pipe.read_fd(), F_GETFL, 0);
  ASSERT_GE(flags, 0);
  ASSERT_EQ(::fcntl(pipe.read_fd(), F_SETFL, flags | O_NONBLOCK), 0);

  Buffer buffer(16);
  int saved_errno = 0;
  const ssize_t bytes_read = buffer.ReadFd(pipe.read_fd(), &saved_errno);

  EXPECT_EQ(bytes_read, -1);
  EXPECT_TRUE(saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
  EXPECT_EQ(buffer.ReadableBytes(), 0U);
}

TEST(BufferTest, ReadFdReturnsZeroAtEof) {
  PipeFds pipe;
  ASSERT_TRUE(pipe.valid());
  pipe.close_write();

  Buffer buffer(16);
  int saved_errno = 0;
  const ssize_t bytes_read = buffer.ReadFd(pipe.read_fd(), &saved_errno);

  EXPECT_EQ(bytes_read, 0);
  EXPECT_EQ(saved_errno, 0);
  EXPECT_EQ(buffer.ReadableBytes(), 0U);
}

}  // namespace
}  // namespace raftkv
