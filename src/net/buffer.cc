#include "raftkv/net/buffer.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <sys/uio.h>
#include <unistd.h>

namespace raftkv {

// 预留固定前置区，初始可写容量由调用方指定。
Buffer::Buffer(size_t initial_size)
    : buffer_(kCheapPrepend + initial_size),
      reader_index_(kCheapPrepend),
      writer_index_(kCheapPrepend) {}

size_t Buffer::ReadableBytes() const {
  return writer_index_ - reader_index_;
}

size_t Buffer::WritableBytes() const {
  return buffer_.size() - writer_index_;
}

size_t Buffer::PrependableBytes() const {
  return reader_index_;
}

const char* Buffer::Peek() const {
  return buffer_.data() + reader_index_;
}

// 确保空间后写入数据，只移动写索引，不改变已读区。
void Buffer::Append(const char* data, size_t len) {
  if (len == 0) {
    return;
  }

  EnsureWritableBytes(len);
  std::memcpy(buffer_.data() + writer_index_, data, len);
  writer_index_ += len;
}

// Slice 只提供视图，数据仍由调用方持有。
void Buffer::Append(Slice data) {
  Append(data.data(), data.size());
}

// 未读完时前移读索引；全部消费时回到固定前置区。
void Buffer::Retrieve(size_t len) {
  if (len < ReadableBytes()) {
    reader_index_ += len;
    return;
  }
  reader_index_ = kCheapPrepend;
  writer_index_ = kCheapPrepend;
}

// 先复制可读内容，再统一消费，保证返回内容不受后续索引变化影响。
std::string Buffer::RetrieveAllAsString() {
  if (ReadableBytes() == 0) {
    return std::string();
  }

  std::string result(Peek(), ReadableBytes());
  Retrieve(ReadableBytes());
  return result;
}

// 同时使用缓冲区尾部空间和栈缓冲，减少读取系统调用。
ssize_t Buffer::ReadFd(int fd, int* saved_errno) {
  std::array<char, 64 * 1024> extra_buffer;
  std::array<struct iovec, 2> vectors{};
  const size_t writable = WritableBytes();

  vectors[0].iov_base = buffer_.data() + writer_index_;
  vectors[0].iov_len = writable;
  vectors[1].iov_base = extra_buffer.data();
  vectors[1].iov_len = extra_buffer.size();

  ssize_t bytes_read = 0;
  // 被信号中断不属于真实读取错误。
  do {
    bytes_read = ::readv(fd, vectors.data(),
                         static_cast<int>(vectors.size()));
  } while (bytes_read < 0 && errno == EINTR);

  if (bytes_read < 0) {
    if (saved_errno != nullptr) {
      *saved_errno = errno;
    }
    return bytes_read;
  }

  const size_t received = static_cast<size_t>(bytes_read);
  if (received <= writable) {
    // 数据全部落在原缓冲区尾部空间。
    writer_index_ += received;
  } else {
    // 超出部分来自栈缓冲，追加后仍保持连续可读布局。
    writer_index_ = buffer_.size();
    Append(extra_buffer.data(), received - writable);
  }
  return bytes_read;
}

// 优先复用已消费的头部空间，仍不足时再扩容。
void Buffer::EnsureWritableBytes(size_t len) {
  if (WritableBytes() >= len) {
    return;
  }

  if (WritableBytes() + PrependableBytes() >=
      len + kCheapPrepend) {
    // 搬移后保留固定前置区，同时让可读数据保持连续。
    const size_t readable = ReadableBytes();
    std::memmove(buffer_.data() + kCheapPrepend,
                 buffer_.data() + reader_index_, readable);
    reader_index_ = kCheapPrepend;
    writer_index_ = reader_index_ + readable;
    return;
  }

  // 扩容只满足本次写入所需空间。
  buffer_.resize(writer_index_ + len);
}

}  // namespace raftkv
