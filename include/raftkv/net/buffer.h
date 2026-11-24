#ifndef RAFTKV_NET_BUFFER_H_
#define RAFTKV_NET_BUFFER_H_

#include <cstddef>
#include <string>
#include <sys/types.h>
#include <vector>

#include "raftkv/common/slice.h"

namespace raftkv {

// 连续动态字节缓冲区，管理 TCP 连接的读写数据。
class Buffer {
 private:
  static constexpr size_t kInitialSize = 1024;
  static constexpr size_t kCheapPrepend = 8;

 public:
  // 创建指定尾部可写容量的缓冲区。
  explicit Buffer(size_t initial_size = kInitialSize);

  // 返回可读字节数。
  size_t ReadableBytes() const;

  // 返回尾部直接可写字节数。
  size_t WritableBytes() const;

  // 返回头部可复用的字节数。
  size_t PrependableBytes() const;

  // 返回当前可读首地址，不转移所有权。
  const char* Peek() const;

  // 追加指定字节。
  void Append(const char* data, size_t len);

  // 追加 Slice 引用的字节。
  void Append(Slice data);

  // 消费指定字节数；超过可读长度时清空全部可读数据。
  void Retrieve(size_t len);

  // 复制并消费全部可读数据。
  std::string RetrieveAllAsString();

  // 从文件描述符读取数据；错误时保存 errno 并返回 -1。
  ssize_t ReadFd(int fd, int* saved_errno);

 private:
  void EnsureWritableBytes(size_t len);

  std::vector<char> buffer_;
  size_t reader_index_;
  size_t writer_index_;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_BUFFER_H_
