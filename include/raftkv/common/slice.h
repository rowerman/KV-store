#ifndef RAFTKV_COMMON_SLICE_H_
#define RAFTKV_COMMON_SLICE_H_

#include <cstring>
#include <string>
#include <string_view>

namespace raftkv {

// 非拥有字节视图；被引用数据必须比 Slice 存活更久。
class Slice {
 public:
  // 创建空视图。
  Slice() = default;

  // 视图指向以 '\0' 结尾的字符串；nullptr 转为空视图。
  Slice(const char* data) : view_(data == nullptr ? std::string_view()
                                                  : std::string_view(data)) {}

  // 视图指向 data 起始的 size 个字节；调用方保证范围有效。
  Slice(const char* data, size_t size) : view_(data, size) {}

  // 复用原视图，不复制数据。
  Slice(std::string_view view) : view_(view) {}

  // 非拥有地引用字符串；字符串必须比 Slice 存活更久。
  Slice(const std::string& value) : view_(value) {}

  // 返回首字节地址。
  const char* data() const { return view_.data(); }

  // 返回引用字节数。
  size_t size() const { return view_.size(); }

  // 未引用任何字节时返回 true。
  bool empty() const { return view_.empty(); }

  // 按字节字典序比较，返回大小关系符号。
  int compare(const Slice& other) const {
    return view_.compare(other.view_);
  }

  // 当前视图以 prefix 开头时返回 true。
  bool starts_with(const Slice& prefix) const {
    return view_.substr(0, prefix.size()) == prefix.view_;
  }

  // 将引用字节复制为字符串。
  std::string ToString() const { return std::string(view_); }

  // 返回底层非拥有视图。
  explicit operator std::string_view() const { return view_; }

 private:
  std::string_view view_;
};

}  // namespace raftkv

#endif  // RAFTKV_COMMON_SLICE_H_
