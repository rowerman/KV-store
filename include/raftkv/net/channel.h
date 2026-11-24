#ifndef RAFTKV_NET_CHANNEL_H_
#define RAFTKV_NET_CHANNEL_H_

#include <cstdint>
#include <functional>   // 提供 std::function，用于包装回调函数
#include <sys/epoll.h>  // 提供 EPOLLIN, EPOLLOUT 等宏

namespace raftkv {

class EpollPoller;

// 非拥有文件描述符的事件通道。
class Channel {
 public:
  using EventCallback = std::function<void()>;

  static constexpr uint32_t kNoneEvent = 0;
  static constexpr uint32_t kReadEvent =
      EPOLLIN | EPOLLPRI | EPOLLRDHUP;
  static constexpr uint32_t kWriteEvent = EPOLLOUT;

  // 绑定文件描述符和所属 Poller。
  Channel(EpollPoller* poller, int fd);

  // 若仍注册，则从 Poller 中移除；不关闭文件描述符。
  ~Channel();

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  // 返回绑定的文件描述符。
  int fd() const { return fd_; }

  // 返回关注事件（用户设置的）
  uint32_t events() const { return events_; }

  // 返回最近一次触发事件（epoll 告诉你的）
  uint32_t revents() const { return revents_; }

  // 是否关注读或写事件。
  bool IsReading() const { return (events_ & kReadEvent) != 0; }
  bool IsWriting() const { return (events_ & kWriteEvent) != 0; }

  // 设置事件回调。
  void SetReadCallback(EventCallback callback);
  void SetWriteCallback(EventCallback callback);
  void SetCloseCallback(EventCallback callback);
  void SetErrorCallback(EventCallback callback);

  // 更新关注事件并同步到 Poller。
  void EnableReading();
  void DisableReading();
  void EnableWriting();
  void DisableWriting();
  void DisableAll();

  // 分发最近一次 epoll 触发事件。
  void HandleEvent();

  // 主动从 Poller 删除。
  void Remove();

 private:
  friend class EpollPoller;

  enum class State {
    kNew,
    kAdded,
  };

  void Update();

  EpollPoller* poller_;
  int fd_;
  uint32_t events_ = kNoneEvent;
  uint32_t revents_ = kNoneEvent;
  State state_ = State::kNew;
  EventCallback read_callback_;
  EventCallback write_callback_;
  EventCallback close_callback_;
  EventCallback error_callback_;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_CHANNEL_H_
