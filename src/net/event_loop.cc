#include "raftkv/net/event_loop.h"

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <system_error>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>
#include <utility>

namespace raftkv {
namespace {

// 创建可跨线程唤醒且被 exec 自动关闭的 eventfd。
int CreateEventFd() {
  const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  return fd;
}

}  // namespace

EventLoop::EventLoop()
    : wakeup_fd_(-1),
      thread_id_(std::this_thread::get_id()),
      quit_(false),
      timer_queue_(&poller_) {
  // eventfd 由 EventLoop 持有，Channel 只借用该 fd。
  wakeup_fd_ = CreateEventFd();
  try {
    wakeup_channel_ = std::make_unique<Channel>(&poller_, wakeup_fd_);
    wakeup_channel_->SetReadCallback([this] { HandleWakeup(); });
    wakeup_channel_->EnableReading();
  } catch (...) {
    // 构造失败时 EventLoop 析构不会执行，必须手动释放 eventfd。
    ::close(wakeup_fd_);
    wakeup_fd_ = -1;
    throw;
  }
}

EventLoop::~EventLoop() {
  // 先从 Poller 注销，再关闭底层 fd，避免 Channel 留下陈旧注册。
  if (wakeup_channel_) {
    try {
      wakeup_channel_->Remove();
    } catch (...) {
      // 析构函数不能传播 Poller 删除错误。
    }
  }
  if (wakeup_fd_ >= 0) {
    ::close(wakeup_fd_);
  }
}

void EventLoop::Loop() {
  if (!IsInLoopThread()) {
    throw std::logic_error("EventLoop::Loop called in wrong thread");
  }

  std::vector<Channel*> active_channels;
  while (!quit_.load(std::memory_order_relaxed)) {
    // eventfd 和 timerfd 均为注册事件，-1 不会导致永不唤醒。
    const int active_count = poller_.Poll(-1, &active_channels);
    if (active_count < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "epoll_wait");
    }

    // 先处理 IO 和定时器事件，再执行本轮排队的普通任务。
    for (Channel* channel : active_channels) {
      channel->HandleEvent();
    }
    DoPendingFunctors();
  }
  DoPendingFunctors();
}

void EventLoop::Quit() {
  // 先设置标志再唤醒，保证循环线程醒来后能看到退出请求。
  quit_.store(true, std::memory_order_relaxed);
  Wakeup();
}

bool EventLoop::IsInLoopThread() const {
  return std::this_thread::get_id() == thread_id_;
}

void EventLoop::RunInLoop(Functor callback) {
  // 已在循环线程时直接执行，避免无意义的队列往返。
  if (IsInLoopThread()) {
    callback();
  } else {
    QueueInLoop(std::move(callback));
  }
}

void EventLoop::QueueInLoop(Functor callback) {
  // 锁只保护队列，不覆盖任务执行。
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_functors_.push_back(std::move(callback));
  }
  // 每次入队都唤醒；eventfd 计数会合并多次唤醒。
  Wakeup();
}

TimerId EventLoop::RunAfter(uint64_t delay_ms,
                            TimerCallback callback) {
  return timer_queue_.RunAfter(delay_ms, std::move(callback));
}

TimerId EventLoop::RunEvery(uint64_t interval_ms,
                            TimerCallback callback) {
  return timer_queue_.RunEvery(interval_ms, std::move(callback));
}

bool EventLoop::CancelTimer(TimerId id) {
  return timer_queue_.CancelTimer(id);
}

void EventLoop::Wakeup() {
  const uint64_t value = 1;
  ssize_t bytes_written = 0;
  do {
    bytes_written = ::write(wakeup_fd_, &value, sizeof(value));
  } while (bytes_written < 0 && errno == EINTR);

  // eventfd 计数已非零表示唤醒待处理，EAGAIN 不是循环错误。
  if (bytes_written < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
    throw std::system_error(errno, std::generic_category(),
                            "write eventfd");
  }
}

void EventLoop::HandleWakeup() {
  uint64_t value = 0;
  // ET 模式下持续读取，直到计数清零并返回 EAGAIN。
  while (true) {
    const ssize_t bytes_read =
        ::read(wakeup_fd_, &value, sizeof(value));
    if (bytes_read == static_cast<ssize_t>(sizeof(value))) {
      continue;
    }
    if (bytes_read < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
}

void EventLoop::DoPendingFunctors() {
  // 在锁内交换队列，回调执行期间允许其他线程继续入队。
  std::vector<Functor> callbacks;
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    callbacks.swap(pending_functors_);
  }

  for (Functor& callback : callbacks) {
    callback();
  }
}

}  // namespace raftkv
