#include "raftkv/net/timer_queue.h"

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <system_error>
#include <sys/timerfd.h>
#include <unistd.h>
#include <utility>

#include "raftkv/net/poller.h"

namespace raftkv {
namespace {

// 创建单调时钟、非阻塞且被 exec 自动关闭的 timerfd。
int CreateTimerFd() {
  const int fd =
      ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "timerfd_create");
  }
  return fd;
}

// 防止超大毫秒值转换为有符号 duration 时溢出。
std::chrono::milliseconds ToMilliseconds(uint64_t value) {
  const uint64_t max_value = static_cast<uint64_t>(
      std::chrono::milliseconds::max().count());
  return std::chrono::milliseconds(
      static_cast<std::chrono::milliseconds::rep>(
          value > max_value ? max_value : value));
}

}  // namespace

TimerQueue::TimerQueue(EpollPoller* poller)
    : timerfd_(CreateTimerFd()), timer_channel_(poller, timerfd_) {
  if (poller == nullptr) {
    ::close(timerfd_);
    throw std::invalid_argument("poller is null");
  }

  try {
    // timerfd 到期后通过 Poller 进入 HandleRead。
    timer_channel_.SetReadCallback([this] { HandleRead(); });
    timer_channel_.EnableReading();
  } catch (...) {
    ::close(timerfd_);
    timerfd_ = -1;
    throw;
  }
}

TimerQueue::~TimerQueue() {
  // 先删除 epoll 注册，再关闭 timerfd。
  try {
    timer_channel_.DisableAll();
  } catch (...) {
    // 析构函数不能传播 Poller 删除错误。
  }
  if (timerfd_ >= 0) {
    ::close(timerfd_);
  }
}

TimerId TimerQueue::RunAfter(uint64_t delay_ms,
                             TimerCallback callback) {
  // 一次性定时器没有周期间隔。
  return AddTimer(delay_ms, 0, std::move(callback));
}

TimerId TimerQueue::RunEvery(uint64_t interval_ms,
                             TimerCallback callback) {
  if (interval_ms == 0) {
    // 零周期会形成忙循环，直接拒绝。
    return 0;
  }
  return AddTimer(interval_ms, interval_ms, std::move(callback));
}

bool TimerQueue::CancelTimer(TimerId id) {
  if (id == 0) {
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  // 只有活跃 ID 才能被取消；重复取消返回 false。
  if (active_ids_.erase(id) == 0) {
    return false;
  }
  // 堆项暂时保留，等它到达堆顶或被扫描时再删除。
  cancelled_ids_.insert(id);
  ResetTimerLocked();
  return true;
}

TimerId TimerQueue::AddTimer(uint64_t delay_ms,
                             uint64_t interval_ms,
                             TimerCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  const TimerId id = next_id_++;
  if (next_id_ == 0) {
    // 0 保留为无效 ID，溢出时跳过。
    next_id_ = 1;
  }

  const auto expiration =
      std::chrono::steady_clock::now() + ToMilliseconds(delay_ms);
  timers_.push(TimerEntry{expiration, id, interval_ms,
                          std::move(callback)});
  active_ids_.insert(id);
  // 新任务可能成为最早到期任务，需要同步 timerfd。
  ResetTimerLocked();
  return id;
}

void TimerQueue::HandleRead() {
  uint64_t expirations = 0;
  ssize_t bytes_read = 0;
  do {
    bytes_read = ::read(timerfd_, &expirations, sizeof(expirations));
  } while (bytes_read < 0 && errno == EINTR);
  if (bytes_read != static_cast<ssize_t>(sizeof(expirations))) {
    // 非完整读取通常表示没有更多到期事件。
    return;
  }

  std::vector<TimerCallback> callbacks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    // 收集所有已到期任务，未到期任务保留在堆中。
    while (!timers_.empty() && timers_.top().expiration <= now) {
      TimerEntry entry = timers_.top();
      timers_.pop();

      if (cancelled_ids_.erase(entry.id) != 0 ||
          active_ids_.find(entry.id) == active_ids_.end()) {
        // 逻辑取消的任务在弹出时真正丢弃。
        continue;
      }

      if (entry.interval_ms == 0) {
        // 一次性任务在回调前结束生命周期。
        active_ids_.erase(entry.id);
      } else {
        // 周期任务先安排下一周期，回调内可立即取消后续触发。
        entry.expiration =
            now + ToMilliseconds(entry.interval_ms);
        timers_.push(entry);
      }
      callbacks.push_back(entry.callback);
    }
    ResetTimerLocked();
  }

  // 回调不持锁执行，可以在回调内继续添加或取消定时器。
  for (TimerCallback& callback : callbacks) {
    callback();
  }
}

void TimerQueue::ResetTimerLocked() {
  // 顺带清理已经到达堆顶的取消项。
  while (!timers_.empty() &&
         cancelled_ids_.erase(timers_.top().id) != 0) {
    timers_.pop();
  }

  struct itimerspec specification {};
  if (timers_.empty()) {
    // it_value 为 0 表示停止 timerfd。
    if (::timerfd_settime(timerfd_, 0, &specification, nullptr) < 0) {
      throw std::system_error(errno, std::generic_category(),
                              "timerfd_settime");
    }
    return;
  }

  auto remaining = timers_.top().expiration -
                   std::chrono::steady_clock::now();
  if (remaining <= std::chrono::steady_clock::duration::zero()) {
    // timerfd 的 0 会关闭定时器，立即到期必须使用正数。
    remaining = std::chrono::nanoseconds(1);
  }

  const auto seconds =
      std::chrono::duration_cast<std::chrono::seconds>(remaining);
  const auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          remaining - seconds);
  specification.it_value.tv_sec = seconds.count();
  specification.it_value.tv_nsec = nanoseconds.count();

  if (::timerfd_settime(timerfd_, 0, &specification, nullptr) < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "timerfd_settime");
  }
}

}  // namespace raftkv
