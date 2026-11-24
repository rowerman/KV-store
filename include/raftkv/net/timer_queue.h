#ifndef RAFTKV_NET_TIMER_QUEUE_H_
#define RAFTKV_NET_TIMER_QUEUE_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include "raftkv/net/channel.h"

namespace raftkv {

class EpollPoller;

using TimerId = uint64_t;
using TimerCallback = std::function<void()>;

// 基于 timerfd 和最小堆的线程安全定时器队列。
// 任意线程可添加或取消定时器，但回调只在注册的 EventLoop 线程执行。
class TimerQueue {
 public:
  // 将 timerfd 注册到指定 Poller。
  explicit TimerQueue(EpollPoller* poller);

  // 注销 timerfd 并关闭文件描述符。
  ~TimerQueue();

  TimerQueue(const TimerQueue&) = delete;
  TimerQueue& operator=(const TimerQueue&) = delete;

  // 在 delay_ms 后执行一次；返回 0 表示无效。
  TimerId RunAfter(uint64_t delay_ms, TimerCallback callback);

  // 每隔 interval_ms 执行；interval_ms 为 0 时返回无效 ID。
  TimerId RunEvery(uint64_t interval_ms, TimerCallback callback);

  // 取消尚未执行的定时器；返回是否取消成功。
  bool CancelTimer(TimerId id);

 private:
  // 最小堆中的单个定时任务。
  struct TimerEntry {
    // 下一次到期时间。
    std::chrono::steady_clock::time_point expiration;
    // 用于取消和排序的稳定 ID。
    TimerId id;
    // 0 表示一次性定时器，大于 0 表示周期间隔。
    uint64_t interval_ms;
    // 到期后在 EventLoop 线程执行。
    TimerCallback callback;

    // 先按到期时间排序，同一时间按 ID 排序。
    bool operator>(const TimerEntry& other) const {
      if (expiration != other.expiration) {
        return expiration > other.expiration;
      }
      return id > other.id;
    }
  };

  // std::greater 将 priority_queue 变成最小堆。
  using TimerHeap = std::priority_queue<
      TimerEntry, std::vector<TimerEntry>,
      std::greater<TimerEntry>>;

  // 在锁内插入新定时器并重新设置 timerfd。
  TimerId AddTimer(uint64_t delay_ms, uint64_t interval_ms,
                   TimerCallback callback);
  // 处理 timerfd 到期并收集待执行回调。
  void HandleRead();
  // 将 timerfd 设置为当前最早到期任务；调用方必须持有 mutex_。
  void ResetTimerLocked();

  // 非阻塞 timerfd，所有到期事件汇合到该 fd。
  int timerfd_;
  // 将 timerfd 接入 EventLoop 的 Poller。
  Channel timer_channel_;
  // 保护堆、ID 集合和下一次 timerfd 设置。
  std::mutex mutex_;
  // 按到期时间排序的定时任务。
  TimerHeap timers_;
  // 当前有效、尚未触发完成的 ID。
  std::unordered_set<TimerId> active_ids_;
  // 已取消但尚未从堆中弹出的 ID。
  std::unordered_set<TimerId> cancelled_ids_;
  // 0 保留为无效 ID。
  TimerId next_id_ = 1;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_TIMER_QUEUE_H_
