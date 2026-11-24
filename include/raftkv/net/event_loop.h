#ifndef RAFTKV_NET_EVENT_LOOP_H_
#define RAFTKV_NET_EVENT_LOOP_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "raftkv/net/channel.h"
#include "raftkv/net/poller.h"
#include "raftkv/net/timer_queue.h"

namespace raftkv {

// 绑定单线程的 epoll 事件循环。
// 网络事件、跨线程任务和定时器回调都在创建该对象的线程执行。
class EventLoop {
 public:
  using Functor = std::function<void()>;

  EventLoop();

  // 必须在 Loop 已退出且独占该 EventLoop 的线程中析构。
  ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // 在当前线程运行事件循环，直到 Quit。
  void Loop();

  // 请求事件循环退出。
  void Quit();

  // 当前线程是否为 EventLoop 所属线程。
  bool IsInLoopThread() const;

  // 在循环线程执行；其他线程调用时自动转交 QueueInLoop。
  void RunInLoop(Functor callback);

  // 将任务加入循环线程队列并唤醒循环。
  void QueueInLoop(Functor callback);

  // 在 delay_ms 后执行一次。
  TimerId RunAfter(uint64_t delay_ms, TimerCallback callback);

  // 每隔 interval_ms 执行一次。
  TimerId RunEvery(uint64_t interval_ms, TimerCallback callback);

  // 取消尚未执行的定时器。
  bool CancelTimer(TimerId id);

 private:
  // 写 eventfd，唤醒阻塞中的 epoll_wait。
  void Wakeup();
  // 清空 eventfd 计数。
  void HandleWakeup();
  // 在循环线程执行全部待处理任务。
  void DoPendingFunctors();

  // Poller 必须先于 Channel 和 TimerQueue 析构。
  EpollPoller poller_;
  // eventfd 文件描述符。
  int wakeup_fd_;
  // EventLoop 所属线程。
  std::thread::id thread_id_;
  // 可被其他线程设置的退出标志。
  std::atomic<bool> quit_;
  // 保护跨线程任务队列。
  std::mutex pending_mutex_;
  // 等待在循环线程执行的任务。
  std::vector<Functor> pending_functors_;
  // 包装 wakeup_fd 的 Channel。
  std::unique_ptr<Channel> wakeup_channel_;
  // 定时器在 EventLoop 线程触发。
  TimerQueue timer_queue_;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_EVENT_LOOP_H_
