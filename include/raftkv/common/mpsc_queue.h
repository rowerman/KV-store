#ifndef RAFTKV_COMMON_MPSC_QUEUE_H_
#define RAFTKV_COMMON_MPSC_QUEUE_H_

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <utility>
#include <vector>

namespace raftkv {

// 多生产者、单消费者阻塞队列。
template <typename T>
class MpscQueue {
 public:
  MpscQueue() = default;
  MpscQueue(const MpscQueue&) = delete;             // 禁止拷贝构造
  MpscQueue& operator=(const MpscQueue&) = delete;  // 禁止拷贝赋值

  // 入队一个元素并唤醒消费者。
  void Push(T value) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      queue_.push_back(std::move(value));
    }
    condition_.notify_one();
  }

  // 等待并取出当前全部元素；返回 false 表示超时。
  bool PopBatch(std::vector<T>& output, uint64_t timeout_ms) {
    output.clear();
    std::unique_lock<std::mutex> lock(mutex_);

    if (queue_.empty()) {
      const uint64_t max_timeout = static_cast<uint64_t>(
          std::chrono::milliseconds::max().count());
      const auto wait_time =
          timeout_ms > max_timeout
              ? std::chrono::milliseconds::max()
              : std::chrono::milliseconds(
                    static_cast<std::chrono::milliseconds::rep>(timeout_ms));
      const bool ready = condition_.wait_for(
          lock, wait_time, [this] { return !queue_.empty(); });
      if (!ready) {
        return false;
      }
    }

    output.reserve(output.size() + queue_.size());
    while (!queue_.empty()) {
      output.push_back(std::move(queue_.front()));
      queue_.pop_front();
    }
    return true;
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<T> queue_;
};

}  // namespace raftkv

#endif  // RAFTKV_COMMON_MPSC_QUEUE_H_
