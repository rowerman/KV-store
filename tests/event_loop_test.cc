#include "raftkv/net/event_loop.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

namespace raftkv {
namespace {

using namespace std::chrono_literals;

TEST(EventLoopTest, IsInLoopThreadMatchesCreator) {
  EventLoop loop;
  EXPECT_TRUE(loop.IsInLoopThread());

  std::atomic<bool> in_loop_thread{true};
  std::thread other([&loop, &in_loop_thread] {
    in_loop_thread = loop.IsInLoopThread();
  });
  other.join();

  EXPECT_FALSE(in_loop_thread.load());
}

TEST(EventLoopTest, RunInLoopExecutesDirectlyInLoopThread) {
  EventLoop loop;
  bool executed = false;
  bool correct_thread = false;

  loop.RunInLoop([&] {
    executed = true;
    correct_thread = loop.IsInLoopThread();
  });

  EXPECT_TRUE(executed);
  EXPECT_TRUE(correct_thread);
}

TEST(EventLoopTest, LoopRejectsOtherThread) {
  EventLoop loop;
  std::atomic<bool> rejected{false};
  std::thread other([&loop, &rejected] {
    try {
      loop.Loop();
    } catch (const std::logic_error&) {
      rejected = true;
    }
  });
  other.join();

  EXPECT_TRUE(rejected.load());
}

TEST(EventLoopTest, QueueInLoopWakesLoopAndRunsInOwnerThread) {
  EventLoop loop;
  const std::thread::id owner = std::this_thread::get_id();
  std::promise<void> initial_task_ran;
  std::future<void> initial_ready = initial_task_ran.get_future();
  std::promise<std::thread::id> callback_thread;
  std::future<std::thread::id> callback_result =
      callback_thread.get_future();

  std::thread producer([&] {
    loop.QueueInLoop([&] { initial_task_ran.set_value(); });
    initial_ready.wait();
    std::this_thread::sleep_for(20ms);
    loop.QueueInLoop([&] {
      callback_thread.set_value(std::this_thread::get_id());
      loop.Quit();
    });
  });

  loop.Loop();
  producer.join();

  EXPECT_EQ(callback_result.get(), owner);
}

TEST(EventLoopTest, ExternalQuitWakesBlockedEpollWait) {
  EventLoop loop;
  const auto start = std::chrono::steady_clock::now();
  std::thread quitter([&loop] {
    std::this_thread::sleep_for(30ms);
    loop.Quit();
  });

  loop.Loop();
  quitter.join();

  EXPECT_LT(std::chrono::steady_clock::now() - start,
            std::chrono::seconds(1));
}

TEST(EventLoopTest, RunAfterRunsInChildLoopThread) {
  std::promise<EventLoop*> loop_ready;
  std::future<EventLoop*> loop_result = loop_ready.get_future();
  std::promise<std::thread::id> callback_thread;
  std::future<std::thread::id> callback_result =
      callback_thread.get_future();
  const std::thread::id main_thread = std::this_thread::get_id();
  bool callback_in_loop = false;

  std::thread loop_thread([&] {
    EventLoop loop;
    loop_ready.set_value(&loop);
    loop.Loop();
  });

  EventLoop* loop = loop_result.get();
  loop->RunAfter(30, [&] {
    callback_in_loop = loop->IsInLoopThread();
    callback_thread.set_value(std::this_thread::get_id());
    loop->Quit();
  });

  if (callback_result.wait_for(2s) != std::future_status::ready) {
    loop->Quit();
  }
  loop_thread.join();

  ASSERT_EQ(callback_result.wait_for(0s), std::future_status::ready);
  EXPECT_TRUE(callback_in_loop);
  EXPECT_NE(callback_result.get(), main_thread);
}

TEST(EventLoopTest, RunAfterUsesRequestedDelay) {
  EventLoop loop;
  const auto start = std::chrono::steady_clock::now();
  std::chrono::steady_clock::duration elapsed;

  loop.RunAfter(50, [&] {
    elapsed = std::chrono::steady_clock::now() - start;
    loop.Quit();
  });
  loop.Loop();

  EXPECT_GE(elapsed, 40ms);
  EXPECT_LT(elapsed, 500ms);
}

TEST(EventLoopTest, CancelTimerPreventsOneShotCallback) {
  EventLoop loop;
  bool fired = false;
  const TimerId timer =
      loop.RunAfter(80, [&fired] { fired = true; });

  EXPECT_TRUE(loop.CancelTimer(timer));
  EXPECT_FALSE(loop.CancelTimer(timer));
  EXPECT_FALSE(loop.CancelTimer(0));

  loop.RunAfter(150, [&loop] { loop.Quit(); });
  loop.Loop();
  EXPECT_FALSE(fired);
}

TEST(EventLoopTest, RunEveryCanCancelItself) {
  EventLoop loop;
  int calls = 0;
  TimerId timer = 0;

  timer = loop.RunEvery(20, [&] {
    ++calls;
    if (calls == 3) {
      EXPECT_TRUE(loop.CancelTimer(timer));
      loop.Quit();
    }
  });

  EXPECT_NE(timer, 0U);
  EXPECT_EQ(loop.RunEvery(0, [] {}), 0U);
  loop.Loop();
  EXPECT_EQ(calls, 3);
}

TEST(EventLoopTest, TimersFireInDeadlineOrder) {
  EventLoop loop;
  std::vector<char> order;

  loop.RunAfter(30, [&order] { order.push_back('A'); });
  loop.RunAfter(10, [&order] { order.push_back('B'); });
  loop.RunAfter(20, [&order] { order.push_back('C'); });
  loop.RunAfter(50, [&loop] { loop.Quit(); });

  loop.Loop();
  EXPECT_EQ(order, std::vector<char>({'B', 'C', 'A'}));
}

}  // namespace
}  // namespace raftkv
