#include "raftkv/net/channel.h"
#include "raftkv/net/poller.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <set>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace raftkv {
namespace {

class ScopedFd {
 public:
  explicit ScopedFd(int fd = -1) : fd_(fd) {}

  ~ScopedFd() { reset(); }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  int get() const { return fd_; }

  void reset(int fd = -1) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_;
};

class FdPair {
 public:
  explicit FdPair(bool use_socket_pair = false) {
    int fds[2] = {-1, -1};
    const int result =
        use_socket_pair
            ? ::socketpair(AF_UNIX, SOCK_STREAM, 0, fds)
            : ::pipe(fds);
    if (result == 0) {
      first_.reset(fds[0]);
      second_.reset(fds[1]);
    }
  }

  bool valid() const {
    return first_.get() >= 0 && second_.get() >= 0;
  }

  int first() const { return first_.get(); }
  int second() const { return second_.get(); }

  void close_first() { first_.reset(); }
  void close_second() { second_.reset(); }

 private:
  ScopedFd first_;
  ScopedFd second_;
};

TEST(PollerTest, SetNonBlockingUpdatesFlagsAndReportsInvalidFd) {
  FdPair pair;
  ASSERT_TRUE(pair.valid());

  const Status status = SetNonBlocking(pair.first());
  ASSERT_TRUE(status.ok()) << status.ToString();

  const int flags = ::fcntl(pair.first(), F_GETFL, 0);
  ASSERT_GE(flags, 0);
  EXPECT_NE(flags & O_NONBLOCK, 0);

  const Status invalid_status = SetNonBlocking(-1);
  EXPECT_EQ(invalid_status.code(), Code::kIOError);
  EXPECT_FALSE(invalid_status.message().empty());
}

TEST(PollerTest, PipeReadChannelDispatchesReadCallback) {
  EpollPoller poller;
  FdPair pair;
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());
  int callbacks = 0;
  channel.SetReadCallback([&callbacks] { ++callbacks; });
  channel.EnableReading();

  ASSERT_EQ(::write(pair.second(), "x", 1), 1);
  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);
  ASSERT_EQ(active.front(), &channel);
  EXPECT_NE(channel.revents() & (EPOLLIN | EPOLLPRI), 0U);

  channel.HandleEvent();
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(channel.revents(), Channel::kNoneEvent);
}

TEST(PollerTest, EdgeTriggerDoesNotRepeatUnreadData) {
  EpollPoller poller;
  FdPair pair;
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());
  channel.SetReadCallback([] {});
  channel.EnableReading();

  ASSERT_EQ(::write(pair.second(), "abc", 3), 3);
  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);

  char byte = '\0';
  ASSERT_EQ(::read(pair.first(), &byte, 1), 1);
  EXPECT_EQ(poller.Poll(0, &active), 0);

  ASSERT_EQ(::write(pair.second(), "de", 2), 2);
  EXPECT_EQ(poller.Poll(100, &active), 1);
}

TEST(PollerTest, SocketWritableChannelDispatchesWriteCallback) {
  EpollPoller poller;
  FdPair pair(true);
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());
  int callbacks = 0;
  channel.SetWriteCallback([&callbacks] { ++callbacks; });
  channel.EnableWriting();

  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);
  ASSERT_EQ(active.front(), &channel);
  EXPECT_NE(channel.revents() & EPOLLOUT, 0U);

  channel.HandleEvent();
  EXPECT_EQ(callbacks, 1);
}

TEST(PollerTest, PeerCloseDispatchesCloseCallback) {
  EpollPoller poller;
  FdPair pair;
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());
  int close_callbacks = 0;
  channel.SetCloseCallback([&close_callbacks] { ++close_callbacks; });
  channel.EnableReading();

  pair.close_second();
  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);
  ASSERT_EQ(active.front(), &channel);

  channel.HandleEvent();
  EXPECT_EQ(close_callbacks, 1);
}

TEST(PollerTest, PeerShutdownDispatchesReadCallbackWithEof) {
  EpollPoller poller;
  FdPair pair(true);
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());
  int read_callbacks = 0;
  bool reached_eof = false;
  channel.SetReadCallback([&] {
    ++read_callbacks;
    char byte = '\0';
    reached_eof = ::read(pair.first(), &byte, 1) == 0;
  });
  channel.EnableReading();

  ASSERT_EQ(::shutdown(pair.second(), SHUT_WR), 0);
  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);
  ASSERT_EQ(active.front(), &channel);
  EXPECT_NE(channel.revents() & EPOLLRDHUP, 0U);

  channel.HandleEvent();
  EXPECT_EQ(read_callbacks, 1);
  EXPECT_TRUE(reached_eof);
}

TEST(PollerTest, ClosedPeerDispatchesErrorCallback) {
  EpollPoller poller;
  FdPair pair;
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.second());
  int error_callbacks = 0;
  channel.SetErrorCallback([&error_callbacks] { ++error_callbacks; });
  channel.EnableWriting();

  pair.close_first();
  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);
  ASSERT_EQ(active.front(), &channel);
  EXPECT_NE(channel.revents() & EPOLLERR, 0U);

  channel.HandleEvent();
  EXPECT_EQ(error_callbacks, 1);
}

TEST(PollerTest, DisableAllRemovesKernelRegistration) {
  EpollPoller poller;
  FdPair pair;
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());
  channel.SetReadCallback([] {});
  channel.EnableReading();
  ASSERT_TRUE(channel.IsReading());

  channel.DisableAll();
  EXPECT_FALSE(channel.IsReading());
  EXPECT_FALSE(channel.IsWriting());
  EXPECT_EQ(channel.events(), Channel::kNoneEvent);

  ASSERT_EQ(::write(pair.second(), "x", 1), 1);
  std::vector<Channel*> active;
  EXPECT_EQ(poller.Poll(50, &active), 0);
  EXPECT_TRUE(active.empty());
}

TEST(PollerTest, EnableDisableReadAndWriteUpdateEvents) {
  EpollPoller poller;
  FdPair pair(true);
  ASSERT_TRUE(pair.valid());
  Channel channel(&poller, pair.first());

  channel.EnableReading();
  channel.EnableWriting();
  EXPECT_TRUE(channel.IsReading());
  EXPECT_TRUE(channel.IsWriting());
  EXPECT_NE(channel.events() & Channel::kReadEvent, 0U);
  EXPECT_NE(channel.events() & Channel::kWriteEvent, 0U);

  channel.DisableReading();
  EXPECT_FALSE(channel.IsReading());
  EXPECT_TRUE(channel.IsWriting());
  channel.DisableWriting();
  EXPECT_FALSE(channel.IsWriting());
  EXPECT_EQ(channel.events(), Channel::kNoneEvent);
}

TEST(PollerTest, RemoveUnregistersWithoutClosingFd) {
  EpollPoller poller;
  FdPair pair;
  ASSERT_TRUE(pair.valid());

  {
    Channel channel(&poller, pair.first());
    channel.EnableReading();
    channel.Remove();
  }

  Channel reused(&poller, pair.first());
  int callbacks = 0;
  reused.SetReadCallback([&callbacks] { ++callbacks; });
  reused.EnableReading();
  ASSERT_EQ(::write(pair.second(), "x", 1), 1);

  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 1);
  active.front()->HandleEvent();
  EXPECT_EQ(callbacks, 1);
}

TEST(PollerTest, MultipleChannelsAreReportedTogether) {
  EpollPoller poller;
  FdPair first_pair;
  FdPair second_pair;
  ASSERT_TRUE(first_pair.valid());
  ASSERT_TRUE(second_pair.valid());
  Channel first(&poller, first_pair.first());
  Channel second(&poller, second_pair.first());
  int first_callbacks = 0;
  int second_callbacks = 0;
  first.SetReadCallback([&first_callbacks] { ++first_callbacks; });
  second.SetReadCallback([&second_callbacks] { ++second_callbacks; });
  first.EnableReading();
  second.EnableReading();

  ASSERT_EQ(::write(first_pair.second(), "1", 1), 1);
  ASSERT_EQ(::write(second_pair.second(), "2", 1), 1);

  std::vector<Channel*> active;
  ASSERT_EQ(poller.Poll(100, &active), 2);
  std::set<int> active_fds;
  for (Channel* channel : active) {
    active_fds.insert(channel->fd());
    channel->HandleEvent();
  }
  EXPECT_EQ(active_fds,
            std::set<int>({first_pair.first(), second_pair.first()}));
  EXPECT_EQ(first_callbacks, 1);
  EXPECT_EQ(second_callbacks, 1);
}

TEST(PollerTest, PollTimeoutClearsCallerOutput) {
  EpollPoller poller;
  std::vector<Channel*> active(1, nullptr);

  EXPECT_EQ(poller.Poll(10, &active), 0);
  EXPECT_TRUE(active.empty());
  EXPECT_THROW(poller.Poll(0, nullptr), std::invalid_argument);
  EXPECT_THROW(poller.UpdateChannel(nullptr), std::invalid_argument);
  EXPECT_NO_THROW(poller.RemoveChannel(nullptr));
}

}  // namespace
}  // namespace raftkv
