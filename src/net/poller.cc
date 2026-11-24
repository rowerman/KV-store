#include "raftkv/net/poller.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace raftkv {

Status SetNonBlocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return Status::IOError(std::strerror(errno));
  }
  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    return Status::IOError(std::strerror(errno));
  }
  return Status::OK();
}

Channel::Channel(EpollPoller* poller, int fd) : poller_(poller), fd_(fd) {}

Channel::~Channel() {
  try {
    Remove();
  } catch (...) {
    // 析构函数不能传播 Poller 删除错误。
  }
}

void Channel::SetReadCallback(EventCallback callback) {
  read_callback_ = std::move(callback);
}

void Channel::SetWriteCallback(EventCallback callback) {
  write_callback_ = std::move(callback);
}

void Channel::SetCloseCallback(EventCallback callback) {
  close_callback_ = std::move(callback);
}

void Channel::SetErrorCallback(EventCallback callback) {
  error_callback_ = std::move(callback);
}

void Channel::EnableReading() {
  events_ |= kReadEvent;
  Update();
}

void Channel::DisableReading() {
  events_ &= ~kReadEvent;
  Update();
}

void Channel::EnableWriting() {
  events_ |= kWriteEvent;
  Update();
}

void Channel::DisableWriting() {
  events_ &= ~kWriteEvent;
  Update();
}

void Channel::DisableAll() {
  events_ = kNoneEvent;
  Update();
}

void Channel::HandleEvent() {
  const uint32_t revents = revents_;
  revents_ = kNoneEvent;

  // 系统错误优先通知，随后仍分发可读写的有效事件。
  if ((revents & EPOLLERR) != 0 && error_callback_) {
    error_callback_();
  }
  if ((revents & kReadEvent) != 0 && read_callback_) {
    read_callback_();
  }
  if ((revents & kWriteEvent) != 0 && write_callback_) {
    write_callback_();
  }
  if ((revents & EPOLLHUP) != 0 && close_callback_) {
    close_callback_();
  }
}

void Channel::Remove() {
  if (poller_ != nullptr) {
    poller_->RemoveChannel(this);
  }
}

void Channel::Update() {
  if (poller_ != nullptr) {
    poller_->UpdateChannel(this);
  }
}

EpollPoller::EpollPoller()
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitialEventListSize) {
  if (epoll_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "epoll_create1");
  }
}

EpollPoller::~EpollPoller() {
  if (epoll_fd_ >= 0) {
    ::close(epoll_fd_);
  }
}

int EpollPoller::Poll(int timeout_ms,
                      std::vector<Channel*>* active_channels) {
  if (active_channels == nullptr) {
    throw std::invalid_argument("active_channels is null");
  }

  active_channels->clear();
  const int count =
      ::epoll_wait(epoll_fd_, events_.data(),
                   static_cast<int>(events_.size()), timeout_ms);
  if (count < 0) {
    // 信号中断由上层继续循环，不视为轮询失败。
    return errno == EINTR ? 0 : -1;
  }

  for (int i = 0; i < count; ++i) {
    const int fd = events_[static_cast<size_t>(i)].data.fd;
    const auto iterator = channels_.find(fd);
    if (iterator == channels_.end()) {
      // 忽略已经删除但仍在事件数组中的陈旧事件。
      continue;
    }
    Channel* channel = iterator->second;
    channel->revents_ = events_[static_cast<size_t>(i)].events;
    active_channels->push_back(channel);
  }

  if (count == static_cast<int>(events_.size())) {
    events_.resize(events_.size() * 2);
  }
  return static_cast<int>(active_channels->size());
}

void EpollPoller::UpdateChannel(Channel* channel) {
  if (channel == nullptr) {
    throw std::invalid_argument("channel is null");
  }

  const auto iterator = channels_.find(channel->fd_);
  const bool registered = iterator != channels_.end();

  if (!registered && channel->events_ == Channel::kNoneEvent) {
    channel->state_ = Channel::State::kNew;
    return;
  }

  struct epoll_event event {};
  event.events = channel->events_ | EPOLLET;
  event.data.fd = channel->fd_;

  int operation = EPOLL_CTL_MOD;
  if (!registered) {
    operation = EPOLL_CTL_ADD;
  } else if (channel->events_ == Channel::kNoneEvent) {
    operation = EPOLL_CTL_DEL;
  }

  if (::epoll_ctl(epoll_fd_, operation, channel->fd_, &event) < 0) {
    if (operation == EPOLL_CTL_DEL && errno == ENOENT) {
      channels_.erase(iterator);
      channel->state_ = Channel::State::kNew;
      return;
    }
    throw std::system_error(errno, std::generic_category(),
                            "epoll_ctl");
  }

  if (operation == EPOLL_CTL_DEL) {
    channels_.erase(iterator);
    channel->state_ = Channel::State::kNew;
  } else {
    if (!registered) {
      channels_.emplace(channel->fd_, channel);
    }
    channel->state_ = Channel::State::kAdded;
  }
}

void EpollPoller::RemoveChannel(Channel* channel) {
  if (channel == nullptr) {
    return;
  }

  const auto iterator = channels_.find(channel->fd_);
  if (iterator != channels_.end()) {
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, channel->fd_, nullptr) < 0 &&
        errno != ENOENT) {
      throw std::system_error(errno, std::generic_category(),
                              "epoll_ctl delete");
    }
    channels_.erase(iterator);
  }
  channel->state_ = Channel::State::kNew;
}

}  // namespace raftkv
