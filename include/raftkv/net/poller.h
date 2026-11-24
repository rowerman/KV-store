#ifndef RAFTKV_NET_POLLER_H_
#define RAFTKV_NET_POLLER_H_

#include <sys/epoll.h>

#include <unordered_map>
#include <vector>

#include "raftkv/common/status.h"
#include "raftkv/net/channel.h"

namespace raftkv {

// 将文件描述符设置为非阻塞；失败时返回带系统错误文本的状态。
Status SetNonBlocking(int fd);

// 基于 epoll ET 模式的事件轮询器。
class EpollPoller {
 public:
  EpollPoller();
  ~EpollPoller();

  EpollPoller(const EpollPoller&) = delete;
  EpollPoller& operator=(const EpollPoller&) = delete;

  // 等待事件并填充活跃 Channel；返回活跃数量，-1 表示轮询失败。
  int Poll(int timeout_ms, std::vector<Channel*>* active_channels);

  // 新增或修改 Channel 关注事件；空事件会删除注册。
  void UpdateChannel(Channel* channel);

  // 删除 Channel 注册。
  void RemoveChannel(Channel* channel);

 private:
  static constexpr int kInitialEventListSize = 128;

  int epoll_fd_;
  std::vector<struct epoll_event> events_;
  std::unordered_map<int, Channel*> channels_;
};

}  // namespace raftkv

#endif  // RAFTKV_NET_POLLER_H_

/*
理解了这套代码，你可以把网络事件的处理流程想象成以下步骤：

准备阶段：你创建了一个 Channel，绑定 socket fd，并使用 SetReadCallback 告诉它：“如果有数据来，请执行这个函数”。

注册阶段：你调用 EnableReading()，Channel 把 EPOLLIN 装进 events_，并让 EpollPoller 把这个 fd 和 events_ 注册到内核的 epoll 实例中。

等待阶段：主线程在 epoll_wait 上阻塞。

触发阶段：客户端发来数据，内核唤醒 epoll_wait，返回该 fd。Poll() 只返回活跃 Channel，随后由事件循环调用 HandleEvent()。

执行阶段：HandleEvent() 发现 revents_ 里有 EPOLLIN，于是立刻调用你之前设置的 read_callback_，你的业务代码开始读取数据。
*/
