# net

负责 Linux 非阻塞网络基础设施，包括 `Buffer`、`Channel`、`EpollPoller`、
`EventLoop`、定时器、`TcpConnection`、`TcpServer` 和 `TcpClient`。

- 上游调用方：`rpc`、服务端入口程序。
- 下游依赖：Linux socket/epoll/timerfd/eventfd、`common`。
- 修改注意：统一使用 ET 模式并处理 `EAGAIN`；明确回调所在线程和对象生命周期。

## Buffer 约定

- `Buffer` 不提供内部同步，同一实例只能由所属事件循环线程访问。
- `Peek()` 返回的指针在下次追加、扩容或搬移后可能失效，调用方不能跨修改长期保存。
- 追加优先使用尾部空间，其次复用已消费的头部空间，空间仍不足时按需扩容。
- `ReadFd()` 返回实际读取字节数；错误时返回 `-1` 并保存 errno，
  `EAGAIN` 由上层 ET 事件循环判断。

## Channel 与 Poller 约定

- `Channel` 不拥有文件描述符；析构时只从 Poller 注销，不关闭 fd。
- Poller 必须比所有注册的 Channel 存活更久。
- `Poll()` 只填充带 `revents` 的活跃 Channel，随后由事件循环调用
  `HandleEvent()` 分发回调。
- Channel 回调在调用 `Poll()` 的 IO 线程同步执行，上层不能在其中运行长耗时任务。
- `EPOLLRDHUP` 会进入读回调；读操作返回 0 时必须由连接层执行关闭和清理。
- ET 模式下读回调必须持续读取直到 `EAGAIN`，否则未读数据可能不再触发事件。

## EventLoop 与 TimerQueue 约定

- 每个 `EventLoop` 只能由创建线程调用 `Loop()`，析构前必须确保循环已经退出。
- `QueueInLoop` 可从任意线程调用，任务通过 `eventfd` 唤醒后统一在循环线程执行。
- 循环线程中的任务和回调不得执行长耗时阻塞操作，否则会延迟其他网络事件和定时器。
- `TimerQueue` 允许其他线程添加或取消定时器，但定时器回调仍在 EventLoop 线程执行。
- `RunEvery` 的周期必须大于 0；周期触发不会补发线程阻塞期间积压的多个周期。
- 已经进入当前到期批次的回调无法被其他线程撤回，取消只保证后续周期不再执行。

