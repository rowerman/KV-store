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

