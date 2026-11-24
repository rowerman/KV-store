# net

负责 Linux 非阻塞网络基础设施，包括 `Buffer`、`Channel`、`EpollPoller`、
`EventLoop`、定时器、`TcpConnection`、`TcpServer` 和 `TcpClient`。

- 上游调用方：`rpc`、服务端入口程序。
- 下游依赖：Linux socket/epoll/timerfd/eventfd、`common`。
- 修改注意：统一使用 ET 模式并处理 `EAGAIN`；明确回调所在线程和对象生命周期。

