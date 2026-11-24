# RaftKV 模块文档

| 模块 | 职责 | 主要上游 | 主要下游 |
| :--- | :--- | :--- | :--- |
| `common` | 错误状态、字节视图、编解码、校验与日志基础设施 | 所有模块 | C++ 标准库、OS |
| `net` | epoll、事件循环、缓冲区与 TCP 连接管理 | `rpc`、`server` | Linux 系统调用、`common` |
| `rpc` | 帧协议、方法路由、异步请求匹配与超时 | `server`、`raft`、`client` | `net`、`common`、Protobuf |
| `storage` | LSM-Tree、WAL、SSTable、快照与迭代器 | Raft Apply、`server` | `common`、文件系统 |
| `raft` | 选举、日志复制、ReadIndex、快照和成员变更 | `server` | `common`、传输及状态机抽象 |
| `server` | 客户端请求编排、幂等去重和 Raft 生命周期 | `rpc`、服务端入口程序 | `raft`、`storage`、`rpc` |
| `client` | 客户端 SDK、Leader 路由与重试 | 外部调用方、CLI | `rpc`、Protobuf |

各模块的边界和修改注意事项见对应目录中的文档。模块之间只通过公开头文件和设计文档
定义的抽象接口交互，禁止直接依赖另一模块的内部实现。

