# raft

负责单线程 Actor 状态机、持久化日志、Leader 选举、日志复制、ReadIndex、
快照和单步成员变更。

- 上游调用方：`server`。
- 下游依赖：`common`、`RaftTransport` 抽象、`StateMachine` 抽象。
- 修改注意：Raft 核心状态只能由 Actor 线程修改；网络与磁盘操作只投递事件或回调；
  任何持久化状态更新都必须先满足 Raft 安全规则。

