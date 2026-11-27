# raft

负责单线程 Actor 状态机、持久化日志、Leader 选举、日志复制、ReadIndex、
快照和单步成员变更。

- 上游调用方：`server`。
- 下游依赖：`common`、`RaftTransport` 抽象、`StateMachine` 抽象。
- 修改注意：Raft 核心状态只能由 Actor 线程修改；网络与磁盘操作只投递事件或回调；
  任何持久化状态更新都必须先满足 Raft 安全规则。

## RaftLog 与持久化格式

- `RaftLog` 持久化 `currentTerm`、`votedFor`、快照边界和 Protobuf `LogEntry`，
  不创建内部线程，仅供后续单线程 `RaftNode` Actor 调用。
- 上游调用方是后续 `RaftNode`；`LogEntry` 直接复用 `raftkv_proto` 中的定义，
  不维护额外的转换类型。
- 元数据保存在 `raft_meta.bin`，日志按 `RAFT-LOG-XXXXXX.wal` 分段。
- 元数据使用固定长度小端布局和 CRC32，通过临时文件写入、文件 fsync、rename 和
  目录 fsync 原子替换。
- 日志记录依次保存 `payload_size + CRC32 + term + index + entry_type + data_size
  + data`。完整记录 CRC 错误返回 `Corruption`，仅最后一个段允许裁剪未写完的尾部。
- `Append` 在修改内存索引前完成全部记录校验、写入和 fsync；单批记录可以跨段，
  失败时回滚已触及文件，无法回滚则对象必须关闭重开。
- 内存索引只保存快照之后的 `(term, segment_slot, offset, record_size)`；
  `EntryAt` 和 `TermAt` 使用 `index - FirstIndex()` 直接定位，时间复杂度为 O(1)。
- `TruncateSuffix` 先删除后继整段，再截断边界段，使每次崩溃后都只留下连续日志前缀。
- `CompactPrefix` 先原子提交新快照边界，再清理完全被覆盖的旧段；边界所在段可以
  保留旧记录，恢复时通过快照 Index 忽略。
- `SaveHardState` 拒绝 term 回退和零 term 携带投票，成功返回前硬状态已经持久化。
- 同一目录只允许一个 `RaftLog` 实例打开；当前 `Append` 每次独立 fsync，
  group commit 留给后续 Actor 批量调度。

