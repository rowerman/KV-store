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

## RaftTransport 与 SimTransport

- `RaftTransport` 同时定义 RequestVote、AppendEntries、InstallSnapshot 的发送方法
  和对应接收 Handler，真实 RPC 与测试网络共享同一接口。
- 发送回调参数中的 `Status` 只表示传输结果；Raft 投票拒绝、日志冲突等协议结果仍以
  `Status::OK()` 和响应中的 `success`/`vote_granted` 字段表示。
- 请求 Handler 显式接收来源节点编号，响应回调最多生效一次。Handler 可以同步或
  异步调用 `done`。
- `SimTransport` 位于 `tests/chaos/`，只用于测试。它不做生产构建依赖，后续
  `RealRaftTransport` 才连接 Phase 2 的真实 RPC。
- `SimNetwork` 使用虚拟毫秒时钟和按 `(timestamp, sequence)` 排序的事件队列；
  `AdvanceBy` 在调用线程串行执行消息，不创建后台线程。
- 延迟在 `[min_latency, max_latency]` 内随机选择；固定随机种子下投递顺序可重复。
- 丢包、节点宕机、双向分区和单向断链都在发送时判断。被接受的消息后续不会因故障
  状态变化而撤销。
- 请求到达 Handler 后，响应作为独立网络消息再次执行延迟、丢包和分区判断。
- `Partition` 只拦截两个不相交节点组之间的双向消息；`Heal` 清除分区和有向断链，
  但不恢复 `DropNode` 的节点。
- `SimNetwork` 的注册表、故障状态和事件队列由同一 mutex 保护，支持多个发送线程；
  投递和响应回调只在调用 `AdvanceBy` 的线程执行。

