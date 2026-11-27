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

## RaftNode Actor

- `RaftNode` 使用一个 `std::thread` 驱动 `MpscQueue<RaftEvent>`；网络、存储和上层
  调用只能投递事件，不能直接修改 Raft 核心字段。
- `RaftEvent` 覆盖 Tick、三类 RPC 请求/响应、Propose、ReadIndex 和 Stop。
- 生命周期为 Created、Running、Stopping、Stopped；Start 幂等，Stop 后不支持重启。
- 外部 Stop 通过 join 等待 Actor 退出；Actor 线程内 Stop 只投递停止事件，不 join
  自身。
- `RaftTransport` 的请求 Handler 和出站回包都通过共享 `ActorEventSink` 投递事件，
  不捕获 `RaftNode::Impl` 的 `this`。Actor 停止后新请求以 Timeout 完成，迟到回包
  被丢弃。
- Actor 使用 `steady_clock` 计算 Tick；事件积压时在事件批次内检查截止时间并补一次
  Tick，避免大量 Propose 延迟心跳，也不突发补发全部错过周期。
- `GetStatus` 读取 Actor 发布的原子镜像，不参与协议决策，也不给核心状态增加锁。
- 选举超时从 `[election_timeout_min_ms, election_timeout_max_ms]` 独立随机选择，
  默认 150-300ms；Leader 默认每 50ms 广播一次空 AppendEntries 心跳。
- 选举超时先进入 `kPreCandidate` 并发送 `is_pre_vote=true` 的 RequestVote，不修改
  term 或投票对象。获得多数派后才递增 term、持久化自投票并进入 `kCandidate`。
- 正式投票要求候选人日志在 `(last_log_term, last_log_index)` 字典序上不落后；
  term 和 `voted_for` 先写入 `RaftLog`，写盘成功后才响应。
- 最近收到合法 Leader 心跳的节点拒绝 Pre-Vote；正式投票拒绝不会重置接收方计时，
  避免多个 Candidate 被反复同步到下一轮。
- Leader 当选后先追加当前 term 的 No-Op，并初始化每个 Follower 的
  `next_index/match_index`。
- AppendEntries 每次最多携带 64 条日志；同一 peer 最多一个复制请求在途，成功后
  立即发送下一批。请求序号和 prev/entry count 随回包返回，迟到回包不会覆盖新进度。
- 复制请求在途时，Tick 仍可发送独立心跳，避免复制回包在事件队列中排队导致
  Follower 选举超时。
- Follower 返回 `conflict_term/conflict_index`；Leader 按冲突 term 整段回退，
  Follower 在首个不同 term 的 index 截断后修复日志。
- Leader 对自身 LastIndex 和各 peer match_index 排序，只有当前 term 的多数派 index
  才能推进 commit。Follower 使用 `min(leader_commit, matched_index)` 保守推进。
- `StateMachine::Apply` 在 Actor 线程按 log index 递增执行；No-Op 跳过业务 Apply。
- Leader 的 Propose 在日志提交并 Apply 后回调；失去 Leader 身份时 pending 回调
  返回 NotLeader。
- 任何更高 term 的请求或响应都会使 PreCandidate、Candidate 或 Leader 降级为
  Follower。
- ReadIndex 在 F4.6 实现前仍返回 NotLeader；Snapshot 与成员变更分别留给 F4.7、
  F4.8。

