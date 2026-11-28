# server

负责组装网络 RPC、Raft、存储引擎和状态机，实现客户端 KV 请求处理、
Leader 路由提示、幂等去重及节点生命周期管理。

- 上游调用方：`raftkv_server` 入口程序和 RPC 方法分发。
- 下游依赖：`rpc`、`raft`、`storage`、`common`。
- 修改注意：网络线程不能直接修改 Raft 状态；写请求必须经共识提交后 Apply；
  ReadIndex 未确认前不得返回线性一致读结果。

## SessionTable 与 KVStateMachine

- `KvCommand` 是 Normal Raft 日志的业务 payload，统一承载 Put/Delete、`client_id`、
  `seq_num`、Key 和 Value。客户端 RPC 消息只用于网络边界，不直接作为状态机命令。
- `KVStateMachine` 借用 `StorageEngine`，不拥有 LSM 生命周期。Server 必须保证存储
  对象晚于状态机和 RaftNode 销毁。
- Apply 严格按 `(client_id, seq_num)` 去重。首次请求先执行 Put/Delete，存储成功后
  才推进该客户端的最大序号；已执行及更小序号直接返回成功，不再次访问存储。
- 非法 protobuf、未知命令类型、零客户端 ID、零序号和携带 Value 的 Delete 均返回
  错误。Raft Actor 遇到 Apply 错误后停止，不能跳过日志继续推进。
- SessionTable 快照文件依次包含 `RFTSESS1` magic、版本、记录数量、按客户端 ID
  升序排列的 `(client_id, seq_num)` 和整体 CRC32。
- SessionTable 保存先编码完整文件，再写同目录临时文件；文件 `fsync` 后原子 rename，
  最后同步父目录。加载拒绝符号链接、截断、CRC 错误、重复客户端和尾随数据。
- 加载 SessionTable 时先解码到临时对象，全部校验成功后才替换当前状态，失败不污染
  已有去重水位。
- 状态机快照目录固定为 `lsm/` 和 `session_table.bin`。Raft 负责创建空状态目录，
  `TakeSnapshot` 先写 LSM Checkpoint，再写 SessionTable。
- `LSMTree::RestoreFromCheckpoint` 会消费源目录，因此 LoadSnapshot 先硬链接复制
  快照中的 `lsm/`，再恢复临时副本。源快照保持完整，可以重复加载。
- LoadSnapshot 先解码 SessionTable，再进行 LSM 恢复；任一前置步骤失败时，当前 LSM
  和 SessionTable 都不发布新状态。
- SessionTable 只在 Raft Actor 线程访问，不增加内部锁。多线程读路径通过
  `StorageEngine` 自身的并发控制访问 LSM。
- F5.1 不处理网络响应回调。写请求在 Raft Propose 完成回调后，由 F5.3 的 Server
  生成响应并投递回对应 RPC 线程。

## 构建

- `raftkv_server` 当前包含 `KVStateMachine` 和 `SessionTable`，公开链接
  `raftkv_raft`、`raftkv_storage` 与 `raftkv_proto`。
- 修改 `KvCommand` 字段或 SessionTable 磁盘格式时，必须同时升级格式版本并验证旧
  快照拒绝或迁移行为。
- 修改状态机快照目录布局时，必须同步检查 Raft snapshot bundle、重复安装和本地
  崩溃恢复路径。

