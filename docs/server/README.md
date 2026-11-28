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

- `raftkv_server` 当前包含 `KVStateMachine`、`SessionTable` 和
  `RealRaftTransport`，公开链接 `raftkv_raft`、`raftkv_rpc`、`raftkv_storage`
  与 `raftkv_proto`。
- 修改 `KvCommand` 字段或 SessionTable 磁盘格式时，必须同时升级格式版本并验证旧
  快照拒绝或迁移行为。
- 修改状态机快照目录布局时，必须同步检查 Raft snapshot bundle、重复安装和本地
  崩溃恢复路径。

## RealRaftTransport

- 真实传输为每个 Peer 维护一个独立 `RpcClient` 长连接，Peer 之间互不共享在途
  请求和断线状态。
- 内部 Raft RPC 固定为 RequestVote 101、AppendEntries 102、InstallSnapshot 103。
- Peer 地址当前只支持数字 IPv4 和显式非 0 端口；Create 会拒绝零本地 ID、零超时、
  重复 ID、自引用 Peer 和非法地址。
- Create 只建立配置与 RpcClient，不发起连接；RegisterRpcMethods 必须在 Start 前
  调用，同一 Transport 只允许成功注册一次。
- Start 异步连接全部 Peer。连接建立前的请求返回 Timeout，由 Raft 按既有协议重试。
- Stop 幂等断开全部 Peer，并让该连接上的挂起调用通过 Timeout 完成。
- 出站请求中的 `candidate_id/leader_id` 是本地发送节点，必须等于配置的
  `local_node_id`；目标 Peer 只由 `Send*` 的 `peer_id` 参数选择。
- 入站 RPC 从 `candidate_id/leader_id` 提取来源节点，再转交 RaftTransport Handler。
  Handler 只向 Raft Actor 投递事件，网络线程不直接修改 Raft 状态。
- Handler 在锁内复制、锁外执行，支持 RaftNode 启动阶段安装和替换。
- RpcServer 注册的处理器捕获 Transport Impl 的弱引用。Transport 销毁后收到的请求
  返回 Timeout，不会访问悬空对象。
- 响应必须为对应方法 ID 的 `kResponse` 帧并能解析成目标 Protobuf；否则返回
  Corruption，不能把错误帧当作默认成功响应。
- 未配置 Peer 返回 NotFound；发送方 ID 错误返回 InvalidArgument；RPC 连接未建立、
  断开或超时返回 Timeout。
- EventLoop 必须在 RpcClient 完成断开前存活；RpcServer 应在 Transport 销毁前停止，
  避免继续分发新的内部 Raft 请求。

## KVServer 全链路

- `KVServer` 独占 RpcServer、LSMTree 和 RaftNode，借用调用方创建的 EventLoop。
  RaftNode 和 KVStateMachine 必须先于 LSMTree 销毁。
- 客户端 RPC 固定为 Put 201、Delete 202、Get 203、Scan 204。
- 写请求在 RPC IO 线程完成解码和 client_id/seq_num 校验，编码为 KvCommand 后投递
  ProposeEvent。只有日志提交并 Apply 后才返回业务成功。
- 重复写使用相同 client_id/seq_num 会重复形成日志，但 SessionTable 会跳过第二次
  存储写入，因此客户端超时重试不会覆盖第一次结果。
- `Get(linearizable=true)` 和全部 Scan 先投递 ReadIndexEvent。确认多数派且
  last_applied 追平后，ReadIndex 回调把 LSM 查询投递回 EventLoop，避免阻塞 Raft
  Actor。
- `Get(linearizable=false)` 在 RPC IO 线程直接读取本地 LSM，可能读到 Follower 的
  旧状态。
- Scan 使用半开区间 `[start_key, end_key)`；limit 必须为 1-10000，通过读取
  limit+1 条计算 has_more。
- 业务结果始终通过正常 RPC 响应中的 StatusCode 返回；只有请求 Protobuf 损坏时
  才使用帧级 InvalidArgument。
- NotLeader 响应根据 RaftNodeStatus.leader_id 填写 `leader_hint=host:port`；未知
  Leader 或地址未配置时留空。
- Create 按 LSM、KVStateMachine、RaftLog、RpcServer、RealRaftTransport、RaftNode
  顺序组装。Start 先启动 RpcServer，再启动 RaftNode；Stop 先停 RaftNode，再停
  RpcServer。
- Stop 不持生命周期锁等待 Raft Actor，使用 stopping/stopped 和条件变量支持并发、
  重复调用。
- RpcServer 的四类客户端 Handler 捕获 Impl 弱引用，KVServer 销毁后不会访问悬空
  状态。

## 服务端主程序

- 可执行目标为 `raftkv_server`，静态库目标为 `raftkv_server_lib`。
- 必填参数：`--id`、`--addr`、`--peers`、`--data-dir`。
- 可选参数：`--listen-ip`、`--rpc-io-threads`、`--help`。
- `--peers` 必须包含本节点；命令行中本地 ID 对应的 Host/Port 可由 `--addr` 覆盖，
  其余节点作为远程 Raft Peers。
- 进程在创建 Raft/LSM 线程前阻塞 SIGINT/SIGTERM。独立线程通过 sigwait 接收信号并
  Quit EventLoop，随后先停 Raft、再停 RPC 服务并恢复信号掩码。
- 修改启动顺序时，必须保持 EventLoop、RpcServer、RaftNode 和 LSMTree 的生命周期
  约束；不得在网络线程直接修改 Raft 状态或写 LSM。

