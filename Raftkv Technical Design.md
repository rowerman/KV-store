# 基于 Raft 与 LSM-Tree 的高可用分布式 KV 存储系统技术设计文档

## 1. 项目概述与设计目标

本项目旨在使用现代 C++（推荐 C++17）从零构建一个对标成熟分布式 KV 存储（如 etcd 核心架构）的高可用、强一致分布式键值存储系统（下文简称 **RaftKV**）。项目完整贯通**高性能网络编程、多线程并发调度、分布式共识算法、持久化存储引擎**四大后端基础架构核心领域，不依赖现成 RPC 框架或嵌入式 KV 引擎。

### 1.1 核心能力清单

- **集群形态**：单 Raft Group 强一致集群（3 / 5 节点部署），全量数据多副本复制，自动故障转移与防脑裂。
- **KV 数据模型**：支持 `Put(k, v)`、`Get(k)`、`Delete(k)` 与有序范围查询 `Scan(start_key, end_key, limit)`。
- **一致性保证**：写请求通过 Raft 共识保证线性一致性，配合 `ClientID + SeqNum` 实现客户端重试幂等去重；读请求通过 **ReadIndex** 机制实现线性一致读，避免读操作写盘开销。
- **存储引擎**：从零实现精简版 **LSM-Tree**（WAL + 跳表 MemTable + Immutable MemTable + 分层 SSTable + 布隆过滤器 + 后台 Leveled Compaction），支持崩溃恢复与快照持久化。
- **网络与 RPC**：手写基于 Linux `epoll`（ET 模式 + 非阻塞 IO）的主从 Reactor 网络框架，采用**自定义定长二进制帧头 + Protobuf 载荷**实现轻量级异步 RPC。
- **工程验证**（推荐默认）：抽象网络传输层以支持确定性故障注入（网络分区、丢包、乱序、节点随机宕机重启），配合 ASan / TSan 竞态检测与吞吐/延迟压测。

### 1.2 核心技术选型总览

| 架构层次 | 核心技术方案 | 关键设计考量 |
| :--- | :--- | :--- |
| 开发语言与构建 | C++17 + CMake + Protobuf + GoogleTest | 兼顾现代 C++ 表达力与工业界通用性，编译与依赖可控 |
| 网络与 RPC 层 | epoll (ET) 主从 Reactor + 自定义二进制帧 + Protobuf | 彻底掌控连接生命周期、粘包拆包与异步回调，避免黑盒封装 |
| 并发调度模型 | IO 线程池 + Actor 单线程 Raft 事件循环 + Storage 后台线程池 | 消除 Raft 核心状态机内部锁竞争，杜绝死锁风险 |
| 共识与高可用层 | Raft（选举 / Pre-Vote / 日志复制 / ReadIndex / Snapshot / 单步成员变更） | 覆盖生产级 Raft 关键工程特性，解决脑裂与日志无限膨胀 |
| 存储引擎层 | 精简版 LSM-Tree（WAL + SkipList + SSTable + BloomFilter + LRU Cache） | 原生支持有序 `Scan`，不可变 SSTable 天然契合轻量 Snapshot |

---

## 2. 整体分层架构与线程模型

### 2.1 系统分层架构

系统自顶向下划分为四层，层间通过清晰的 C++ 纯虚接口解耦，便于单元测试与故障注入替换：

```text
+-------------------------------------------------------------------+
|                        Client SDK / CLI                           |
|          (Leader 自动路由、失败重试、ClientID + SeqNum 透传)         |
+-------------------------------------------------------------------+
                                  |  Custom Binary Frame + Protobuf
                                  v
+-------------------------------------------------------------------+
|  [Layer 1] 网络与异步 RPC 层 (epoll 主从 Reactor + RpcServer/Client)|
+-------------------------------------------------------------------+
                                  |  Request / Response Event
                                  v
+-------------------------------------------------------------------+
|  [Layer 2] KV 服务与协调层 (KVServer + SessionTable 幂等去重)       |
|            - 写路径: 提交 Raft Proposal，注册异步响应回调           |
|            - 读路径: 触发 Raft ReadIndex，就绪后直读本地引擎        |
+-------------------------------------------------------------------+
                                  |  Propose / ReadIndex / Apply
                                  v
+-------------------------------------------------------------------+
|  [Layer 3] Raft 共识层 (Actor 单线程事件循环 + Raft WAL)            |
|            - Leader 选举 & Pre-Vote & 心跳保活                     |
|            - 日志复制 & 快速回退 & Snapshot & 单步成员变更          |
+-------------------------------------------------------------------+
                                  |  Apply Command / Checkpoint
                                  v
+-------------------------------------------------------------------+
|  [Layer 4] 存储引擎层 (精简版 LSM-Tree)                             |
|            - Write: WAL -> SkipList MemTable -> Immutable -> L0   |
|            - Read:  MemTable -> Imm -> BlockCache -> SSTable      |
|            - BG:    Flush 线程 + Leveled Compaction 线程           |
+-------------------------------------------------------------------+
```

### 2.2 多线程模型与锁边界设计

分布式存储最常见的工程陷阱是在网络回调、定时器、状态机应用之间互相加锁导致死锁或严重锁竞争。本系统采用 **“IO 多线程 + 共识单线程 Actor + 存储后台线程池”** 的三区隔离模型：

| 线程组 | 线程数量 | 职责范围 | 跨线程通信方式 |
| :--- | :--- | :--- | :--- |
| Main Reactor | 1 | 监听 `listen_fd`，接收新 TCP 连接并按轮询分发给 Sub Reactor | `eventfd` 唤醒目标 Sub Reactor |
| Sub Reactor 池 | N（如 CPU 核数） | 负责已建立连接的非阻塞读写、帧解码、Protobuf 序列化/反序列化 | 向 Raft Actor 的 MPSC 任务队列投递事件 |
| Raft Actor 线程 | 1 | 驱动 Raft 状态机、选举/心跳定时器、日志索引推进、ReadIndex 确认 | 独占 Raft 核心状态，无锁访问；磁盘持久化与网络发送均为异步投递 |
| Apply / Storage 线程池 | 2–4 | 顺序执行状态机 Apply（1 线程）；后台执行 MemTable Flush、SSTable Compaction 与 Snapshot 生成 | 通过有界队列接收已提交日志批次；LSM-Tree 内部使用读写锁保护版本指针（`VersionSet`） |

**核心收益**：
1. **Raft 核心零锁化**：所有 RPC 到达、心跳超时、持久化完成通知均封装为事件（`RaftEvent`）推入 MPSC 队列，由 Raft Actor 单线程顺序处理，从根本上消除并发竞态与死锁。
2. **网络与磁盘隔离**：磁盘 `fsync` 与 Compaction 耗时长，绝不阻塞 Sub Reactor 和 Raft 逻辑判断，保障心跳稳定，防止误触发 Leader 选举。

### 2.3 核心请求全链路时序

#### 写路径（Put / Delete）
1. **接收与解码**：Sub Reactor 读取字节流，校验定长帧头与 CRC32，反序列化为 `KvWriteRequest`。
2. **Leader 校验与提议**：事件投递至 Raft Actor；若当前节点非 Leader，直接返回 `ERR_NOT_LEADER` 及已知 `leader_hint` 地址。
3. **日志追加与复制**：Leader 将写命令（含 `client_id`, `seq_num`, `op`, `key`, `value`）追加到本地 Raft Log，并异步向所有 Follower 发送 `AppendEntries` RPC，同时触发本地 Raft WAL 刷盘（支持 Group Commit 批量刷盘）。
4. **多数派确认与提交**：当多数派节点持久化成功并响应后，Leader 推进 `commitIndex`。
5. **状态机应用与幂等检查**：Apply 线程按 `logIndex` 顺序消费已提交日志，先查 `SessionTable` 判断 `(client_id, seq_num)` 是否已执行；未执行则写入 LSM-Tree 并更新 `SessionTable`。
6. **异步回包**：通过 `RequestID` 找到挂起的回调上下文，由对应 Sub Reactor 将 `KvWriteResponse` 写回客户端。

#### 线性一致读路径（ReadIndex Get / Scan）
1. **确认 Leader 与记录位点**：Leader 收到读请求后，记录当前 `readIndex = commitIndex`。
2. **确认租约/多数派活性**：Leader 向集群广播一轮携带 `read_ctx` 的心跳（`AppendEntries` 空日志），等待收到多数派响应，证明自己在此刻仍是合法 Leader（防止旧 Leader 发生网络分区后返回脏数据）。
3. **等待状态机追平**：等待本地 `lastApplied >= readIndex`。
4. **本地直读**：直接查询本地 LSM-Tree（`Get` 或 `Scan`）并返回结果，全程不写 Raft Log、不触发磁盘写入。

---

## 3. 模块一：网络通信与异步 RPC 层设计

### 3.1 主从 Reactor 核心组件

- **`Poller`（基于 `epoll`）**：统一使用 `EPOLLET`（边缘触发）配合 `O_NONBLOCK` 套接字，单次事件循环内循环读写直至 `EAGAIN` / `EWOULDBLOCK`。
- **`EventLoop`**：每个线程绑定唯一 `EventLoop`，内部维护 `epoll_fd`、定时器队列（基于 `timerfd` + 最小堆实现毫秒级超时）和跨线程任务队列（基于 `eventfd` 唤醒）。
- **`Buffer`**：借鉴 Muduo 风格的连续字节缓冲区，区分 `readIndex` 与 `writeIndex`，减少内存拷贝与系统调用次数（支持 `readv` 栈上临时缓冲）。
- **`TcpConnection`**：管理连接生命周期（`std::shared_ptr` 防悬空），处理半包缓冲、写缓冲区高水位回调（背压控制）与优雅关闭。

### 3.2 自定义二进制帧协议

为解决 TCP 字节流粘包/拆包问题并兼顾调试与完整性校验，设计 **20 字节定长帧头 + 变长 Protobuf 载荷**：

| 字段名称 | 字节偏移 | 长度（Bytes） | 字节序 | 说明 |
| :--- | :--- | :--- | :--- | :--- |
| `magic` | 0 | 2 | Big-Endian | 固定魔数 `0x524B`（"RK"），用于快速校验非法连接 |
| `version` | 2 | 1 | - | 协议版本号，初始为 `0x01` |
| `msg_type` | 3 | 1 | - | 消息类型枚举（Request / Response / Heartbeat / Error） |
| `service_method_id` | 4 | 4 | Big-Endian | RPC 方法路由 ID（如 `RAFT_APPEND_ENTRIES = 101`） |
| `request_id` | 8 | 8 | Big-Endian | 全局递增请求 ID，用于全双工异步多路复用匹配响应 |
| `payload_len` | 16 | 4 | Big-Endian | 后续 Protobuf 二进制载荷长度（上限如 16 MB） |
| `payload` | 20 | `payload_len` | - | Protobuf 序列化后的消息体 |
| `crc32` | `20 + payload_len` | 4 | Big-Endian | 对 Header + Payload 计算的 CRC32 校验和 |

### 3.3 核心 Protobuf 接口定义

拆分为外部客户端协议与内部共识协议两个 `.proto` 文件：

```protobuf
syntax = "proto3";
package raftkv.proto;

// ===== 1. 客户端 KV 协议 =====
enum StatusCode {
  OK = 0;
  ERR_NOT_FOUND = 1;
  ERR_NOT_LEADER = 2;
  ERR_TIMEOUT = 3;
  ERR_INTERNAL = 4;
}

message KvPutRequest {
  uint64 client_id = 1;
  uint64 seq_num = 2;
  bytes key = 3;
  bytes value = 4;
}

message KvGetRequest {
  bytes key = 1;
  bool linearizable = 2; // true: 走 ReadIndex; false: 允许本地读
}

message KvScanRequest {
  bytes start_key = 1;
  bytes end_key = 2;
  uint32 limit = 3;
}

// ===== 2. Raft 节点间共识协议 =====
message LogEntry {
  uint64 term = 1;
  uint64 index = 2;
  uint32 entry_type = 3; // 0: Normal KV, 1: ConfChange, 2: NoOp
  bytes payload = 4;
}

message RequestVoteRequest {
  uint64 term = 1;
  uint32 candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
  bool is_pre_vote = 5;  // 支持 Pre-Vote 阶段
}

message AppendEntriesRequest {
  uint64 term = 1;
  uint32 leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
  uint64 read_ctx_id = 7; // 非 0 表示附带 ReadIndex 心跳确认
}

message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
  uint64 conflict_term = 3;  // 快速日志回退优化
  uint64 conflict_index = 4;
  uint64 read_ctx_id = 5;
}

message InstallSnapshotRequest {
  uint64 term = 1;
  uint32 leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  uint64 offset = 5;
  bytes data = 6;
  bool done = 7;
}
```

### 3.4 异步 RPC 客户端与超时管理

- **连接复用**：节点间通信维护长连接池（每个目标 Peer 1 条全双工 TCP 连接），通过 `request_id` 映射挂起的 `PendingCall`（含回调 `std::function` 与超时定时器 ID），支持并发流水线发送，避免队头阻塞。
- **自动重连与退避**：连接断开时触发指数退避重连（如 50ms → 100ms → 500ms 封顶），并立即以 `ERR_TIMEOUT` 回调所有在途请求，防止 Raft 状态机永久挂起。

---

## 4. 模块二：Raft 共识与高可用层设计

### 4.1 持久化状态与日志存储分离

为避免 Raft 日志频繁追加、截断与 KV 业务数据混杂导致写放大，系统将持久化分为两套独立目录：
- **Raft 元数据与日志（`raft_wal/`）**：采用顺序追加的分段日志（Segment Log，单段如 64 MB），保存 `currentTerm`、`votedFor` 以及 `LogEntry`。支持通过内存索引数组 `offset_table` 实现 $O(1)$ 按 `logIndex` 随机读取，并在回退冲突日志时直接 `ftruncate` 截断。
- **KV 状态机数据（`kv_lsm/`）**：由 LSM-Tree 管理实际 KV 数据与 `SessionTable` 去重状态。

### 4.2 关键工程优化机制

1. **Pre-Vote 预投票机制**：
   - 当 Follower 发生网络分区被隔离时，直接自增 `term` 发起选举会导致其恢复连入集群后迫使当前健康 Leader 下台。
   - 节点选举超时后，先发送 `is_pre_vote = true` 的探测请求（不自增本地 `term`）；仅在获得多数派预投票认可后，才正式递增 `term` 进入 Candidate 状态。
2. **Leader 上任立即提交 No-Op 日志**：
   - 新 Leader 当选后立即写入一条当前任期的空日志（`EntryType::NoOp`）。一旦该日志提交，即可安全提交之前任期遗留的日志，同时确保 `ReadIndex` 不会阻塞在旧任期日志上。
3. **日志冲突快速回退**：
   - Follower 拒绝 `AppendEntries` 时，在响应中携带 `conflict_term` 与 `conflict_index`（该冲突任期的第一条日志下标），Leader 按任期跳过整段冲突日志，避免逐条递减 `nextIndex` 造成的数百次 RTT 浪费。
4. **批处理与流水线（Batching & Pipelining）**：
   - 多个并发写请求在 Raft Actor 队列中自动合并为单次 `AppendEntries` 批次和单次 WAL `fsync`（Group Commit），显著提升高并发下的 QPS。

### 4.3 Snapshot 日志压缩与 LSM-Tree 联动

当已提交日志体积超过阈值（如 64 MB 或 100,000 条）时触发快照：
1. **零停顿快照创建**：由于 LSM-Tree 的 SSTable 文件不可变，先强制将当前 `MemTable` 转为 `Immutable` 并刷盘，再对当前活跃 `Version` 的所有 SSTable 文件及 `MANIFEST` 创建目录硬链接（`link()`），形成轻量 **Checkpoint**，同时序列化当前 `SessionTable`、`lastIncludedIndex`、`lastIncludedTerm` 与集群配置（`ConfState`）。
2. **安全截断日志**：Checkpoint 落盘完成后，异步删除 `raft_wal/` 中 `index <= lastIncludedIndex` 的旧分段文件。
3. **落后节点追赶**：当某 Follower 所需日志已被压缩时，Leader 通过 `InstallSnapshot` 分块（如每块 1 MB）流式发送 Checkpoint 打包数据；Follower 接收校验完毕后原子替换本地 `kv_lsm/` 目录并重载 LSM-Tree 实例。

### 4.4 成员变更（单步变更 Single-Server Change）

采用 Raft 论文推荐的**单步成员变更**（每次只增加或移除 1 个节点），避开 Joint Consensus 的状态复杂度：
- 规则约束：若当前存在未提交的成员变更日志，Leader 拒绝新的成员变更请求，确保新旧配置的多数派必然存在交集，杜绝双 Leader 脑裂。
- 新节点追赶预热（Learner 角色可选）：新增节点可先以非投票成员身份同步 Snapshot 与日志，追平后再升级为正式投票成员，避免新节点拖慢集群可用性。

---

## 5. 模块三：精简版 LSM-Tree 存储引擎设计

### 5.1 内存组件与写入路径

- **Write-Ahead Log (WAL)**：所有写操作（`Put` / `Delete` 墓碑标记）先以 `[CRC32 | Type | KeyLen | Key | ValLen | Val]` 格式追加到 WAL 文件，确保节点断电后内存数据不丢失。
- **SkipList MemTable**：采用自研无锁读/单写跳表（最大层高 12，概率因子 0.25）按 Key 字典序存储数据，同时支持 $O(\log N)$ 点查与有序范围遍历。
- **双 MemTable 切换**：当活跃 `MemTable` 达到阈值（如 4 MB 或 16 MB）时，切换为只读的 `Immutable MemTable`，创建新 `MemTable` 与新 WAL 继续承接写入，同时通知后台 Flush 线程将 `Immutable MemTable` 排序写入 L0 层 SSTable。

### 5.2 SSTable 磁盘物理格式（LevelDB 精简风格）

每个 SSTable 文件（`.sst`）不可变，按固定布局组织，兼顾顺序写与高效点查：

| 区块 | 内容与结构 | 作用 |
| :--- | :--- | :--- |
| Data Blocks | 多个定长约 4 KB 的数据块，块内 KV 按 Key 升序排列（支持前缀压缩） | 实际数据载体，也是 LRU Block Cache 的缓存粒度 |
| Filter Block | 为每个 Data Block 生成的布隆过滤器（默认 10 bits/key，误判率约 1%） | 点查时快速过滤不存在的 Key，避免无效磁盘随机读 |
| Index Block | 记录每个 Data Block 的最大 Key（或分隔 Key）及其在文件中的 `(offset, size)` | 常驻内存或 Cache，二分查找直接定位目标 Data Block |
| Footer（定长 48B） | 存放 Filter Block 与 Index Block 的句柄信息 + 8 字节 Magic Number | 从文件末尾固定偏移解析整张 SSTable 的元信息 |

### 5.3 读取与范围查询（`Get` / `Scan`）

- **单点查询 `Get(key)`**：
  1. 查活跃 `MemTable` → 查 `Immutable MemTable`（命中即返回；若为墓碑标记则直接返回 `NOT_FOUND`）。
  2. 查 L0 层所有 SSTable（L0 文件间 Key 范围可能重叠，需按文件编号从新到旧逐个检查 Bloom Filter → Index Block → Block Cache / 磁盘）。
  3. 查 L1 至 Ln 层（每层文件间 Key 范围严格有序且互不重叠，先二分定位唯一候选 SSTable，再查 Bloom Filter 与 Data Block）。
- **范围查询 `Scan(start, end, limit)`**：
  - 为 `MemTable`、`Immutable MemTable`、L0 所有重叠文件以及 L1..Ln 对应文件构建子迭代器，统一包装为基于**最小堆的多路归并迭代器（`MergingIterator`）**。
  - 归并过程中按 `(Key 升序, Sequence/层级从新到旧)` 排序：同一 Key 仅保留最新版本，遇到墓碑标记自动跳过，精准输出最多 `limit` 条有效记录。

### 5.4 后台 Leveled Compaction 与元数据管理

- **层级容量设计**：L0 按文件数触发（默认 ≥ 4 个文件）；L1 总容量上限 10 MB，之后每层按 10 倍递增（L2 = 100 MB，L3 = 1 GB）。
- **合并流程**：后台 Compaction 线程选取超出阈值的源层文件与目标层 Key 范围重叠的文件，通过 `MergingIterator` 归并去重、清理底层墓碑，写出新的目标层 SSTable（单文件上限如 2 MB）。
- **版本原子切换（`VersionSet` + `MANIFEST`）**：新 SSTable 落盘后，将文件增删变更记录追加到 `MANIFEST` 日志并更新内存 `Version` 引用计数；待所有正在进行的读请求释放旧 `Version` 引用后，安全删除废弃 SSTable 文件。

---

## 6. 核心 C++ 接口抽象与项目目录结构

### 6.1 推荐工程目录结构

```text
raftkv/
├── CMakeLists.txt
├── proto/
│   ├── rpc_kv.proto
│   └── rpc_raft.proto
├── include/raftkv/
│   ├── common/          # Status, Slice, Coding(Varint/Fixed), CRC32, Logger
│   ├── net/             # EventLoop, Poller(epoll), TcpServer, TcpClient, Buffer
│   ├── rpc/             # Codec, RpcServer, RpcChannel, Transport 接口
│   ├── raft/            # RaftNode, RaftLog, StateMachine 接口, ReadIndex, Snapshot
│   ├── storage/         # StorageEngine 接口, LSMTree, SkipList, SSTable, BloomFilter
│   └── server/          # KVServer, SessionTable, ClusterConfig
├── src/                 # 与 include 对应的 .cc 实现
├── tests/
│   ├── unit/            # SkipList, SSTable, Codec, RaftLog 单元测试
│   ├── chaos/           # 确定性模拟网络与故障注入测试（脑裂、宕机、分区）
│   └── bench/           # 多线程读写压测客户端（QPS / P50 / P99 统计）
└── scripts/             # 本地 3/5 节点集群一键启停与 kill-9 演练脚本
```

### 6.2 关键解耦接口设计

通过将**网络传输**与**存储引擎**抽象为纯虚接口，Raft 核心模块既可挂载真实 epoll RPC + LSM-Tree 运行在生产环境，也可挂载内存模拟网络运行在确定性故障注入测试中：

```cpp
namespace raftkv {

// 1. 存储引擎抽象接口
class StorageEngine {
 public:
  virtual ~StorageEngine() = default;
  virtual Status Put(const std::string& key, const std::string& value) = 0;
  virtual Status Delete(const std::string& key) = 0;
  virtual Status Get(const std::string& key, std::string* value) = 0;
  virtual Status Scan(const std::string& start_key,
                      const std::string& end_key,
                      size_t limit,
                      std::vector<std::pair<std::string, std::string>>* out) = 0;
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir) = 0;
  virtual Status RestoreFromCheckpoint(const std::string& checkpoint_dir) = 0;
};

// 2. Raft 传输层抽象（真实 epoll RPC 与模拟故障网络共享同一接口）
class RaftTransport {
 public:
  virtual ~RaftTransport() = default;
  virtual void SendRequestVote(uint32_t peer_id,
                               const proto::RequestVoteRequest& req,
                               std::function<void(Status, proto::RequestVoteResponse)> cb) = 0;
  virtual void SendAppendEntries(uint32_t peer_id,
                                 const proto::AppendEntriesRequest& req,
                                 std::function<void(Status, proto::AppendEntriesResponse)> cb) = 0;
  virtual void SendInstallSnapshot(uint32_t peer_id,
                                   const proto::InstallSnapshotRequest& req,
                                   std::function<void(Status, proto::InstallSnapshotResponse)> cb) = 0;
};

// 3. 状态机应用接口（解耦 Raft 共识与 KV 业务语义）
class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void Apply(uint64_t log_index, const std::string& command_bytes) = 0;
  virtual Status TakeSnapshot(uint64_t last_index, const std::string& dir) = 0;
  virtual Status LoadSnapshot(const std::string& dir) = 0;
};

}  // namespace raftkv
```

---

## 7. 测试验证、故障注入与性能压测方案

### 7.1 正确性与高可用故障注入矩阵

借鉴 MIT 6.824 的测试理念，利用 `RaftTransport` 接口实现内存级可编程模拟网络（`SimTransport`），支持注入延迟、丢包、乱序与双向分区，自动化验证以下核心不变量：

| 故障测试场景 | 注入手段 | 必须满足的正确性不变量 |
| :--- | :--- | :--- |
| Leader 突然宕机 | 进程 `kill -9` 或模拟节点断电 | 剩余多数派在 1–2 个选举超时周期内选出新 Leader，已提交数据零丢失 |
| 网络分区与脑裂 | 将 5 节点划分为 `{2 节点}` 与 `{3 节点}` 两个孤岛 | 少数派旧 Leader 无法提交新写请求，且 `ReadIndex` 超时拒绝服务，绝不返回脏读 |
| 孤立节点重连 | 少数派隔离超时后恢复网络 | 得益于 Pre-Vote，不触发无效任期暴涨，自动回滚未提交日志并追平多数派 |
| 客户端重试幂等 | 模拟 Leader 提交后回包丢包，客户端重复发送同 `SeqNum` 写请求 | 状态机仅执行一次，返回一致成功结果 |
| Snapshot 期间崩溃 | 在 `InstallSnapshot` 传输中途杀死 Follower 并重启 | 重启后丢弃半成品快照，重新接收完整 Checkpoint 并恢复一致状态 |
| LSM 崩溃恢复 | 写入 MemTable 后未 Flush 时强制终止进程 | 重启通过 Replay WAL 完整恢复内存数据，无数据丢失或损坏 |

### 7.2 并发安全与性能观测

- **内存与竞态检测**：CI / 本地构建开启 `-fsanitize=address,undefined`（ASan / UBSan）与 `-fsanitize=thread`（TSan），在高压并发读写与频繁 Leader 切换下跑通全套测试，确保零内存泄漏、零 Data Race。
- **压测与火焰图**：
  - 编写多线程压测工具，分别测试纯写（100% Put）、纯读（100% ReadIndex Get）、混合负载（70% 读 / 30% 写）与范围扫描（Scan）场景，记录 **QPS、P50 / P99 / P999 延迟**。
  - 使用 Linux `perf` 采集 CPU 火焰图，重点验证 Group Commit 批量刷盘、ReadIndex 与布隆过滤器开启前后的性能提升幅度（可直接作为项目文档与简历的量化亮点）。

---

## 8. 分阶段开发路线图

建议按“自底向上、每阶段均可独立编译测试”的节奏分 6 个里程碑推进，避免一次性集成导致排错困难：

| 阶段 | 里程碑名称 | 核心交付物 | 阶段验收标准 |
| :--- | :--- | :--- | :--- |
| M1 | 网络与异步 RPC 基座 | `EventLoop`、epoll ET `Poller`、主从 Reactor、二进制帧编解码、`RpcServer` / `RpcClient` | 完成 Echo 与 Protobuf RPC 压测；支持粘包/半包/超时重试，ASan/TSan 零告警 |
| M2 | 精简版 LSM-Tree 引擎 | WAL、跳表 MemTable、SSTable 读写、BloomFilter、LRU Block Cache、Leveled Compaction、`MergingIterator` | 百万级随机读写与 `Scan` 正确性测试通过；`kill -9` 崩溃后 WAL 恢复 100% 一致 |
| M3 | Raft 核心共识协议 | Actor 事件循环、Raft WAL、Leader 选举、Pre-Vote、日志复制、快速回退 | 在 `SimTransport` 下通过选举、日志一致性、丢包、乱序与网络分区全套故障测试 |
| M4 | 全链路集成与 ReadIndex | 接入真实 epoll RPC 与 LSM-Tree、实现 `KVServer`、`SessionTable` 幂等去重与 `ReadIndex` 线性一致读 | 本地 3/5 节点集群正常提供 `Put/Get/Delete/Scan`，验证脑裂下旧 Leader 不返回脏读 |
| M5 | Snapshot 与成员变更 | LSM Checkpoint 联动快照、`InstallSnapshot` 分块传输、日志截断、单步成员变更 | 持续写入触发多次日志压缩；新节点加入空集群后自动通过快照追平并参与共识 |
| M6 | 工程化压测与调优 | 集群演练脚本、多线程压测工具、Group Commit 优化、性能对比报告与火焰图分析 | 产出完整的性能基准数据（开启/关闭 ReadIndex、BloomFilter、Group Commit 对比） |
