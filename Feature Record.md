# RaftKV 逐特性开发与验收清单（Feature Record）

本清单面向从零动手实现的开发者，将整个项目拆分为 **7 个阶段、32 个可独立编码与验证的 Feature**。每个 Feature 都明确了**前置依赖、要建的文件、核心数据结构、必写函数、以及编译测试方法**。你可以严格按顺序勾选 `[x]` 推进，确保每写完一小块就能编译运行、立即得到正反馈。

---

## Phase 0：开发环境准备与工程脚手架

### [x] F0.1 安装基础开发工具与第三方库（Ubuntu / Debian 示例）

- **目标**：备齐 C++17 编译器、CMake 构建工具、Protobuf 序列化库和 GoogleTest 单元测试框架。
- **安装命令**：

```bash
sudo apt update
sudo apt install -y \
  build-essential g++ cmake ninja-build pkg-config \
  libprotobuf-dev protobuf-compiler \
  libgtest-dev gdb valgrind linux-tools-common
```

- **版本检查（验收标准）**：
  - `g++ --version`：建议 9.0 及以上（完整支持 C++17）。
  - `cmake --version`：建议 3.16 及以上。
  - `protoc --version`：建议 3.12 及以上。

### [x] F0.2 初始化项目目录与顶层 `CMakeLists.txt`

- **目标**：搭好空工程骨架，配置好 Debug / Release 与 ASan / TSan 编译选项，确保 Protobuf 和 GoogleTest 能正常链接。
- **新建目录结构**：
  - `proto/`、`include/raftkv/{common,net,rpc,storage,raft,server,client}/`、`src/{common,net,rpc,storage,raft,server,client}/`、`tests/{unit,chaos,bench}/`、`scripts/`
- **关键配置项（`CMakeLists.txt`）**：
  - 设置 `CMAKE_CXX_STANDARD 17`、`CMAKE_CXX_STANDARD_REQUIRED ON`。
  - 使用 `find_package(Protobuf REQUIRED)`、`find_package(GTest REQUIRED)`、`find_package(Threads REQUIRED)`。
  - 定义编译开关：`option(ENABLE_ASAN "Enable AddressSanitizer" OFF)` 与 `option(ENABLE_TSAN "Enable ThreadSanitizer" OFF)`，分别追加 `-fsanitize=address,undefined` 和 `-fsanitize=thread`。
  - 开启 `enable_testing()` 以支持 `ctest`。
- **编译与验收**：
  - 写一个只包含 `EXPECT_EQ(1 + 1, 2);` 的 `tests/unit/smoke_test.cc`。
  - 执行 `cmake -B build -G Ninja && cmake --build build && ctest --test-dir build --output-on-failure`，看到 `100% tests passed` 即算完成。

---

## Phase 1：公共基础组件（Common Layer）

> **阶段目标**：先把所有模块都会用到的“工具箱”写好，这些类完全独立，不涉及网络和多线程复杂逻辑，最适合热身。

### [x] F1.1 错误状态封装 `Status` 与零拷贝视图 `Slice`

- **目标**：统一全项目的函数返回值（避免到处返回 `-1` / `false` 不知道错在哪），并用轻量视图减少字符串拷贝。
- **文件位置**：`include/raftkv/common/status.h`、`include/raftkv/common/slice.h`
- **核心数据结构**：
  - `enum class Code { kOk, kNotFound, kCorruption, kIOError, kNotLeader, kTimeout, kInvalidArgument };`
  - `class Status`：成员 `Code code_`、`std::string msg_`。
  - `class Slice`：成员 `const char* data_`、`size_t size_`（或直接基于 `std::string_view` 封装）。
- **必写函数**：
  - `Status::OK()`、`Status::NotFound(msg)`、`Status::IOError(msg)` 等静态工厂方法；`ok()`、`code()`、`ToString()`。
  - `Slice::compare(const Slice& b)`、`starts_with()`、`ToString()`。
- **测试验收**：编写 `tests/unit/common_test.cc`，验证不同错误码构造、拷贝与字典序比较。

### [x] F1.2 二进制编解码工具 `Coding` 与 `CRC32` 校验

- **目标**：解决大小端字节序问题（网络帧头用大端，磁盘文件用小端），并提供防数据损坏的 CRC32 校验。
- **文件位置**：`include/raftkv/common/coding.h`、`src/common/coding.cc`、`include/raftkv/common/crc32.h`、`src/common/crc32.cc`
- **必写函数**：
  - 定长小端（磁盘用）：`EncodeFixed32` / `DecodeFixed32`、`EncodeFixed64` / `DecodeFixed64`。
  - 定长大端（网络帧头用）：`EncodeBigEndian16/32/64`、`DecodeBigEndian16/32/64`。
  - 长度前缀字符串：`PutLengthPrefixedSlice(std::string* dst, Slice value)`、`GetLengthPrefixedSlice(Slice* input, Slice* result)`。
  - `uint32_t Crc32(const char* data, size_t n)`（标准查表法实现）。
- **测试验收**：随机生成整数与字符串，编码后解码与原值比对；篡改任意 1 字节验证 CRC32 必然变化。

### [x] F1.3 线程安全异步/同步日志器与 MPSC 任务队列

- **目标**：调试分布式程序极度依赖带时间戳、线程 ID 和节点 ID 的日志；同时为后续 Raft Actor 准备好跨线程“收件箱队列”。
- **文件位置**：`include/raftkv/common/logger.h`、`include/raftkv/common/mpsc_queue.h`
- **核心数据结构**：
  - `Logger`：支持 `INFO / WARN / ERROR / DEBUG` 级别，内部 `std::mutex` 保护输出流，格式 `[时间戳][级别][线程ID][文件:行号] 消息`。
  - `template <typename T> class MpscQueue`：初期先用 `std::mutex + std::condition_variable + std::deque<T>` 实现**多生产者单消费者阻塞队列**（支持 `Push(T)`、`PopBatch(std::vector<T>&, timeout_ms)`），保证 100% 正确。
- **测试验收**：启动 4 个生产者线程各 Push 10,000 个整数，1 个消费者线程 Pop，验证总数与校验和完全一致，开启 `-DENABLE_TSAN=ON` 零竞态警告。

---

## Phase 2：网络与异步 RPC 层（Net & RPC Layer）

> **阶段目标**：手写一个能稳定扛住高并发、支持粘包拆包和异步回调的 C++ RPC 框架。

### [x] F2.1 动态字节缓冲区 `Buffer`

- **目标**：作为每个 TCP 连接的收发缓存，避免频繁 `malloc` 和内存搬移。
- **文件位置**：`include/raftkv/net/buffer.h`、`src/net/buffer.cc`
- **核心数据结构**：`std::vector<char> buffer_`、`size_t reader_index_`、`size_t writer_index_`。
- **必写函数**：
  - `ReadableBytes()`、`WritableBytes()`、`Peek()`（查看当前可读首地址）。
  - `Append(const char* data, size_t len)`、`Retrieve(size_t len)`（移动读指针）、`RetrieveAllAsString()`。
  - `ReadFd(int fd, int* saved_errno)`（使用 `readv` + 64KB 栈上临时缓冲区一次读尽内核缓冲）。
- **测试验收**：编写 `buffer_test.cc`，测试连续追加触发扩容、读走前半段后复用头部空间（内部腾挪）。

### [x] F2.2 `Channel` 与 `EpollPoller`（epoll ET 模式封装）

- **目标**：封装 Linux `epoll_create1`、`epoll_ctl`、`epoll_wait`，把底层文件描述符事件转化为 C++ 回调。
- **文件位置**：`include/raftkv/net/channel.h`、`include/raftkv/net/poller.h`、`src/net/poller.cc`
- **核心数据结构**：
  - `Channel`：绑定一个 `int fd_`，保存关注的事件 `events_`、实际触发的事件 `revents_`，以及 4 个回调 `read_cb_`、`write_cb_`、`close_cb_`、`error_cb_`。
  - `EpollPoller`：保存 `int epoll_fd_`、`std::vector<struct epoll_event> events_`、`std::unordered_map<int, Channel*> channels_`。
- **必写函数**：
  - `SetNonBlocking(int fd)`（设置 `O_NONBLOCK`）。
  - `EpollPoller::Poll(int timeout_ms, std::vector<Channel*>* active_channels)`。
  - `EpollPoller::UpdateChannel(Channel*)`、`RemoveChannel(Channel*)`（统一开启 `EPOLLET`）。
- **测试验收**：使用 Linux `pipe()` 创建管道，把读端注册到 `EpollPoller`，向写端写入数据，验证 `Poll()` 能精准唤醒并触发 `read_cb_`。

### [x] F2.3 `EventLoop` 与定时器队列（`timerfd` + `eventfd`）

- **目标**：实现“One Loop Per Thread”核心引擎，支持跨线程安全投递任务和毫秒级定时回调（后续 Raft 选举超时与 RPC 超时全靠它）。
- **文件位置**：`include/raftkv/net/event_loop.h`、`src/net/event_loop.cc`、`include/raftkv/net/timer_queue.h`
- **核心数据结构**：
  - `EventLoop`：持有 `EpollPoller`、当前线程 ID `thread_id_`、`int wakeup_fd_`（`eventfd`）、待执行跨线程函数列表 `pending_functors_` + 互斥锁。
  - `TimerQueue`：基于 `timerfd_create` + `std::priority_queue`（按到期时间戳排序的小顶堆）。
- **必写函数**：
  - `Loop()`（主事件循环）、`Quit()`、`IsInLoopThread()`。
  - `RunInLoop(Functor cb)`、`QueueInLoop(Functor cb)`（跨线程安全投递并用 `wakeup_fd_` 唤醒）。
  - `RunAfter(uint64_t delay_ms, Functor cb)`、`RunEvery(uint64_t interval_ms, Functor cb)`、`CancelTimer(TimerId id)`。
- **测试验收**：在子线程运行 `EventLoop`，主线程调用 `QueueInLoop` 和 `RunAfter(100ms)`，验证回调在子线程按时触发。

### [x] F2.4 `Acceptor`、`TcpConnection` 与主从 Reactor `TcpServer`

- **目标**：完成多线程高并发 TCP 服务端，1 个主线程接收连接，N 个子线程处理收发。
- **文件位置**：`include/raftkv/net/tcp_connection.h`、`include/raftkv/net/tcp_server.h` 及对应 `.cc`
- **核心数据结构**：
  - `TcpConnection`（继承 `std::enable_shared_from_this<TcpConnection>` 防回调期间对象被销毁）：持有 `Socket fd`、`Channel`、`Buffer input_buf_`、`Buffer output_buf_`、连接状态枚举。
  - `EventLoopThreadPool`：启动 N 个子线程，每个线程跑一个 `EventLoop`，轮询（Round-Robin）分配给新连接。
  - `TcpServer`：持有 `Acceptor`（监听 `listen_fd`）、`EventLoopThreadPool`、活跃连接表 `std::unordered_map<std::string, TcpConnectionPtr>`。
- **必写函数**：
  - `TcpConnection::HandleRead()`（ET 模式循环 `ReadFd` 直到 `EAGAIN`）、`HandleWrite()`、`Send(Slice data)`、`Shutdown()`。
  - `TcpServer::Start()`、`SetMessageCallback(...)`、`SetConnectionCallback(...)`。
- **测试验收**：编写 Echo Server 示例程序，用 `nc` 或多线程压测脚本同时建立 100 个连接收发随机长度数据，验证回显 100% 正确且断开无内存泄漏（开启 ASan）。

### [x] F2.5 `TcpClient` 与自动重连

- **目标**：提供非阻塞异步 TCP 客户端，支持断线指数退避重连，供节点间 Raft 通信与外部 Client SDK 使用。
- **文件位置**：`include/raftkv/net/tcp_client.h`、`src/net/tcp_client.cc`
- **必写函数**：`Connect()`、`Disconnect()`、`Send(Slice data)`、非阻塞 `connect` 可写事件检查（`getsockopt(SO_ERROR)`）、失败后通过 `EventLoop::RunAfter` 指数退避重试。
- **测试验收**：先启动 `TcpClient`（此时服务端未开），2 秒后再启动 Echo Server，验证客户端自动重连成功并正常收发消息。

### [x] F2.6 自定义 20 字节帧协议编解码器与 `.proto` 定义

- **目标**：彻底解决 TCP 粘包/拆包问题，把字节流切分成一条条完整的 Protobuf RPC 消息。
- **文件位置**：`proto/rpc_kv.proto`、`proto/rpc_raft.proto`、`include/raftkv/rpc/frame_codec.h`、`src/rpc/frame_codec.cc`
- **核心数据结构**：
  - `struct RpcFrame { uint16_t magic; uint8_t version; uint8_t msg_type; uint32_t method_id; uint64_t request_id; std::string payload; };`
- **必写函数**：
  - `FrameCodec::Encode(const RpcFrame& frame, Buffer* out)`（写 20 字节大端头 + payload + 4 字节 CRC32）。
  - `FrameCodec::TryDecode(Buffer* in, RpcFrame* out)`（返回 `kOk` / `kIncomplete` 半包继续等 / `kCorrupt` 魔数或 CRC 错）。
- **测试验收**：把 3 个帧拼在一起、再故意按“每次只喂 1 个字节”或“随机长度切分”送入 `TryDecode`，验证能准确解出全部 3 个完整帧。

### [x] F2.7 异步 `RpcServer` 与 `RpcClient`

- **目标**：在 `TcpServer` / `TcpClient` 之上封装方法路由、全双工 `request_id` 异步匹配和请求超时控制。
- **文件位置**：`include/raftkv/rpc/rpc_server.h`、`include/raftkv/rpc/rpc_client.h` 及对应 `.cc`
- **核心数据结构**：
  - `RpcServer`：方法注册表 `std::unordered_map<uint32_t, RpcHandler>`。
  - `RpcClient`：在途请求表 `std::unordered_map<uint64_t, PendingCall>`（保存回调函数 + 超时 `TimerId`），配合 `std::atomic<uint64_t> next_req_id_`。
- **必写函数**：
  - `RpcServer::RegisterMethod(uint32_t method_id, RpcHandler handler)`。
  - `RpcClient::Call(uint32_t method_id, const google::protobuf::Message& req, ResponseCallback cb, uint64_t timeout_ms)`。
- **测试验收**：启动 `RpcServer` 注册一个测试 RPC，客户端并发发起 1,000 个异步调用（含故意延迟触发超时的用例），验证正常请求全部准确匹配回包、超时请求按时回调 `ERR_TIMEOUT`。

---

## Phase 3：精简版 LSM-Tree 存储引擎（Storage Layer）

> **阶段目标**：不依赖任何网络代码，独立写出一个支持 `Put / Get / Delete / Scan`、崩溃恢复和快照的嵌入式 KV 存储引擎。

### [x] F3.1 内存跳表 `SkipList` 与 `MemTable`

- **目标**：实现高性能内存有序数据结构，作为 LSM-Tree 接收写入的第一站。
- **文件位置**：`include/raftkv/storage/skiplist.h`、`include/raftkv/storage/memtable.h`
- **核心数据结构**：
  - `SkipList`：节点 `Node { InternalKey key; std::string value; Node* next[kMaxHeight]; }`（`InternalKey` 包含 `user_key + seq_num + value_type(Put/Delete)`，保证同 Key 新版本排前面）。
  - `MemTable`：封装 `SkipList` 与当前近似内存占用 `size_t approx_memory_usage_`。
- **必写函数**：
  - `MemTable::Add(uint64_t seq, ValueType type, Slice key, Slice value)`。
  - `MemTable::Get(Slice key, std::string* value, bool* is_deleted)`。
  - `MemTable::NewIterator()`（返回有序迭代器，支持 `Seek(target)`、`Next()`、`Valid()`）。
- **测试验收**：插入 10 万条乱序 KV、覆盖更新、删除部分 Key，验证点查与全量有序遍历结果与 `std::map` 100% 一致。

### [x] F3.2 预写日志 `WAL`（写入与崩溃回放）

- **目标**：保证写入 `MemTable` 前先顺序落盘，进程 `kill -9` 后数据不丢。
- **文件位置**：`include/raftkv/storage/wal.h`、`src/storage/wal.cc`
- **记录格式**：`[CRC32 (4B) | SeqNum (8B) | Type (1B) | KeyLen (4B) | Key | ValLen (4B) | Value]`。
- **必写函数**：
  - `WalWriter::Append(uint64_t seq, ValueType type, Slice key, Slice value)`、`Sync()`（调用 `fsync`）。
  - `WalReader::Replay(std::function<void(uint64_t, ValueType, Slice, Slice)> cb)`（遇到文件末尾半条损坏记录时安全截断停止）。
- **测试验收**：写入 1,000 条记录，故意截断文件最后 5 字节模拟断电，验证 `Replay` 能无损恢复前 999 条且不崩溃。

### [x] F3.3 布隆过滤器 `BloomFilter` 与 `LRUCache`

- **目标**：为磁盘 SSTable 提供查询加速，过滤不存在的 Key 并缓存热点 Data Block。
- **文件位置**：`include/raftkv/storage/bloom_filter.h`、`include/raftkv/storage/lru_cache.h`
- **核心数据结构**：
  - `BloomFilter`：默认每 Key 10 bits（双哈希法模拟 $k=7$ 个哈希函数）。
  - `LRUCache`：双向链表 + `std::unordered_map` + `std::mutex`（或分片锁），按容量上限淘汰最久未使用的 Block。
- **必写函数**：
  - `BloomFilter::Build(const std::vector<Slice>& keys, std::string* dst)`、`KeyMayMatch(Slice key, Slice filter)`。
  - `LRUCache::Insert(key, value)`、`Lookup(key)`、`Erase(key)`。
- **测试验收**：向布隆过滤器写入 10 万个 Key，验证已存在的 Key 召回率 100%（零漏报），不存在的 Key 误判率接近 1%。

### [x] F3.4 `SSTableBuilder` 与 `SSTableReader`

- **目标**：实现不可变磁盘有序文件 `.sst` 的生成与高效读取。
- **文件位置**：`include/raftkv/storage/sstable.h`、`src/storage/sstable.cc`
- **物理布局**：`[Data Block 0..N (每块 ~4KB)] + [Filter Block] + [Index Block] + [Footer (48B 定长)]`。
- **必写函数**：
  - `SSTableBuilder::Add(InternalKey, Slice value)`（满 4KB 自动刷写一个 Data Block 并记录索引）、`Finish()`（写 Filter、Index、Footer 并 `fsync`）。
  - `SSTableReader::Open(path, cache)`（只读尾部 48B Footer + Index + Filter）。
  - `SSTableReader::Get(Slice key, ...)`（先查布隆过滤器 → 二分查 Index Block → 查 LRU Cache / `pread` 读 Data Block → 块内二分）。
  - `SSTableReader::NewIterator()`（两级迭代器，支持顺序扫描）。
- **测试验收**：将 5 万条有序 KV 写入单个 `.sst` 文件，重新打开文件验证随机点查、不存在 Key 过滤、以及 `Seek` 范围遍历全部正确。

### [x] F3.5 多路归并迭代器 `MergingIterator`

- **目标**：同时遍历 `MemTable`、`Immutable` 和多层 `SSTable`，自动处理新版本覆盖旧版本和墓碑（Delete）过滤，为 `Scan` 和 `Compaction` 提供统一底座。
- **文件位置**：`include/raftkv/storage/iterator.h`、`include/raftkv/storage/merging_iterator.h`、`src/storage/merging_iterator.cc`
- **核心数据结构**：`StorageIterator` 统一 MemTable 与 SSTable 迭代器；MergingIterator 持有多个子迭代器 `std::vector<std::unique_ptr<StorageIterator>>` 和最小堆（比较规则：`user_key` 升序，同 `user_key` 时 `seq_num` 降序）。
- **必写函数**：`SeekToFirst()`、`Seek(Slice target)`、`Seek(InternalKey target)`、`Next()`、`key()`、`value()`、`Valid()`、`status()`。
- **测试验收**：构造 4 个有重叠 Key 和删除墓碑的有序集合，验证归并输出严格有序、无重复 Key 且被删除的 Key 正确隐藏。

### [x] F3.6 `VersionSet` / `MANIFEST` 与后台 Flush / Leveled Compaction

- **目标**：管理多层 SSTable 文件清单，在后台自动将写满的 MemTable 刷盘并合并多层文件、回收过期垃圾。
- **文件位置**：`include/raftkv/storage/version_set.h`、`src/storage/version_set.cc`、`src/storage/compaction.cc`、`src/storage/compaction_internal.h`
- **核心数据结构**：
  - `struct FileMetaData { uint64_t file_number; uint64_t file_size; std::string smallest_key; std::string largest_key; };`
  - `struct Version`（引用计数 `std::shared_ptr<Version>`）：保存 `std::vector<FileMetaData> levels_[kNumLevels]`。
  - `MANIFEST` 日志：记录每次 Flush / Compaction 的文件增删变更，配合 `CURRENT` 文件记录当前活跃的 MANIFEST 文件名。
- **必写函数**：
  - `MaybeScheduleFlush()`（把 `Immutable MemTable` 写成 L0 `.sst` 并删除对应旧 WAL）。
  - `MaybeScheduleCompaction()`（L0 文件数达 4 个或 Ln 层超容量阈值时，挑选重叠文件用 `MergingIterator` 归并生成下一层 `.sst`，更新 `MANIFEST` 并原子切换 `Version`）。
- **测试验收**：把 MemTable 阈值临时调小为 64 KB，连续写入与覆盖更新 5 万条数据，观察 L0→L1→L2 文件自动生成与旧文件清理，重启后能通过 `MANIFEST` 完整加载所有层级。

### [x] F3.7 `LSMTree` 顶层组装与 `CreateCheckpoint` 硬链接快照

- **目标**：对外暴露完整的 `StorageEngine` 接口，并实现与 Raft Snapshot 联动的秒级 Checkpoint。
- **文件位置**：`include/raftkv/storage/lsm_tree.h`、`src/storage/lsm_tree.cc`
- **必写函数**：
  - `Open(dir)`、`Put(k, v)`、`Delete(k)`、`Get(k, *v)`、`Scan(start, end, limit, *out)`。
  - `CreateCheckpoint(checkpoint_dir)`：强制 Flush 当前 MemTable，对当前活跃 `Version` 的所有 `.sst`、`MANIFEST`、`CURRENT` 调用 POSIX `link()` 创建硬链接。
  - `RestoreFromCheckpoint(checkpoint_dir)`：关闭当前句柄，用快照目录原子替换数据目录并重新 `Open`。
- **测试验收**：编写综合测试，执行混合 `Put/Delete/Get/Scan` 与 `std::map` 对拍；生成 Checkpoint 后继续写入新数据，再从 Checkpoint 恢复，验证数据精准回退到快照时刻。

---

## Phase 4：Raft 共识与高可用层（Raft Layer）

> **阶段目标**：先不接真实网络和 LSM-Tree，用纯内存模拟网络把 Raft 协议本身测到固若金汤（MIT 6.824 黄金实践）。

### [x] F4.1 `RaftLog` 与分段持久化 `RaftWAL`

- **目标**：持久化存储 Raft 的 `currentTerm`、`votedFor` 和日志条目，支持按 `index` 快速读取与冲突截断。
- **文件位置**：`include/raftkv/raft/raft_log.h`、`src/raft/raft_log.cc`
- **核心数据结构**：
  - 持久化元数据文件 `raft_meta.bin`（写入临时文件 + `fsync` + `rename` 保证原子更新）。
  - 日志文件 `RAFT-LOG-XXXXXX.wal` 分段存储 + 内存 `(segment, offset, size)` 索引表 + `uint64_t snapshot_last_index_` / `snapshot_last_term_`。
- **必写函数**：`Append(entries)`、`TruncateSuffix(from_index)`、`CompactPrefix(up_to_index, last_term)`、`TermAt(index)`、`LastIndex()`、`SaveHardState(term, voted_for)`。
- **测试验收**：验证追加、截断、快照前截断以及重启恢复后各项 `TermAt` 与 `LastIndex` 均精确一致。

### [x] F4.2 `RaftTransport` 抽象与确定性内存模拟网络 `SimTransport`

- **目标**：把网络通信抽象为接口，在内存里模拟 3/5 节点互通，并支持注入丢包、延迟、单向/双向网络分区。
- **文件位置**：`include/raftkv/raft/transport.h`、`tests/chaos/sim_transport.h`
- **必写函数**：
  - `SimTransport::DropNode(id)`（模拟宕机）、`Partition(groupA, groupB)`（模拟脑裂）、`Heal()`（恢复网络）、`SetDropRate(0.1)`（模拟 10% 丢包）。
- **测试验收**：两个模拟节点互发测试消息，开启分区后消息被拦截，恢复分区后重新送达。
- **实现补充**：`RaftTransport` 同时提供三类请求 Handler；`SimTransport` 使用虚拟毫秒时钟、固定随机种子和 `AdvanceBy()` 驱动延迟、乱序、丢包、宕机、双向分区及单向断链。

### [x] F4.3 `RaftNode` Actor 单线程事件循环骨架

- **目标**：搭建 Raft 的单线程事件驱动核心，确保所有状态修改都在同一个线程完成，零锁竞争。
- **文件位置**：`include/raftkv/raft/raft_node.h`、`src/raft/raft_node.cc`
- **核心数据结构**：
  - `enum class Role { kFollower, kPreCandidate, kCandidate, kLeader };`
  - `struct RaftEvent`（用 `std::variant` 封装：收到各类 RPC 请求/响应、定时 Tick、上层 Propose、ReadIndex 请求）。
  - `MpscQueue<RaftEvent> event_queue_` + 独立工作线程 `raft_thread_` + `10ms` 定时 Tick 触发器。
- **必写函数**：`Start()`、`Stop()`、`PostEvent(RaftEvent)`、`Step(const RaftEvent& ev)`（核心状态分发函数）。
- **实现补充**：Actor 线程、Start/Stop 生命周期、10ms Tick、Transport Handler 桥接和线程安全状态快照已完成；三类 RPC 在 F4.3 返回明确拒绝占位响应。

### [x] F4.4 Leader 选举与 Pre-Vote 预投票

- **目标**：实现集群启动自动选主、Leader 失联自动重选，并防止孤立节点恢复后破坏健康集群。
- **关键参数与逻辑**：
  - 选举超时随机化：`150ms ~ 300ms`；心跳间隔：`50ms`。
  - 超时先进入 `kPreCandidate`，发 `is_pre_vote = true` 的 `RequestVote`（不递增 `currentTerm`）；获得多数派认可后再转 `kCandidate` 递增 `term` 正式拉票。
  - 严格检查候选人日志新旧程度（先比 `last_log_term`，再比 `last_log_index`）。
- **测试验收**：在 `SimTransport` 下启动 3 节点，验证 1 秒内选出唯一 Leader；隔离 Leader 后剩余 2 节点选出新 Leader；恢复旧 Leader 后其自动降级为 Follower 且不引发新一轮选举风暴。
- **实现补充**：完成随机化选举超时、Pre-Vote、正式投票持久化、日志新旧校验、空心跳保活和更高 term 降级；Actor 入站请求与出站回包统一经线程安全事件桥接。新增 7 个选举测试，Debug、ASan/UBSan、TSan 均通过。

### [x] F4.5 心跳保活、日志复制、快速回退与 No-Op 提交

- **目标**：实现稳定的多副本日志同步，保证所有健康节点按完全相同的顺序提交日志。
- **核心数据结构**（仅 Leader 维护）：`std::unordered_map<uint32_t, uint64_t> next_index_`、`match_index_`。
- **必写逻辑**：
  - Leader 当选立即追加一条 `EntryType::NoOp` 日志。
  - Follower 日志不匹配时返回 `conflict_term` 和 `conflict_index`，Leader 一步回退整段冲突任期。
  - Leader 统计 `match_index_` 中位数（多数派已复制且属于当前 `term` 的最大索引），推进 `commit_index_` 并推送给 Apply 回调。
- **测试验收**：在 `SimTransport` 下模拟连续提交 500 条日志、中途反复制造网络分区与 10% 随机丢包，验证最终所有恢复节点的已提交日志逐条 100% 相同。
- **实现补充**：完成 No-Op、per-peer next/match、每批 64 条复制、请求序号防迟到覆盖、conflict term/index 快速回退、当前 term 多数派提交、Actor 线程顺序 Apply 和 Propose 生命周期；事件积压期间仍会触发 Tick 与独立心跳。新增 4 个复制测试，Debug、ASan/UBSan、TSan 均通过。

### [x] F4.6 `ReadIndex` 线性一致读

- **目标**：在不写 Raft 日志的前提下，保证客户端读请求永远不会读到过期脏数据。
- **核心数据结构**：`struct PendingRead { uint64_t read_ctx_id; uint64_t read_index; std::unordered_set<uint32_t> acks; ReadCallback cb; };`
- **必写流程**：
  1. Leader 确认已提交过当前任期的日志（保证 `commit_index_` 最新），记录 `read_index = commit_index_`。
  2. 生成 `read_ctx_id`，广播一轮附带 `read_ctx_id` 的心跳。
  3. 收到多数派带相同 `read_ctx_id` 的响应后，确认自己仍是合法 Leader；一旦 `last_applied >= read_index`，立即触发 `cb(Status::OK())` 通知上层直读本地存储。
- **测试验收**：把旧 Leader 隔离在少数派分区，向旧 Leader 发起 `ReadIndex`，验证其永远不会返回成功（最终超时拒绝），杜绝脑裂脏读。
- **实现补充**：完成当前 term 提交门槛、独立 `read_ctx_id` 确认心跳、成功后回填上下文、多数派去重确认、Apply 进度等待、降级/超时/Actor 停止清理；ReadIndex 心跳不占用复制序号。新增 9 个 ReadIndex 测试，Debug、ASan/UBSan、TSan 均通过，TSan 连续 10 轮通过。

### [x] F4.7 `InstallSnapshot` 日志压缩与分块传输

- **目标**：限制 Raft 日志无限增长，并让大幅落后的节点通过快照快速追平。
- **必写逻辑**：
  - Apply 进度超过快照阈值（如每 10,000 条日志）时，调用 `StateMachine::TakeSnapshot`，完成后截断 `RaftLog`。
  - 当 Leader 发现某 Follower 的 `next_index <= snapshot_last_index_` 时，改为发送 `InstallSnapshotRequest`（按 1 MB 分块，带 `offset` 和 `done` 标记）。
  - Follower 收齐全部数据块后调用 `StateMachine::LoadSnapshot` 并重置 `last_applied` 与 `commit_index`。
- **测试验收**：关闭 1 个 Follower，向集群写入足够触发 2 次快照的日志，再重新拉起该 Follower，验证其通过快照追平并正确包含最新数据。
- **实现补充**：完成目录快照、确定性 bundle、CRC 与路径校验、RaftLog 快照边界重置、阈值触发压缩、分块发送和 offset 回包关联、Follower 完整接收后 LoadSnapshot，以及 `INSTALLING` 崩溃恢复事务。新增 10 个快照相关测试，Debug、ASan/UBSan、TSan 均 45/45 通过，两套快照测试 TSan 连续 10 轮通过。

### [ ] F4.8 单步成员变更（Single-Server Change）

- **目标**：支持在线安全扩容（如 3 节点扩到 5 节点）或下线故障节点。
- **必写逻辑**：
  - 定义 `ConfChange` 特殊日志类型（`AddNode` / `RemoveNode`）。
  - 每次只允许集群中存在 1 条未提交的 `ConfChange` 日志，收到新变更立即应用到路由配置；若被移除的是 Leader 自己，在该日志提交后主动退位。
- **测试验收**：3 节点集群动态新增第 4、第 5 个空节点，验证新节点通过快照同步数据并正常参与后续选主。

---

## Phase 5：全链路组装与服务层（Server & Client SDK）

> **阶段目标**：像拼乐高一样，把 Phase 2（网络 RPC）、Phase 3（LSM-Tree）和 Phase 4（Raft）真正串成一个可运行的服务。

### [ ] F5.1 幂等去重表 `SessionTable` 与 `KVStateMachine`

- **目标**：防止网络超时重试导致同一写请求被执行两次，并把 Raft Apply 和 LSM-Tree 粘合在一起。
- **文件位置**：`include/raftkv/server/session_table.h`、`include/raftkv/server/kv_state_machine.h`
- **核心数据结构**：`std::unordered_map<uint64_t, uint64_t> last_seq_by_client_`（记录每个 `client_id` 已执行的最大 `seq_num`）。
- **必写函数**：
  - `KVStateMachine::Apply(log_index, cmd_bytes)`：反序列化 KV 命令 → 查 `SessionTable` 去重 → 调用 `LSMTree::Put/Delete` → 更新 `SessionTable` → 唤醒挂起的客户端 RPC 回调。
  - `TakeSnapshot` / `LoadSnapshot`：把 `SessionTable` 序列化文件一并打包进 LSM-Tree Checkpoint 目录，确保快照恢复后去重状态不丢失。

### [ ] F5.2 真实网络传输实现 `RealRaftTransport`

- **目标**：用 Phase 2 的 `RpcClient` 实现 Phase 4 定义的 `RaftTransport` 接口，替换掉测试用的 `SimTransport`。
- **文件位置**：`include/raftkv/server/real_raft_transport.h`、`src/server/real_raft_transport.cc`
- **核心逻辑**：为每个 Peer 节点维护一个长连接 `RpcClient`，将 `SendRequestVote` / `SendAppendEntries` / `SendInstallSnapshot` 映射为异步 RPC 调用，并在回调里把响应重新封装成 `RaftEvent` 投递回 `RaftNode`。

### [ ] F5.3 `KVServer` 与服务端主程序 `raftkv_server`

- **目标**：组装所有组件，编译出可独立运行的后端服务可执行文件。
- **文件位置**：`include/raftkv/server/kv_server.h`、`src/server/main.cc`
- **启动组装顺序（非常关键，务必按此顺序初始化）**：
  1. 解析命令行参数或配置文件（`--id=1 --addr=127.0.0.1:8001 --peers=1@...,2@...,3@... --data_dir=./data/node1`）。
  2. 启动 `LSMTree::Open(data_dir + "/kv_lsm")`。
  3. 初始化 `KVStateMachine`（挂载 `LSMTree`）。
  4. 初始化 `RealRaftTransport` 与 `RaftNode`（加载 `raft_wal`）。
  5. 初始化 `RpcServer`，注册客户端 KV RPC（Put/Get/Delete/Scan）与内部 Raft RPC。
  6. 启动 `RaftNode::Start()` 与 `RpcServer::Start()`，注册 `SIGINT / SIGTERM` 信号实现优雅关闭。

### [ ] F5.4 `KVClient` SDK 与交互式命令行工具 `raftkv_cli`

- **目标**：封装屏蔽分布式复杂度的客户端，让调用者像用单机哈希表一样使用集群。
- **文件位置**：`include/raftkv/client/kv_client.h`、`src/client/cli_main.cc`
- **必写逻辑**：
  - 自动生成唯一 `client_id_` 与单调递增 `seq_num_`。
  - 发送写请求遇到 `ERR_NOT_LEADER` 时，自动根据响应里的 `leader_hint` 切换目标节点并携带**相同 `seq_num_`**重试；遇到超时则轮询尝试其他节点。
- **测试验收**：本机开 3 个终端分别启动 3 个 `raftkv_server`，再用 `raftkv_cli` 执行 `put hello world`、`get hello`、`scan a z 10`、`del hello`，观察全部正常返回。

---

## Phase 6：集群演练、并发扫描与性能压测（Verification & Benchmark）

> **阶段目标**：用硬核测试和量化数据证明你的系统不仅能跑，而且高可用、无竞态、性能可观。

### [ ] F6.1 本地 3/5 节点集群一键脚本与 `kill -9` 演练

- **目标**：实现一键拉起集群、随机杀掉 Leader、验证业务自动恢复的自动化验收脚本。
- **文件位置**：`scripts/cluster_demo.sh`
- **验证动作**：启动 3 节点 → 持续写入 1,000 条数据 → `kill -9` 杀掉当前 Leader → 等待 1 秒继续写入 1,000 条 → 重启被杀节点 → 全量 `Scan` 校验 3 个节点数据 100% 一致。

### [ ] F6.2 ASan / UBSan / TSan 全量稳定性扫描

- **目标**：消灭隐藏的野指针、内存泄漏与多线程 Data Race。
- **执行命令**：

```bash
# 1. 内存与未定义行为检测
cmake -B build-asan -G Ninja -DENABLE_ASAN=ON && cmake --build build-asan
ctest --test-dir build-asan --output-on-failure

# 2. 多线程竞态检测
cmake -B build-tsan -G Ninja -DENABLE_TSAN=ON && cmake --build build-tsan
ctest --test-dir build-tsan --output-on-failure
```

### [ ] F6.3 多线程压测工具与性能优化对比报告

- **目标**：量化系统性能，产出可写进简历的真实基准数据。
- **文件位置**：`tests/bench/kv_bench.cc`
- **必测场景与指标**：
  - 分别跑 **100% Put**、**100% ReadIndex Get**、**70% 读 / 30% 写混合**、**Scan** 四组压测，统计 **QPS、P50 / P99 / P999 延迟**。
  - 对比开关变量并记录提升幅度：
    1. 读路径：**走 Raft 日志读** vs **开启 ReadIndex** 的 QPS 与延迟对比。
    2. 存储引擎：**关闭布隆过滤器** vs **开启布隆过滤器** 在随机查不存在 Key 时的延迟对比。
    3. 磁盘刷盘：**逐条 fsync** vs **开启 Group Commit 批量刷盘** 的写入 QPS 对比。
  - 使用 `perf record -F 99 -g -p <pid> -- sleep 30` 采集热点，确认无明显锁瓶颈或无效拷贝。

---

## 开发节奏与避坑速查表

| 常见新手坑 | 正确规避策略 |
| :--- | :--- |
| 一上来就把网络、Raft、LSM 混在一起写，出了 Bug 无从下手 | 严格按 Phase 1 → 2 → 3 → 4 的顺序，**每层单独写完并通过单元测试**，Phase 5 再组装 |
| 在 Raft 里到处加 `std::mutex` 导致死锁 | 坚持 **F4.3 的单线程 Actor 事件队列**，所有状态变更只在 `raft_thread_` 发生 |
| 先连真实 epoll 网络再调 Raft，排错极慢 | 务必先用 **F4.2 的内存 `SimTransport`** 把选举、分区、快照全测通，再换 `RealRaftTransport` |
| 忘记给 LSM-Tree 写 `MANIFEST` 或 WAL 恢复测试 | 写完 F3.2 和 F3.6 立刻用 `kill -9` 测试崩溃恢复，不要留到最后 |
