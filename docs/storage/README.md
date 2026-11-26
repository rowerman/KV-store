# storage

负责 LSM-Tree 存储引擎、WAL 恢复、内存表、SSTable、缓存、归并迭代器、
版本管理和 Checkpoint。

- 上游调用方：状态机 Apply、服务端存储装配、Raft 快照流程。
- 下游依赖：`common`、文件系统。
- 修改注意：写入顺序必须保持 WAL 先于可见内存状态；文件切换和 MANIFEST 更新要
  保证崩溃后可恢复；读路径不得持有阻塞后台线程的锁。

## InternalKey 与排序

- `InternalKey` 由 `user_key`、严格递增的 `sequence` 和 `ValueType` 组成。
- 排序先按用户键字节升序，再按序列号降序，使同一用户键的新版本排在旧版本前。
- 序列号相同时按 `ValueType` 数值降序，保证比较结果稳定。
- `kMaxSequenceNumber` 是点查哨兵，正常 MemTable 写入不能使用该序列号。
- `ValueType::kDeletion = 0`、`ValueType::kValue = 1`，后续 WAL 和 SSTable 必须
  保持相同磁盘编码。

## SkipList

- 最大高度固定为 12，提升概率为 1/4，随机种子由构造函数指定。
- 采用单写多读模型：只允许一个写线程调用 `Add`，任意多个读者可以使用迭代器和
  `Get`。
- 节点构造完成后通过 release store 发布 next 指针，读者使用 acquire load 获取。
- 节点在 SkipList 生命周期内只新增不删除；析构时必须确保没有并发读者。
- `Seek(Slice)` 使用最大序列号定位最新用户版本；`Seek(InternalKey)` 用于后续
  多路归并按完整内部键定位。
- `Next()` 只沿第 0 层前进，因此一次迭代按内部键全序返回全部版本。

## MemTable

- `Add` 接收严格递增序列号；非法枚举、最大序列号和重复/倒退序列号返回
  `InvalidArgument`。
- Put 保存 Value；Delete 以空 Value 墓碑写入，不能直接丢弃，否则无法屏蔽旧层。
- `Get` 返回指定用户键的最新版本。普通值返回
  `OK + is_deleted=false`，墓碑返回 `OK + 空值 + is_deleted=true`。
- `Get` 未找到返回 `NotFound`，且不修改输出参数。
- `NewIterator` 返回全部内部版本，不在 MemTable 层去重；Flush 写出全部版本，
  用户可见的去重由 MergingIterator 负责。
- `ApproximateMemoryUsage` 包含节点固定开销和字符串容量，用于后续 Flush 阈值，
  不要求与 allocator 实际占用逐字节一致。

## WAL

- WAL 记录由小端 `CRC32 + sequence + type + key_size + key + value_size + value`
  组成，CRC 覆盖除自身外的完整记录。
- `WalWriter::Append` 只写内核页缓存；只有 `Sync` 成功才保证崩溃持久性。
- `WalWriter` 使用 `O_CLOEXEC`，支持截断新建和 `O_APPEND` 追加两种打开方式。
- Delete 的 WAL Value 固定为空，与 MemTable 墓碑表示一致。
- 单条记录上限为 64 MiB，Writer 和 Reader 都在分配或写入前校验长度。
- `WalReader::Replay` 在正常 EOF 或末尾半条记录处返回 `OK`；CRC、非法类型、保留
  序列号和超限长度返回 `Corruption`。
- `Replay` 可选返回 CRC 完整前缀长度，便于上层修复时截断损坏尾部；回放本身只读。
- 回调中的 Key/Value Slice 只在当前回调期间有效。
- 当前不负责 WAL 轮转、删除、Group Commit 或 MemTable 恢复装配；上层必须在
  `Sync` 成功后再将写入应用到 MemTable。

## BloomFilter

- SSTable Filter Block 使用每 Key 10 bit、7 个哈希位置的 Bloom Filter。
- 持久化格式为 `magic + bit_count + bitmap`，整数使用小端编码。
- 哈希使用自实现 FNV-1a 双哈希，避免依赖 `std::hash` 的平台差异。
- `Build` 在临时字符串中完成构建，失败不修改输出。
- `KeyMayMatch` 返回 false 表示 Key 一定不存在；返回 true 表示可能存在。
- magic、长度或 bitmap 损坏时查询返回 true，宁可多读一次 Block，也不能漏掉真实 Key。

## LRUCache

- Block Cache 按近似字节容量淘汰，成本包含固定开销、Key 和 Value 大小。
- Value 使用 `shared_ptr<const std::string>`，淘汰只释放缓存持有的引用，正在读取的
  Block 仍然有效。
- 链表头为最近使用项，链表尾为最久未使用项；`Lookup` 命中会把节点移动到头部。
- 所有链表、哈希表、使用量和访问顺序都由同一个 mutex 保护。
- 单项超过总容量或 Value 为空时返回 `InvalidArgument`，已有缓存状态保持不变。
- 修改缓存键编码或成本公式时，应同步检查后续 SSTable Block Cache 的容量预算。

## SSTable

- 文件布局为 Data Blocks、Filter Block、Index Block、48 字节 Footer。
- Data、Filter、Index 三类块末尾都有 `payload_size + CRC32` 8 字节 trailer。
- Data Block 内按 InternalKey 严格升序，用户 Key 使用相邻前缀压缩。
- Delete 在 SSTable 中仍保存为空 Value 的墓碑，不能提前丢弃。
- Index 每个 Data Block 保存最后 InternalKey 和 `(offset, size)`。
- Reader Open 时校验 Footer magic、区域边界、Filter/Index CRC、索引顺序和 Data Block
  偏移连续性；Data Block 在首次读取时校验 CRC。
- Get 依次执行 Bloom Filter、索引二分、LRU Block Cache、Data Block 解码和块内二分。
- Block Cache key 为 `sstable path + block offset`，缓存内容为去掉 trailer 的 payload。
- Iterator 使用“索引 + 当前 Block 条目”的两级结构，跨块自动加载；后续块损坏通过
  `status()` 报告。
- SSTable 文件必须不可变。复用同一路径覆盖内容时不得继续使用旧 Block Cache。

## StorageIterator 与 MergingIterator

- `StorageIterator` 是存储层只向前的有序 InternalKey 接口，提供 `Valid`、
  `SeekToFirst`、两种 `Seek`、`Next`、`key`、`value` 和 `status`。
- `SkipList::Iterator` 与 `SSTableReader::Iterator` 都继承该接口；具体迭代器不拥有
  底层 MemTable 或 SSTable，数据源必须比迭代器存活更久。
- `MergingIterator` 通过 `unique_ptr` 独占全部子迭代器，内部堆只保存指向这些对象
  的非拥有裸指针，裸指针不得逃逸。
- 最小堆使用 `CompareInternalKeys`：用户 Key 升序；同 Key 序列号降序；同序列号时
  ValueType 数值降序。
- 每轮归并会消费当前用户 Key 在全部子迭代器中的版本。堆顶最新版本若是墓碑，整个
  用户 Key 隐藏；若是普通值，只输出该最新版本。
- 当前输出的 InternalKey 和 Value 会被复制，因此子迭代器 `Next` 使内部引用失效后
  仍可安全读取。
- 子迭代器 Seek 或 Next 失败后，归并迭代器立即失效并保留原始错误；再次 Seek 会
  清空旧错误并从头定位。
- 空 children 和空数据源合法。构造后必须显式调用 `SeekToFirst` 或 `Seek`。
- `Seek(Slice)` 定位用户 Key 的最新版本；`Seek(InternalKey)` 按完整内部顺序定位，
  因此在同用户 Key 内会先跳过内部顺序更靠前的更新版本。
- `MergingIterator` 自身有可变遍历状态，不允许并发调用；不同线程可以使用各自实例
  读取同一批只读数据源。
- 当前还没有具体 `Immutable MemTable` 类型和 LSMTree 顶层装配；VersionSet 已提供
  Flush、Compaction 和归并所需的基础设施。

## VersionSet、MANIFEST 与 Compaction

- 固定使用 L0-L3 四层；L0 文件数达到 4 触发 Compaction，L1-L3 容量分别为
  10 MiB、100 MiB、1 GiB，输出 SSTable 目标大小约 2 MiB。
- `Version` 保存各层 `FileMetaData`，发布后不可修改。读者持有
  `shared_ptr<const Version>` 期间，旧文件不会被删除。
- L0 按文件编号从新到旧排列并允许范围重叠；L1-L3 按最小用户 Key 升序且范围严格
  不重叠。
- `VersionEdit` 描述一次 Flush/Compaction 的新增和删除文件；Apply 先完整校验再
  产出新 Version，失败不修改输出。
- MANIFEST 以 Snapshot 开头，后续追加 Edit。每条记录使用
  `payload_size + CRC32 + payload`；完整帧 CRC 错误返回 Corruption，仅尾部物理截断
  可安全忽略并裁剪。
- CURRENT 通过临时文件、fsync、rename 和目录 fsync 原子引用当前 MANIFEST。恢复时
  只接受规范化的 `MANIFEST-XXXXXX` basename。
- `LogAndApply` 的顺序固定为：校验新增文件存在且大小一致、Apply、追加 MANIFEST
  并 fsync、发布新 Version。Manifest 写入失败后停止继续修改元数据。
- Flush 将只读 MemTable 的全部 InternalKey 写入 L0，MANIFEST 提交成功后才删除 WAL。
- L0 Compaction 选择全部 L0 文件；L1/L2 超容量时轮转选择文件。目标层按全部源文件
  的最小/最大 Key 包络选择重叠文件，避免输出跨过间隔后与保留文件重叠。
- Compaction 使用 `DeletionOutputPolicy::kKeep` 去重并保留最新墓碑。只有输出到 L3
  且所有更高层都不包含该用户 Key 时，才允许删除墓碑。
- 后台只有一个工作线程，Flush 优先于 Compaction。任务失败后保存错误、清空后续
  任务并停止自动调度。
- `WaitForBackgroundWork` 等待队列和当前任务全部结束，并触发无引用旧 Version 与
  崩溃遗留 SSTable 的清理。
- 当前使用单个 MANIFEST，不执行日志轮转；也不支持多进程同时打开同一数据目录。

## 构建与依赖

- `raftkv_storage` 当前只依赖 `raftkv_common`，不依赖网络、RPC 或 Protobuf。
- 修改跳表并发模型时，必须同步检查节点发布顺序、迭代器生命周期和销毁约束。
- 修改内部键比较规则时，必须同步更新 MemTable、后续 SSTable 比较器和磁盘格式。
- 修改 `StorageIterator` 接口时，必须同步检查 SkipList、SSTable 和 MergingIterator。
- 修改层数、文件命名或 MANIFEST 编码时，必须同时升级格式版本并验证 CURRENT 恢复。

