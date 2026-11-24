# storage

负责 LSM-Tree 存储引擎、WAL 恢复、内存表、SSTable、缓存、归并迭代器、
版本管理和 Checkpoint。

- 上游调用方：状态机 Apply、服务端存储装配、Raft 快照流程。
- 下游依赖：`common`、文件系统。
- 修改注意：写入顺序必须保持 WAL 先于可见内存状态；文件切换和 MANIFEST 更新要
  保证崩溃后可恢复；读路径不得持有阻塞后台线程的锁。

