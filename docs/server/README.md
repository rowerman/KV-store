# server

负责组装网络 RPC、Raft、存储引擎和状态机，实现客户端 KV 请求处理、
Leader 路由提示、幂等去重及节点生命周期管理。

- 上游调用方：`raftkv_server` 入口程序和 RPC 方法分发。
- 下游依赖：`rpc`、`raft`、`storage`、`common`。
- 修改注意：网络线程不能直接修改 Raft 状态；写请求必须经共识提交后 Apply；
  ReadIndex 未确认前不得返回线性一致读结果。

