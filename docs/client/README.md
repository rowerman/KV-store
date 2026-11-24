# client

负责客户端 SDK 和命令行工具，包括连接复用、请求序号、Leader 路由、
超时与幂等重试。

- 上游调用方：外部应用程序、交互式 CLI。
- 下游依赖：`rpc`、Protobuf 生成类型。
- 修改注意：同一次逻辑写入重试时必须保持 `client_id + seq_num` 不变；切换 Leader
  后不得复用其他逻辑请求的序号。

