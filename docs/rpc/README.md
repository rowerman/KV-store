# rpc

负责帧编解码、方法路由、请求匹配、超时控制和服务端/客户端 RPC 封装。

- 上游调用方：`server`、`raft` 的真实传输实现、`client`。
- 下游依赖：`net`、`common`、`proto` 生成的 Protobuf 类型。
- 修改注意：帧长度必须限制上限；校验失败不能继续消费载荷；超时与连接断开必须
  确保每个挂起请求只完成一次。

## 帧协议约定

- 帧头固定 20 字节，依次包含大端 `magic/version/msg_type/method_id/request_id/
  payload_len`；payload 后跟 4 字节大端 CRC32。
- `magic` 固定为 `0x524B`，当前版本为 `1`，payload 上限为 16 MiB。
- `TryDecode` 只有成功时才消费输入；半包和损坏帧都保留原 Buffer。
- 遇到非法 magic、version、消息类型、超长 payload 或 CRC 错误时返回
  `kCorrupt`，调用方应关闭连接，不做流内重新同步。
- 单次成功解码只消费一个帧，后续粘连帧保留在 Buffer 中。

## Protobuf 与构建约定

- `proto/rpc_kv.proto` 定义客户端 KV 请求和响应。
- `proto/rpc_raft.proto` 定义投票、日志复制和快照传输消息。
- `raftkv_proto` 生成并公开 Protobuf C++ 代码，`raftkv_rpc` 单向依赖
  `raftkv_net` 与 `raftkv_proto`。
- Protobuf 36 在 ASan 下会自动切换 `ClassData` ABI；构建时必须定义
  `PROTOBUF_MESSAGE_GLOBALS_TEMPORARY_OPTOUT`，与当前预编译 Protobuf 库保持
  一致。

