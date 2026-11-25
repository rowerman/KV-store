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

## RPC 服务端

- `RpcServer` 在 `TcpServer` 上解析请求帧，按 `method_id` 路由到注册处理器。
- 方法只能在 `Start()` 前注册；空处理器、重复 ID 和运行中注册都返回
  `InvalidArgument`。
- 处理器收到原始 `RpcFrame` 和一次性 `RpcReplyCallback`。成功时回复
  `kResponse`，错误时回复带 `RpcErrorResponse` 的 `kError`。
- `RpcReplyCallback` 可从其他线程调用。处理器必须先完成响应序列化再返回，且应
  避免在 IO 线程执行耗时同步工作。
- 请求帧损坏、非 `kRequest` 或 `request_id == 0` 时关闭连接；未知方法只返回错误
  响应，不影响连接继续使用。

## RPC 客户端

- `RpcClient::Call` 可在任意线程调用，请求消息在调用线程完成序列化，回调统一在
  客户端 `EventLoop` 线程执行。
- 每个请求使用独立非 0 `request_id` 和超时定时器。超时先移除在途记录，再回调
  `kTimeout`；迟到响应直接忽略。
- 已知请求 ID 的响应必须匹配原 `method_id`。错误响应必须能解析为
  `RpcErrorResponse`，否则按协议损坏处理。
- 连接断开时，该连接上的全部在途请求立即回调 `kTimeout`。新连接建立后只接受新
  请求，不自动重放旧请求。
- 响应损坏时完成全部在途请求并调用 `TcpClient::CloseConnection()`；该方法只关闭
  当前连接，不改变自动重连意图。

## 修改注意

- `RpcServer` 的响应发送只捕获连接和实现对象的弱引用，服务器停止后不得继续访问
  旧连接。
- `RpcClient` 的请求完成顺序必须是：移除在途表项、取消定时器、执行用户回调。
  回调内允许重新发起请求或销毁客户端。
- `TcpClient::CloseConnection()` 与 `Disconnect()` 语义不同：前者保留重连，
  后者由用户主动停止连接。

