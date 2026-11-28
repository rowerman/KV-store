# client

负责客户端 SDK 和命令行工具，包括连接复用、请求序号、Leader 路由、
超时与幂等重试。

- 上游调用方：外部应用程序、交互式 CLI。
- 下游依赖：`rpc`、Protobuf 生成类型、`net/EventLoop`。
- 修改注意：同一次逻辑写入重试时必须保持 `client_id + seq_num` 不变；切换 Leader
  后不得复用其他逻辑请求的序号。

## KVClient

- `KVClient::Create` 校验 Endpoint、请求超时并生成非零客户端 ID；不会发起连接。
- `Start` 幂等，为每个初始 Endpoint 建立独立 `RpcClient` 长连接。
- `Stop` 幂等并禁止重启；停止时所有已接受请求都以 Timeout 完成一次。
- `KVClient` 不拥有 EventLoop。EventLoop 必须晚于客户端连接和挂起回调销毁。
- 公共操作返回的 Status 只表示请求是否被接受，网络和业务结果通过回调返回。
- 所有回调在客户端所属 EventLoop 线程执行；调用方可在任意线程提交请求。

Endpoint 当前只支持数字 IPv4 和显式非零端口。收到合法 `leader_hint` 时，
KVClient 会动态加入 Hint 指向的 Endpoint，因此只配置部分 Follower 也能切换
到真实 Leader。Hint 在创建连接前仍执行 IPv4、端口和规范化校验。

## 写路径

- Put 与 Delete 共用单调递增序号，`client_id` 在客户端生命周期内不变。
- 同一客户端的逻辑写请求串行执行，前一个完成后才分配下一个序号。
- 串行化用于避免较大的序号先提交后，较小序号被 SessionTable 判为旧请求而跳过。
- NotLeader 优先按 `leader_hint` 重试；Timeout、连接断开和协议损坏轮询其他节点。
- 每次重试复用同一 `client_id + seq_num`，包括第一次请求已经提交但响应丢失的情况。
- 重试之间使用 TcpClient 的首个重连间隔退避，并在等待期间预先连接目标节点。
- 写请求最终 Timeout 时结果可能已提交，也可能未提交。客户端会进入写不可继续状态，
  后续 Put/Delete 返回 Timeout；调用方应创建新 KVClient 后重新发起逻辑请求。

## 读路径

- Get 显式传入 `linearizable`：true 走 KVServer 的 ReadIndex 路径，false 允许本地读。
- Scan 使用半开区间 `[start_key, end_key)`，结束 Key 为空表示扫描到末尾。
- Scan limit 必须为 1-10000；结果通过 `KVClientScanResult.entries/has_more` 返回。
- Get 和 Scan 可以并发；NotLeader、Timeout 和协议损坏会按与写路径相同的规则重试。
- 业务 NotFound、InvalidArgument、Internal 不重试；NotLeader 在重试耗尽后保留
  Leader Hint 文本供调用方诊断。

## raftkv_cli

```bash
raftkv_cli \
  --endpoints=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 \
  --timeout-ms=2000 \
  --max-retries=3
```

交互命令：

```text
put <key> <value>
get <key>
del <key>
scan <start> <end|-> <limit>
help
quit
```

Scan 的 `-` 表示空结束 Key。`--help` 不要求 Endpoint；其余启动场景必须提供
`--endpoints`。

## 构建与测试

- 静态库目标为 `raftkv_client`，命令行目标为 `raftkv_cli`。
- `raftkv_client` 公开依赖 `raftkv_rpc`，测试目标额外链接 `raftkv_server_lib`
  以运行三个真实 KVServer 的端到端用例。
- `tests/kv_client_test.cc` 覆盖配置、生命周期、Leader Hint、超时换节点、固定幂等
  序号、写串行化、读写重试、不确定写结果和停机回调。
- Debug、ASan/UBSan、TSan 下均为 10/10 通过；TSan 每项连续重复 10 轮通过。

