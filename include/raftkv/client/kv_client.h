#ifndef RAFTKV_CLIENT_KV_CLIENT_H_
#define RAFTKV_CLIENT_KV_CLIENT_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "raftkv/common/status.h"
#include "raftkv/net/tcp_client.h"

namespace raftkv {

class EventLoop;

// 与服务端 Scan 上限保持一致，避免发起必然失败的请求。
inline constexpr uint32_t kKvClientMaxScanLimit = 10000;

// 一个 KVServer 对外 RPC 地址。
struct KVClientEndpoint {
  std::string host;
  uint16_t port = 0;
};

struct KVClientOptions {
  // 初始集群地址；至少配置一个。
  std::vector<KVClientEndpoint> endpoints;
  // 单次 RPC 尝试的超时，不是整个逻辑请求的总超时。
  uint64_t request_timeout_ms = 2000;
  // 首次尝试之外的最大重试次数。
  uint32_t max_retries = 3;
  TcpClientOptions tcp_client_options;
};

// 写请求最终完成回调；成功时 status 为 OK。
using KVClientWriteCallback = std::function<void(Status status)>;

// Get 完成回调；失败时 value 为空。
using KVClientGetCallback =
    std::function<void(Status status, std::string value)>;

struct KVClientScanResult {
  std::vector<std::pair<std::string, std::string>> entries;
  bool has_more = false;
};

// Scan 完成回调；失败时 entries 为空且 has_more 为 false。
using KVClientScanCallback =
    std::function<void(Status status, KVClientScanResult result)>;

// 自动处理 Leader 路由、节点切换和写请求幂等重试的异步客户端。
class KVClient final {
 public:
  static Status Create(
      EventLoop* loop,
      KVClientOptions options,
      std::unique_ptr<KVClient>* client);

  // 停止连接并完成挂起请求；不影响调用方拥有的 EventLoop。
  ~KVClient();

  KVClient(const KVClient&) = delete;
  KVClient& operator=(const KVClient&) = delete;

  // 连接全部初始 Endpoint；只允许启动一次。
  Status Start();

  // 主动停止并禁止重新启动；幂等。
  void Stop();

  // 返回值只表示请求是否被接受，最终结果通过 callback 返回。
  Status Put(std::string key, std::string value,
             KVClientWriteCallback callback);
  Status Delete(std::string key,
                KVClientWriteCallback callback);
  Status Get(std::string key, bool linearizable,
             KVClientGetCallback callback);
  Status Scan(std::string start_key, std::string end_key,
              uint32_t limit, KVClientScanCallback callback);

  // 自动生成且在整个客户端生命周期内保持不变。
  uint64_t ClientId() const;

 private:
  struct Impl;

  explicit KVClient(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;
};

}  // namespace raftkv

#endif  // RAFTKV_CLIENT_KV_CLIENT_H_
