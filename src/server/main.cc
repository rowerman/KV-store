#include <arpa/inet.h>
#include <charconv>
#include <csignal>
#include <cstdint>
#include <iostream>
#include <memory>
#include <pthread.h>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "raftkv/net/event_loop.h"
#include "raftkv/server/kv_server.h"

namespace raftkv {
namespace {

struct CommandLineOptions {
  uint32_t node_id = 0;
  RaftPeerAddress local_node;
  std::vector<RaftPeerAddress> peers;
  std::string listen_ip = "0.0.0.0";
  std::string data_directory;
  int rpc_io_threads = 0;
};

void PrintUsage(const char* program) {
  std::cerr
      << "usage: " << program
      << " --id=<id> --addr=<host:port>"
      << " --peers=<id@host:port,...> --data-dir=<path>"
      << " [--listen-ip=<ip>] [--rpc-io-threads=<n>]\n";
}

bool StartsWith(const std::string& value, const char* prefix) {
  return value.rfind(prefix, 0) == 0;
}

Status ParseUint32(const std::string& value, uint32_t* output) {
  if (output == nullptr || value.empty()) {
    return Status::InvalidArgument("unsigned integer is empty");
  }
  uint32_t parsed = 0;
  const auto result = std::from_chars(
      value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc() ||
      result.ptr != value.data() + value.size()) {
    return Status::InvalidArgument("invalid unsigned integer: " + value);
  }
  *output = parsed;
  return Status::OK();
}

Status ParseInt(const std::string& value, int* output) {
  if (output == nullptr || value.empty()) {
    return Status::InvalidArgument("integer is empty");
  }
  int parsed = 0;
  const auto result = std::from_chars(
      value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc() ||
      result.ptr != value.data() + value.size()) {
    return Status::InvalidArgument("invalid integer: " + value);
  }
  *output = parsed;
  return Status::OK();
}

Status ParseAddress(
    const std::string& value, std::string* host, uint16_t* port) {
  const size_t separator = value.find(':');
  if (separator == std::string::npos ||
      value.find(':', separator + 1) != std::string::npos) {
    return Status::InvalidArgument("invalid host:port address");
  }
  *host = value.substr(0, separator);
  uint32_t parsed_port = 0;
  Status status = ParseUint32(value.substr(separator + 1), &parsed_port);
  if (!status.ok() || parsed_port == 0 || parsed_port > 65535) {
    return Status::InvalidArgument("invalid address port");
  }
  *port = static_cast<uint16_t>(parsed_port);
  in_addr address {};
  if (host->empty() ||
      ::inet_pton(AF_INET, host->c_str(), &address) != 1) {
    return Status::InvalidArgument("address host is not IPv4");
  }
  return Status::OK();
}

Status ParsePeer(
    const std::string& value, RaftPeerAddress* peer) {
  const size_t at = value.find('@');
  if (at == std::string::npos ||
      value.find('@', at + 1) != std::string::npos) {
    return Status::InvalidArgument("invalid peer entry");
  }
  Status status = ParseUint32(value.substr(0, at), &peer->node_id);
  if (!status.ok() || peer->node_id == 0) {
    return Status::InvalidArgument("invalid peer node id");
  }
  return ParseAddress(
      value.substr(at + 1), &peer->host, &peer->port);
}

Status ParsePeerList(
    const std::string& value,
    std::vector<RaftPeerAddress>* peers) {
  peers->clear();
  if (value.empty()) {
    return Status::InvalidArgument("peer list is empty");
  }
  size_t begin = 0;
  while (begin <= value.size()) {
    const size_t end = value.find(',', begin);
    const std::string entry = value.substr(
        begin, end == std::string::npos
                   ? std::string::npos
                   : end - begin);
    RaftPeerAddress peer;
    Status status = ParsePeer(entry, &peer);
    if (!status.ok()) {
      return status;
    }
    peers->push_back(std::move(peer));
    if (end == std::string::npos) {
      break;
    }
    begin = end + 1;
    if (begin == value.size()) {
      return Status::InvalidArgument("peer list has an empty entry");
    }
  }
  return Status::OK();
}

Status ParseCommandLine(
    int argc, char** argv, CommandLineOptions* options,
    bool* help_requested) {
  std::string addr;
  std::string peers_text;
  bool has_id = false;
  bool has_addr = false;
  bool has_peers = false;
  bool has_data = false;
  *help_requested = false;

  for (int index = 1; index < argc; ++index) {
    const std::string argument(argv[index]);
    if (argument == "--help") {
      *help_requested = true;
      return Status::OK();
    }
    if (StartsWith(argument, "--id=")) {
      Status status =
          ParseUint32(argument.substr(5), &options->node_id);
      if (!status.ok() || options->node_id == 0) {
        return Status::InvalidArgument("invalid --id");
      }
      has_id = true;
    } else if (StartsWith(argument, "--addr=")) {
      addr = argument.substr(7);
      has_addr = true;
    } else if (StartsWith(argument, "--peers=")) {
      peers_text = argument.substr(8);
      has_peers = true;
    } else if (StartsWith(argument, "--data-dir=")) {
      options->data_directory = argument.substr(11);
      has_data = true;
    } else if (StartsWith(argument, "--listen-ip=")) {
      options->listen_ip = argument.substr(12);
    } else if (StartsWith(argument, "--rpc-io-threads=")) {
      Status status = ParseInt(
          argument.substr(17), &options->rpc_io_threads);
      if (!status.ok() || options->rpc_io_threads < 0) {
        return Status::InvalidArgument("invalid --rpc-io-threads");
      }
    } else {
      return Status::InvalidArgument(
          "unknown command line option: " + argument);
    }
  }
  if (!has_id || !has_addr || !has_peers || !has_data) {
    return Status::InvalidArgument(
        "--id, --addr, --peers and --data-dir are required");
  }

  std::vector<RaftPeerAddress> configured_peers;
  Status status = ParsePeerList(peers_text, &configured_peers);
  if (!status.ok()) {
    return status;
  }
  bool found_local = false;
  std::unordered_set<uint32_t> ids;
  for (const RaftPeerAddress& peer : configured_peers) {
    if (!ids.insert(peer.node_id).second) {
      return Status::InvalidArgument("duplicate peer node id");
    }
    if (peer.node_id == options->node_id) {
      found_local = true;
    } else {
      options->peers.push_back(peer);
    }
  }
  if (!found_local) {
    return Status::InvalidArgument(
        "--id is not present in --peers");
  }

  options->local_node.node_id = options->node_id;
  status = ParseAddress(
      addr, &options->local_node.host, &options->local_node.port);
  return status;
}

}  // namespace
}  // namespace raftkv

int main(int argc, char** argv) {
  using namespace raftkv;

  CommandLineOptions command_line;
  bool help_requested = false;
  Status status = ParseCommandLine(
      argc, argv, &command_line, &help_requested);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    PrintUsage(argv[0]);
    return 1;
  }
  if (help_requested) {
    PrintUsage(argv[0]);
    return 0;
  }

  // 在创建 Raft/LSM 线程前屏蔽信号，确保只有 sigwait 线程消费终止信号。
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  sigaddset(&signals, SIGTERM);
  if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
    std::cerr << "failed to block termination signals\n";
    return 1;
  }

  EventLoop loop;
  KVServerOptions options;
  options.local_node = command_line.local_node;
  options.peers = std::move(command_line.peers);
  options.listen_ip = command_line.listen_ip;
  options.data_directory = command_line.data_directory;
  options.rpc_io_threads = command_line.rpc_io_threads;

  std::unique_ptr<KVServer> server;
  status = KVServer::Create(&loop, std::move(options), &server);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    pthread_sigmask(SIG_UNBLOCK, &signals, nullptr);
    return 1;
  }
  status = server->Start();
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    server->Stop();
    pthread_sigmask(SIG_UNBLOCK, &signals, nullptr);
    return 1;
  }

  std::thread signal_thread([&loop, signals] {
    int signal = 0;
    (void)sigwait(&signals, &signal);
    loop.Quit();
  });

  std::cout << "raftkv_server node " << command_line.node_id
            << " listening on " << command_line.local_node.host
            << ":" << server->Port() << '\n';
  loop.Loop();

  server->Stop();
  signal_thread.join();
  pthread_sigmask(SIG_UNBLOCK, &signals, nullptr);
  return 0;
}
