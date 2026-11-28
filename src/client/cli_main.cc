#include <charconv>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "raftkv/client/kv_client.h"
#include "raftkv/net/event_loop.h"

namespace raftkv {
namespace {

struct CommandLineOptions {
  KVClientOptions client_options;
  bool help = false;
};

void PrintUsage(const char* program) {
  std::cerr
      << "usage: " << program
      << " --endpoints=<host:port,...>"
      << " [--timeout-ms=<n>] [--max-retries=<n>]\n";
}

bool StartsWith(const std::string& value, const char* prefix) {
  return value.rfind(prefix, 0) == 0;
}

template <typename Integer>
Status ParseInteger(const std::string& value, Integer* output) {
  if (output == nullptr || value.empty()) {
    return Status::InvalidArgument("integer is empty");
  }
  Integer parsed = 0;
  const auto result = std::from_chars(
      value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc() ||
      result.ptr != value.data() + value.size()) {
    return Status::InvalidArgument(
        "invalid integer: " + value);
  }
  *output = parsed;
  return Status::OK();
}

Status ParseEndpoint(
    const std::string& value, KVClientEndpoint* endpoint) {
  const size_t separator = value.find(':');
  if (separator == std::string::npos ||
      value.find(':', separator + 1) != std::string::npos) {
    return Status::InvalidArgument(
        "endpoint must be host:port");
  }
  endpoint->host = value.substr(0, separator);
  uint32_t port = 0;
  Status status =
      ParseInteger(value.substr(separator + 1), &port);
  if (!status.ok() || port == 0 || port > 65535) {
    return Status::InvalidArgument(
        "endpoint port is invalid");
  }
  endpoint->port = static_cast<uint16_t>(port);
  return Status::OK();
}

Status ParseEndpointList(
    const std::string& value,
    std::vector<KVClientEndpoint>* endpoints) {
  endpoints->clear();
  if (value.empty()) {
    return Status::InvalidArgument(
        "endpoint list is empty");
  }

  size_t begin = 0;
  while (begin <= value.size()) {
    const size_t end = value.find(',', begin);
    const std::string item = value.substr(
        begin, end == std::string::npos
                   ? std::string::npos
                   : end - begin);
    KVClientEndpoint endpoint;
    Status status = ParseEndpoint(item, &endpoint);
    if (!status.ok()) {
      return status;
    }
    endpoints->push_back(std::move(endpoint));
    if (end == std::string::npos) {
      break;
    }
    begin = end + 1;
    if (begin == value.size()) {
      return Status::InvalidArgument(
          "endpoint list has an empty entry");
    }
  }
  return Status::OK();
}

Status ParseCommandLine(
    int argc, char** argv, CommandLineOptions* options) {
  std::string endpoints;
  bool has_endpoints = false;
  for (int index = 1; index < argc; ++index) {
    const std::string argument(argv[index]);
    if (argument == "--help") {
      options->help = true;
      return Status::OK();
    }
    if (StartsWith(argument, "--endpoints=")) {
      endpoints = argument.substr(12);
      has_endpoints = true;
    } else if (StartsWith(argument, "--timeout-ms=")) {
      Status status = ParseInteger(
          argument.substr(13),
          &options->client_options.request_timeout_ms);
      if (!status.ok() ||
          options->client_options.request_timeout_ms == 0) {
        return Status::InvalidArgument(
            "invalid --timeout-ms");
      }
    } else if (StartsWith(argument, "--max-retries=")) {
      Status status = ParseInteger(
          argument.substr(14),
          &options->client_options.max_retries);
      if (!status.ok()) {
        return Status::InvalidArgument(
            "invalid --max-retries");
      }
    } else {
      return Status::InvalidArgument(
          "unknown command line option: " + argument);
    }
  }

  if (!has_endpoints) {
    return Status::InvalidArgument(
        "--endpoints is required");
  }
  return ParseEndpointList(
      endpoints, &options->client_options.endpoints);
}

std::string TrimLeadingWhitespace(std::string value) {
  const size_t begin = value.find_first_not_of(" \t");
  return begin == std::string::npos ? std::string() : value.substr(begin);
}

bool IsQuitCommand(const std::string& line) {
  std::istringstream input(line);
  std::string command;
  input >> command;
  return command == "quit" || command == "exit";
}

void PrintStatus(const Status& status) {
  std::cout << status.ToString() << '\n';
}

void ExecutePut(
    KVClient* client, std::istringstream* input) {
  std::string key;
  if (!(*input >> key)) {
    std::cout << "usage: put <key> <value>\n";
    return;
  }
  std::string value;
  std::getline(*input, value);
  value = TrimLeadingWhitespace(std::move(value));
  Status status = client->Put(
      std::move(key), std::move(value),
      [](Status result) {
        std::cout << (result.ok() ? "OK" : result.ToString())
                  << '\n';
      });
  if (!status.ok()) {
    PrintStatus(status);
  }
}

void ExecuteGet(
    KVClient* client, std::istringstream* input) {
  std::string key;
  if (!(*input >> key)) {
    std::cout << "usage: get <key>\n";
    return;
  }
  Status status = client->Get(
      std::move(key), true,
      [](Status result, std::string value) {
        if (!result.ok()) {
          PrintStatus(result);
          return;
        }
        std::cout << value << '\n';
      });
  if (!status.ok()) {
    PrintStatus(status);
  }
}

void ExecuteDelete(
    KVClient* client, std::istringstream* input) {
  std::string key;
  if (!(*input >> key)) {
    std::cout << "usage: del <key>\n";
    return;
  }
  Status status = client->Delete(
      std::move(key),
      [](Status result) {
        std::cout << (result.ok() ? "OK" : result.ToString())
                  << '\n';
      });
  if (!status.ok()) {
    PrintStatus(status);
  }
}

void ExecuteScan(
    KVClient* client, std::istringstream* input) {
  std::string start;
  std::string end;
  std::string limit_text;
  if (!(*input >> start >> end >> limit_text)) {
    std::cout << "usage: scan <start> <end|-> <limit>\n";
    return;
  }
  uint32_t limit = 0;
  Status status = ParseInteger(limit_text, &limit);
  if (!status.ok() || limit == 0 ||
      limit > kKvClientMaxScanLimit) {
    std::cout << "scan limit must be in [1, "
              << kKvClientMaxScanLimit << "]\n";
    return;
  }
  if (end == "-") {
    end.clear();
  }

  status = client->Scan(
      std::move(start), std::move(end), limit,
      [](Status result, KVClientScanResult scan_result) {
        if (!result.ok()) {
          PrintStatus(result);
          return;
        }
        for (const auto& entry : scan_result.entries) {
          std::cout << entry.first << '\t' << entry.second << '\n';
        }
        std::cout << "has_more="
                  << (scan_result.has_more ? "true" : "false")
                  << '\n';
      });
  if (!status.ok()) {
    PrintStatus(status);
  }
}

void ExecuteCommand(KVClient* client, const std::string& line) {
  std::istringstream input(line);
  std::string command;
  input >> command;
  if (command.empty()) {
    return;
  }
  if (command == "help") {
    std::cout
        << "commands:\n"
        << "  put <key> <value>\n"
        << "  get <key>\n"
        << "  del <key>\n"
        << "  scan <start> <end|-> <limit>\n"
        << "  quit\n";
    return;
  }
  if (command == "put") {
    ExecutePut(client, &input);
    return;
  }
  if (command == "get") {
    ExecuteGet(client, &input);
    return;
  }
  if (command == "del" || command == "delete") {
    ExecuteDelete(client, &input);
    return;
  }
  if (command == "scan") {
    ExecuteScan(client, &input);
    return;
  }
  std::cout << "unknown command: " << command << '\n';
}

}  // namespace
}  // namespace raftkv

int main(int argc, char** argv) {
  using namespace raftkv;

  CommandLineOptions command_line;
  Status status = ParseCommandLine(argc, argv, &command_line);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    PrintUsage(argv[0]);
    return 1;
  }
  if (command_line.help) {
    PrintUsage(argv[0]);
    return 0;
  }

  EventLoop loop;
  std::unique_ptr<KVClient> client;
  status = KVClient::Create(
      &loop, std::move(command_line.client_options), &client);
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }
  status = client->Start();
  if (!status.ok()) {
    std::cerr << status.ToString() << '\n';
    return 1;
  }

  std::thread input_thread([&loop, &client] {
    std::string line;
    while (true) {
      std::cout << "raftkv> " << std::flush;
      if (!std::getline(std::cin, line)) {
        break;
      }
      const bool quit = IsQuitCommand(line);
      loop.QueueInLoop(
          [&loop, &client, line = std::move(line), quit]() mutable {
            if (quit) {
              client->Stop();
              loop.Quit();
              return;
            }
            ExecuteCommand(client.get(), line);
          });
      if (quit) {
        return;
      }
    }
    loop.QueueInLoop([&loop, &client] {
      client->Stop();
      loop.Quit();
    });
  });

  loop.Loop();
  input_thread.join();
  client.reset();
  return 0;
}
