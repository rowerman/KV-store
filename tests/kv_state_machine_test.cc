#include "raftkv/server/kv_state_machine.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "raftkv/storage/lsm_tree.h"
#include "rpc_kv.pb.h"

namespace raftkv {
namespace {

class TempDir {
 public:
  TempDir() {
    char pattern[] = "/tmp/raftkv-kv-state-machine-XXXXXX";
    const char* created = ::mkdtemp(pattern);
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed");
    }
    path_ = created;
  }

  ~TempDir() {
    std::error_code error;
    std::filesystem::remove_all(path_, error);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  std::string Path(const std::string& name) const {
    return path_ + "/" + name;
  }

 private:
  std::string path_;
};

std::string EncodeCommand(
    proto::KvCommandType type, uint64_t client_id, uint64_t seq_num,
    const std::string& key, const std::string& value) {
  proto::KvCommand command;
  command.set_type(type);
  command.set_client_id(client_id);
  command.set_seq_num(seq_num);
  command.set_key(key);
  command.set_value(value);
  return command.SerializeAsString();
}

Status ApplyCommand(
    KVStateMachine* machine, uint64_t index,
    proto::KvCommandType type, uint64_t client_id, uint64_t seq_num,
    const std::string& key, const std::string& value) {
  return machine->Apply(
      index,
      EncodeCommand(type, client_id, seq_num, key, value));
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("open file for read failed");
  }
  return std::string(
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>());
}

void WriteFile(const std::string& path, const std::string& data) {
  std::ofstream output(
      path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("open file for write failed");
  }
  output.write(data.data(), static_cast<std::streamsize>(data.size()));
  if (!output.good()) {
    throw std::runtime_error("write file failed");
  }
}

class FailingStorage final : public StorageEngine {
 public:
  Status Put(const std::string&, const std::string&) override {
    ++put_calls;
    return fail_writes
               ? Status::IOError("injected put failure")
               : Status::OK();
  }

  Status Delete(const std::string&) override {
    ++delete_calls;
    return fail_writes
               ? Status::IOError("injected delete failure")
               : Status::OK();
  }

  Status Get(const std::string&, std::string*) override {
    return Status::NotFound("not stored");
  }

  Status Scan(
      const std::string&, const std::string&, size_t,
      std::vector<std::pair<std::string, std::string>>*) override {
    return Status::OK();
  }

  Status CreateCheckpoint(const std::string&) override {
    return Status::OK();
  }

  Status RestoreFromCheckpoint(const std::string&) override {
    return Status::OK();
  }

  bool fail_writes = false;
  uint64_t put_calls = 0;
  uint64_t delete_calls = 0;
};

TEST(KVStateMachineTest, AppliesPutDeleteAndDeduplicatesPerClient) {
  TempDir directory;
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), &tree).ok());
  KVStateMachine machine(tree.get());

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 1, proto::KV_COMMAND_TYPE_PUT,
          7, 1, "a", "first")
          .ok());
  std::string value;
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "first");

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 2, proto::KV_COMMAND_TYPE_PUT,
          7, 1, "a", "retry-must-not-win")
          .ok());
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "first");

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 3, proto::KV_COMMAND_TYPE_DELETE,
          7, 2, "a", "")
          .ok());
  EXPECT_EQ(tree->Get("a", &value).code(), Code::kNotFound);

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 4, proto::KV_COMMAND_TYPE_PUT,
          7, 1, "a", "old-sequence")
          .ok());
  EXPECT_EQ(tree->Get("a", &value).code(), Code::kNotFound);

  const std::string binary_key("key\0binary", 10);
  const std::string binary_value("value\0binary", 12);
  ASSERT_TRUE(
      ApplyCommand(
          &machine, 5, proto::KV_COMMAND_TYPE_PUT,
          8, 1, binary_key, binary_value)
          .ok());
  ASSERT_TRUE(tree->Get(binary_key, &value).ok());
  EXPECT_EQ(value, binary_value);
}

TEST(KVStateMachineTest, RejectsMalformedCommandsBeforeStorage) {
  TempDir directory;
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), &tree).ok());
  KVStateMachine machine(tree.get());

  EXPECT_EQ(
      machine.Apply(1, std::string("\xff", 1)).code(),
      Code::kCorruption);
  EXPECT_EQ(
      machine.Apply(
          2, EncodeCommand(
                 proto::KV_COMMAND_TYPE_UNSPECIFIED,
                 1, 1, "a", "v"))
          .code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      machine.Apply(
          3, EncodeCommand(
                 proto::KV_COMMAND_TYPE_PUT,
                 0, 1, "a", "v"))
          .code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      machine.Apply(
          4, EncodeCommand(
                 proto::KV_COMMAND_TYPE_PUT,
                 1, 0, "a", "v"))
          .code(),
      Code::kInvalidArgument);
  EXPECT_EQ(
      machine.Apply(
          5, EncodeCommand(
                 proto::KV_COMMAND_TYPE_DELETE,
                 1, 1, "a", "unexpected"))
          .code(),
      Code::kInvalidArgument);

  std::string value;
  EXPECT_EQ(tree->Get("a", &value).code(), Code::kNotFound);
}

TEST(KVStateMachineTest, StorageFailureDoesNotAdvanceSession) {
  FailingStorage storage;
  storage.fail_writes = true;
  KVStateMachine machine(&storage);

  const std::string command =
      EncodeCommand(
          proto::KV_COMMAND_TYPE_PUT, 4, 9, "a", "value");
  EXPECT_EQ(machine.Apply(1, command).code(), Code::kIOError);
  EXPECT_EQ(storage.put_calls, 1U);

  storage.fail_writes = false;
  EXPECT_TRUE(machine.Apply(2, command).ok());
  EXPECT_EQ(storage.put_calls, 2U);
  EXPECT_TRUE(machine.Apply(3, command).ok());
  EXPECT_EQ(storage.put_calls, 2U);
}

TEST(KVStateMachineTest, SnapshotRollsBackAndCanBeLoadedRepeatedly) {
  TempDir directory;
  const std::string state_directory =
      directory.Path("snapshot");
  ASSERT_TRUE(
      std::filesystem::create_directory(state_directory));

  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), &tree).ok());
  KVStateMachine machine(tree.get());

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 1, proto::KV_COMMAND_TYPE_PUT,
          1, 1, "a", "old-a")
          .ok());
  ASSERT_TRUE(
      ApplyCommand(
          &machine, 2, proto::KV_COMMAND_TYPE_PUT,
          1, 2, "b", "value-b")
          .ok());
  ASSERT_TRUE(machine.TakeSnapshot(2, state_directory).ok());
  EXPECT_TRUE(
      std::filesystem::is_directory(state_directory + "/lsm"));
  EXPECT_TRUE(
      std::filesystem::is_regular_file(
          state_directory + "/session_table.bin"));

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 3, proto::KV_COMMAND_TYPE_PUT,
          1, 3, "a", "new-a")
          .ok());
  ASSERT_TRUE(
      ApplyCommand(
          &machine, 4, proto::KV_COMMAND_TYPE_DELETE,
          1, 4, "b", "")
          .ok());
  ASSERT_TRUE(tree->Put("c", "value-c").ok());

  ASSERT_TRUE(machine.LoadSnapshot(state_directory).ok());
  std::string value;
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "old-a");
  ASSERT_TRUE(tree->Get("b", &value).ok());
  EXPECT_EQ(value, "value-b");
  EXPECT_EQ(tree->Get("c", &value).code(), Code::kNotFound);

  ASSERT_TRUE(machine.LoadSnapshot(state_directory).ok());

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 5, proto::KV_COMMAND_TYPE_PUT,
          1, 1, "a", "duplicate-must-not-win")
          .ok());
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "old-a");

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 6, proto::KV_COMMAND_TYPE_PUT,
          1, 3, "a", "post-restore")
          .ok());
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "post-restore");
}

TEST(KVStateMachineTest, CorruptSessionSnapshotDoesNotTouchLsmOrSession) {
  TempDir directory;
  const std::string state_directory =
      directory.Path("snapshot");
  ASSERT_TRUE(
      std::filesystem::create_directory(state_directory));

  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), &tree).ok());
  KVStateMachine machine(tree.get());

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 1, proto::KV_COMMAND_TYPE_PUT,
          1, 1, "a", "old-a")
          .ok());
  ASSERT_TRUE(machine.TakeSnapshot(1, state_directory).ok());
  ASSERT_TRUE(
      ApplyCommand(
          &machine, 2, proto::KV_COMMAND_TYPE_PUT,
          1, 2, "b", "new-b")
          .ok());

  const std::string session_path =
      state_directory + "/session_table.bin";
  std::string encoded = ReadFile(session_path);
  ASSERT_FALSE(encoded.empty());
  encoded.back() ^= 0x01;
  WriteFile(session_path, encoded);

  EXPECT_EQ(
      machine.LoadSnapshot(state_directory).code(),
      Code::kCorruption);
  std::string value;
  ASSERT_TRUE(tree->Get("a", &value).ok());
  EXPECT_EQ(value, "old-a");
  ASSERT_TRUE(tree->Get("b", &value).ok());
  EXPECT_EQ(value, "new-b");

  ASSERT_TRUE(
      ApplyCommand(
          &machine, 3, proto::KV_COMMAND_TYPE_PUT,
          1, 2, "b", "duplicate-must-not-win")
          .ok());
  ASSERT_TRUE(tree->Get("b", &value).ok());
  EXPECT_EQ(value, "new-b");
}

TEST(KVStateMachineTest, SnapshotRequiresEmptyDirectory) {
  TempDir directory;
  std::unique_ptr<LSMTree> tree;
  ASSERT_TRUE(
      LSMTree::Open(directory.Path("db"), &tree).ok());
  KVStateMachine machine(tree.get());

  const std::string state_directory =
      directory.Path("snapshot");
  ASSERT_TRUE(
      std::filesystem::create_directory(state_directory));
  WriteFile(state_directory + "/foreign", "data");
  EXPECT_EQ(
      machine.TakeSnapshot(1, state_directory).code(),
      Code::kInvalidArgument);
}

}  // namespace
}  // namespace raftkv
