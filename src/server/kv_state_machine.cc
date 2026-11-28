#include "raftkv/server/kv_state_machine.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <string>
#include <system_error>
#include <utility>

#include "rpc_kv.pb.h"

namespace raftkv {
namespace {

namespace fs = std::filesystem;

std::string JoinPath(const fs::path& parent, const char* child) {
  return (parent / child).string();
}

Status CheckEmptyDirectory(const std::string& directory) {
  if (directory.empty()) {
    return Status::InvalidArgument(
        "kv snapshot directory is empty");
  }
  const fs::path path(directory);
  std::error_code error;
  const fs::file_status file_status =
      fs::symlink_status(path, error);
  if (error) {
    return Status::IOError(
        "inspect kv snapshot directory: " + error.message());
  }
  if (!fs::is_directory(file_status)) {
    return Status::InvalidArgument(
        "kv snapshot path is not a directory");
  }
  fs::directory_iterator iterator(path, error);
  if (error) {
    return Status::IOError(
        "scan kv snapshot directory: " + error.message());
  }
  if (iterator != fs::directory_iterator()) {
    return Status::InvalidArgument(
        "kv snapshot directory is not empty");
  }
  return Status::OK();
}

Status LinkDirectoryContents(
    const fs::path& source, const fs::path& destination) {
  std::error_code error;
  fs::directory_iterator iterator(source, error);
  if (error) {
    return Status::IOError(
        "scan lsm checkpoint: " + error.message());
  }
  const fs::directory_iterator end;
  while (iterator != end) {
    const fs::path target =
        destination / iterator->path().filename();
    const fs::file_status file_status =
        iterator->symlink_status(error);
    if (error) {
      return Status::IOError(
          "inspect lsm checkpoint entry: " + error.message());
    }
    if (fs::is_directory(file_status)) {
      if (!fs::create_directory(target, error) || error) {
        return Status::IOError(
            "create lsm checkpoint subdirectory: " +
            error.message());
      }
      Status status =
          LinkDirectoryContents(iterator->path(), target);
      if (!status.ok()) {
        return status;
      }
    } else if (fs::is_regular_file(file_status)) {
      fs::create_hard_link(iterator->path(), target, error);
      if (error) {
        return Status::IOError(
            "link lsm checkpoint file: " + error.message());
      }
    } else {
      return Status::Corruption(
          "lsm checkpoint contains an unsupported entry");
    }
    iterator.increment(error);
    if (error) {
      return Status::IOError(
          "advance lsm checkpoint scan: " + error.message());
    }
  }
  return Status::OK();
}

Status PrepareLsmRestoreCopy(
    const fs::path& state_directory, std::string* copy_path) {
  const fs::path source = state_directory / "lsm";
  std::error_code error;
  const fs::file_status source_status =
      fs::symlink_status(source, error);
  if (error) {
    return Status::IOError(
        "inspect lsm checkpoint: " + error.message());
  }
  if (!fs::is_directory(source_status)) {
    return Status::Corruption("lsm checkpoint is missing");
  }

  std::string pattern =
      (state_directory / "restore-XXXXXX").string();
  char* created = ::mkdtemp(pattern.data());
  if (created == nullptr) {
    return Status::IOError(
        "create lsm restore temporary directory");
  }
  *copy_path = created;

  // 快照会消费 Restore 源目录，因此先硬链接一份，保留可重复加载的源快照。
  Status status = LinkDirectoryContents(source, *copy_path);
  if (!status.ok()) {
    fs::remove_all(*copy_path, error);
    copy_path->clear();
  }
  return status;
}

}  // namespace

KVStateMachine::KVStateMachine(StorageEngine* storage)
    : storage_(storage) {}

Status KVStateMachine::Apply(
    uint64_t, const std::string& command) {
  if (storage_ == nullptr) {
    return Status::InvalidArgument(
        "kv state machine storage is null");
  }
  if (command.size() >
      static_cast<size_t>(std::numeric_limits<int>::max())) {
    return Status::Corruption("kv command payload is too large");
  }

  proto::KvCommand decoded;
  if (!decoded.ParseFromArray(
          command.data(), static_cast<int>(command.size()))) {
    return Status::Corruption("kv command cannot be decoded");
  }
  if (decoded.type() != proto::KV_COMMAND_TYPE_PUT &&
      decoded.type() != proto::KV_COMMAND_TYPE_DELETE) {
    return Status::InvalidArgument("kv command type is invalid");
  }
  if (decoded.client_id() == 0 || decoded.seq_num() == 0) {
    return Status::InvalidArgument(
        "kv command client id or sequence is zero");
  }
  if (decoded.type() == proto::KV_COMMAND_TYPE_DELETE &&
      !decoded.value().empty()) {
    return Status::InvalidArgument(
        "kv delete command has a value");
  }

  // 已执行序号及更小序号直接成功返回，避免重试产生第二次存储写入。
  if (!session_table_.ShouldApply(
          decoded.client_id(), decoded.seq_num())) {
    return Status::OK();
  }

  Status status = decoded.type() == proto::KV_COMMAND_TYPE_PUT
                      ? storage_->Put(decoded.key(), decoded.value())
                      : storage_->Delete(decoded.key());
  if (!status.ok()) {
    return status;
  }

  // 存储成功后推进水位；失败路径不得污染 SessionTable。
  session_table_.RecordApplied(
      decoded.client_id(), decoded.seq_num());
  return Status::OK();
}

Status KVStateMachine::TakeSnapshot(
    uint64_t, const std::string& directory) {
  if (storage_ == nullptr) {
    return Status::InvalidArgument(
        "kv state machine storage is null");
  }
  Status status = CheckEmptyDirectory(directory);
  if (!status.ok()) {
    return status;
  }

  const fs::path state_directory(directory);
  status = storage_->CreateCheckpoint(
      JoinPath(state_directory, "lsm"));
  if (!status.ok()) {
    return status;
  }
  return session_table_.Save(
      JoinPath(state_directory, "session_table.bin"));
}

Status KVStateMachine::LoadSnapshot(
    const std::string& directory) {
  if (storage_ == nullptr) {
    return Status::InvalidArgument(
        "kv state machine storage is null");
  }
  if (directory.empty()) {
    return Status::InvalidArgument(
        "kv snapshot directory is empty");
  }

  const fs::path state_directory(directory);
  std::error_code error;
  const fs::file_status state_status =
      fs::symlink_status(state_directory, error);
  if (error) {
    return Status::IOError(
        "inspect kv snapshot directory: " + error.message());
  }
  if (!fs::is_directory(state_status)) {
    return Status::InvalidArgument(
        "kv snapshot path is not a directory");
  }

  const fs::path session_path =
      state_directory / "session_table.bin";
  const fs::file_status session_status =
      fs::symlink_status(session_path, error);
  if (error) {
    return Status::IOError(
        "inspect session table snapshot: " + error.message());
  }
  if (!fs::is_regular_file(session_status)) {
    return Status::Corruption(
        "session table snapshot is missing");
  }

  // 先完整解码新去重表，LSM 恢复失败时不修改当前 SessionTable。
  SessionTable loaded_sessions;
  Status status = loaded_sessions.Load(session_path.string());
  if (!status.ok()) {
    return status;
  }

  std::string restore_copy;
  status = PrepareLsmRestoreCopy(
      state_directory, &restore_copy);
  if (!status.ok()) {
    return status;
  }
  status =
      storage_->RestoreFromCheckpoint(restore_copy);
  if (!status.ok()) {
    fs::remove_all(restore_copy, error);
    return status;
  }

  session_table_ = std::move(loaded_sessions);
  fs::remove_all(restore_copy, error);
  if (error) {
    return Status::IOError(
        "remove lsm restore temporary directory: " +
        error.message());
  }
  return Status::OK();
}

}  // namespace raftkv
