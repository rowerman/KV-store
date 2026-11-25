#include "raftkv/storage/sstable.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "raftkv/common/coding.h"
#include "raftkv/common/crc32.h"
#include "raftkv/storage/bloom_filter.h"
#include "raftkv/storage/lru_cache.h"

namespace raftkv {
namespace {

struct BlockHandle {
  uint64_t offset = 0;
  uint32_t size = 0;
};

struct IndexEntry {
  InternalKey last_key;
  BlockHandle block;
};

struct BlockEntry {
  InternalKey key;
  std::string value;
};

struct Footer {
  uint64_t filter_offset = 0;
  uint64_t filter_size = 0;
  uint64_t index_offset = 0;
  uint64_t index_size = 0;
  uint64_t entry_count = 0;
};

Status MakeSSTableIoError(const char* operation) {
  return Status::IOError(
      std::string(operation) + ": " + std::strerror(errno));
}

bool RangeFits(uint64_t offset, uint64_t size, uint64_t limit) {
  return offset <= limit && size <= limit - offset;
}

Status AppendBlock(Slice payload, std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("sstable block output is null");
  }
  if (payload.size() > kSSTableMaxBlockSize ||
      payload.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("sstable block is too large");
  }

  std::string block(kSSTableBlockTrailerSize + payload.size(), '\0');
  if (!payload.empty()) {
    std::memcpy(block.data(), payload.data(), payload.size());
  }
  char* trailer = block.data() + payload.size();
  EncodeFixed32(trailer, static_cast<uint32_t>(payload.size()));
  EncodeFixed32(trailer + sizeof(uint32_t),
                Crc32(payload.data(), payload.size()));
  output->swap(block);
  return Status::OK();
}

Status DecodeBlock(Slice block, std::string* payload) {
  if (payload == nullptr) {
    return Status::InvalidArgument("sstable block payload is null");
  }
  if (block.size() < kSSTableBlockTrailerSize) {
    return Status::Corruption("truncated sstable block trailer");
  }

  const size_t trailer_offset =
      block.size() - kSSTableBlockTrailerSize;
  const uint32_t declared_size =
      DecodeFixed32(block.data() + trailer_offset);
  if (declared_size != trailer_offset ||
      declared_size > kSSTableMaxBlockSize) {
    return Status::Corruption("invalid sstable block size");
  }
  const uint32_t expected_checksum =
      DecodeFixed32(block.data() + trailer_offset +
                    sizeof(uint32_t));
  const uint32_t actual_checksum =
      Crc32(block.data(), trailer_offset);
  if (actual_checksum != expected_checksum) {
    return Status::Corruption("sstable block checksum mismatch");
  }

  payload->assign(block.data(), trailer_offset);
  return Status::OK();
}

size_t SharedPrefixLength(Slice lhs, Slice rhs) {
  const size_t limit = std::min(lhs.size(), rhs.size());
  size_t shared = 0;
  while (shared < limit && lhs.data()[shared] == rhs.data()[shared]) {
    ++shared;
  }
  return shared;
}

Status EncodeDataEntry(const InternalKey& key, Slice value,
                       Slice previous_user_key,
                       std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("sstable entry output is null");
  }
  if (!IsValidValueType(key.type) ||
      key.sequence == kMaxSequenceNumber) {
    return Status::InvalidArgument("invalid sstable internal key");
  }
  if (key.user_key.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("sstable user key is too large");
  }

  const size_t value_size =
      key.type == ValueType::kValue ? value.size() : 0;
  if (value_size > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("sstable value is too large");
  }

  const size_t shared = SharedPrefixLength(
      previous_user_key, Slice(key.user_key));
  const size_t unshared = key.user_key.size() - shared;
  if (shared > std::numeric_limits<uint32_t>::max() ||
      unshared > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("sstable key prefix is too large");
  }

  constexpr size_t kEntryFixedSize =
      sizeof(uint32_t) * 3 + sizeof(uint64_t) + sizeof(uint8_t);
  const size_t entry_size =
      kEntryFixedSize + unshared + value_size;
  if (entry_size > kSSTableMaxBlockSize) {
    return Status::InvalidArgument("sstable entry is too large");
  }

  std::string entry(entry_size, '\0');
  char* current = entry.data();
  EncodeFixed32(current, static_cast<uint32_t>(shared));
  current += sizeof(uint32_t);
  EncodeFixed32(current, static_cast<uint32_t>(unshared));
  current += sizeof(uint32_t);
  EncodeFixed32(current, static_cast<uint32_t>(value_size));
  current += sizeof(uint32_t);
  EncodeFixed64(current, key.sequence);
  current += sizeof(uint64_t);
  *current++ = static_cast<char>(key.type);
  if (unshared != 0) {
    std::memcpy(current, key.user_key.data() + shared, unshared);
    current += unshared;
  }
  if (value_size != 0) {
    std::memcpy(current, value.data(), value_size);
  }
  output->swap(entry);
  return Status::OK();
}

Status DecodeDataPayload(Slice payload,
                         std::vector<BlockEntry>* entries) {
  if (entries == nullptr) {
    return Status::InvalidArgument("sstable entries output is null");
  }

  std::vector<BlockEntry> decoded;
  Slice remaining = payload;
  std::string previous_user_key;
  InternalKey previous_internal_key;
  bool has_previous = false;
  while (!remaining.empty()) {
    constexpr size_t kEntryFixedSize =
        sizeof(uint32_t) * 3 + sizeof(uint64_t) + sizeof(uint8_t);
    if (remaining.size() < kEntryFixedSize) {
      return Status::Corruption("truncated sstable data entry");
    }

    const uint32_t shared = DecodeFixed32(remaining.data());
    const uint32_t unshared =
        DecodeFixed32(remaining.data() + 4);
    const uint32_t value_size =
        DecodeFixed32(remaining.data() + 8);
    const uint64_t sequence =
        DecodeFixed64(remaining.data() + 12);
    const ValueType type = static_cast<ValueType>(
        static_cast<unsigned char>(remaining.data()[20]));
    if (!IsValidValueType(type) ||
        sequence == kMaxSequenceNumber ||
        shared > previous_user_key.size()) {
      return Status::Corruption("invalid sstable data entry header");
    }

    const size_t body_size =
        static_cast<size_t>(unshared) + value_size;
    if (remaining.size() - kEntryFixedSize < body_size) {
      return Status::Corruption("truncated sstable data entry body");
    }

    std::string user_key = previous_user_key.substr(0, shared);
    user_key.append(remaining.data() + kEntryFixedSize, unshared);
    BlockEntry entry;
    entry.key.user_key = std::move(user_key);
    entry.key.sequence = sequence;
    entry.key.type = type;
    entry.value.assign(
        remaining.data() + kEntryFixedSize + unshared, value_size);

    if (has_previous &&
        CompareInternalKeys(previous_internal_key, entry.key) >= 0) {
      return Status::Corruption(
          "sstable data entries are not strictly ordered");
    }

    previous_user_key = entry.key.user_key;
    previous_internal_key = entry.key;
    has_previous = true;
    decoded.push_back(std::move(entry));
    remaining = Slice(
        remaining.data() + kEntryFixedSize + body_size,
        remaining.size() - kEntryFixedSize - body_size);
  }
  entries->swap(decoded);
  return Status::OK();
}

Status EncodeIndexEntry(const IndexEntry& entry,
                        std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("sstable index output is null");
  }
  if (!IsValidValueType(entry.last_key.type) ||
      entry.last_key.sequence == kMaxSequenceNumber ||
      entry.last_key.user_key.size() >
          std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("invalid sstable index entry");
  }

  constexpr size_t kFixedSize =
      sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint8_t) +
      sizeof(uint64_t) + sizeof(uint32_t);
  std::string encoded(
      kFixedSize + entry.last_key.user_key.size(), '\0');
  char* current = encoded.data();
  EncodeFixed32(current,
                static_cast<uint32_t>(entry.last_key.user_key.size()));
  current += sizeof(uint32_t);
  std::memcpy(current, entry.last_key.user_key.data(),
              entry.last_key.user_key.size());
  current += entry.last_key.user_key.size();
  EncodeFixed64(current, entry.last_key.sequence);
  current += sizeof(uint64_t);
  *current++ = static_cast<char>(entry.last_key.type);
  EncodeFixed64(current, entry.block.offset);
  current += sizeof(uint64_t);
  EncodeFixed32(current, entry.block.size);
  output->swap(encoded);
  return Status::OK();
}

Status DecodeIndexBlock(Slice payload,
                        std::vector<IndexEntry>* entries) {
  if (entries == nullptr) {
    return Status::InvalidArgument("sstable index entries output is null");
  }

  std::vector<IndexEntry> decoded;
  Slice remaining = payload;
  bool has_previous = false;
  while (!remaining.empty()) {
    constexpr size_t kFixedSize =
        sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint8_t) +
        sizeof(uint64_t) + sizeof(uint32_t);
    if (remaining.size() < kFixedSize) {
      return Status::Corruption("truncated sstable index entry");
    }

    const uint32_t user_key_size = DecodeFixed32(remaining.data());
    if (remaining.size() - kFixedSize < user_key_size) {
      return Status::Corruption("truncated sstable index user key");
    }

    const char* key_data = remaining.data() + sizeof(uint32_t);
    IndexEntry entry;
    entry.last_key.user_key.assign(key_data, user_key_size);
    entry.last_key.sequence =
        DecodeFixed64(key_data + user_key_size);
    entry.last_key.type = static_cast<ValueType>(
        static_cast<unsigned char>(
            key_data[user_key_size + sizeof(uint64_t)]));
    entry.block.offset = DecodeFixed64(
        key_data + user_key_size + sizeof(uint64_t) +
        sizeof(uint8_t));
    entry.block.size = DecodeFixed32(
        key_data + user_key_size + sizeof(uint64_t) +
        sizeof(uint8_t) + sizeof(uint64_t));

    if (!IsValidValueType(entry.last_key.type) ||
        entry.last_key.sequence == kMaxSequenceNumber ||
        entry.block.size < kSSTableBlockTrailerSize ||
        entry.block.size >
            kSSTableMaxBlockSize + kSSTableBlockTrailerSize) {
      return Status::Corruption("invalid sstable index entry");
    }
    if (has_previous &&
        CompareInternalKeys(decoded.back().last_key,
                            entry.last_key) >= 0) {
      return Status::Corruption(
          "sstable index entries are not strictly ordered");
    }

    const size_t consumed = kFixedSize + user_key_size;
    remaining = Slice(remaining.data() + consumed,
                      remaining.size() - consumed);
    decoded.push_back(std::move(entry));
    has_previous = true;
  }
  entries->swap(decoded);
  return Status::OK();
}

Status EncodeFooter(const Footer& footer, std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("sstable footer output is null");
  }
  std::string encoded(kSSTableFooterSize, '\0');
  char* current = encoded.data();
  EncodeFixed64(current, footer.filter_offset);
  current += sizeof(uint64_t);
  EncodeFixed64(current, footer.filter_size);
  current += sizeof(uint64_t);
  EncodeFixed64(current, footer.index_offset);
  current += sizeof(uint64_t);
  EncodeFixed64(current, footer.index_size);
  current += sizeof(uint64_t);
  EncodeFixed64(current, footer.entry_count);
  current += sizeof(uint64_t);
  EncodeFixed64(current, kSSTableMagic);
  output->swap(encoded);
  return Status::OK();
}

Status DecodeFooter(Slice encoded, Footer* footer) {
  if (footer == nullptr) {
    return Status::InvalidArgument("sstable footer is null");
  }
  if (encoded.size() != kSSTableFooterSize) {
    return Status::Corruption("invalid sstable footer size");
  }

  Footer decoded;
  decoded.filter_offset = DecodeFixed64(encoded.data());
  decoded.filter_size = DecodeFixed64(encoded.data() + 8);
  decoded.index_offset = DecodeFixed64(encoded.data() + 16);
  decoded.index_size = DecodeFixed64(encoded.data() + 24);
  decoded.entry_count = DecodeFixed64(encoded.data() + 32);
  if (DecodeFixed64(encoded.data() + 40) != kSSTableMagic) {
    return Status::Corruption("invalid sstable magic");
  }
  *footer = decoded;
  return Status::OK();
}

}  // namespace

struct SSTableBuilder::Impl {
  Impl(std::string file_path, size_t target_block_size)
      : path(std::move(file_path)),
        block_size(target_block_size) {}

  ~Impl() {
    if (fd >= 0) {
      ::close(fd);
    }
  }

  Status OpenIfNeeded() {
    if (fd >= 0) {
      return Status::OK();
    }
    if (path.empty()) {
      return Status::InvalidArgument("sstable path is empty");
    }
    if (block_size == 0 || block_size > kSSTableMaxBlockSize) {
      return Status::InvalidArgument("invalid sstable block size");
    }

    fd = ::open(path.c_str(),
                O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
      return MakeSSTableIoError("open sstable for write");
    }
    return Status::OK();
  }

  Status WriteAll(Slice data) {
    size_t written = 0;
    while (written < data.size()) {
      const ssize_t result =
          ::write(fd, data.data() + written, data.size() - written);
      if (result > 0) {
        const size_t count = static_cast<size_t>(result);
        written += count;
        file_offset += static_cast<uint64_t>(count);
        continue;
      }
      if (result < 0 && errno == EINTR) {
        continue;
      }
      return MakeSSTableIoError("write sstable");
    }
    return Status::OK();
  }

  Status FlushBlock() {
    if (block_buffer.empty()) {
      return Status::OK();
    }
    if (!has_block_last_key) {
      return Status::Corruption("sstable block has no last key");
    }

    std::string block;
    Status status = AppendBlock(Slice(block_buffer), &block);
    if (!status.ok()) {
      return status;
    }
    if (block.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::InvalidArgument("sstable block is too large");
    }

    IndexEntry index_entry;
    index_entry.last_key = block_last_key;
    index_entry.block.offset = file_offset;
    index_entry.block.size = static_cast<uint32_t>(block.size());
    status = WriteAll(Slice(block));
    if (!status.ok()) {
      return status;
    }

    index_entries.push_back(std::move(index_entry));
    block_buffer.clear();
    block_previous_user_key.clear();
    has_block_last_key = false;
    return Status::OK();
  }

  Status SyncAndClose() {
    while (::fsync(fd) < 0) {
      if (errno == EINTR) {
        continue;
      }
      return MakeSSTableIoError("fsync sstable");
    }
    const int current_fd = fd;
    fd = -1;
    if (::close(current_fd) < 0) {
      return MakeSSTableIoError("close sstable");
    }
    return Status::OK();
  }

  void Close() {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }

  std::string path;
  size_t block_size;
  int fd = -1;
  uint64_t file_offset = 0;
  bool finished = false;
  bool has_last_key = false;
  InternalKey last_key;
  std::string block_buffer;
  std::string block_previous_user_key;
  bool has_block_last_key = false;
  InternalKey block_last_key;
  std::vector<IndexEntry> index_entries;
  std::string bloom_last_user_key;
  bool has_bloom_key = false;
  std::vector<std::string> bloom_user_keys;
  uint64_t entry_count = 0;
};

SSTableBuilder::SSTableBuilder(std::string path,
                               size_t block_size)
    : impl_(std::make_unique<Impl>(
          std::move(path), block_size)) {}

SSTableBuilder::~SSTableBuilder() = default;

Status SSTableBuilder::Add(const InternalKey& key, Slice value) {
  Impl& impl = *impl_;
  if (impl.finished) {
    return Status::InvalidArgument("sstable builder is finished");
  }
  if (!IsValidValueType(key.type) ||
      key.sequence == kMaxSequenceNumber) {
    return Status::InvalidArgument("invalid sstable internal key");
  }
  if (impl.has_last_key &&
      CompareInternalKeys(impl.last_key, key) >= 0) {
    return Status::InvalidArgument(
        "sstable keys must increase strictly");
  }

  Status status = impl.OpenIfNeeded();
  if (!status.ok()) {
    return status;
  }

  std::string encoded;
  status = EncodeDataEntry(
      key, value, Slice(impl.block_previous_user_key), &encoded);
  if (!status.ok()) {
    return status;
  }

  if (!impl.block_buffer.empty() &&
      impl.block_buffer.size() + encoded.size() >
          impl.block_size) {
    status = impl.FlushBlock();
    if (!status.ok()) {
      impl.finished = true;
      impl.Close();
      return status;
    }
    status = EncodeDataEntry(key, value, Slice(), &encoded);
    if (!status.ok()) {
      return status;
    }
  }
  if (encoded.size() > kSSTableMaxBlockSize ||
      impl.block_buffer.size() >
          kSSTableMaxBlockSize - encoded.size()) {
    return Status::InvalidArgument("sstable data block is too large");
  }

  impl.block_buffer.append(encoded);
  impl.block_previous_user_key = key.user_key;
  impl.block_last_key = key;
  impl.has_block_last_key = true;
  impl.last_key = key;
  impl.has_last_key = true;

  if (!impl.has_bloom_key ||
      impl.bloom_last_user_key != key.user_key) {
    impl.bloom_user_keys.push_back(key.user_key);
    impl.bloom_last_user_key = key.user_key;
    impl.has_bloom_key = true;
  }
  ++impl.entry_count;
  return Status::OK();
}

Status SSTableBuilder::Finish() {
  Impl& impl = *impl_;
  if (impl.finished) {
    return Status::InvalidArgument(
        "sstable builder already finished");
  }
  impl.finished = true;

  Status status = impl.OpenIfNeeded();
  if (status.ok()) {
    status = impl.FlushBlock();
  }

  std::string filter_payload;
  if (status.ok()) {
    std::vector<Slice> bloom_keys;
    bloom_keys.reserve(impl.bloom_user_keys.size());
    for (const std::string& key : impl.bloom_user_keys) {
      bloom_keys.emplace_back(key);
    }
    status = BloomFilter::Build(bloom_keys, &filter_payload);
  }

  Footer footer;
  std::string filter_block;
  if (status.ok()) {
    status = AppendBlock(Slice(filter_payload), &filter_block);
  }
  if (status.ok()) {
    footer.filter_offset = impl.file_offset;
    status = impl.WriteAll(Slice(filter_block));
    footer.filter_size =
        static_cast<uint64_t>(filter_block.size());
  }

  std::string index_payload;
  if (status.ok()) {
    for (const IndexEntry& entry : impl.index_entries) {
      std::string encoded;
      status = EncodeIndexEntry(entry, &encoded);
      if (!status.ok()) {
        break;
      }
      index_payload.append(encoded);
    }
  }

  std::string index_block;
  if (status.ok()) {
    status = AppendBlock(Slice(index_payload), &index_block);
  }
  if (status.ok()) {
    footer.index_offset = impl.file_offset;
    status = impl.WriteAll(Slice(index_block));
    footer.index_size =
        static_cast<uint64_t>(index_block.size());
  }
  footer.entry_count = impl.entry_count;

  std::string footer_data;
  if (status.ok()) {
    status = EncodeFooter(footer, &footer_data);
  }
  if (status.ok()) {
    status = impl.WriteAll(Slice(footer_data));
  }
  if (status.ok()) {
    status = impl.SyncAndClose();
  }
  if (!status.ok()) {
    impl.Close();
  }
  return status;
}

namespace {

Status ReadRange(int fd, uint64_t offset, size_t size,
                 std::string* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("sstable read output is null");
  }
  const uint64_t max_offset = static_cast<uint64_t>(
      std::numeric_limits<off_t>::max());
  if (offset > max_offset ||
      size > max_offset - static_cast<off_t>(offset)) {
    return Status::Corruption("sstable read range overflow");
  }

  std::string data(size, '\0');
  size_t read_size = 0;
  while (read_size < size) {
    const ssize_t result = ::pread(
        fd, data.data() + read_size, size - read_size,
        static_cast<off_t>(offset + read_size));
    if (result > 0) {
      read_size += static_cast<size_t>(result);
      continue;
    }
    if (result == 0) {
      return Status::Corruption("truncated sstable read");
    }
    if (errno == EINTR) {
      continue;
    }
    return MakeSSTableIoError("pread sstable");
  }
  output->swap(data);
  return Status::OK();
}

Status ValidateBloomPayload(Slice payload) {
  if (payload.size() < kBloomFilterHeaderSize) {
    return Status::Corruption("truncated sstable bloom filter");
  }
  if (DecodeFixed32(payload.data()) != kBloomFilterMagic) {
    return Status::Corruption("invalid sstable bloom filter magic");
  }
  const uint32_t bit_count = DecodeFixed32(payload.data() + 4);
  const size_t expected_size =
      kBloomFilterHeaderSize +
      (static_cast<size_t>(bit_count) + 7U) / 8U;
  if (payload.size() != expected_size) {
    return Status::Corruption("invalid sstable bloom filter size");
  }
  return Status::OK();
}

}  // namespace

struct SSTableReader::Impl {
  ~Impl() {
    if (fd >= 0) {
      ::close(fd);
    }
  }

  Status LoadBlock(
      const BlockHandle& handle,
      std::shared_ptr<const std::string>* payload) const {
    if (payload == nullptr) {
      return Status::InvalidArgument("sstable block output is null");
    }
    if (handle.size < kSSTableBlockTrailerSize ||
        handle.size >
            kSSTableMaxBlockSize + kSSTableBlockTrailerSize) {
      return Status::Corruption("invalid sstable data block size");
    }

    const std::string cache_key =
        path + "#" + std::to_string(handle.offset);
    if (block_cache != nullptr) {
      const auto cached = block_cache->Lookup(cache_key);
      if (cached != nullptr) {
        *payload = cached;
        return Status::OK();
      }
    }

    std::string block;
    Status status =
        ReadRange(fd, handle.offset, handle.size, &block);
    if (!status.ok()) {
      return status;
    }

    std::string decoded;
    status = DecodeBlock(Slice(block), &decoded);
    if (!status.ok()) {
      return status;
    }

    auto shared_payload =
        std::make_shared<const std::string>(std::move(decoded));
    if (block_cache != nullptr) {
      // 缓存容量不足不影响本次读取。
      (void)block_cache->Insert(cache_key, shared_payload);
    }
    *payload = std::move(shared_payload);
    return Status::OK();
  }

  std::string path;
  int fd = -1;
  uint64_t file_size = 0;
  Footer footer;
  std::string filter_payload;
  std::vector<IndexEntry> index_entries;
  std::shared_ptr<LRUCache> block_cache;
};

Status SSTableReader::Open(
    std::string path,
    std::shared_ptr<LRUCache> block_cache,
    std::unique_ptr<SSTableReader>* reader) {
  if (reader == nullptr) {
    return Status::InvalidArgument("sstable reader output is null");
  }
  reader->reset();
  if (path.empty()) {
    return Status::InvalidArgument("sstable path is empty");
  }

  const int fd =
      ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return MakeSSTableIoError("open sstable for read");
  }

  struct stat file_stat {};
  if (::fstat(fd, &file_stat) < 0) {
    const Status status = MakeSSTableIoError("fstat sstable");
    ::close(fd);
    return status;
  }
  if (file_stat.st_size < 0 ||
      static_cast<uint64_t>(file_stat.st_size) < kSSTableFooterSize) {
    ::close(fd);
    return Status::Corruption("sstable file is too small");
  }

  const uint64_t file_size =
      static_cast<uint64_t>(file_stat.st_size);
  const uint64_t footer_offset = file_size - kSSTableFooterSize;
  std::string footer_data;
  Status status =
      ReadRange(fd, footer_offset, kSSTableFooterSize, &footer_data);
  Footer footer;
  if (status.ok()) {
    status = DecodeFooter(Slice(footer_data), &footer);
  }
  if (status.ok() &&
      (footer.filter_size < kSSTableBlockTrailerSize ||
       footer.index_size < kSSTableBlockTrailerSize ||
       footer.filter_size >
           kSSTableMaxBlockSize + kSSTableBlockTrailerSize ||
       footer.index_size >
           kSSTableMaxBlockSize + kSSTableBlockTrailerSize ||
       !RangeFits(footer.filter_offset, footer.filter_size,
                  footer_offset) ||
       !RangeFits(footer.index_offset, footer.index_size,
                  footer_offset) ||
       footer.filter_offset + footer.filter_size !=
           footer.index_offset ||
       footer.index_offset + footer.index_size != footer_offset)) {
    status = Status::Corruption("invalid sstable footer ranges");
  }

  std::string filter_payload;
  if (status.ok()) {
    std::string filter_block;
    status = ReadRange(fd, footer.filter_offset,
                       static_cast<size_t>(footer.filter_size),
                       &filter_block);
    if (status.ok()) {
      status = DecodeBlock(Slice(filter_block), &filter_payload);
    }
    if (status.ok()) {
      status = ValidateBloomPayload(Slice(filter_payload));
    }
  }

  std::vector<IndexEntry> index_entries;
  if (status.ok()) {
    std::string index_block;
    status = ReadRange(fd, footer.index_offset,
                       static_cast<size_t>(footer.index_size),
                       &index_block);
    std::string index_payload;
    if (status.ok()) {
      status = DecodeBlock(Slice(index_block), &index_payload);
    }
    if (status.ok()) {
      status = DecodeIndexBlock(Slice(index_payload), &index_entries);
    }
  }

  uint64_t expected_offset = 0;
  if (status.ok()) {
    for (const IndexEntry& entry : index_entries) {
      if (entry.block.offset != expected_offset ||
          !RangeFits(entry.block.offset, entry.block.size,
                     footer.filter_offset)) {
        status = Status::Corruption(
            "invalid sstable data block layout");
        break;
      }
      expected_offset += entry.block.size;
    }
    if (status.ok() &&
        (expected_offset != footer.filter_offset ||
         (index_entries.empty() !=
          (footer.entry_count == 0)))) {
      status = Status::Corruption(
          "sstable data block index mismatch");
    }
  }

  if (!status.ok()) {
    ::close(fd);
    return status;
  }

  auto impl = std::make_unique<Impl>();
  impl->path = std::move(path);
  impl->fd = fd;
  impl->file_size = file_size;
  impl->footer = footer;
  impl->filter_payload = std::move(filter_payload);
  impl->index_entries = std::move(index_entries);
  impl->block_cache = std::move(block_cache);
  *reader = std::unique_ptr<SSTableReader>(
      new SSTableReader(std::move(impl)));
  return Status::OK();
}

SSTableReader::SSTableReader(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

SSTableReader::~SSTableReader() = default;

Status SSTableReader::Get(Slice user_key,
                          SSTableLookupResult* result) const {
  if (result == nullptr) {
    return Status::InvalidArgument("sstable lookup result is null");
  }
  if (!BloomFilter::KeyMayMatch(
          user_key, Slice(impl_->filter_payload))) {
    return Status::NotFound("sstable key not found");
  }

  std::string key = user_key.ToString();
  InternalKey target;
  target.user_key = key;
  target.sequence = kMaxSequenceNumber;
  target.type = ValueType::kValue;

  const auto index = std::lower_bound(
      impl_->index_entries.begin(), impl_->index_entries.end(), target,
      [](const IndexEntry& entry, const InternalKey& wanted) {
        return CompareInternalKeys(entry.last_key, wanted) < 0;
      });
  if (index == impl_->index_entries.end()) {
    return Status::NotFound("sstable key not found");
  }

  std::shared_ptr<const std::string> payload;
  Status status = impl_->LoadBlock(index->block, &payload);
  if (!status.ok()) {
    return status;
  }
  std::vector<BlockEntry> entries;
  status = DecodeDataPayload(Slice(*payload), &entries);
  if (!status.ok()) {
    return status;
  }

  const auto entry = std::lower_bound(
      entries.begin(), entries.end(), target,
      [](const BlockEntry& current, const InternalKey& wanted) {
        return CompareInternalKeys(current.key, wanted) < 0;
      });
  if (entry == entries.end() ||
      entry->key.user_key != key) {
    return Status::NotFound("sstable key not found");
  }

  SSTableLookupResult found;
  found.key = entry->key;
  found.value = entry->value;
  *result = std::move(found);
  return Status::OK();
}

std::unique_ptr<SSTableReader::Iterator>
SSTableReader::NewIterator() const {
  return std::unique_ptr<Iterator>(new Iterator(this));
}

uint64_t SSTableReader::EntryCount() const {
  return impl_->footer.entry_count;
}

const std::string& SSTableReader::path() const {
  return impl_->path;
}

struct SSTableReader::Iterator::Impl {
  explicit Impl(const SSTableReader::Impl* reader_impl)
      : reader(reader_impl) {}

  Status LoadBlock(size_t index, bool seek_first_entry) {
    valid = false;
    entries.clear();
    current_block = index;
    current_entry = 0;
    status = Status::OK();
    if (index >= reader->index_entries.size()) {
      return Status::OK();
    }

    std::shared_ptr<const std::string> payload;
    status = reader->LoadBlock(
        reader->index_entries[index].block, &payload);
    if (!status.ok()) {
      return status;
    }
    status = DecodeDataPayload(Slice(*payload), &entries);
    if (!status.ok()) {
      entries.clear();
      return status;
    }
    if (entries.empty()) {
      status = Status::Corruption("empty sstable data block");
      return status;
    }
    current_entry = seek_first_entry ? 0 : entries.size();
    valid = seek_first_entry;
    return status;
  }

  const SSTableReader::Impl* reader;
  size_t current_block = 0;
  size_t current_entry = 0;
  std::vector<BlockEntry> entries;
  Status status;
  bool valid = false;
};

SSTableReader::Iterator::Iterator(const SSTableReader* reader)
    : impl_(std::make_unique<Impl>(reader->impl_.get())) {}

SSTableReader::Iterator::~Iterator() = default;

bool SSTableReader::Iterator::Valid() const {
  return impl_->valid;
}

void SSTableReader::Iterator::SeekToFirst() {
  impl_->valid = false;
  impl_->status = Status::OK();
  for (size_t i = 0; i < impl_->reader->index_entries.size(); ++i) {
    impl_->LoadBlock(i, true);
    if (!impl_->status.ok() || impl_->valid) {
      return;
    }
  }
}

void SSTableReader::Iterator::Seek(Slice user_key) {
  InternalKey target;
  target.user_key = user_key.ToString();
  target.sequence = kMaxSequenceNumber;
  target.type = ValueType::kValue;
  Seek(target);
}

void SSTableReader::Iterator::Seek(const InternalKey& target) {
  impl_->valid = false;
  impl_->status = Status::OK();

  const auto index = std::lower_bound(
      impl_->reader->index_entries.begin(),
      impl_->reader->index_entries.end(), target,
      [](const IndexEntry& entry, const InternalKey& wanted) {
        return CompareInternalKeys(entry.last_key, wanted) < 0;
      });
  if (index == impl_->reader->index_entries.end()) {
    return;
  }

  const size_t first_block = static_cast<size_t>(
      index - impl_->reader->index_entries.begin());
  for (size_t block = first_block;
       block < impl_->reader->index_entries.size(); ++block) {
    impl_->LoadBlock(block, true);
    if (!impl_->status.ok()) {
      return;
    }
    const auto entry = std::lower_bound(
        impl_->entries.begin(), impl_->entries.end(), target,
        [](const BlockEntry& current, const InternalKey& wanted) {
          return CompareInternalKeys(current.key, wanted) < 0;
        });
    if (entry != impl_->entries.end()) {
      impl_->current_entry = static_cast<size_t>(
          entry - impl_->entries.begin());
      impl_->valid = true;
      return;
    }
  }
}

void SSTableReader::Iterator::Next() {
  if (!impl_->valid) {
    return;
  }
  ++impl_->current_entry;
  if (impl_->current_entry < impl_->entries.size()) {
    return;
  }

  impl_->valid = false;
  for (size_t block = impl_->current_block + 1;
       block < impl_->reader->index_entries.size(); ++block) {
    impl_->LoadBlock(block, true);
    if (!impl_->status.ok() || impl_->valid) {
      return;
    }
  }
}

const InternalKey& SSTableReader::Iterator::key() const {
  return impl_->entries[impl_->current_entry].key;
}

Slice SSTableReader::Iterator::value() const {
  return Slice(impl_->entries[impl_->current_entry].value);
}

const Status& SSTableReader::Iterator::status() const {
  return impl_->status;
}

}  // namespace raftkv
