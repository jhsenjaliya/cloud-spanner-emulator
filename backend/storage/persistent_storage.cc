//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/storage/persistent_storage.h"

#include <cstdint>
#include <cstring>
#include <filesystem>  // C++17
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "backend/storage/in_memory_iterator.h"
#include "backend/storage/key_codec.h"
#include "backend/storage/value_codec.h"
#include "common/errors.h"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

static constexpr char kExistsColumn[] = "_exists";

absl::Status LevelDBStatusToAbsl(const leveldb::Status& status) {
  if (status.ok()) return absl::OkStatus();
  if (status.IsNotFound())
    return absl::NotFoundError(status.ToString());
  if (status.IsCorruption())
    return absl::DataLossError(status.ToString());
  if (status.IsIOError())
    return absl::UnavailableError(status.ToString());
  if (status.IsNotSupportedError())
    return absl::UnimplementedError(status.ToString());
  if (status.IsInvalidArgument())
    return absl::InvalidArgumentError(status.ToString());
  return absl::InternalError(status.ToString());
}

// Appends a 4-byte big-endian length prefix followed by the data.
void AppendLengthPrefixed(std::string* out, const std::string& data) {
  uint32_t len = static_cast<uint32_t>(data.size());
  char buf[4];
  buf[0] = static_cast<char>((len >> 24) & 0xFF);
  buf[1] = static_cast<char>((len >> 16) & 0xFF);
  buf[2] = static_cast<char>((len >> 8) & 0xFF);
  buf[3] = static_cast<char>(len & 0xFF);
  out->append(buf, 4);
  out->append(data);
}

// Reads a 4-byte big-endian length at the given offset and returns the
// length. Returns false if there isn't enough data.
bool ReadLengthPrefix(const char* data, size_t data_size, size_t offset,
                      uint32_t* len) {
  if (offset + 4 > data_size) return false;
  const auto* p = reinterpret_cast<const unsigned char*>(data + offset);
  *len = (static_cast<uint32_t>(p[0]) << 24) |
         (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) |
         static_cast<uint32_t>(p[3]);
  return true;
}

// Extracts the encoded key portion from a length-prefixed LevelDB key.
// Format: {table_id_len:4BE}{table_id}{encoded_key_len:4BE}{encoded_key}
//         {column_id_len:4BE}{column_id}{timestamp:8}
std::string ExtractEncodedKeyFromLevelDBKey(const leveldb::Slice& ldb_key) {
  const char* data = ldb_key.data();
  size_t size = ldb_key.size();
  size_t offset = 0;

  // Skip table_id.
  uint32_t table_id_len;
  if (!ReadLengthPrefix(data, size, offset, &table_id_len)) return "";
  offset += 4 + table_id_len;

  // Read encoded_key.
  uint32_t encoded_key_len;
  if (!ReadLengthPrefix(data, size, offset, &encoded_key_len)) return "";
  offset += 4;
  if (offset + encoded_key_len > size) return "";
  std::string result(data + offset, encoded_key_len);
  return result;
}

// Extracts the column_id portion from a length-prefixed LevelDB key.
std::string ExtractColumnIdFromLevelDBKey(const leveldb::Slice& ldb_key) {
  const char* data = ldb_key.data();
  size_t size = ldb_key.size();
  size_t offset = 0;

  // Skip table_id.
  uint32_t table_id_len;
  if (!ReadLengthPrefix(data, size, offset, &table_id_len)) return "";
  offset += 4 + table_id_len;

  // Skip encoded_key.
  uint32_t encoded_key_len;
  if (!ReadLengthPrefix(data, size, offset, &encoded_key_len)) return "";
  offset += 4 + encoded_key_len;

  // Read column_id.
  uint32_t column_id_len;
  if (!ReadLengthPrefix(data, size, offset, &column_id_len)) return "";
  offset += 4;
  if (offset + column_id_len > size) return "";
  return std::string(data + offset, column_id_len);
}

}  // namespace

PersistentStorage::PersistentStorage(std::unique_ptr<leveldb::DB> db)
    : db_(std::move(db)) {}

PersistentStorage::~PersistentStorage() = default;

absl::StatusOr<std::unique_ptr<PersistentStorage>> PersistentStorage::Create(
    const std::string& data_dir) {
  leveldb::Options options;
  options.create_if_missing = true;
  // Use a larger write buffer for better performance.
  options.write_buffer_size = 64 * 1024 * 1024;  // 64MB
  options.max_open_files = 1000;

  // Create all intermediate directories (mkdir -p equivalent).
  // leveldb::DB::Open() with create_if_missing only creates the leaf directory,
  // not parent directories. Without this, new databases fail with LOCK errors.
  std::error_code ec;
  std::filesystem::create_directories(data_dir, ec);
  if (ec) {
    return absl::InternalError(
        absl::StrCat("Failed to create directory ", data_dir, ": ",
                     ec.message()));
  }

  leveldb::DB* raw_db = nullptr;
  leveldb::Status status = leveldb::DB::Open(options, data_dir, &raw_db);
  if (!status.ok()) {
    return absl::InternalError(
        absl::StrCat("Failed to open LevelDB at ", data_dir, ": ",
                     status.ToString()));
  }

  return absl::WrapUnique(
      new PersistentStorage(std::unique_ptr<leveldb::DB>(raw_db)));
}

std::string PersistentStorage::EncodeTimestamp(absl::Time timestamp) {
  // Encode as big-endian int64 microseconds for proper ordering.
  int64_t micros = absl::ToUnixMicros(timestamp);
  // Flip sign bit for unsigned comparison ordering.
  uint64_t encoded = static_cast<uint64_t>(micros) ^ (1ULL << 63);
  char buf[8];
  buf[0] = static_cast<char>((encoded >> 56) & 0xFF);
  buf[1] = static_cast<char>((encoded >> 48) & 0xFF);
  buf[2] = static_cast<char>((encoded >> 40) & 0xFF);
  buf[3] = static_cast<char>((encoded >> 32) & 0xFF);
  buf[4] = static_cast<char>((encoded >> 24) & 0xFF);
  buf[5] = static_cast<char>((encoded >> 16) & 0xFF);
  buf[6] = static_cast<char>((encoded >> 8) & 0xFF);
  buf[7] = static_cast<char>(encoded & 0xFF);
  return std::string(buf, 8);
}

std::string PersistentStorage::MakeLevelDBKey(const TableID& table_id,
                                              const std::string& encoded_key,
                                              const ColumnID& column_id,
                                              absl::Time timestamp) {
  std::string result;
  result.reserve(4 + table_id.size() + 4 + encoded_key.size() +
                 4 + column_id.size() + 8);
  AppendLengthPrefixed(&result, table_id);
  AppendLengthPrefixed(&result, encoded_key);
  AppendLengthPrefixed(&result, column_id);
  result.append(EncodeTimestamp(timestamp));
  return result;
}

std::string PersistentStorage::MakeRowPrefix(const TableID& table_id,
                                             const std::string& encoded_key) {
  std::string result;
  result.reserve(4 + table_id.size() + 4 + encoded_key.size());
  AppendLengthPrefixed(&result, table_id);
  AppendLengthPrefixed(&result, encoded_key);
  return result;
}

std::string PersistentStorage::MakeTablePrefix(const TableID& table_id) {
  std::string result;
  result.reserve(4 + table_id.size());
  AppendLengthPrefixed(&result, table_id);
  return result;
}

zetasql::Value PersistentStorage::GetCellValueAtTimestamp(
    const TableID& table_id, const std::string& encoded_key,
    const ColumnID& column_id, absl::Time timestamp) const {
  // Build a seek key just past the target timestamp (upper bound).
  // Since timestamps are encoded in ascending order, we seek to the key
  // with the target timestamp and then look for the entry at or before it.
  std::string seek_key =
      MakeLevelDBKey(table_id, encoded_key, column_id, timestamp);

  // Build the prefix for this specific cell (table+key+column).
  std::string cell_prefix;
  AppendLengthPrefixed(&cell_prefix, table_id);
  AppendLengthPrefixed(&cell_prefix, encoded_key);
  AppendLengthPrefixed(&cell_prefix, column_id);

  std::unique_ptr<leveldb::Iterator> it(
      db_->NewIterator(leveldb::ReadOptions()));

  // Seek to the position just past the target timestamp.
  // We want the largest timestamp <= target, so we seek to target+1 and
  // go back, or seek to target and check.
  it->Seek(seek_key);

  // Check if we landed exactly on the target or need to go to previous.
  if (it->Valid()) {
    leveldb::Slice found_key = it->key();
    // Check if this key has the right cell prefix.
    if (found_key.starts_with(cell_prefix)) {
      // The key at this position has timestamp >= target.
      // If it's exactly the target, use it.
      if (found_key.compare(seek_key) == 0) {
        return DecodeValue(it->value().ToString());
      }
      // Otherwise the timestamp is > target, need to go to previous entry.
      it->Prev();
    } else {
      // We overshot past this cell entirely, go back.
      it->Prev();
    }
  } else {
    // Past the end of the database, go to the last entry.
    it->SeekToLast();
  }

  // Now check if the current position is within the right cell prefix.
  if (it->Valid()) {
    leveldb::Slice found_key = it->key();
    if (found_key.starts_with(cell_prefix)) {
      return DecodeValue(it->value().ToString());
    }
  }

  return zetasql::Value();
}

bool PersistentStorage::Exists(const TableID& table_id,
                               const std::string& encoded_key,
                               absl::Time timestamp) const {
  zetasql::Value value =
      GetCellValueAtTimestamp(table_id, encoded_key, kExistsColumn, timestamp);
  return value.is_valid() && !value.is_null() && value.bool_value();
}

std::vector<std::string> PersistentStorage::CollectKeysInRange(
    const TableID& table_id, const std::string& start_encoded,
    const std::string& limit_encoded) const {
  std::string table_prefix = MakeTablePrefix(table_id);
  std::string seek_start = MakeRowPrefix(table_id, start_encoded);
  std::string seek_limit = MakeRowPrefix(table_id, limit_encoded);

  std::set<std::string> unique_keys;
  std::unique_ptr<leveldb::Iterator> it(
      db_->NewIterator(leveldb::ReadOptions()));

  for (it->Seek(seek_start); it->Valid(); it->Next()) {
    leveldb::Slice ldb_key = it->key();
    std::string ldb_key_str = ldb_key.ToString();

    // Stop if we've passed the table prefix.
    if (!ldb_key.starts_with(table_prefix)) break;

    // Stop if we've reached or passed the limit.
    if (!limit_encoded.empty() && ldb_key_str >= seek_limit) break;

    // Extract the encoded key from the LevelDB key.
    std::string encoded_key = ExtractEncodedKeyFromLevelDBKey(ldb_key);
    if (!encoded_key.empty()) {
      unique_keys.insert(encoded_key);
    }
  }

  return std::vector<std::string>(unique_keys.begin(), unique_keys.end());
}

absl::Status PersistentStorage::Lookup(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    std::vector<zetasql::Value>* values) const {
  absl::ReaderMutexLock lock(&mu_);

  if (!column_ids.empty() && values == nullptr) {
    return error::Internal(
        "PersistentStorage::Lookup was passed a nullptr for "
        "values, but had non-empty column_ids.");
  }
  if (values != nullptr) {
    values->clear();
  }

  std::string encoded_key = EncodeKey(key);

  // Check if the row exists.
  if (!Exists(table_id, encoded_key, timestamp)) {
    return absl::Status(
        absl::StatusCode::kNotFound,
        absl::StrCat("Key: ", key.DebugString(), " not found for table: ",
                     table_id, " at timestamp: ", absl::FormatTime(timestamp)));
  }

  if (column_ids.empty()) {
    return absl::OkStatus();
  }

  for (const ColumnID& column_id : column_ids) {
    values->emplace_back(
        GetCellValueAtTimestamp(table_id, encoded_key, column_id, timestamp));
  }

  return absl::OkStatus();
}

absl::Status PersistentStorage::Read(
    absl::Time timestamp, const TableID& table_id, const KeyRange& key_range,
    const std::vector<ColumnID>& column_ids,
    std::unique_ptr<StorageIterator>* itr) const {
  absl::ReaderMutexLock lock(&mu_);

  if (!key_range.IsClosedOpen()) {
    return error::Internal(
        absl::StrCat("PersistentStorage::Read should be called "
                     "with ClosedOpen key range, found: ",
                     key_range.DebugString()));
  }

  std::vector<FixedRowStorageIterator::Row> rows;

  if (key_range.start_key() >= key_range.limit_key()) {
    *itr = std::make_unique<FixedRowStorageIterator>();
    return absl::OkStatus();
  }

  std::string start_encoded = EncodeKey(key_range.start_key());
  std::string limit_encoded = EncodeKey(key_range.limit_key());

  // Collect all unique keys in the range.
  std::vector<std::string> encoded_keys =
      CollectKeysInRange(table_id, start_encoded, limit_encoded);

  for (const std::string& encoded_key : encoded_keys) {
    if (!Exists(table_id, encoded_key, timestamp)) {
      continue;
    }

    std::vector<zetasql::Value> values;
    values.reserve(column_ids.size());
    for (const ColumnID& column_id : column_ids) {
      values.emplace_back(
          GetCellValueAtTimestamp(table_id, encoded_key, column_id, timestamp));
    }

    // Reconstruct the Key from "__key_data__", which stores serialized key
    // columns written alongside every row for round-trip reconstruction.
    Key reconstructed_key;
    zetasql::Value key_data = GetCellValueAtTimestamp(
        table_id, encoded_key, "__key_data__", timestamp);
    if (key_data.is_valid() && !key_data.is_null()) {
      // Decode the key from the stored bytes.
      // The __key_data__ column stores a serialized representation of the
      // key columns as: num_columns, then for each column:
      //   is_descending(1 byte), is_nulls_last(1 byte), encoded_value
      std::string data = key_data.bytes_value();
      const char* ptr = data.data();
      const char* end = ptr + data.size();
      if (ptr < end) {
        int32_t num_cols;
        if (end - ptr >= 4) {
          // Decode num_cols as explicit little-endian.
          const auto* up = reinterpret_cast<const unsigned char*>(ptr);
          num_cols = static_cast<int32_t>(up[0]) |
                     (static_cast<int32_t>(up[1]) << 8) |
                     (static_cast<int32_t>(up[2]) << 16) |
                     (static_cast<int32_t>(up[3]) << 24);
          ptr += 4;
          for (int i = 0; i < num_cols && ptr < end; ++i) {
            bool desc = (*ptr++ != 0);
            bool nulls_last = (*ptr++ != 0);
            // Read the encoded value length as explicit little-endian.
            int32_t val_len;
            if (end - ptr < 4) break;
            const auto* vp = reinterpret_cast<const unsigned char*>(ptr);
            val_len = static_cast<int32_t>(vp[0]) |
                      (static_cast<int32_t>(vp[1]) << 8) |
                      (static_cast<int32_t>(vp[2]) << 16) |
                      (static_cast<int32_t>(vp[3]) << 24);
            ptr += 4;
            if (end - ptr < val_len) break;
            std::string encoded_val(ptr, val_len);
            ptr += val_len;
            zetasql::Value col_val = DecodeValue(encoded_val);
            reconstructed_key.AddColumn(col_val, desc, nulls_last);
          }
        }
      }
    }

    rows.emplace_back(std::make_pair(reconstructed_key, std::move(values)));
  }

  *itr = std::make_unique<FixedRowStorageIterator>(std::move(rows));
  return absl::OkStatus();
}

absl::Status PersistentStorage::Write(
    absl::Time timestamp, const TableID& table_id, const Key& key,
    const std::vector<ColumnID>& column_ids,
    const std::vector<zetasql::Value>& values) {
  absl::MutexLock lock(&mu_);

  std::string encoded_key = EncodeKey(key);
  leveldb::WriteBatch batch;

  // Write _exists column if the row doesn't exist yet.
  if (!Exists(table_id, encoded_key, timestamp)) {
    std::string exists_ldb_key =
        MakeLevelDBKey(table_id, encoded_key, kExistsColumn, timestamp);
    std::string exists_value = EncodeValue(zetasql::values::Bool(true));
    batch.Put(exists_ldb_key, exists_value);
  }

  // Write the key metadata for reconstruction during Read().
  // Format: num_columns(4 bytes LE), then per column:
  //   is_descending(1 byte), is_nulls_last(1 byte),
  //   value_len(4 bytes LE), encoded_value
  {
    std::string key_data;
    int32_t num_cols = key.NumColumns();
    // Encode num_cols as explicit little-endian.
    char nc_buf[4];
    nc_buf[0] = static_cast<char>(num_cols & 0xFF);
    nc_buf[1] = static_cast<char>((num_cols >> 8) & 0xFF);
    nc_buf[2] = static_cast<char>((num_cols >> 16) & 0xFF);
    nc_buf[3] = static_cast<char>((num_cols >> 24) & 0xFF);
    key_data.append(nc_buf, 4);
    for (int i = 0; i < num_cols; ++i) {
      key_data.push_back(key.IsColumnDescending(i) ? 1 : 0);
      key_data.push_back(key.IsColumnNullsLast(i) ? 1 : 0);
      std::string encoded_val = EncodeValue(key.ColumnValue(i));
      int32_t val_len = static_cast<int32_t>(encoded_val.size());
      // Encode val_len as explicit little-endian.
      char vl_buf[4];
      vl_buf[0] = static_cast<char>(val_len & 0xFF);
      vl_buf[1] = static_cast<char>((val_len >> 8) & 0xFF);
      vl_buf[2] = static_cast<char>((val_len >> 16) & 0xFF);
      vl_buf[3] = static_cast<char>((val_len >> 24) & 0xFF);
      key_data.append(vl_buf, 4);
      key_data.append(encoded_val);
    }
    std::string key_meta_ldb_key =
        MakeLevelDBKey(table_id, encoded_key, "__key_data__", timestamp);
    std::string key_meta_value =
        EncodeValue(zetasql::values::Bytes(key_data));
    batch.Put(key_meta_ldb_key, key_meta_value);
  }

  // Write the column values.
  for (size_t i = 0; i < column_ids.size(); ++i) {
    std::string ldb_key =
        MakeLevelDBKey(table_id, encoded_key, column_ids[i], timestamp);
    std::string encoded_value = EncodeValue(values[i]);
    batch.Put(ldb_key, encoded_value);
  }

  leveldb::Status status =
      db_->Write(leveldb::WriteOptions(), &batch);
  if (!status.ok()) {
    return LevelDBStatusToAbsl(status);
  }

  return absl::OkStatus();
}

absl::Status PersistentStorage::Delete(absl::Time timestamp,
                                       const TableID& table_id,
                                       const KeyRange& key_range) {
  absl::MutexLock lock(&mu_);

  if (!key_range.IsClosedOpen()) {
    return error::Internal(
        absl::StrCat("PersistentStorage::Delete should be called "
                     "with ClosedOpen key range, found: ",
                     key_range.DebugString()));
  }
  if (key_range.start_key() >= key_range.limit_key()) {
    return absl::OkStatus();
  }

  std::string start_encoded = EncodeKey(key_range.start_key());
  std::string limit_encoded = EncodeKey(key_range.limit_key());

  std::vector<std::string> encoded_keys =
      CollectKeysInRange(table_id, start_encoded, limit_encoded);

  leveldb::WriteBatch batch;

  for (const std::string& encoded_key : encoded_keys) {
    if (!Exists(table_id, encoded_key, timestamp)) {
      continue;
    }

    // Mark _exists as false.
    std::string exists_ldb_key =
        MakeLevelDBKey(table_id, encoded_key, kExistsColumn, timestamp);
    std::string exists_value = EncodeValue(zetasql::values::Bool(false));
    batch.Put(exists_ldb_key, exists_value);

    // Scan all columns for this row and mark them as invalid.
    std::string row_prefix = MakeRowPrefix(table_id, encoded_key);
    std::unique_ptr<leveldb::Iterator> it(
        db_->NewIterator(leveldb::ReadOptions()));

    std::set<std::string> seen_columns;
    for (it->Seek(row_prefix); it->Valid(); it->Next()) {
      leveldb::Slice ldb_key = it->key();
      if (!ldb_key.starts_with(row_prefix)) break;

      // Extract column_id from the length-prefixed LevelDB key.
      std::string col_id = ExtractColumnIdFromLevelDBKey(ldb_key);
      if (col_id.empty()) continue;
      if (col_id == kExistsColumn || col_id == "__key_data__") continue;

      if (seen_columns.insert(col_id).second) {
        // Write an invalid value for this column at the delete timestamp.
        std::string del_ldb_key =
            MakeLevelDBKey(table_id, encoded_key, col_id, timestamp);
        std::string invalid_value = EncodeValue(zetasql::Value());
        batch.Put(del_ldb_key, invalid_value);
      }
    }
  }

  leveldb::Status status =
      db_->Write(leveldb::WriteOptions(), &batch);
  if (!status.ok()) {
    return LevelDBStatusToAbsl(status);
  }

  return absl::OkStatus();
}

void PersistentStorage::SetVersionRetentionPeriod(
    absl::Duration version_retention_period) {
  absl::MutexLock lock(&version_retention_period_mu_);
  version_retention_period_ = version_retention_period;
}

void PersistentStorage::CleanUpDeletedTables(absl::Time timestamp) {
  absl::MutexLock lock(&mu_);
  absl::MutexLock version_retention_period_lock(&version_retention_period_mu_);
  absl::Time expiration_time = timestamp - version_retention_period_;

  for (auto it = dropped_tables_.begin();
       it != dropped_tables_.upper_bound(expiration_time);) {
    // Delete all entries with the table prefix.
    std::string table_prefix = MakeTablePrefix(it->second);
    leveldb::WriteBatch batch;

    std::unique_ptr<leveldb::Iterator> db_it(
        db_->NewIterator(leveldb::ReadOptions()));
    for (db_it->Seek(table_prefix); db_it->Valid(); db_it->Next()) {
      if (!db_it->key().starts_with(table_prefix)) break;
      batch.Delete(db_it->key());
    }
    db_->Write(leveldb::WriteOptions(), &batch);

    it = dropped_tables_.erase(it);
  }
}

void PersistentStorage::CleanUpDeletedColumns(absl::Time timestamp) {
  absl::MutexLock lock(&mu_);
  absl::MutexLock version_retention_period_lock(&version_retention_period_mu_);
  absl::Time expiration_time = timestamp - version_retention_period_;

  for (auto it = dropped_columns_.begin();
       it != dropped_columns_.upper_bound(expiration_time);) {
    auto [table_id, column_id] = it->second;

    // Scan all rows of the table and delete entries for this column.
    std::string table_prefix = MakeTablePrefix(table_id);
    leveldb::WriteBatch batch;

    std::unique_ptr<leveldb::Iterator> db_it(
        db_->NewIterator(leveldb::ReadOptions()));
    for (db_it->Seek(table_prefix); db_it->Valid(); db_it->Next()) {
      leveldb::Slice ldb_key = db_it->key();
      if (!ldb_key.starts_with(table_prefix)) break;

      // Check if this entry belongs to the dropped column.
      std::string found_col = ExtractColumnIdFromLevelDBKey(ldb_key);
      if (found_col == column_id) {
        batch.Delete(ldb_key);
      }
    }
    db_->Write(leveldb::WriteOptions(), &batch);

    it = dropped_columns_.erase(it);
  }
}

void PersistentStorage::MarkDroppedTable(absl::Time timestamp,
                                         TableID dropped_table_id) {
  absl::MutexLock lock(&mu_);
  dropped_tables_[timestamp] = dropped_table_id;
}

void PersistentStorage::MarkDroppedColumn(absl::Time timestamp,
                                          TableID dropped_table_id,
                                          ColumnID dropped_column_id) {
  absl::MutexLock lock(&mu_);
  dropped_columns_[timestamp] =
      std::make_pair(dropped_table_id, dropped_column_id);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
