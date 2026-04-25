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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_PERSISTENT_STORAGE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_PERSISTENT_STORAGE_H_

#include <memory>
#include <string>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/storage/iterator.h"
#include "backend/storage/storage.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// PersistentStorage implements a LevelDB-backed multi-version data store.
//
// LevelDB key format (length-prefixed, no separators):
//   {table_id_len:4BE}{table_id}{encoded_key_len:4BE}{encoded_key}
//   {column_id_len:4BE}{column_id}{timestamp:8}
//
// Each component except the fixed-size timestamp is preceded by a 4-byte
// big-endian length prefix, eliminating ambiguity from embedded \x00 bytes.
//
// The encoded_key is produced by key_codec.h and preserves the sort order
// defined by Key::Compare().
//
// Values are serialized using value_codec.h.
//
// This class is thread-safe.
class PersistentStorage : public Storage {
 public:
  // Creates a new PersistentStorage backed by a LevelDB database at the
  // given directory path.
  static absl::StatusOr<std::unique_ptr<PersistentStorage>> Create(
      const std::string& data_dir);

  ~PersistentStorage() override;

  absl::Status Lookup(absl::Time timestamp, const TableID& table_id,
                      const Key& key, const std::vector<ColumnID>& column_ids,
                      std::vector<zetasql::Value>* values) const override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Read(absl::Time timestamp, const TableID& table_id,
                    const KeyRange& key_range,
                    const std::vector<ColumnID>& column_ids,
                    std::unique_ptr<StorageIterator>* itr) const override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Write(absl::Time timestamp, const TableID& table_id,
                     const Key& key, const std::vector<ColumnID>& column_ids,
                     const std::vector<zetasql::Value>& values) override
      ABSL_LOCKS_EXCLUDED(mu_);

  absl::Status Delete(absl::Time timestamp, const TableID& table_id,
                      const KeyRange& key_range) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void SetVersionRetentionPeriod(
      absl::Duration version_retention_period) override;

  void CleanUpDeletedTables(absl::Time timestamp) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void CleanUpDeletedColumns(absl::Time timestamp) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void MarkDroppedTable(absl::Time timestamp, TableID dropped_table_id) override
      ABSL_LOCKS_EXCLUDED(mu_);

  void MarkDroppedColumn(absl::Time timestamp, TableID dropped_table_id,
                         ColumnID dropped_column_id) override
      ABSL_LOCKS_EXCLUDED(mu_);

 private:
  explicit PersistentStorage(std::unique_ptr<leveldb::DB> db);

  // Builds a LevelDB key from the components.
  static std::string MakeLevelDBKey(const TableID& table_id,
                                    const std::string& encoded_key,
                                    const ColumnID& column_id,
                                    absl::Time timestamp);

  // Builds a LevelDB key prefix for scanning all columns of a row.
  static std::string MakeRowPrefix(const TableID& table_id,
                                   const std::string& encoded_key);

  // Builds a LevelDB key prefix for scanning a table.
  static std::string MakeTablePrefix(const TableID& table_id);

  // Encodes a timestamp as 8 big-endian bytes.
  static std::string EncodeTimestamp(absl::Time timestamp);

  // Returns true if the row identified by the given encoded key exists at the
  // specified timestamp by checking the _exists column.
  bool Exists(const TableID& table_id, const std::string& encoded_key,
              absl::Time timestamp) const ABSL_SHARED_LOCKS_REQUIRED(mu_);

  // Finds the value for a specific cell at or before the given timestamp.
  // Uses LevelDB iterator seek to find the most recent version.
  zetasql::Value GetCellValueAtTimestamp(const TableID& table_id,
                                           const std::string& encoded_key,
                                           const ColumnID& column_id,
                                           absl::Time timestamp) const
      ABSL_SHARED_LOCKS_REQUIRED(mu_);

  // Collects all distinct encoded keys in a table within a key range.
  std::vector<std::string> CollectKeysInRange(
      const TableID& table_id, const std::string& start_encoded,
      const std::string& limit_encoded) const
      ABSL_SHARED_LOCKS_REQUIRED(mu_);

  // Removes old versions of a cell that are past the retention period.
  // Mirrors InMemoryStorage::RemoveExpiredVersions behavior: keeps the most
  // recent version within the retention window, deletes everything older.
  void RemoveExpiredVersions(const std::string& cell_prefix,
                             absl::Time timestamp,
                             leveldb::WriteBatch* batch)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable absl::Mutex mu_;
  std::unique_ptr<leveldb::DB> db_ ABSL_GUARDED_BY(mu_);

  // Tracks when tables were dropped.
  std::map<absl::Time, TableID> dropped_tables_ ABSL_GUARDED_BY(mu_);

  // Tracks when columns were dropped.
  std::map<absl::Time, std::pair<TableID, ColumnID>> dropped_columns_
      ABSL_GUARDED_BY(mu_);

  mutable absl::Mutex version_retention_period_mu_ ABSL_ACQUIRED_AFTER(mu_);
  absl::Duration version_retention_period_
      ABSL_GUARDED_BY(version_retention_period_mu_) = absl::Hours(1);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_PERSISTENT_STORAGE_H_
