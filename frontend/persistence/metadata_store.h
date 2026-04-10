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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_PERSISTENCE_METADATA_STORE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_PERSISTENCE_METADATA_STORE_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// MetadataStore persists emulator metadata (instances, databases, DDL, ID
// counters) to a JSON file under --data_dir. This enables the emulator to
// restore its full state after a restart.
//
// The metadata file is written atomically via write-tmp-then-rename to prevent
// corruption on crash. The file is read once on startup and written on every
// metadata mutation.
//
// Thread-safe: all public methods acquire a mutex.
class MetadataStore {
 public:
  struct IdCounters {
    int64_t table_id = 0;
    int64_t column_id = 0;
    int64_t change_stream_id = 0;
    int64_t sequence_id = 0;
    int64_t named_schema_id = 0;
  };

  struct DatabaseInfo {
    std::string dialect;  // "GOOGLE_STANDARD_SQL" or "POSTGRESQL"
    std::vector<std::string> ddl_statements;
    IdCounters id_counters;
  };

  struct InstanceInfo {
    std::string display_name;
    std::string config;
    int32_t processing_units = 1000;
    std::map<std::string, std::string> labels;
    std::string create_time;
    std::map<std::string, DatabaseInfo> databases;
  };

  explicit MetadataStore(const std::string& data_dir);

  // Loads metadata from {data_dir}/metadata.json.
  // Returns OK on success, empty state on missing file (first run),
  // or empty state with logged error on corrupt file.
  absl::Status Load() ABSL_LOCKS_EXCLUDED(mu_);

  // Saves metadata to {data_dir}/metadata.json atomically.
  absl::Status Save() ABSL_LOCKS_EXCLUDED(mu_);

  // Instance operations.
  void AddInstance(const std::string& name, const std::string& display_name,
                   const std::string& config, int32_t processing_units,
                   const std::map<std::string, std::string>& labels,
                   const std::string& create_time) ABSL_LOCKS_EXCLUDED(mu_);
  void RemoveInstance(const std::string& name) ABSL_LOCKS_EXCLUDED(mu_);

  // Database operations.
  void AddDatabase(const std::string& instance_name,
                   const std::string& db_name, const std::string& dialect,
                   const std::vector<std::string>& ddl_statements)
      ABSL_LOCKS_EXCLUDED(mu_);
  void RemoveDatabase(const std::string& instance_name,
                      const std::string& db_name) ABSL_LOCKS_EXCLUDED(mu_);
  void UpdateDdl(const std::string& instance_name,
                 const std::string& db_name,
                 const std::vector<std::string>& ddl_statements)
      ABSL_LOCKS_EXCLUDED(mu_);
  void UpdateIdCounters(const std::string& instance_name,
                        const std::string& db_name,
                        const IdCounters& counters) ABSL_LOCKS_EXCLUDED(mu_);

  // Accessors for restore. Returns a copy for thread safety.
  std::map<std::string, InstanceInfo> instances() const
      ABSL_LOCKS_EXCLUDED(mu_);
  bool has_metadata() const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  std::string metadata_path_;
  mutable absl::Mutex mu_;
  std::map<std::string, InstanceInfo> instances_ ABSL_GUARDED_BY(mu_);
  bool has_metadata_ ABSL_GUARDED_BY(mu_) = false;
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_PERSISTENCE_METADATA_STORE_H_
