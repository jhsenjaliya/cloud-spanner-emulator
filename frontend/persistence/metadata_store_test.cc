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

#include "frontend/persistence/metadata_store.h"

#include <filesystem>
#include <fstream>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

std::string MakeTempDir(const std::string& suffix) {
  const char* test_tmpdir = std::getenv("TEST_TMPDIR");
  std::string base = test_tmpdir ? test_tmpdir : "/tmp";
  std::string path = base + "/metadata_store_test_" + suffix + "_" +
                     std::to_string(getpid());
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path;
}

TEST(MetadataStoreTest, SaveAndLoadRoundtrip) {
  std::string dir = MakeTempDir("roundtrip");
  {
    MetadataStore store(dir);
    store.AddInstance("projects/p/instances/i1", "Instance 1",
                      "emulator-config", 1000, {{"env", "test"}},
                      "2026-04-10T00:00:00Z");
    store.AddDatabase("projects/p/instances/i1", "db1",
                      "GOOGLE_STANDARD_SQL",
                      {"CREATE TABLE T (K INT64) PRIMARY KEY (K)"});
    store.UpdateIdCounters("projects/p/instances/i1", "db1",
                           {.table_id = 3, .column_id = 8});
    EXPECT_TRUE(store.Save().ok());
  }
  {
    MetadataStore store(dir);
    EXPECT_TRUE(store.Load().ok());
    EXPECT_TRUE(store.has_metadata());

    const auto& instances = store.instances();
    ASSERT_EQ(instances.size(), 1);

    const auto& inst = instances.at("projects/p/instances/i1");
    EXPECT_EQ(inst.display_name, "Instance 1");
    EXPECT_EQ(inst.config, "emulator-config");
    EXPECT_EQ(inst.processing_units, 1000);
    EXPECT_EQ(inst.labels.at("env"), "test");
    EXPECT_EQ(inst.create_time, "2026-04-10T00:00:00Z");

    ASSERT_EQ(inst.databases.size(), 1);
    const auto& db = inst.databases.at("db1");
    EXPECT_EQ(db.dialect, "GOOGLE_STANDARD_SQL");
    ASSERT_EQ(db.ddl_statements.size(), 1);
    EXPECT_EQ(db.ddl_statements[0],
              "CREATE TABLE T (K INT64) PRIMARY KEY (K)");
    EXPECT_EQ(db.id_counters.table_id, 3);
    EXPECT_EQ(db.id_counters.column_id, 8);
  }
  std::filesystem::remove_all(dir);
}

TEST(MetadataStoreTest, MissingFileReturnsEmptyState) {
  std::string dir = MakeTempDir("missing");
  MetadataStore store(dir);
  EXPECT_TRUE(store.Load().ok());
  EXPECT_FALSE(store.has_metadata());
  EXPECT_TRUE(store.instances().empty());
  std::filesystem::remove_all(dir);
}

TEST(MetadataStoreTest, CorruptFileReturnsEmptyState) {
  std::string dir = MakeTempDir("corrupt");
  {
    std::ofstream f(dir + "/metadata.json");
    f << "{{not valid json!!!";
  }
  MetadataStore store(dir);
  EXPECT_TRUE(store.Load().ok());
  EXPECT_FALSE(store.has_metadata());
  std::filesystem::remove_all(dir);
}

TEST(MetadataStoreTest, AtomicWriteCleansTmpFile) {
  std::string dir = MakeTempDir("atomic");
  MetadataStore store(dir);
  store.AddInstance("projects/p/instances/i1", "I1", "emulator-config", 1000,
                    {}, "");
  EXPECT_TRUE(store.Save().ok());

  EXPECT_TRUE(std::filesystem::exists(dir + "/metadata.json"));
  EXPECT_FALSE(std::filesystem::exists(dir + "/metadata.json.tmp"));
  std::filesystem::remove_all(dir);
}

TEST(MetadataStoreTest, MultipleInstancesAndDatabases) {
  std::string dir = MakeTempDir("multi");
  {
    MetadataStore store(dir);
    store.AddInstance("projects/p/instances/i1", "I1", "cfg", 1000, {}, "");
    store.AddInstance("projects/p/instances/i2", "I2", "cfg", 2000, {}, "");
    store.AddDatabase("projects/p/instances/i1", "db1", "GOOGLE_STANDARD_SQL",
                      {"CREATE TABLE A (K INT64) PRIMARY KEY (K)"});
    store.AddDatabase("projects/p/instances/i1", "db2", "POSTGRESQL",
                      {"CREATE TABLE B (K INT8 PRIMARY KEY)"});
    store.AddDatabase("projects/p/instances/i2", "db3", "GOOGLE_STANDARD_SQL",
                      {});
    EXPECT_TRUE(store.Save().ok());
  }
  {
    MetadataStore store(dir);
    EXPECT_TRUE(store.Load().ok());
    const auto& instances = store.instances();
    EXPECT_EQ(instances.size(), 2);
    EXPECT_EQ(instances.at("projects/p/instances/i1").databases.size(), 2);
    EXPECT_EQ(instances.at("projects/p/instances/i2").databases.size(), 1);
    EXPECT_EQ(instances.at("projects/p/instances/i1")
                  .databases.at("db2")
                  .dialect,
              "POSTGRESQL");
  }
  std::filesystem::remove_all(dir);
}

TEST(MetadataStoreTest, RemoveInstanceAndDatabase) {
  std::string dir = MakeTempDir("remove");
  MetadataStore store(dir);
  store.AddInstance("projects/p/instances/i1", "I1", "cfg", 1000, {}, "");
  store.AddDatabase("projects/p/instances/i1", "db1", "GOOGLE_STANDARD_SQL",
                    {});
  store.AddDatabase("projects/p/instances/i1", "db2", "GOOGLE_STANDARD_SQL",
                    {});

  store.RemoveDatabase("projects/p/instances/i1", "db1");
  EXPECT_TRUE(store.Save().ok());
  EXPECT_TRUE(store.Load().ok());
  EXPECT_EQ(
      store.instances().at("projects/p/instances/i1").databases.size(), 1);

  store.RemoveInstance("projects/p/instances/i1");
  EXPECT_TRUE(store.Save().ok());
  EXPECT_TRUE(store.Load().ok());
  EXPECT_TRUE(store.instances().empty());

  std::filesystem::remove_all(dir);
}

TEST(MetadataStoreTest, UpdateDdlReplacesStatements) {
  std::string dir = MakeTempDir("ddl");
  MetadataStore store(dir);
  store.AddInstance("projects/p/instances/i1", "I1", "cfg", 1000, {}, "");
  store.AddDatabase("projects/p/instances/i1", "db1", "GOOGLE_STANDARD_SQL",
                    {"CREATE TABLE T1 (K INT64) PRIMARY KEY (K)"});
  store.UpdateDdl("projects/p/instances/i1", "db1",
                  {"CREATE TABLE T1 (K INT64) PRIMARY KEY (K)",
                   "CREATE TABLE T2 (K INT64) PRIMARY KEY (K)"});
  EXPECT_TRUE(store.Save().ok());
  EXPECT_TRUE(store.Load().ok());
  EXPECT_EQ(store.instances()
                .at("projects/p/instances/i1")
                .databases.at("db1")
                .ddl_statements.size(),
            2);
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
