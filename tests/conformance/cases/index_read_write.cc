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

#include <cstdint>
#include <string>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using zetasql_base::testing::StatusIs;

class IndexTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("index.test");
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectIndexTest, IndexTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<IndexTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(IndexTest, ReturnsRowsInDescendingOrder) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {5, Null<std::string>(), 18}));

  // Read back all rows.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        ReadAllWithIndex("Users", "UsersByNameDescending", {"Name", "ID"}),
        IsOkAndHoldsRows({{"Peter", 2},
                          {"Matthew", 4},
                          {"John", 1},
                          {"Adam", 0},
                          {Null<std::string>(), 5}}));
  } else {
    EXPECT_THAT(
        ReadAllWithIndex("Users", "UsersByNameDescending", {"Name", "ID"}),
        IsOkAndHoldsRows({{Null<std::string>(), 5},
                          {"Peter", 2},
                          {"Matthew", 4},
                          {"John", 1},
                          {"Adam", 0}}));
  }
}

TEST_P(IndexTest, ReturnsRowsInAscendingOrder) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {5, Null<std::string>(), 18}));

  // Read back all rows.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
                IsOkAndHoldsRows({{Null<std::string>(), 5},
                                  {"Adam", 0},
                                  {"John", 1},
                                  {"Matthew", 4},
                                  {"Peter", 2}}));
  } else {
    EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
                IsOkAndHoldsRows({{"Adam", 0},
                                  {"John", 1},
                                  {"Matthew", 4},
                                  {"Peter", 2},
                                  {Null<std::string>(), 5}}));
  }
}

TEST_P(IndexTest, IndexEntriesAreUpdated) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));
  ZETASQL_EXPECT_OK(Update("Users", {"ID", "Name", "Age"}, {2, "Samantha", 24}));
  ZETASQL_EXPECT_OK(Update("Users", {"ID", "Name", "Age"}, {4, "Alice", 21}));

  // Read back all rows.
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({
                  {"Adam", 0},
                  {"Alice", 4},
                  {"John", 1},
                  {"Samantha", 2},
              }));
}

TEST_P(IndexTest, IndexEntriesAreDeleted) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "John", 22}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {2, "Peter", 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {4, "Matthew", 33}));

  ZETASQL_EXPECT_OK(Delete("Users", {Key(0), Key(2)}));
  // Read back all rows.
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({{"John", 1}, {"Matthew", 4}}));

  ZETASQL_EXPECT_OK(Delete("Users", {Key(1), Key(4)}));
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({}));
}

TEST_P(IndexTest, EmptyIndexReturnsZeroRows) {
  // Read back all rows.
  EXPECT_THAT(ReadAll("Users", {"ID", "Name", "Age"}), IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAllWithIndex("Users", "UsersByName", {"Name", "ID"}),
              IsOkAndHoldsRows({}));
}

TEST_P(IndexTest, NullEntriesAreFiltered) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "", 22}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {2, Null<std::string>(), 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {3, "John", 28}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"},
                   {4, "Matthew", Null<std::int64_t>()}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameNullFiltered",
                       {"Name", "Age", "ID"}),
      IsOkAndHoldsRows({{"", 22, 1}, {"Adam", 20, 0}, {"John", 28, 3}}));
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameAgeNotNull", {"Name", "Age", "ID"}),
      IsOkAndHoldsRows({{"", 22, 1}, {"Adam", 20, 0}, {"John", 28, 3}}));
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(ReadAllWithIndex("Users", "UsersByNameStoreAgeNotNull",
                                 {"Name", "Age", "ID"}),
                IsOkAndHoldsRows({{Null<std::string>(), 41, 2},
                                  {"", 22, 1},
                                  {"Adam", 20, 0},
                                  {"John", 28, 3}}));
  } else {
    EXPECT_THAT(ReadAllWithIndex("Users", "UsersByNameStoreAgeNotNull",
                                 {"Name", "Age", "ID"}),
                IsOkAndHoldsRows({{"", 22, 1},
                                  {"Adam", 20, 0},
                                  {"John", 28, 3},
                                  {Null<std::string>(), 41, 2}}));
  }
}

TEST_P(IndexTest, AllEntriesAreUnique) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "", 22}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {2, Null<std::string>(), 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {3, "John", 28}));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {4, "Adam", 20}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {5, "", 20}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    // ZetaSQL treats NULLs as not-distinct values, so an error is triggered
    // when inserting multiple entries with NULL values.
    EXPECT_THAT(
        Insert("Users", {"ID", "Name", "Age"}, {6, Null<std::string>(), 41}),
        StatusIs(absl::StatusCode::kAlreadyExists));
  } else {
    // PostgreSQL treats NULLs as distinct values, so it is possible to insert
    // multiple entries with NULL values even in the presence of a UNIQUE index.
    ZETASQL_EXPECT_OK(
        Insert("Users", {"ID", "Name", "Age"}, {6, Null<std::string>(), 41}));
  }
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"},
                   {7, "Matthew", Null<std::int64_t>()}));

  // Read back all rows.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(ReadAllWithIndex("Users", "UsersByNameAgeUnique",
                                 {"Name", "Age", "ID"}),
                IsOkAndHoldsRows({{Null<std::string>(), 41, 2},
                                  {"", 22, 1},
                                  {"Adam", 20, 0},
                                  {"John", 28, 3},
                                  {"Matthew", Null<std::int64_t>(), 7}}));
  } else {
    // In Spangres, UNIQUE INDEXes exclude NULL values.
    EXPECT_THAT(
        ReadAllWithIndex("Users", "UsersByNameAgeUnique",
                         {"Name", "Age", "ID"}),
        IsOkAndHoldsRows({{"", 22, 1}, {"Adam", 20, 0}, {"John", 28, 3}}));
  }
}

TEST_P(IndexTest, ReadOnIndexWithSingletonRow) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL does not support singleton rows.";
  }
  ZETASQL_EXPECT_OK(Insert("NoPkTable", {"Col1"}, {20}));
  EXPECT_THAT(Insert("NoPkTable", {"Col1"}, {30}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(ReadAllWithIndex("NoPkTable", "NoPkTableIdx", {"Col1"}),
              IsOkAndHoldsRows({{
                  20,
              }}));
}

TEST_P(IndexTest, TriggersUniqueIndexViolationWithImplicitNulls) {
  if (dialect_ == database_api::POSTGRESQL) {
    GTEST_SKIP() << "PostgreSQL treats NULL as distinct values";
  }

  // In both cases, NULL value triggers a Unique index violations for primary
  // key "Name, Age" in UsersByNameAgeUnique index.

  // Executed across separate transactions.
  {
    // Index UsersByNameAgeUnique will add NULL, NULL for Name & Age column.
    ZETASQL_EXPECT_OK(Insert("Users", {"ID"}, {0}));

    // This should fail because it is also adding NULL, NULL to unique Index
    // UsersByNameAgeUnique.
    EXPECT_THAT(Insert("Users", {"ID"}, {1}),
                StatusIs(absl::StatusCode::kAlreadyExists));
  }

  // Executed within same transaction.
  {
    auto txn = Transaction(Transaction::ReadWriteOptions());
    EXPECT_THAT(
        CommitTransaction(txn, {MakeInsertOrUpdate("Users", {"ID"}, Value(0)),
                                MakeInsert("Users", {"ID"}, Value(1))}),
        StatusIs(absl::StatusCode::kAlreadyExists));
  }
}

TEST_P(IndexTest, AllEntriesAreUniqueAndNullFiltered) {
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {0, "Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {1, "", 22}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {2, Null<std::string>(), 41}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"}, {3, "John", 28}));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {4, "Adam", 20}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {5, "", 22}),
              StatusIs(absl::StatusCode::kAlreadyExists));
  // A duplicate index entry that is null filtered should not trigger a UNIQUE
  // violation.
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Age"}, {6, Null<std::string>(), 43}));
  ZETASQL_EXPECT_OK(Insert("Users", {"ID", "Name", "Age"},
                   {7, "Matthew", Null<std::int64_t>()}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameUniqueFiltered", {"Name", "ID"}),
      IsOkAndHoldsRows({{"", 1}, {"Adam", 0}, {"John", 3}, {"Matthew", 7}}));
}

TEST_P(IndexTest, ValidateKeyTooLargeFails) {
  std::string long_name(8192, 'a');
  EXPECT_THAT(Insert("Users", {"ID", "Name", "Age"}, {1, long_name, 20}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

class NumericIndexTest : public DatabaseTest {
 public:
  void SetUp() override { DatabaseTest::SetUp(); }

  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchemaFromFile("numeric_index.test");
  }
};

TEST_F(NumericIndexTest, BasicRead) {
  Numeric adam_money =
      cloud::spanner::MakeNumeric("-9999999999999999123.456789").value();
  Numeric bill_money = cloud::spanner::MakeNumeric("123.456789").value();
  Numeric john_money = cloud::spanner::MakeNumeric("0").value();
  Numeric zack_money = cloud::spanner::MakeNumeric("999999999.456789").value();

  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {0, "Zack", zack_money}));
  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {1, "John", john_money}));
  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {2, "Adam", adam_money}));
  ZETASQL_EXPECT_OK(
      Insert("Accounts", {"ID", "Name", "Money"}, {3, "Bill", bill_money}));

  EXPECT_THAT(ReadAllWithIndex("Accounts", "AccountsByNameStoringMoney",
                               {"Name", "Money"}),
              IsOkAndHoldsRows({{"Adam", adam_money},
                                {"Bill", bill_money},
                                {"John", john_money},
                                {"Zack", zack_money}}));

  EXPECT_THAT(ReadAllWithIndex("Accounts", "AccountsByMoney", {"Money"}),
              IsOkAndHoldsRows(
                  {{adam_money}, {john_money}, {bill_money}, {zack_money}}));
}

class JsonIndexTest : public DatabaseTest {
 public:
  void SetUp() override { DatabaseTest::SetUp(); }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("json_index.test");
  }
};

TEST_F(JsonIndexTest, BasicRead) {
  Json adam_config = Json("{\"role\":\"accountant\"}");
  Json bill_config = Json("{\"floor\":2}");
  Json john_config = Json();

  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Config"}, {1, "Bill", bill_config}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Config"}, {2, "John", john_config}));
  ZETASQL_EXPECT_OK(
      Insert("Users", {"ID", "Name", "Config"}, {0, "Adam", adam_config}));

  EXPECT_THAT(
      ReadAllWithIndex("Users", "UsersByNameStoringConfig", {"Name", "Config"}),
      IsOkAndHoldsRows({
          {"Adam", adam_config},
          {"Bill", bill_config},
          {"John", john_config},
      }));
}

class RemoteIndexTest : public DatabaseTest {
 public:
  void SetUp() override { DatabaseTest::SetUp(); }

  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_interleave_in = true;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchemaFromFile("remote_index.test");
  }
};

TEST_F(RemoteIndexTest, InsertsThenDeletes) {
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Timothy", 40}));

  ZETASQL_EXPECT_OK(
      Insert("TestTable", {"id", "testname", "info"}, {1, "Adam", "AdamInfo"}));
  // Can insert a row in remote index without parent row.
  ZETASQL_EXPECT_OK(
      Insert("TestTable", {"id", "testname", "info"}, {2, "Bill", "BillInfo"}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"}, {"Bill", "BillInfo"}}));

  // Delete the parent row, the index should stay the same.
  ZETASQL_EXPECT_OK(Delete("ParentTable", {Key("Adam")}));
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"}, {"Bill", "BillInfo"}}));

  // Delete the indexed table row, the index should contain only 1 row now.
  ZETASQL_EXPECT_OK(Delete("TestTable", {Key(2)}));
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"}}));
}

TEST_F(RemoteIndexTest, InsertsThenUpdates) {
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Charlie", 30}));

  ZETASQL_EXPECT_OK(
      Insert("TestTable", {"id", "testname", "info"}, {1, "Adam", "AdamInfo"}));
  // Insert a row in remote index without parent row.
  ZETASQL_EXPECT_OK(
      Insert("TestTable", {"id", "testname", "info"}, {2, "Bill", "BillInfo"}));
  // Insert another row in remote index with parent row.
  ZETASQL_EXPECT_OK(Insert("TestTable", {"id", "testname", "info"},
                   {3, "Charlie", "CharlieInfo"}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"},
                        {"Bill", "BillInfo"},
                        {"Charlie", "CharlieInfo"}}));

  // Update an indexed table row, the index should be updated accordingly.
  ZETASQL_EXPECT_OK(Update("TestTable", {"id", "testname", "info"},
                   {2, "NoLongerBill", "NotBillInfo"}));
  ZETASQL_EXPECT_OK(Update("TestTable", {"id", "testname", "info"},
                   {3, "NoLongerCharlie", "NotCharlieInfo"}));
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"},
                        {"NoLongerBill", "NotBillInfo"},
                        {"NoLongerCharlie", "NotCharlieInfo"}}));
}

class RemoteIndexWithInterleaveInTest : public DatabaseTest {
 public:
  void SetUp() override { DatabaseTest::SetUp(); }

  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_interleave_in = true;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchemaFromFile("remote_index_with_interleave_in.test");
  }
};

TEST_F(RemoteIndexWithInterleaveInTest, InsertsThenDeletes) {
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Timothy", 40}));

  ZETASQL_EXPECT_OK(Insert("TestTable", {"name", "testname", "info"},
                   {"Adam", "Adam", "AdamInfo"}));
  // Can insert a row in remote index without parent row.
  ZETASQL_EXPECT_OK(Insert("TestTable", {"name", "testname", "info"},
                   {"Bill", "Bill", "BillInfo"}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"}, {"Bill", "BillInfo"}}));

  // Delete the parent row, the index should stay the same.
  ZETASQL_EXPECT_OK(Delete("ParentTable", {Key("Adam")}));
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"}, {"Bill", "BillInfo"}}));

  // Delete the indexed table row, the index should contain only 1 row now.
  ZETASQL_EXPECT_OK(Delete("TestTable", {Key("Bill")}));
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"}}));
}

TEST_F(RemoteIndexWithInterleaveInTest, InsertsThenUpdates) {
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Adam", 20}));
  ZETASQL_EXPECT_OK(Insert("ParentTable", {"name", "age"}, {"Charlie", 30}));

  ZETASQL_EXPECT_OK(Insert("TestTable", {"name", "testname", "info"},
                   {"Adam", "Adam", "AdamInfo"}));
  // Insert a row in remote index without parent row.
  ZETASQL_EXPECT_OK(Insert("TestTable", {"name", "testname", "info"},
                   {"Bill", "Bill", "BillInfo"}));
  // Insert another row in remote index with parent row.
  ZETASQL_EXPECT_OK(Insert("TestTable", {"name", "testname", "info"},
                   {"Charlie", "Charlie", "CharlieInfo"}));

  // Read back all rows.
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"},
                        {"Bill", "BillInfo"},
                        {"Charlie", "CharlieInfo"}}));

  // Update an indexed table row, the index should be updated accordingly.
  ZETASQL_EXPECT_OK(Update("TestTable", {"name", "testname", "info"},
                   {"Bill", "NoLongerBill", "NotBillInfo"}));
  ZETASQL_EXPECT_OK(Update("TestTable", {"name", "testname", "info"},
                   {"Charlie", "NoLongerCharlie", "NotCharlieInfo"}));
  EXPECT_THAT(
      ReadAllWithIndex("TestTable", "RemoteIndex", {"testname", "info"}),
      IsOkAndHoldsRows({{"Adam", "AdamInfo"},
                        {"NoLongerBill", "NotBillInfo"},
                        {"NoLongerCharlie", "NotCharlieInfo"}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
