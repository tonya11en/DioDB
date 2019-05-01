#include "gtest/gtest.h"

#include "src/memtable.h"

namespace diverdb {
namespace test {

class MemtableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memtable_ = Memtable();
  }
  Memtable memtable_;
};

TEST_F(MemtableTest, TestKeyExistsBasic) {
  EXPECT_FALSE(memtable_.KeyExists("bogus_key"));

  memtable_.Put("testkey1", "testval1");
  EXPECT_TRUE(memtable_.KeyExists("testkey1"));

  memtable_.Erase("testkey1");
  EXPECT_FALSE(memtable_.KeyExists("testkey1"));

  memtable_.Put("testkey1", "testval1");
  EXPECT_TRUE(memtable_.KeyExists("testkey1"));
  memtable_.Put("testkey2", "testval2");
  EXPECT_TRUE(memtable_.KeyExists("testkey2"));
}

TEST_F(MemtableTest, TestMemtableStats) {
  EXPECT_EQ(0, memtable_.num_valid_entries());
  EXPECT_EQ(0, memtable_.num_delete_entries());
  memtable_.Put("testkey1", "testval1");
  EXPECT_EQ(1, memtable_.num_valid_entries());
  EXPECT_EQ(0, memtable_.num_delete_entries());
  memtable_.Put("testkey1", "testval1");
  EXPECT_EQ(1, memtable_.num_valid_entries());
  EXPECT_EQ(0, memtable_.num_delete_entries());
  memtable_.Erase("testkey1");
  EXPECT_EQ(0, memtable_.num_valid_entries());
  EXPECT_EQ(1, memtable_.num_delete_entries());
  memtable_.Erase("testkey1");
  EXPECT_EQ(0, memtable_.num_valid_entries());
  EXPECT_EQ(1, memtable_.num_delete_entries());
  memtable_.Put("testkey1", "testval1");
  EXPECT_EQ(1, memtable_.num_valid_entries());
  EXPECT_EQ(0, memtable_.num_delete_entries());
  memtable_.Put("testkey2", "testval2");
  EXPECT_EQ(2, memtable_.num_valid_entries());
  EXPECT_EQ(0, memtable_.num_delete_entries());
  memtable_.Erase("testkey1");
  EXPECT_EQ(1, memtable_.num_valid_entries());
  EXPECT_EQ(1, memtable_.num_delete_entries());
  memtable_.Erase("testkey2");
  EXPECT_EQ(0, memtable_.num_valid_entries());
  EXPECT_EQ(2, memtable_.num_delete_entries());
}

TEST_F(MemtableTest, TestGetBasic) {
  // Should return empty buffer for keys that don't exist.
  EXPECT_EQ(memtable_.Get("key1"), "");

  memtable_.Put("key1", "val1");
  EXPECT_EQ(memtable_.Get("key1"), "val1");

  // Verify overwrites.
  memtable_.Put("key1", "val2");
  EXPECT_EQ(memtable_.Get("key1"), "val2");

  // Verify Erasure.
  memtable_.Erase("key1");
  EXPECT_EQ(memtable_.Get("key1"), "");
}

}  // namespace test
}  // namespace diverdb
