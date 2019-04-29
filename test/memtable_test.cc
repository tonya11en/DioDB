#include "gtest/gtest.h"

#include "src/memtable.h"

class MemtableTest : public ::testing::Test {
 protected:
  DioDB::Memtable memtable_;
};

TEST_F(MemtableTest, TestKeyExistsBasic) {
  EXPECT_FALSE(memtable_.KeyExists("bogus_key"));

  memtable_.Put("testkey1", "testval1");
  EXPECT_TRUE(memtable_.KeyExists("testkey1"));

  memtable_.Erase("testkey1");
  EXPECT_FALSE(memtable_.KeyExists("testkey1"));
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

TEST_F(MemtableTest, TestSize) {
  EXPECT_EQ(0, memtable_.Size());
  memtable_.Put("key1", "val1");
  EXPECT_EQ(1, memtable_.Size());
  memtable_.Erase("key1");
  EXPECT_EQ(0, memtable_.Size());
}
