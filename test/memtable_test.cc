#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "src/memtable.h"

namespace diodb {
namespace test {

class MemtableTest : public ::testing::Test {
 protected:
  void SetUp() override { memtable_ = Memtable(); }
  std::vector<char> S2Vec(const std::string&& s) {
    std::vector<char> v(s.begin(), s.end());
    return v;
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

TEST_F(MemtableTest, TestMemtableLocking) {
  memtable_.Put("testkey1", "testval1");
  memtable_.Lock();
  ASSERT_DEATH({ memtable_.Put("foo", "bar"); }, "is_locked");
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
  EXPECT_EQ(memtable_.Get("key1"), S2Vec(""));

  memtable_.Put("key1", "val1");
  EXPECT_EQ(memtable_.Get("key1"), S2Vec("val1"));

  // Verify overwrites.
  memtable_.Put("key1", "val2");
  EXPECT_EQ(memtable_.Get("key1"), S2Vec("val2"));

  // Verify Erasure.
  memtable_.Erase("key1");
  EXPECT_EQ(memtable_.Get("key1"), S2Vec(""));
}

}  // namespace test
}  // namespace diodb
