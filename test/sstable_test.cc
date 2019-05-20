#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include "gtest/gtest.h"

#include "src/memtable.h"
#include "test/mocks/sstable_mock.h"

using std::string;
using std::pair;
using std::vector;
using std::function;

namespace fs = boost::filesystem;
namespace diodb {
namespace test {

class SSTableTest : public ::testing::Test {
 protected:
  vector<char> S2Vec(const string&& s) {
    vector<char> v(s.begin(), s.end());
    return v;
  }
  void TearDown() override {
    for (const auto& fp : files_to_clean_) {
      fs::remove_all(fp);
    }
  }

  std::string Vec2String(std::vector<char>& v) {
    return std::string(v.begin(), v.end());
  }

  std::vector<char> String2Vec(std::string s) {
    return std::vector<char>(s.begin(), s.end());
  }

  // Returns a path to a touched file with a given filename, then cleans up
  // the file at the end of the test.
  fs::path TouchFile(const string& filename) {
    fs::path filepath = filename;
    std::ofstream ofs = std::ofstream(filepath.generic_string());
    files_to_clean_.emplace_back(filepath);
    return filepath;
  }

  // Returns a filepath prefixed by the current working directory, then cleans
  // up the file at the end of the test.
  fs::path GetTempFilename(const string& filename) {
    auto p = filename;
    files_to_clean_.emplace_back(p);
    if (fs::exists(fs::path(filename))) {
      fs::remove(fs::path(filename));
    }
    return p;
  }

  // Makes an SSTable object from a vector of KV pairs.
  static MockSSTable::SSTablePtr MakeSST(fs::path filename, vector<pair<string, string>> kvs) {
    Memtable memtable;
    for (const auto& kv : kvs) {
      memtable.Put(kv.first, kv.second);
    }
    memtable.Lock();
    return std::make_shared<SSTable>(filename, memtable);
  }

 private:
  vector<fs::path> files_to_clean_;
};

TEST_F(SSTableTest, ValidExistingFileTest) {
  // TODO: Create a valid SSTable file here later. Touching a file for now.
  fs::path sstable_filepath = TouchFile("valid_sstable");

  MockSSTable sstable(sstable_filepath);
}

TEST_F(SSTableTest, NonExistingFileTest) {
  fs::path sstable_filepath = fs::current_path() / "bogus_filename";

  const string failure_regex = "SSTable file .* does not exist";
  ASSERT_DEATH({ MockSSTable sstable(sstable_filepath); }, failure_regex);
}

TEST_F(SSTableTest, VerifyLockedMemtableFlush) {
  Memtable memtable;
  const string failure_regex = "unlocked memtable";
  ASSERT_DEATH(
      {
        MockSSTable sstable(GetTempFilename("VerifyLockedMemtableFlush"),
                            memtable);
      },
      failure_regex);
}

TEST_F(SSTableTest, UnexpectedExistingFileTestMemtable) {
  fs::path sstable_filepath = TouchFile("unexpected_file");
  Memtable memtable;
  memtable.Lock();

  const string failure_regex = "SSTable file .* exists";
  ASSERT_DEATH({ MockSSTable sstable(sstable_filepath, memtable); },
               failure_regex);
}

TEST_F(SSTableTest, UnexpectedExistingFileTestSSTable) {
  vector<fs::path> fpv;
  fpv.emplace_back(TouchFile("unexpected_file1"));
  fpv.emplace_back(TouchFile("unexpected_file2"));
  fpv.emplace_back(TouchFile("unexpected_file3"));

  vector<MockSSTable::SSTablePtr> sstv;
  for (const auto& p : fpv) {
    sstv.emplace_back(std::make_shared<MockSSTable>(p));
  }

  const string failure_regex = "SSTable file .* exists";
  const auto& existing_file = fpv[0];
  ASSERT_DEATH({ MockSSTable sstable(existing_file, sstv); }, failure_regex);
}

TEST_F(SSTableTest, MemtableFlushBasic) {
  const int num_entries = 32;

  // Populate memtable.
  Memtable memtable;
  vector<pair<string, string>> kvs;
  for (int ii = 0; ii < num_entries; ++ii) {
    kvs.emplace_back("foo" + std::to_string(ii), "bar" + std::to_string(ii));
  }
  std::random_shuffle(kvs.begin(), kvs.end());
  for (size_t ii = 0; ii < kvs.size(); ++ii) {
    memtable.Put(std::move(kvs[ii].first), std::move(kvs[ii].second));
  }
  memtable.Lock();

  // Create the SSTable file using the memtable.
  fs::path filename = GetTempFilename("MemtableFlushBasic");
  MockSSTable sstable(filename, memtable);

  // Verify the file exists.
  ASSERT_TRUE(fs::exists(sstable.filepath()));

  Segment segment;

  IOHandle iohandle(filename);
  vector<char> last_key;
  for (int ii = 0; ii < num_entries; ++ii) {
    iohandle.ParseNext(&segment);
    if (ii != 0) {
      // Make sure the SSTable is actually sorted by the keys.
      ASSERT_LE(last_key, segment.key);
    }
    last_key = segment.key;
  }
}

TEST_F(SSTableTest, SSTableParserMalformedFile) {
  // Let's just build an sstable from a memtable for simplicity.
  Memtable memtable;
  for (int ii = 0; ii < 8 * 1024; ++ii) {
    memtable.Put(std::to_string(ii), std::to_string(ii) + "-val");
  }
  memtable.Lock();
  fs::path filename = GetTempFilename("SSTableParserMalformedFile1");
  MockSSTable sstable(filename, memtable);

  // Corrupt the file by shaving off the last byte. Assuming there's no
  // mechanism in place to detect two SSTs pointing to same file, so let's just
  // build a new one on top.
  auto file_size = fs::file_size(filename);
  fs::resize_file(filename, file_size - 1);
  ASSERT_DEATH({ MockSSTable sstable2(filename); }, "Error reading segment");
}

TEST_F(SSTableTest, SSTableKeyExists) {
  Memtable memtable;
  memtable.Put("b", "xxx");
  memtable.Put("d", "xxx");
  memtable.Put("f", "xxx");
  memtable.Lock();
  fs::path filename = GetTempFilename("SSTableKeyExists");
  MockSSTable sstable(filename, memtable);

  // Test key absent.
  ASSERT_FALSE(sstable.KeyExists("a"));
  ASSERT_FALSE(sstable.KeyExists("c"));
  ASSERT_FALSE(sstable.KeyExists("e"));
  ASSERT_FALSE(sstable.KeyExists("g"));

  // Test key present.
  ASSERT_TRUE(sstable.KeyExists("b"));
  ASSERT_TRUE(sstable.KeyExists("d"));
  ASSERT_TRUE(sstable.KeyExists("f"));
}

TEST_F(SSTableTest, SSTableMergeBasicAdjacent) {
  using KVPair = pair<string, string>;
  vector<KVPair> kvs0, kvs1;
  for (int ii = 0; ii < 100; ++ii) {
    kvs0.emplace_back(std::to_string(ii), std::to_string(ii) + "-val");
    kvs1.emplace_back(std::to_string(100 + ii), std::to_string(100 + ii) + "-val");
  }

  vector<MockSSTable::SSTablePtr> ssts;
  auto filename = GetTempFilename("SSTableMergeBasicAdjacent-0");
  ssts.emplace_back(MakeSST(filename, kvs0));
  filename = GetTempFilename("SSTableMergeBasicAdjacent-1");
  ssts.emplace_back(MakeSST(filename, kvs1));

  filename = GetTempFilename("SSTableMergeBasicAdjacent-merged");
  MockSSTable sstable(filename, ssts);
  CHECK(sstable.SanityCheck());
  for (int ii = 0; ii < 200; ++ii) {
    ASSERT_TRUE(sstable.KeyExists(std::to_string(ii))) << ii;
  }
}

TEST_F(SSTableTest, SSTableMergeBasicInterleave) {
  auto merge_helper =
    [](int n, int size, int offset,
       fs::path filename,
       function<MockSSTable::SSTablePtr(fs::path, vector<pair<string, string>>)> make_sst) {

    using KVPair = pair<string, string>;
    vector<KVPair> kvs;
    for (int ii = offset; ii < n * size; ii += n) {
      kvs.emplace_back(std::to_string(ii), std::to_string(ii) + "-val");
    }

    MockSSTable::SSTablePtr sst = make_sst(filename, kvs);
    return sst;
  };

  auto filename = GetTempFilename("SSTableMergeBasicInterleave-0");
  vector<MockSSTable::SSTablePtr> ssts;
  ssts.emplace_back(merge_helper(3, 100, 0, filename, MakeSST));
  filename = GetTempFilename("SSTableMergeBasicInterleave-1");
  ssts.emplace_back(merge_helper(3, 100, 1, filename, MakeSST));
  filename = GetTempFilename("SSTableMergeBasicInterleave-2");
  ssts.emplace_back(merge_helper(3, 100, 2, filename, MakeSST));

  ASSERT_TRUE(ssts.at(0)->KeyExists(std::to_string(0)));
  ASSERT_FALSE(ssts.at(0)->KeyExists(std::to_string(1)));
  ASSERT_TRUE(ssts.at(1)->KeyExists(std::to_string(1)));
  ASSERT_FALSE(ssts.at(1)->KeyExists(std::to_string(2)));
  ASSERT_TRUE(ssts.at(2)->KeyExists(std::to_string(2)));
  ASSERT_FALSE(ssts.at(2)->KeyExists(std::to_string(3)));

  filename = GetTempFilename("SSTableMergeBasicInterleave-merged");
  MockSSTable sstable(filename, ssts);
  CHECK(sstable.SanityCheck());
  for (int ii = 0; ii < 300; ++ii) {
    ASSERT_TRUE(sstable.KeyExists(std::to_string(ii))) << ii;
  }
}

TEST_F(SSTableTest, SSTableMergeDuplicates) {
  using KVPair = pair<string, string>;
  vector<KVPair> kvs0, kvs1;
  kvs0.emplace_back("0", "0-new");
  kvs0.emplace_back("1", "1-new");
  kvs0.emplace_back("3", "3-new");

  kvs1.emplace_back("0", "0-old");
  kvs1.emplace_back("2", "2-old");
  kvs1.emplace_back("3", "3-old");

  vector<MockSSTable::SSTablePtr> ssts;
  auto filename = GetTempFilename("SSTableMergeBasicAdjacent-0");
  ssts.emplace_back(MakeSST(filename, kvs0));
  filename = GetTempFilename("SSTableMergeBasicAdjacent-1");
  ssts.emplace_back(MakeSST(filename, kvs1));

  filename = GetTempFilename("SSTableMergeBasicAdjacent-merged");
  MockSSTable sstable(filename, ssts);
  CHECK(sstable.SanityCheck());

  ASSERT_EQ(sstable.Get("0"), String2Vec("0-new"));
  ASSERT_EQ(sstable.Get("1"), String2Vec("1-new"));
  ASSERT_EQ(sstable.Get("2"), String2Vec("2-old"));
  ASSERT_EQ(sstable.Get("3"), String2Vec("3-new"));
}

TEST_F(SSTableTest, SSTableGetBasic) {
  Memtable memtable;
  memtable.Put("holy", "diver");
  memtable.Put("you", "been");
  memtable.Erase("gone");
  memtable.Erase("long");
  memtable.Lock();

  auto filename = GetTempFilename("SSTableGetBasic");
  MockSSTable sstable(filename, memtable);

  ASSERT_EQ(sstable.Get("holy"), String2Vec("diver"));
  ASSERT_EQ(sstable.Get("you"), String2Vec("been"));
  ASSERT_FALSE(sstable.KeyExists("gone"));
  ASSERT_FALSE(sstable.KeyExists("long"));

  CHECK(sstable.SanityCheck());
}

}  // namespace test
}  // namespace diodb
