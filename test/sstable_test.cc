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

namespace fs = boost::filesystem;
namespace diodb {
namespace test {

class SSTableTest : public ::testing::Test {
 protected:
  std::vector<char> S2Vec(const std::string&& s) {
    std::vector<char> v(s.begin(), s.end());
    return v;
  }
  void TearDown() override {
    for (const auto& fp : files_to_clean_) {
      fs::remove_all(fp);
    }
  }

  // Returns a path to a touched file with a given filename, then cleans up
  // the file at the end of the test.
  fs::path TouchFile(const std::string& filename) {
    fs::path filepath = filename;
    std::ofstream ofs = std::ofstream(filepath.generic_string());
    files_to_clean_.emplace_back(filepath);
    return filepath;
  }

  // Returns a filepath prefixed by the current working directory, then cleans
  // up the file at the end of the test.
  fs::path GetTempFilename(const std::string& filename) {
    auto p = filename;
    files_to_clean_.emplace_back(p);
    if (fs::exists(fs::path(filename))) {
      fs::remove(fs::path(filename));
    }
    return p;
  }

 private:
  std::vector<fs::path> files_to_clean_;
};

TEST_F(SSTableTest, ValidExistingFileTest) {
  // TODO: Create a valid SSTable file here later. Touching a file for now.
  fs::path sstable_filepath = TouchFile("valid_sstable");

  MockSSTable sstable(sstable_filepath);
}

TEST_F(SSTableTest, NonExistingFileTest) {
  fs::path sstable_filepath = fs::current_path() / "bogus_filename";

  const std::string failure_regex = "SSTable file .* does not exist";
  ASSERT_DEATH({ MockSSTable sstable(sstable_filepath); }, failure_regex);
}

TEST_F(SSTableTest, VerifyLockedMemtableFlush) {
  Memtable memtable;
  const std::string failure_regex = "unlocked memtable";
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

  const std::string failure_regex = "SSTable file .* exists";
  ASSERT_DEATH({ MockSSTable sstable(sstable_filepath, memtable); },
               failure_regex);
}

TEST_F(SSTableTest, UnexpectedExistingFileTestSSTable) {
  std::vector<fs::path> fpv;
  fpv.emplace_back(TouchFile("unexpected_file1"));
  fpv.emplace_back(TouchFile("unexpected_file2"));
  fpv.emplace_back(TouchFile("unexpected_file3"));

  std::vector<MockSSTable::SSTablePtr> sstv;
  for (const auto& p : fpv) {
    sstv.emplace_back(std::make_shared<MockSSTable>(p));
  }

  const std::string failure_regex = "SSTable file .* exists";
  const auto& existing_file = fpv[0];
  ASSERT_DEATH({ MockSSTable sstable(existing_file, sstv); }, failure_regex);
}

TEST_F(SSTableTest, MemtableFlushBasic) {
  const int num_entries = 32;

  // Populate memtable.
  Memtable memtable;
  std::vector<std::pair<std::string, std::string>> kvs;
  for (int ii = 0; ii < num_entries; ++ii) {
    kvs.emplace_back("foo" + std::to_string(ii), "bar" + std::to_string(ii));
  }
  std::random_shuffle(kvs.begin(), kvs.end());
  for (int ii = 0; ii < kvs.size(); ++ii) {
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
  std::vector<char> last_key;
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

}  // namespace test
}  // namespace diodb
