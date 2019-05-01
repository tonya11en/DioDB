#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <boost/filesystem.hpp>
#include "gtest/gtest.h"

#include "proto/segment.pb.h"
#include "src/memtable.h"
#include "src/sstable.h"

using google::protobuf::io::FileInputStream;
using google::protobuf::io::ZeroCopyInputStream;
using google::protobuf::util::ParseDelimitedFromZeroCopyStream;

namespace fs = boost::filesystem;
namespace diverdb {
namespace test {

class SSTableTest : public ::testing::Test {
 protected:
  // Returns a path to a touched file with a given filename.
  fs::path TouchFile(const std::string& filename) {
    fs::path filepath = fs::current_path() / filename;
    std::ofstream ofs = std::ofstream(filepath.generic_string());
    return filepath;
  }
};

TEST_F(SSTableTest, ValidExistingFileTest) {
  // TODO: Create a valid SSTable file here later. Touching a file for now.
  fs::path sstable_filepath = TouchFile("valid_sstable");

  SSTable sstable(sstable_filepath);
}

TEST_F(SSTableTest, NonExistingFileTest) {
  fs::path sstable_filepath = fs::current_path() / "bogus_filename";

  const std::string failure_regex = "SSTable file .* does not exist";
  ASSERT_DEATH({ SSTable sstable = SSTable(sstable_filepath); }, failure_regex);
}

TEST_F(SSTableTest, VerifyLockedMemtableFlush) {
  Memtable memtable;
  const std::string failure_regex = "unlocked memtable";
  ASSERT_DEATH({ SSTable sstable = SSTable("some_file", memtable); },
               failure_regex);
}

TEST_F(SSTableTest, UnexpectedExistingFileTestMemtable) {
  fs::path sstable_filepath = TouchFile("unexpected_file");
  Memtable memtable;
  memtable.Lock();

  const std::string failure_regex = "SSTable file .* exists";
  ASSERT_DEATH({ SSTable sstable = SSTable(sstable_filepath, memtable); },
               failure_regex);
}

TEST_F(SSTableTest, UnexpectedExistingFileTestSSTable) {
  std::vector<fs::path> fpv;
  fpv.emplace_back(TouchFile("unexpected_file1"));
  fpv.emplace_back(TouchFile("unexpected_file2"));
  fpv.emplace_back(TouchFile("unexpected_file3"));

  std::vector<SSTable::SSTablePtr> sstv;
  for (const auto& p : fpv) {
    sstv.emplace_back(std::make_shared<SSTable>(p));
  }

  const std::string failure_regex = "SSTable file .* exists";
  const auto& existing_file = fpv[0];
  ASSERT_DEATH({ SSTable sstable = SSTable(existing_file, sstv); },
               failure_regex);
}

TEST_F(SSTableTest, MemtableFlushBasic) {
  const int num_entries = 100;

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
  const std::string filename = "test_file";
  SSTable sstable(filename, memtable);

  // Verify the file exists.
  ASSERT_TRUE(fs::exists(sstable.filepath()));

  // Deserialize the segments written to disk and verify they are ordered by
  // key.
  int fd = open(filename.c_str(), O_RDONLY);
  ZeroCopyInputStream* input = new FileInputStream(fd);
  ASSERT_NE(input, nullptr);
  Segment segment;
  bool clean_eof;
  std::string last_key;
  for (int ii = 0; ii < num_entries; ++ii) {
    // Make sure the parse is successful for the first 99 entries.
    ASSERT_TRUE(ParseDelimitedFromZeroCopyStream(&segment, input, &clean_eof));
    // clean_eof should not be true until the end of the file.
    ASSERT_FALSE(clean_eof);
    if (ii != 0) {
      // Make sure the SSTable is actually sorted by the keys.
      ASSERT_LE(last_key, segment.key());
    }
    last_key = segment.key();
  }

  // There should be no more segments in the file.
  ASSERT_FALSE(ParseDelimitedFromZeroCopyStream(&segment, input, &clean_eof));
  ASSERT_TRUE(clean_eof);

  close(fd);
}

}  // namespace test
}  // namespace diverdb
