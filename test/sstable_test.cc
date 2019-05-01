#include <memory>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include "gtest/gtest.h"

#include "src/sstable.h"
#include "src/memtable.h"

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

TEST_F(SSTableTest, UnexpectedExistingFileTestMemtable) {
  fs::path sstable_filepath = TouchFile("unexpected_file");
  Memtable memtable;

  const std::string failure_regex = "SSTable file .* exists";
  ASSERT_DEATH({ SSTable sstable = SSTable(sstable_filepath, memtable); }, failure_regex);
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
  ASSERT_DEATH({ SSTable sstable = SSTable(existing_file, sstv); }, failure_regex);
}

}  // namespace test
}  // namespace diverdb
