#include <string>

#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include "gtest/gtest.h"

#include "src/sstable.h"

namespace fs = boost::filesystem;
namespace diodb {
namespace test {

class SSTableTest : public ::testing::Test {};

TEST_F(SSTableTest, ValidExistingFileTest) {
  // TODO: Create a valid SSTable file here later. Touching a file for now.
  fs::path sstable_filepath = fs::current_path() / "valid_sstable";
  std::ofstream sstable_ofstream =
      std::ofstream(sstable_filepath.generic_string());

  SSTable sstable(sstable_filepath);

  sstable_ofstream.close();
}

TEST_F(SSTableTest, NonExistingFileTest) {
  fs::path sstable_filepath = fs::current_path() / "bogus_filename";

  const std::string failure_regex = "SSTable file .* does not exist";
  ASSERT_DEATH({ SSTable sstable = SSTable(sstable_filepath); }, failure_regex);
}

}  // namespace test
}  // namespace diodb
