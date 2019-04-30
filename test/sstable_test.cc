#include <boost/filesystem.hpp>
#include "gtest/gtest.h"
#include <glog/logging.h>

#include "src/sstable.h"
#include "test/util/filesystem_utils.h"

using diodb::test::util::ScopedTempDirectory;
namespace fs = boost::filesystem;

namespace diodb {
namespace test {

class SSTableTest : public ::testing::Test {
  public:
   SSTableTest() : tmp_dir_(ScopedTempDirectory()) {}

  protected:
   ScopedTempDirectory tmp_dir_; 
};

TEST_F(SSTableTest, ExistingFileTest) {
  
}

} // namespace test
} // namespace diodb
