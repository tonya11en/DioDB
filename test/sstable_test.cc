#include <filesystem>

#include "gtest/gtest.h"

#include "src/memtable.h"

using fs = std::filesystem;

class SSTableTest : public ::testing::Test {
  protected:
    void SetUp() override {
      tmp_dir_ = fs::temp_directory_path();
    }

    void TearDown() override {
      fs::remove_all(tmp_dir_);
    }

    fs::path tmp_dir_;
};

TEST_F(SStableTest, ExistingFileTest) {
  
}
