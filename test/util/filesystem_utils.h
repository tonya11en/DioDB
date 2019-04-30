#include <string>

#include <boost/filesystem.hpp>
#include <glog/logging.h>

namespace fs = boost::filesystem;

namespace diodb {
namespace test {
namespace util {

// When instantiated, this class will create an appropriate temporary directory
// that can be filled with files. When the object is destroyed, it will clean
// up the directory.
class ScopedTempDirectory {
 public:
  ScopedTempDirectory() {
    tmp_directory_ = fs::unique_path();
    LOG(INFO) << "Creating unique directory " << fs::absolute(tmp_directory_);
  }

  ~ScopedTempDirectory() {
    const auto num_removed = fs::remove_all(tmp_directory_);
    LOG(INFO) << "Removed " << num_removed
              << " files rooted at " << fs::absolute(tmp_directory_);
  }

  fs::path Path() { return tmp_directory_; }

 private:
  fs::path tmp_directory_;
};

} // namespace util
} // namespace test
} // namespace diodb
