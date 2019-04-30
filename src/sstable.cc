#include <experimental/filesystem>

namespace diodb {

using fs = std::experimental::filesystem;

SSTable::SSTable(const std::string filename) {
  CHECK(fs::exists(filename));
}

SSTable::SSTable(const std::string filename, const Memtable memtable) {

}

SSTable::SSTable(const std::string filename,
                 const std::vector<SSTable> sstables) {

}

} // namespace diodb
