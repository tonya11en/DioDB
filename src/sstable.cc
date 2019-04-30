#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include "sstable.h"

namespace diodb {

SSTable::SSTable(const fs::path sstable_path) {
  CHECK(fs::exists(sstable_path))
      << "SSTable file " << sstable_path << " does not exist";
}

SSTable::SSTable(const fs::path new_sstable_path, const Memtable memtable) {}

SSTable::SSTable(const fs::path new_sstable_path,
                 const std::vector<SSTable> sstables) {}

}  // namespace diodb
