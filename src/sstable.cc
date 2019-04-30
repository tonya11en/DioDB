#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include "sstable.h"

DEFINE_uint64(index_offset_bytes, 4096,
              "The minimum number of bytes between each segment that is "
              "referenced in the sparse index. Increasing this will decrease "
              "memory usage, but at the cost of higher read overhead");

namespace diodb {

SSTable::SSTable(const fs::path sstable_path)
    : index_offset_bytes_(FLAGS_index_offset_bytes) {
  CHECK(fs::exists(sstable_path))
      << "SSTable file " << sstable_path << " does not exist";
}

SSTable::SSTable(const fs::path new_sstable_path, const Memtable memtable)
    : index_offset_bytes_(FLAGS_index_offset_bytes) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << new_sstable_path << " exists";
}

SSTable::SSTable(const fs::path new_sstable_path,
                 const std::vector<SSTable> sstables)
    : index_offset_bytes_(FLAGS_index_offset_bytes) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << new_sstable_path << " exists";
}

}  // namespace diodb
