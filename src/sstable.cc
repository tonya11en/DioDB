#include <fstream>
#include <memory>

#include <glog/logging.h>
#include <google/protobuf/util/delimited_message_util.h>
#include <boost/filesystem.hpp>

#include "proto/segment.pb.h"
#include "sstable.h"

DEFINE_uint64(index_offset_bytes, 4096,
              "The minimum number of bytes between each segment that is "
              "referenced in the sparse index. Increasing this will decrease "
              "memory usage, but at the cost of higher read overhead");

using google::protobuf::util::SerializeDelimitedToOstream;

namespace diverdb {

SSTable::SSTable(const fs::path sstable_path)
    : filepath_(sstable_path), index_offset_bytes_(FLAGS_index_offset_bytes) {
  CHECK(fs::exists(sstable_path))
      << "SSTable file " << sstable_path << " does not exist";
}

SSTable::SSTable(const fs::path& new_sstable_path, const Memtable& memtable)
    : filepath_(new_sstable_path),
      index_offset_bytes_(FLAGS_index_offset_bytes) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << filepath_ << " exists";
  CHECK(memtable.is_locked())
      << "Attempting to flush an unlocked memtable to " << new_sstable_path;

  LOG(INFO) << "Flushing memtable into SSTable " << filepath_;
  FlushMemtable(filepath_, memtable);
}

SSTable::SSTable(const fs::path new_sstable_path,
                 const std::vector<SSTablePtr>& sstables)
    : filepath_(new_sstable_path),
      index_offset_bytes_(FLAGS_index_offset_bytes) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << filepath_ << " exists";

  std::string parent_paths;
  for (const auto& sst : sstables) {
    parent_paths.append(sst->filepath().generic_string() + ", ");
  }
  LOG(INFO) << "Merging parent SSTables " << parent_paths << "into "
            << filepath_;
}

bool SSTable::FlushMemtable(const fs::path& new_sstable_path,
                            const Memtable& memtable) {
  // Relying on the ofstream destructor to close the fd.
  fs::ofstream ofs(new_sstable_path);

  for (const auto& entry : memtable) {
    // TODO: Investigate using protobuf zero-copy streams.
    // TODO: Investigate protobuf arena.
    Segment segment = entry.second;
    if (!SerializeDelimitedToOstream(segment, &ofs)) {
      return false;
    }
  }

  return true;
}

}  // namespace diverdb
