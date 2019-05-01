#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <boost/filesystem.hpp>

#include "buffer.h"
#include "memtable.h"

namespace fs = boost::filesystem;
namespace diverdb {

class SSTable {
 public:
  using SSTablePtr = std::shared_ptr<SSTable>;

  // Constructing an SSTable object with just a filename implies that we are
  // simply representing an SSTable file that already exists. If the file
  // indicated by 'sstable_path' DOES NOT exist, DiverDB will abort.
  SSTable(const fs::path sstable_path);

  // Constructing an SSTable using a filename and a memtable implies we are
  // flushing the memtable to disk. The file indicated by 'new_sstable_path'
  // MUST NOT exist, or DiverDB will abort.
  SSTable(const fs::path new_sstable_path, const Memtable& memtable);

  // Constructing an SSTable from other SSTable objects will merge the provided
  // SSTables into a new object at the provided filename. The file indicated by
  // 'new_sstable_path' MUST NOT exist, or DiverDB will abort.
  SSTable(const fs::path new_sstable_path, const std::vector<SSTablePtr>& sstables);

  ~SSTable() {}

  // TODO: stats such as num_bytes..

 private:
  // Persists an SSTable to disk from a provided memtable by appending segment
  // files to each other on disk. Returns true on success.
  bool FlushMemtable(const fs::path new_sstable_path, const Memtable& memtable);

 private:
  // The minimum number of bytes between each segment referenced in the
  // sparse index.
  size_t index_offset_bytes_;

  // A sparse index of the keys and offsets of the associated SSTable entries
  // in the file.
  std::unordered_map<Buffer, uint64_t> sparse_index_;
};

}  // namespace diverdb
