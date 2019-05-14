#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "buffer.h"
#include "iohandle.h"
#include "memtable.h"
#include "readable_table_base.h"

namespace fs = boost::filesystem;
namespace diodb {

class SSTable : public TableStats, public ReadableTable {
 public:
  using SSTablePtr = std::shared_ptr<SSTable>;

  // Constructing an SSTable object with just a filename implies that we are
  // simply representing an SSTable file that already exists. If the file
  // indicated by 'sstable_path' DOES NOT exist, DiverDB will abort.
  SSTable(const fs::path sstable_path);

  // Constructing an SSTable using a filename and a memtable implies we are
  // flushing the memtable to disk. The file indicated by 'new_sstable_path'
  // MUST NOT exist, or DiverDB will abort.
  SSTable(const fs::path& new_sstable_path, const Memtable& memtable);

  // Constructing an SSTable from other SSTable objects will merge the provided
  // SSTables into a new object at the provided filename. The file indicated by
  // 'new_sstable_path' MUST NOT exist, or DiverDB will abort.
  SSTable(const fs::path new_sstable_path,
          const std::vector<SSTablePtr>& sstables);

  virtual ~SSTable() {}

  // ReadableTable.
  virtual bool KeyExists(const Buffer& key) const override;
  inline bool KeyExists(const std::string&& key) const override {
    Buffer k(key.begin(), key.end());
    return KeyExists(std::move(k));
  }
  virtual Buffer Get(const Buffer& key) const override;
  inline Buffer Get(const std::string&& key) const override {
    Buffer k(key.begin(), key.end());
    return Get(std::move(k));
  }

  virtual size_t Size() const override { return num_valid_entries(); }

  // Verify SSTable invariants.
  bool SanityCheck();

  // Accessors.
  fs::path filepath() const { return filepath_; }
  size_t table_id() const { return table_id_; }

  // TODO: stats such as num_bytes..

 private:
  // Persists an SSTable to disk from a provided memtable by appending segment
  // files to each other on disk. Returns true on success.
  bool FlushMemtable(const fs::path& new_sstable_path,
                     const Memtable& memtable);

  // Builds the sparse in-memory segment index from a provided filepath.
  void BuildSparseIndexFromFile(const fs::path filepath);

  // Returns the minimum key offset bytes for the sparse index.
  virtual off_t KeyIndexOffsetBytes() const;

  // Takes a vector of existing SSTable files that are sorted chronologically
  // (newest to oldest) and creates a new SSTable at 'filepath_' with the merged
  // contents. The merge keeps the most recent version of any segment.
  void MergeSSTables(const std::vector<SSTablePtr>& sstables);

  // Batches up writes, such that identical keys are deduped and age
  // priority is resolved. A call to Flush() is necessary to fully clear the
  // batch.
  void ResolveWrite(Segment&& segment, int age);

  // Sync writes to disk.
  void Flush();

  // Finds a segment in the SSTable given a key and puts it in the provided
  // segment reference. Returns true if one is found.
  bool FindSegment(const Buffer& key, Segment *segment) const;

  // Writes the remaining items in the segment cache in the correct order, while
  // also keeping track of the age of each item.
  void FinishSegmentCache(std::vector<std::shared_ptr<Segment>>& segment_cache);

  // Accessor.
  int32_t file_size() { return file_size_; }

 private:
  // Filepath of this SSTable.
  fs::path filepath_;

  // Size of the SSTable file after being written.
  int32_t file_size_;

  // Unique identifier for this SSTable. Calculated as a hash of the file path.
  size_t table_id_;

  // A sparse index of the keys and offsets of the associated SSTable entries
  // in the file.
  std::map<Buffer, off_t> sparse_index_;

  // SSTable segment file controller.
  std::unique_ptr<IOHandle> io_handle_;

  // Buffer maintained for resolving write batches during merge. Contains a
  // segment and its age. Younger (lower number) sstables will replace old ones
  // with the same key.
  std::pair<Segment, int> merge_buffer_;
};

}  // namespace diodb
