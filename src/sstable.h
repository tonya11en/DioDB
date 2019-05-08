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

  // Accessors.
  fs::path filepath() { return filepath_; }

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
};

}  // namespace diodb
