#pragma once

#include <thread>

#include "buffer.h"

namespace diodb {

class DBController {
 public:
  DBController(const fs::path db_directory);
  ~DBController() {}

  // Flush the memtable into an SSTable and roll the existing SSTable into the
  // base-level SSTable.
  void RollTables();

  // Returns true if a key exists in the database.
  bool KeyExists(const Buffer& key) const;

  // Get the value associated with a key.
  Buffer Get(const Buffer& key) const;

  // Inserts a key/value pair into the database.
  void Put(Buffer&& key, Buffer&& val);

  // Erases a key/value pair from the database.
  void Erase(Buffer&& key);

 private:

 private:
  // The active memtable that services all I/O.
  std::unique_ptr<Memtable> primary_memtable_;

  // The memtable that is not currently in use. This could potentially be in the
  // process of being flushed.
  std::unique_ptr<Memtable> secondary_memtable_;

  // The active collection of pointers to SSTables ordered from newest to
  // oldest. The last table is the base table.
  std::vector<SSTable::SSTablePtr> primary_sstables_;

  // A secondary collection of SSTables ordered from newest to oldest. The last
  // table is the base table.
  std::vector<SSTable::SSTablePtr> secondary_sstables_;

  // Thread pool that executes all the tasks.
  threadpool::Threadpool threadpool_;
};

} // namespace diodb
