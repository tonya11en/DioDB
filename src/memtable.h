#pragma once

#include <string>
#include <map>

#include "buffer.h"

namespace DioDB {

class Memtable {
public:
  Memtable() {}
  ~Memtable() {}

  // Dumps the contents of the memtable onto disk as an immutable SSTable.
  void Flush(const std::string& filename);

  // Returns true if the given key exists.
  bool KeyExists(const Buffer& key) const;

  // Inserts a key/value pair into the memtable.
  void Put(Buffer&& key, Buffer&& val);

  // Gets the value associated with a particular key.
  Buffer Get(const Buffer key) const;

  // Erases a key/value pair from the memtable.
  void Erase(const Buffer& key);

  // Return the number of key/value pairs stored in the memtable.
  size_t Size() const {
    return kv_map_.size();
  }

  // TODO: stats such as num_bytes..

private:
  // Sorted structure for key/value mappings.
  // TODO: Use some abseil map.
  std::map<Buffer, Buffer> kv_map_;
};

} // namespace DioDB
