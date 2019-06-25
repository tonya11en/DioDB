#pragma once

#include <string>
#include <utility>

#include "buffer.h"

namespace diodb {

class ReadableTable {
 public:
  // Returns true if the given key exists.
  virtual bool KeyExists(const Buffer& key) const {
    DetailedKeyResponse r = DeletedKeyExists(key);
    return r.exists ? !r.is_deleted : false;
  }
  virtual bool KeyExists(const std::string&& key) const {
    Buffer k(key.begin(), key.end());
    return KeyExists(k);
  }

  // Returns struct indicating whether a key exists and if found, whether the
  // key is a delete entry.
  typedef struct DetailedKeyResponse {
    // True if the key is found. However, the delete flag could potentially be
    // set.
    bool exists;

    // True if the delete flag is set for a key.
    bool is_deleted;
  } DetailedKeyResponse;
  virtual DetailedKeyResponse DeletedKeyExists(const Buffer& key) const = 0;

  // Gets the value associated with a particular key.
  virtual Buffer Get(const Buffer& key) const = 0;

  // Returns the number of non-deleted key/value pairs in the memtable.
  virtual size_t Size() const = 0;
};

}  // namespace diodb
