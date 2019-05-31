#pragma once

#include <string>
#include <utility>

#include "buffer.h"

namespace diodb {

class ReadableTable {
 public:
  // Returns true if the given key exists.
  virtual bool KeyExists(const Buffer& key) const {
    Buffer k(key.begin(), key.end());
    const auto p = DeletedKeyExists(key);
    return p.first ? !p.second : false;
  }
  virtual bool KeyExists(const std::string&& key) const {
    Buffer k(key.begin(), key.end());
    return KeyExists(k);
  }

  // Returns a pair indicating whether a key exists and if found, whether the
  // key is a delete entry.
  virtual std::pair<bool, bool> DeletedKeyExists(const Buffer& key) const = 0;

  // Gets the value associated with a particular key.
  virtual Buffer Get(const Buffer& key) const = 0;

  // Returns the number of non-deleted key/value pairs in the memtable.
  virtual size_t Size() const = 0;
};

}  // namespace diodb
