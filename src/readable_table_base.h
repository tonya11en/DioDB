#pragma once

#include <string>

#include "buffer.h"

namespace diodb {

class ReadableTable {
 public:
  // Returns true if the given key exists.
  virtual bool KeyExists(const Buffer& key) const = 0;
  virtual bool KeyExists(const std::string&& key) const = 0;

  // Gets the value associated with a particular key.
  virtual Buffer Get(const Buffer& key) const = 0;
  virtual Buffer Get(const std::string&& key) const = 0;

  // Returns the number of non-deleted key/value pairs in the memtable.
  virtual size_t Size() const = 0;
};

}  // namespace diodb
