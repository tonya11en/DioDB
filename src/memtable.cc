#include <string>

#include <glog/logging.h>

#include "memtable.h"

namespace DioDB {

void Memtable::Flush(const std::string& filename) {
  // TODO
}

bool Memtable::KeyExists(const Buffer& key) const {
  return kv_map_.count(key) > 0;
}

void Memtable::Put(Buffer&& key, Buffer&& val) {
  if (KeyExists(key)) {
    kv_map_[key] = std::move(val);
  } else {
    kv_map_.emplace(std::move(key), std::move(val));
  }
}

Buffer Memtable::Get(const Buffer key) const {
  if (!KeyExists(key)) {
    return Buffer();
  }
  return kv_map_.at(key);
}

void Memtable::Erase(const Buffer& key) { kv_map_.erase(key); }

}  // namespace DioDB
