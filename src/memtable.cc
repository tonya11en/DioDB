#include <utility>
#include <vector>
#include <string>

#include <glog/logging.h>

#include "memtable.h"

namespace diodb {

Memtable::Memtable() : is_locked_(false) {}

bool Memtable::KeyExists(const Buffer& key) const {
  if (memtable_map_.count(key) > 0) {
    return !memtable_map_.at(key).delete_entry;
  }
  return false;
}

void Memtable::Put(const std::string& key, const std::string& val,
                   const bool del) {
  Buffer key_buf(key.begin(), key.end());
  Buffer val_buf(val.begin(), val.end());
  Put(std::move(key_buf), std::move(val_buf), del);
}

void Memtable::Put(Buffer&& key, Buffer&& val, const bool del) {
  CHECK(!is_locked_);

  Segment segment(key, val, del);

  if (memtable_map_.count(key) > 0) {
    if (memtable_map_[key].delete_entry) {
      --mutable_num_delete_entries();
      ++mutable_num_valid_entries();
    }
    memtable_map_[key] = std::move(segment);
  } else {
    memtable_map_.emplace(std::move(key), std::move(segment));
    ++mutable_num_valid_entries();
  }
}

Buffer Memtable::Get(const Buffer& key) const {
  if (!KeyExists(key)) {
    return Buffer();
  }
  return memtable_map_.at(key).val;
}

void Memtable::Erase(Buffer&& key) {
  CHECK(!is_locked_);

  if (memtable_map_.count(key) > 0 && memtable_map_[key].delete_entry) {
    return;
  } else if (memtable_map_.count(key) > 0 && !memtable_map_[key].delete_entry) {
    memtable_map_[key].delete_entry = true;
  } else if (memtable_map_.count(key) == 0) {
    Buffer buf;
    Put(std::move(key), std::move(buf), true /* del */);
  }

  --mutable_num_valid_entries();
  ++mutable_num_delete_entries();
}

}  // namespace diodb
