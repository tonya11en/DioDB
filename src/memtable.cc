#include <string>

#include <glog/logging.h>

#include "memtable.h"

namespace diverdb {

Memtable::Memtable() : is_locked_(false) {}

bool Memtable::KeyExists(const Buffer& key) const {
  if (segment_map_.count(key) > 0) {
    return !segment_map_.at(key).delete_entry();
  }
  return false;
}

void Memtable::Put(Buffer&& key, Buffer&& val) {
  CHECK(!is_locked_);

  // TODO: Investigate protobuf arena.
  Segment segment;
  segment.set_key(key);
  segment.set_val(val);
  segment.set_delete_entry(false);

  if (segment_map_.count(key) > 0) {
    if (segment_map_[key].delete_entry()) {
      --mutable_num_delete_entries();
      ++mutable_num_valid_entries();
    }
    segment_map_[key] = std::move(segment);
  } else {
    segment_map_.emplace(std::move(key), std::move(segment));
    ++mutable_num_valid_entries();
  }
}

Buffer Memtable::Get(const Buffer key) const {
  if (!KeyExists(key)) {
    return Buffer();
  }
  return segment_map_.at(key).val();
}

void Memtable::Erase(const Buffer& key) {
  CHECK(!is_locked_);

  if (segment_map_.count(key) > 0 && !segment_map_[key].delete_entry()) {
    segment_map_[key].set_delete_entry(true);
    --mutable_num_valid_entries();
    ++mutable_num_delete_entries();
  }
}

}  // namespace diverdb
