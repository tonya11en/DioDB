#pragma once

#include <map>
#include <string>

#include "buffer.h"
#include "proto/segment.pb.h"
#include "table_stats.h"

namespace diverdb {

class Memtable : public TableStats {
 public:
  Memtable();
  ~Memtable() {}

  // Returns true if the given key exists.
  bool KeyExists(const Buffer& key) const;

  // Inserts a key/value pair into the memtable.
  void Put(Buffer&& key, Buffer&& val);

  // Gets the value associated with a particular key.
  Buffer Get(const Buffer key) const;

  // Erases a key/value pair from the memtable.
  void Erase(const Buffer& key);

  // Returns the number of non-deleted key/value pairs in the memtable.
  size_t Size() const { return num_valid_entries(); }

  // Locks the memtable, rendering it immutable.
  inline void Lock() { is_locked_ = true; }

  // Accessors.
  bool is_locked() const { return is_locked_; }

 private:
  void InitializeStats();

 private:
  // Sorted structure for key/value mappings.
  // TODO: Use some abseil map.
  std::map<Buffer, Segment> segment_map_;

  // If the memtable is locked, no further Put/Erase operations are allowed.
  // This is asserted.
  bool is_locked_;

 public:
  // Iterator will just iterate over the internal map type.
  using MapType = decltype(segment_map_);
  using iterator = MapType::iterator;
  using const_iterator = MapType::const_iterator;
  inline iterator begin() { return segment_map_.begin(); }
  inline iterator end() { return segment_map_.end(); }
  inline const_iterator begin() const { return segment_map_.begin(); }
  inline const_iterator end() const { return segment_map_.end(); }
  inline const_iterator cbegin() const { return segment_map_.cbegin(); }
  inline const_iterator cend() const { return segment_map_.cend(); }
};

}  // namespace diverdb
