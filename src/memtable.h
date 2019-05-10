#pragma once

#include <vector>
#include <string>
#include <map>
#include <string>
#include <utility>

#include "buffer.h"
#include "readable_table_base.h"
#include "table_stats.h"

namespace diodb {

class Memtable : public TableStats, public ReadableTable {
 public:
  Memtable();
  Memtable(std::vector<std::pair<std::string, std::string>> init_vec);

  ~Memtable() {}

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

  // Inserts a key/value pair into the memtable.
  void Put(Buffer&& key, Buffer&& val, const bool del = false);
  void Put(const std::string& key, const std::string& val,
           const bool del = false);

  // Erases a key/value pair from the memtable.
  void Erase(Buffer&& key);
  void Erase(const std::string&& key) {
    Buffer k(key.begin(), key.end());
    Erase(std::move(k));
  }

  // Locks the memtable, rendering it immutable.
  inline void Lock() { is_locked_ = true; }

  // Accessors.
  bool is_locked() const { return is_locked_; }

 private:
  void InitializeStats();

 private:
  // Sorted structure for key/value mappings.
  // TODO: Use some abseil map.
  std::map<Buffer, Segment> memtable_map_;

  // If the memtable is locked, no further Put/Erase operations are allowed.
  // This is asserted.
  bool is_locked_;

 public:
  // Iterator will just iterate over the internal map type.
  using MapType = decltype(memtable_map_);
  using iterator = MapType::iterator;
  using const_iterator = MapType::const_iterator;
  inline iterator begin() { return memtable_map_.begin(); }
  inline iterator end() { return memtable_map_.end(); }
  inline const_iterator begin() const { return memtable_map_.begin(); }
  inline const_iterator end() const { return memtable_map_.end(); }
  inline const_iterator cbegin() const { return memtable_map_.cbegin(); }
  inline const_iterator cend() const { return memtable_map_.cend(); }
};

}  // namespace diodb
