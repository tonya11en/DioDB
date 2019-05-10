#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace diodb {

// TODO: I don't quite know what's needed for this object yet, so rather than
// implementing a buffer class right now, this will just be a string until I
// have a better handle on this.
typedef std::vector<char> Buffer;

struct Segment {
  Segment(Buffer&& key_buf, Buffer&& val_buf, const bool del = false)
      : key_size(key_buf.size()),
        val_size(val_buf.size()),
        key(std::move(key_buf)),
        val(std::move(val_buf)),
        delete_entry(del) {}

  Segment(const Buffer& key_buf, const Buffer& val_buf, const bool del = false)
      : key_size(key_buf.size()),
        val_size(val_buf.size()),
        key(key_buf),
        val(val_buf),
        delete_entry(del) {}

  Segment(const std::string& key_buf, const std::string& val_buf,
          const bool del = false)
      : key_size(key_buf.size()),
        val_size(val_buf.size()),
        key(key_buf.begin(), key_buf.end()),
        val(val_buf.begin(), val_buf.end()),
        delete_entry(del) {}

  Segment() : key_size(0), val_size(0), key(), val(), delete_entry(false) {}

  std::string DebugString() const {
    std::string k(key.begin(), key.end());
    std::string v(val.begin(), val.end());
    return "{ key_size=" + std::to_string(key_size) +
           ", val_size=" + std::to_string(val_size) + ", key=" + k +
           ", val=" + v + ", delete=" + std::to_string(delete_entry) + " }";
  }

  bool operator>(Segment other) const { return key > other.key; }

  uint32_t key_size;
  uint32_t val_size;
  Buffer key;
  Buffer val;
  bool delete_entry;
};
typedef struct Segment Segment;

}  // namespace diodb
