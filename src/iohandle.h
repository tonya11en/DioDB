#pragma once

#include <boost/filesystem.hpp>

#include "buffer.h"

namespace fs = boost::filesystem;
namespace diodb {

class IOHandle {
 public:
  IOHandle(const fs::path &filepath);
  ~IOHandle();

  // Reset all internal state to a post-initialization state.
  void Reset();

  // Parses the segment at the current stream offset and returns the parsed
  // message.
  void ParseNext(Segment *segment);

  // Serializes a segment and writes it to the file.
  bool SegmentWrite(const Segment &segment);

  // Current offset in the coded stream.
  int64_t Offset() const;

  // Jump to specified offset in the SSTable file.
  void Seek(int64_t offset);

  // Sync writes to disk.
  void Flush();

  // True if current offset is at the end of the file.
  bool End() const {
    return Offset() == static_cast<int64_t>(fs::file_size(filepath_));
  };

  // Accessors.
  fs::path filepath() { return filepath_; }

 private:
  // The filepath being parsed.
  fs::path filepath_;

  // File pointer.
  FILE *fp_;

  // Format of information that is persisted on disk during a segment write.
  // Format is as follows:
  //
  //   [key size][val size][key bytes][val bytes]
  inline static constexpr const char *persisted_fmt_ = "%x%x%s%s";
};

}  // namespace diodb
