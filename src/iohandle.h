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
  int InputOffset() const;

  // Sync writes to disk.
  void Flush();

  // True if at EOF.
  bool Eof();

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
