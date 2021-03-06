#include <stdio.h>
#include <algorithm>
#include <memory>

#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/functional/hash.hpp>

#include "src/buffer.h"
#include "src/iohandle.h"

using namespace std;

namespace diodb {

IOHandle::IOHandle(const fs::path& filepath) : filepath_(filepath) {
  LOG(INFO) << "Creating file handle for " << filepath_;

  // If the file exists, open it for update but don't overwrite it. If it
  // does not exist, create it and begin writing to it.
  if (fs::exists(filepath_)) {
    fp_ = fopen(filepath_.generic_string().data(), "r+");
  } else {
    fp_ = fopen(filepath_.generic_string().data(), "w+");
  }
  filepath_ = fs::canonical(filepath_);

  PCHECK(fp_) << "Unable to open file stream";

  Reset();
  CHECK_EQ(ferror(fp_), 0);
}

IOHandle::~IOHandle() { fclose(fp_); }

void IOHandle::Reset() { fseek(fp_, 0, SEEK_SET); }

void IOHandle::ParseNext(Segment* segment) {
  CHECK(!End());
  CHECK(segment);

  // Key size.
  size_t ret = fread(&(segment->key_size), sizeof(uint32_t), 1, fp_);
  PCHECK(ret == 1) << "Error reading segment ret=" << ret;

  // Val size.
  ret = fread(&(segment->val_size), sizeof(uint32_t), 1, fp_);
  PCHECK(ret == 1) << "Error reading segment ret=" << ret;

  segment->key.resize(segment->key_size);
  segment->val.resize(segment->val_size);

  // Key.
  if (segment->key_size > 0) {
    ret = fread(segment->key.data(), segment->key_size, 1, fp_);
    PCHECK(ret == 1) << "Error reading segment ret=" << ret;
  }

  // Val.
  if (segment->val_size > 0) {
    ret = fread(segment->val.data(), segment->val_size, 1, fp_);
    PCHECK(ret == 1) << "Error reading segment ret=" << ret;
  }

  // Determine delete.
  ret = fread(&(segment->delete_entry), sizeof(bool), 1, fp_);
  PCHECK(ret == 1) << "Error reading segment ret=" << ret;
}

bool IOHandle::SegmentWrite(const Segment& segment) {
  // Key and val size.
  size_t ret = fwrite(&segment.key_size, sizeof(uint32_t), 2, fp_);
  PCHECK(ret == 2) << "Error writing segment ret=" << ret;

  // Key.
  if (segment.key_size > 0) {
    ret = fwrite(segment.key.data(), segment.key_size, 1, fp_);
    PCHECK(ret == 1) << "Error writing segment ret=" << ret;
  }

  // Val.
  if (segment.val_size > 0) {
    ret = fwrite(segment.val.data(), segment.val_size, 1, fp_);
    PCHECK(ret == 1) << "Error writing segment ret=" << ret;
  }

  // Determine delete.
  ret = fwrite(&segment.delete_entry, sizeof(bool), 1, fp_);
  PCHECK(ret == 1) << "Error writing segment ret=" << ret;

  return true;
}

void IOHandle::Flush() { PCHECK(fflush(fp_) == 0) << "error flushing"; }

int64_t IOHandle::Offset() const {
  int64_t pos = ftell(fp_);
  PCHECK(pos >= 0) << "Failed to get offset";
  return pos;
}

void IOHandle::Seek(int64_t offset) { fseek(fp_, offset, SEEK_SET); }

}  // namespace diodb
