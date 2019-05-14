#include <stdio.h>
#include <algorithm>
#include <memory>

#include <glog/logging.h>
#include <google/protobuf/util/delimited_message_util.h>
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

  auto validate_return = [](const size_t ret, const int expected_val) {
    PCHECK(ret == expected_val) << "Error reading segment ret=" << ret;
  };

  // Key size.
  size_t ret = fread(&(segment->key_size), sizeof(uint32_t), 1, fp_);
  validate_return(ret, 1);

  // Val size.
  ret = fread(&(segment->val_size), sizeof(uint32_t), 1, fp_);
  validate_return(ret, 1);

  segment->key.resize(segment->key_size);
  segment->val.resize(segment->val_size);

  // Key.
  ret = fread(segment->key.data(), segment->key_size, 1, fp_);
  validate_return(ret, 1);

  // Val.
  ret = fread(segment->val.data(), segment->val_size, 1, fp_);
  validate_return(ret, 1);

  // Determine delete.
  ret = fread(&(segment->delete_entry), sizeof(bool), 1, fp_);
  validate_return(ret, 1);
}

bool IOHandle::SegmentWrite(const Segment& segment) {
  // TODO: Do something with string_view.
  auto validate_return = [](const size_t ret, const int expected_val) {
    PCHECK(ret == expected_val) << "Error writing segment ret=" << ret;
  };

  // Key and val size.
  size_t ret = fwrite(&segment.key_size, sizeof(uint32_t), 2, fp_);
  validate_return(ret, 2);

  // Key.
  ret = fwrite(segment.key.data(), segment.key_size, 1, fp_);
  validate_return(ret, 1);

  // Val.
  ret = fwrite(segment.val.data(), segment.val_size, 1, fp_);
  validate_return(ret, 1);

  // Determine delete.
  ret = fwrite(&segment.delete_entry, sizeof(bool), 1, fp_);
  validate_return(ret, 1);

  return true;
}

void IOHandle::Flush() { PCHECK(fflush(fp_) == 0) << "error flushing"; }

int64_t IOHandle::Offset() const {
  int64_t pos = ftell(fp_);
  PCHECK(pos >= 0) << "Failed to get offset";
  return pos;
}

void IOHandle::Seek(int64_t offset) {
  fseek(fp_, offset, SEEK_SET);
}

}  // namespace diodb
