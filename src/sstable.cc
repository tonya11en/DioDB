#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>

#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/functional/hash.hpp>

#include "iohandle.h"
#include "sstable.h"

using namespace std;

DEFINE_uint64(sstable_index_offset_bytes, 4 * 1024,
              "The minimum number of bytes between each segment that is "
              "referenced in the sparse index. Increasing this will decrease "
              "memory usage, but at the cost of higher read overhead");

namespace diodb {

// Constructor for recovering SSTable from an existing file.
SSTable::SSTable(const fs::path sstable_path)
    : filepath_(sstable_path), table_id_(fs::hash_value(filepath_)) {
  CHECK(fs::exists(sstable_path))
      << "SSTable file " << sstable_path << " does not exist";

  LOG(INFO) << "Restoring SSTable from file " << filepath_
            << " with id=" << table_id_;

  file_size_ = fs::file_size(filepath_);
  io_handle_ = make_unique<IOHandle>(filepath_);
  BuildSparseIndexFromFile(filepath_);
}

// Constructor for flushing a memtable to an SSTable file.
SSTable::SSTable(const fs::path& new_sstable_path, const Memtable& memtable)
    : filepath_(new_sstable_path), table_id_(fs::hash_value(filepath_)) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << filepath_ << " exists";
  CHECK(memtable.is_locked())
      << "Attempting to flush an unlocked memtable to " << new_sstable_path;

  io_handle_ = make_unique<IOHandle>(filepath_);

  LOG(INFO) << "Flushing memtable into SSTable " << filepath_
            << " with id=" << table_id_;

  CHECK(FlushMemtable(filepath_, memtable))
      << "Error flushing memtable into SSTable " << filepath_
      << " with id=" << table_id_;

  file_size_ = fs::file_size(filepath_);

  BuildSparseIndexFromFile(filepath_);
}

// Constructor for merging multiple SSTables into a new one.
SSTable::SSTable(const fs::path new_sstable_path,
                 const vector<SSTablePtr>& sstables)
    : filepath_(new_sstable_path), table_id_(fs::hash_value(filepath_)) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << filepath_ << " exists";

  string parent_paths;
  for (const auto& sst : sstables) {
    const string parent_path = sst->filepath().generic_string() + ", ";
    DLOG(INFO) << "Adding parent path " << parent_path;
    parent_paths.append(parent_path);
  }
  LOG(INFO) << "Merging parent SSTables " << parent_paths << "into "
            << filepath_ << " with id=" << table_id_;

  io_handle_ = make_unique<IOHandle>(filepath_);

  MergeSSTables(sstables);
  file_size_ = fs::file_size(filepath_);
  BuildSparseIndexFromFile(filepath_);
}

void SSTable::MergeSSTables(const vector<SSTablePtr>& sstables) {
  // Build new IOHandles for the parent SSTs and count up the segments that need to be merged.
  vector<IOHandle> parent_sst_handles_;
  parent_sst_handles_.reserve(sstables.size());
  for (const auto& sst : sstables) {
    parent_sst_handles_.emplace_back(sst->filepath());
  }

  using AgedSegment = pair<Segment, uint32_t>;
  set<AgedSegment> segment_queue;

  auto load_queue = [&segment_queue](uint32_t age, IOHandle& h) {
    if (h.End()) {
      // Already at the end of the file.
      return;
    }

    Segment segment;
    h.ParseNext(&segment);
    auto success = segment_queue.emplace(move(segment), age);
    CHECK(success.second);
  };

  // Run through each SSTable and populate the segment queue with its first entry.
  for (int idx = 0; idx < parent_sst_handles_.size(); ++idx) {
    IOHandle& h = parent_sst_handles_.at(idx);
    load_queue(idx, h);
  }

  // Perform the merge.
  while (!segment_queue.empty()) {
    const uint32_t age = segment_queue.begin()->second;
    auto segment = segment_queue.begin()->first;
    ResolveWrite(move(segment), age);
    segment_queue.erase(segment_queue.begin());

    // Pull the next segment from the sstable we just resolved a write from.
    IOHandle& h = parent_sst_handles_.at(age);
    load_queue(age, h);
  }

  // There's nothing left to read from file. Merge the final item in the merge buffer by resolving a
  // write with a bogus segment.
  ResolveWrite(Segment(), 1337);

  Flush();
  file_size_ = fs::file_size(io_handle_->filepath());
}

void SSTable::ResolveWrite(Segment&& segment, const int age) {
  if (merge_buffer_.first.key.empty()) {
    merge_buffer_.first = segment;
    return;
  }

  if (merge_buffer_.first.key != segment.key) {
    // Since we're trying to resolve a segment that's not what is currencly in the buffer, the
    // segment that is in the buffer is the most recent write for that key. Persist it.
    if (!segment.delete_entry) {
      io_handle_->SegmentWrite(merge_buffer_.first);
    }
    merge_buffer_.first = move(segment);
    merge_buffer_.second = age;
  } else if (merge_buffer_.first.key == segment.key &&
             age < merge_buffer_.second) {
    // Segment is the same as what is in the merge buffer and the segment is more recent. We can
    // replace the segment in the merge buffer with the more recent one.
    merge_buffer_.first = move(segment);
  }
}

void SSTable::BuildSparseIndexFromFile(const fs::path filepath) {
  LOG(INFO) << "building sparse index for SSTable " << table_id_;

  if (file_size_ == 0) {
    LOG(WARNING) << "building sparse index from empty file";
    return;
  }

  off_t last_offset = 0;
  io_handle_->Reset();
  while (!io_handle_->End()) {
    Segment segment;
    CHECK_LE(last_offset, io_handle_->Offset());

    // Always add the first key in the SSTable and index after the appropriate
    // number of bytes has been cycled through.
    int64_t offset = -1;
    if (io_handle_->Offset() == 0 ||
        io_handle_->Offset() - last_offset >= KeyIndexOffsetBytes()) {
      // We're past the minimum file offset, so insert into the sparse index.
      offset = io_handle_->Offset();
    }

    io_handle_->ParseNext(&segment);
    if (offset >= 0) {
      sparse_index_.emplace(segment.key, offset);
      last_offset = offset;
    }
  }

  LOG(INFO) << "built sparse index of size " << sparse_index_.size();
}

bool SSTable::FlushMemtable(const fs::path& new_sstable_path,
                            const Memtable& memtable) {
  for (const auto& entry : memtable) {
    Segment segment = entry.second;
    const bool ok = io_handle_->SegmentWrite(segment);
    if (!ok) {
      return false;
    }
  }

  io_handle_->Flush();

  return true;
}

ReadableTable::DetailedKeyResponse SSTable::DeletedKeyExists(const Buffer& key) const {
  // TODO: Bloom filter to speed this up. It's not really useful without it.

  ReadableTable::DetailedKeyResponse ret;

  Segment segment;
  if (FindSegment(key, &segment)) {
    ret.exists = true;
    ret.is_deleted = segment.delete_entry;
  } else {
    ret.exists = false;
    ret.is_deleted = false;
  }

  return ret;
}

bool SSTable::FindSegment(const Buffer& key, Segment* segment) const {
  CHECK(segment);
  if (sparse_index_.empty() || key < sparse_index_.begin()->first) {
    return false;
  }

  auto it = sparse_index_.lower_bound(key);
  if (it->first == key) {
    // Exact match in the sparse index.
    io_handle_->Seek(it->second);
    io_handle_->ParseNext(segment);
    return true;
  } else if (it == sparse_index_.cbegin()) {
    // The index entry is guaranteed to evaluate greater than the key now, so
    // if it's the smallest item in the index, it's not a match.
    return false;
  } else {
    it = prev(it);
  }

  io_handle_->Seek(it->second);
  while (!io_handle_->End()) {
    io_handle_->ParseNext(segment);
    if (segment->key == key) {
      return true;
    } else if (key < segment->key) {
      // It's impossible to encounter the key since the remaining values to
      // check will only be increasing.
      return false;
    }
  }

  return false;
}

Buffer SSTable::Get(const Buffer& key) const {
  Segment segment;
  const bool found = FindSegment(key, &segment);

  return found ? segment.val : Buffer();
}

off_t SSTable::KeyIndexOffsetBytes() const {
  return FLAGS_sstable_index_offset_bytes;
}

bool SSTable::SanityCheck() {
  DLOG(INFO) << "sanity checking sstable " << table_id_;
  io_handle_->Reset();

  Segment segment, last_segment;
  while (!io_handle_->End()) {
    last_segment = segment;
    io_handle_->ParseNext(&segment);
    if (!last_segment.key.empty() && last_segment.key > segment.key) {
      DLOG(INFO) << "failed sanity check. " << segment.DebugString()
                 << " comes after " << last_segment.DebugString();
      return false;
    }
  }

  return true;
}

void SSTable::Flush() {
  if (!merge_buffer_.first.key.empty()) {
    io_handle_->SegmentWrite(merge_buffer_.first);
    merge_buffer_.first = Segment();
  }
  io_handle_->Flush();
}

}  // namespace diodb
