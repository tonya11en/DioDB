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
    parent_paths.append(sst->filepath().generic_string() + ", ");
  }
  LOG(INFO) << "Merging parent SSTables " << parent_paths << "into "
            << filepath_ << " with id=" << table_id_;

  io_handle_ = make_unique<IOHandle>(filepath_);

  MergeSSTables(sstables);
  file_size_ = fs::file_size(filepath_);
  BuildSparseIndexFromFile(filepath_);
}

void SSTable::MergeSSTables(const vector<SSTablePtr>& sstables) {
  // Build new IOHandles for the parent SSTs.
  vector<IOHandle> parent_sst_handles_;
  parent_sst_handles_.reserve(sstables.size());
  for (const auto& sst : sstables) {
    parent_sst_handles_.emplace_back(sst->filepath());
  }

  // The segment cache maps the index of the parent SST to the last object read
  // from it that was not yet written to disk. The parent SST vector index
  // corresponds with the cache's index.
  vector<shared_ptr<Segment>> segment_cache(sstables.size());
  function<bool(const shared_ptr<Segment> s)> cache_empty =
      [](const shared_ptr<Segment> p) { return p == nullptr; };

  auto ssts_exhausted = [&parent_sst_handles_]() -> bool {
    for (int ii = 0; ii < parent_sst_handles_.size(); ++ii) {
      if (!parent_sst_handles_.at(ii).End()) {
        return false;
      }
    }
    return true;
  };

  // Perform the merge.
  //
  // TODO: This could be micro-optimized to avoid multiple evaluations, but
  //       right now the focus is on correctness.
  while (!ssts_exhausted()) {
    // The index containing the segment whose key evaluates as the minimum.
    int current_min_idx = 0;
    for (int idx = 0; idx < parent_sst_handles_.size(); ++idx) {
      IOHandle& h = parent_sst_handles_.at(idx);
      if (segment_cache[idx] == nullptr && h.End()) {
        continue;
      }

      // We're not at the end of the file, so read from disk and opulate the
      // cache if it's empty. We only write to disk from the cache.
      if (segment_cache[idx] == nullptr) {
        CHECK(!h.End());
        auto segment = make_shared<Segment>();
        h.ParseNext(segment.get());
        segment_cache[idx] = move(segment);
      }

      CHECK(segment_cache[current_min_idx]);
      CHECK(segment_cache[idx]);
      if (segment_cache[current_min_idx]->key > segment_cache[idx]->key) {
        current_min_idx = idx;
      }
    }

    // This is where we actually delete segments by not including them in the
    // new, merged sstable.
    if (!segment_cache[current_min_idx]->delete_entry) {
      ResolveWrite(move(*segment_cache[current_min_idx]), current_min_idx);
    }
    segment_cache[current_min_idx].reset();
  }

  // There's nothing left to read from file. Merge the remainder of the segment
  // cache.
  FinishSegmentCache(segment_cache);

  Flush();
  file_size_ = fs::file_size(io_handle_->filepath());
}

void SSTable::FinishSegmentCache(vector<shared_ptr<Segment>>& segment_cache) {
  // Inserting the youngest segments into a sorted set only if they don't exist
  // inside the set will maintain the most recent items while also sorting those
  // keys.
  using SegmentPtr = shared_ptr<Segment>;
  static auto sptr_compare = [](const SegmentPtr& a,
                                const SegmentPtr& b) -> bool {
    return a->key < b->key;
  };
  set<SegmentPtr, decltype(sptr_compare)> young_segments(sptr_compare);
  for (int ii = 0; ii < segment_cache.size(); ++ii) {
    if (segment_cache[ii] != nullptr) {
      young_segments.emplace(move(segment_cache[ii]));
    }
  }
  for (auto& s : young_segments) {
    // Age doesn't matter here since all segments are unique.
    ResolveWrite(move(*s), 0);
  }
}

void SSTable::ResolveWrite(Segment&& segment, const int age) {
  if (merge_buffer_.first.key.empty()) {
    merge_buffer_.first = move(segment);
    return;
  }

  if (merge_buffer_.first.key != segment.key) {
    // Segment in the buffer is the youngest write for that key. Persist it.
    io_handle_->SegmentWrite(merge_buffer_.first);
    merge_buffer_.first = move(segment);
    merge_buffer_.second = age;
  } else if (merge_buffer_.first.key == segment.key &&
             age < merge_buffer_.second) {
    // Replace a stale write with a more recent one.
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

std::pair<bool, bool> SSTable::DeletedKeyExists(const Buffer& key) const {
  // TODO: Bloom filter to speed this up. It's not really useful without it.

  Segment segment;
  if (FindSegment(key, &segment)) {
    return make_pair(true, segment.delete_entry);
  }

  return make_pair(false, false);
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
