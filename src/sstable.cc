#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <queue>
#include <fstream>
#include <memory>

#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/functional/hash.hpp>

#include "iohandle.h"
#include "sstable.h"

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
  io_handle_ = std::make_unique<IOHandle>(filepath_);
  BuildSparseIndexFromFile(filepath_);
}

// Constructor for flushing a memtable to an SSTable file.
SSTable::SSTable(const fs::path& new_sstable_path, const Memtable& memtable)
    : filepath_(new_sstable_path), table_id_(fs::hash_value(filepath_)) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << filepath_ << " exists";
  CHECK(memtable.is_locked())
      << "Attempting to flush an unlocked memtable to " << new_sstable_path;

  io_handle_ = std::make_unique<IOHandle>(filepath_);

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
                 const std::vector<SSTablePtr>& sstables)
    : filepath_(new_sstable_path), table_id_(fs::hash_value(filepath_)) {
  CHECK(!fs::exists(new_sstable_path))
      << "SSTable file " << filepath_ << " exists";

  std::string parent_paths;
  for (const auto& sst : sstables) {
    parent_paths.append(sst->filepath().generic_string() + ", ");
  }
  LOG(INFO) << "Merging parent SSTables " << parent_paths << "into "
            << filepath_ << " with id=" << table_id_;

  io_handle_ = std::make_unique<IOHandle>(filepath_);

  // TODO
  // file_size_ = fs::file_size(filepath_);
}

void SSTable::MergeSSTables(const std::vector<SSTablePtr>& sstables) {
  // Build new IOHandles for the parent SSTs.
  std::vector<IOHandle> parent_sst_handles_;
  parent_sst_handles_.reserve(sstables.size());
  for (const auto& sst : sstables) {
    parent_sst_handles_.emplace_back(sst->filepath());
  }

  auto all_handles_finished = [&parent_sst_handles_]() -> bool {
    for (const auto& h : parent_sst_handles_) {
      if (!h.End()) {
        return false;
      }
    }
    return true;
  };

  // The segment cache maps the index of the parent SST to the last object read
  // from it that was not yet written to disk. The parent SST vector index
  // corresponds with the cache's index.
  std::vector<std::shared_ptr<Segment>> segment_cache;
  std::function<bool(const std::shared_ptr<Segment> s)> cache_empty =
    [](const std::shared_ptr<Segment> p){ return p == nullptr;};

  // Perform the merge.
  //
  // TODO: This could be micro-optimized to avoid multiple evaluations, but
  //       right now the focus is on correctness.
  while (!all_handles_finished() &&
         !std::all_of(segment_cache.begin(), segment_cache.end(), cache_empty)) {
    // The index containing the segment whose key evaluates as the minimum.
    // Set to -1 to indicate whether a minimum has been selected yet for a loop.
    int current_min_idx = 0;
    for (int idx = 0; idx < parent_sst_handles_.size(); ++idx) {
      IOHandle& h = parent_sst_handles_.at(idx);
      if (segment_cache[idx] == nullptr && h.End()) {
        continue;
      }

      // We're not at the end of the file, so read from disk and opulate the
      // cache if it's empty. We only write to disk from the cache.
      if (segment_cache[idx] == nullptr) {
        auto segment = std::make_shared<Segment>();
        h.ParseNext(segment.get());
        segment_cache[idx] = std::move(segment);
      }

      CHECK(segment_cache[current_min_idx]);
      CHECK(segment_cache[idx]);
      if (segment_cache[current_min_idx]->key > segment_cache[idx]->key) {
        current_min_idx = idx;
      }
    }
    io_handle_->SegmentWrite(*segment_cache[current_min_idx]);
    segment_cache[current_min_idx].reset();
  }

  file_size_ = fs::file_size(io_handle_->filepath());
}

void SSTable::BuildSparseIndexFromFile(const fs::path filepath) {
  LOG(INFO) << "Building sparse index for SSTable " << table_id_;

  if (file_size_ == 0) {
    LOG(INFO) << "Building sparse index from empty file";
    return;
  }

  off_t last_offset = 0;
  io_handle_->Reset();
  while (!io_handle_->End()) {
    Segment segment;
    CHECK_LE(last_offset, io_handle_->InputOffset());

    // Always add the first key in the SSTable and index after the appropriate
    // number of bytes has been cycled through.
    if (io_handle_->InputOffset() == 0 ||
        io_handle_->InputOffset() - last_offset >= KeyIndexOffsetBytes()) {
      // We're past the minimum file offset, so insert into the sparse index.
      sparse_index_[std::move(segment.key)] = io_handle_->InputOffset();
      last_offset = io_handle_->InputOffset();
    }

    io_handle_->ParseNext(&segment);
  }
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

bool SSTable::KeyExists(const Buffer& key) const {
  // TODO: Bloom filter to speed this up. It's not really useful without it.

  if (sparse_index_.empty() || key < sparse_index_.begin()->first) {
    return false;
  }

  auto it = sparse_index_.lower_bound(key);
  if (it->first == key) {
    // Exact match in the sparse index.
    return true;
  } else if (it == sparse_index_.cbegin()) {
    // The index entry is guaranteed to evaluate greater than the key now, so
    // if it's the smallest item in the index, it's not a match.
    return false;
  }

  io_handle_->Reset();
  Segment segment;
  while (!io_handle_->End()) {
    io_handle_->ParseNext(&segment);
    if (key < segment.key) {
      return false;
    } else if (key == segment.key) {
      return true;
    }
  }

  return false;
}

Buffer SSTable::Get(const Buffer& key) const {
  // TODO
  return Buffer();
}

off_t SSTable::KeyIndexOffsetBytes() const {
  return FLAGS_sstable_index_offset_bytes;
}

}  // namespace diodb
