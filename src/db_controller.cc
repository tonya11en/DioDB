#include <memory>

#include <glog/logging.h>
#include <boost/filesystem.hpp>

#include "buffer.h"
#include "db_controller.h"
#include "memtable.h"

using namespace std;
namespace fs = boost::filesystem;

using threadpool::Threadpool;

DEFINE_int32(background_task_min_gap_secs, 5,
             "Minimum number of seconds between background tasks"
             "are triggered.");

DEFINE_int32(num_worker_threads, 0,
             "Number of worker threads in the thread pool. Setting this value"
             "to 0 will use maximum hardware concurrency.");

namespace diodb {

DBController::DBController(const fs::path db_directory)
    : primary_memtable_(make_unique<Memtable>()),
      secondary_memtable_(make_unique<Memtable>()),
      threadpool_(FLAGS_num_worker_threads < 1 ? thread::hardware_concurrency()
                                               : FLAGS_num_worker_threads) {
  // The secondary memtable should never be unlocked. It's immutable.
  secondary_memtable_->Lock();

  LOG(INFO) << "Starting DB controller with concurrency "
            << threadpool_.num_threads();
}

void DBController::RollTables() {
  CHECK(secondary_memtable_->is_locked());
  CHECK_EQ(secondary_sstables_.size(), 0);
  LOG(INFO) << "Performing table merge";

  // There's no point in rolling if the primary memtable is empty.
  if (primary_memtable_->Size() == 0) {
    LOG(INFO) << "Primary memtable is empty- won't merge";
    return;
  }

  // Lock primary memtable.
  primary_memtable_->Lock();

  // Swap primary/secondary memtable
  LOG(INFO) << "Swapping primary/secondary memtables";
  swap(primary_memtable_, secondary_memtable_);

  // Secondary memtable to new sstable.
  if (secondary_memtable_->Size() > 0) {
    LOG(INFO) << "Dumping secondary memtable to disk";
    auto sst = make_shared<SSTable>(fs::path("lvl_0.diodb.secondary"),
                                    *secondary_memtable_);
    secondary_sstables_.emplace_back(move(sst));
  }
  
  // Merge existing primary sstables.
  LOG(INFO) << "Merging primary sstables";
  auto secondary_sst =
    make_shared<SSTable>("lvl_base.diodb.secondary", primary_sstables_);
  secondary_sstables_.emplace_back(std::move(secondary_sst));

  swap(primary_sstables_, secondary_sstables_);

  fs::remove("lvl_0.diodb");
  fs::remove("lvl_base.diodb");
  fs::rename("lvl_0.diodb.secondary", "lvl_0.diodb");
  fs::rename("lvl_base.diodb.secondary", "lvl_base.diodb");

  std::this_thread::sleep_for(
    std::chrono::seconds(FLAGS_background_task_min_gap_secs));
  Threadpool::Job roller = [this]() { this->RollTables(); };
  threadpool_.Enqueue(move(roller));
}

bool DBController::KeyExistsAction(const Buffer& key) const {
  auto p = primary_memtable_->DeletedKeyExists(key);
  if (p.first) {
    return !p.second;
  }

  p = secondary_memtable_->DeletedKeyExists(key);
  if (p.first) {
    return !p.second;
  }

  // Iterator is valid for objects that are swapped, so background tasks should
  // not interfere with this.
  for (const auto& sst : primary_sstables_) {
    p = sst->DeletedKeyExists(key);
    if (p.first) {
      return !p.second;
    }
  }

  return false;
}

void DBController::Put(Buffer&& key, Buffer&& val) {
  // The memtable might be locked because it's about to be swapped with the
  // secondary memtable and flushed. Keep retrying until the primary memtable is
  // not locked.
  while (!primary_memtable_->Put(move(key), move(val))) {
  }
}

}  // namespace diodb
