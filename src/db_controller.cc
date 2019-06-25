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

  const auto start_time = chrono::system_clock::now();

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
  secondary_sstables_.emplace_back(move(secondary_sst));

  swap(primary_sstables_, secondary_sstables_);

  fs::remove("lvl_0.diodb");
  fs::remove("lvl_base.diodb");
  fs::rename("lvl_0.diodb.secondary", "lvl_0.diodb");
  fs::rename("lvl_base.diodb.secondary", "lvl_base.diodb");

  // Guarantee that the minimum time has passed before scheduling the next
  // rollover.
  //
  // An assumption is made here that a negative time duration passed into the
  // 'sleep_for' function won't break anything.
  //
  // TODO: Add a delayed enqueue function to the threadpool.
  const auto elapsed_time = chrono::system_clock::now() - start_time;
  this_thread::sleep_for(
    chrono::seconds(FLAGS_background_task_min_gap_secs) - elapsed_time);
  Threadpool::Job roller = [this]() { this->RollTables(); };
  threadpool_.Enqueue(move(roller));
}

bool DBController::KeyExistsAction(const Buffer& key) const {
  auto key_info = primary_memtable_->DeletedKeyExists(key);
  if (key_info.exists) {
    return !key_info.is_deleted;
  }

  key_info = secondary_memtable_->DeletedKeyExists(key);
  if (key_info.exists) {
    return !key_info.is_deleted;
  }

  // Iterator is valid for objects that are swapped, so background tasks should
  // not interfere with this.
  for (const auto& sst : primary_sstables_) {
    key_info = sst->DeletedKeyExists(key);
    if (key_info.exists) {
      return !key_info.is_deleted;
    }
  }

  return false;
}

void DBController::Put(Buffer&& key, Buffer&& val) {
  // The memtable might be locked because it's about to be swapped with the
  // secondary memtable and flushed. Keep retrying until the primary memtable is
  // not locked.
  //
  // Note: This is not expected to loop for very long, because at the time of
  // writing, the creation of a new memtable takes a negligible amount of time.
  // If that changes, this will cause CPU spikes during table merges.
  while (!primary_memtable_->Put(move(key), move(val))) {
  }
}

void DBController::Erase(Buffer&& key) {
  // See comment block in DBController::Put().
  while (!primary_memtable_->Erase(move(key))) {
  }
}

}  // namespace diodb
