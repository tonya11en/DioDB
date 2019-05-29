#include <memory>

#include <glog/logging.h>

#include "db_controller.h"
#include "memtable.h"
#include "buffer.h"

using namespace std;

DEFINE_int32(background_task_min_gap_secs, 15, 
             "Minimum number of seconds between background tasks"
             "are triggered.");

DEFINE_int32(num_worker_threads, 0,
             "Number of worker threads in the thread pool. Setting this value"
             "to 0 will use maximum hardware concurrency.");

namespace diodb {

DBController::DBController(const fs::path db_directory) :
  primary_memtable_(make_unique<Memtable>()),
  secondary_memtable_(make_unique<Memtable>()),
  threadpool_(FLAGS_num_worker_threads < 1 ?
              thread::hardware_concurrency() :
              FLAGS_num_worker_threads) {
  LOG(INFO) << "Starting DB controller with concurrency "
            << threadpool_.num_threads();
}

void DBController::RollTables() {

}

bool DBController::KeyExistsAction(const Buffer& key) const {

bool DBController::KeyExistsAction(const Buffer& key) const {
  if (primary_memtable_->KeyExists(key)) {
    return true;
  }

  if (secondary_memtable_->KeyExists(key)) {
    return true;
  }

  // Iterator is valid for objects that are swapped, so background tasks should
  // not interfere with this.
  for (const auto& sst : primary_sstables_) {
    if (sst.KeyExists(key)) {
      return true;
    }
  }
}

void DBController::Put(Buffer&& key, Buffer&& val) {
  // The memtable might be locked because it's about to be swapped with the
  // secondary memtable and flushed. Keep retrying until the primary memtable is
  // not locked.
  while (!primary_memtable_->Put(key, val)) {
  }
}

} // namespace diodb
