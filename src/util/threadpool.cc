#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

#include "threadpool.h"

namespace util {

Threadpool::Threadpool(const int num_threads)
    : num_threads_(num_threads), distribution_(0, num_threads_ - 1) {
  for (int ii = 0; ii < num_threads_; ++ii) {
    auto w = std::make_shared<Worker>();
    workers_.emplace_back(w);
    workers_[ii]->wthread =
        std::move(std::thread(&Threadpool::Toil, ii, workers_[ii]));
  }
}

void Threadpool::Enqueue(Job&& fn) {
  std::shared_ptr<Worker> worker = Select();
  {
    std::unique_lock<std::mutex> lock(worker->mtx);
    worker->workq.push(std::move(fn));
  }
  worker->cv.notify_one();
}

void Threadpool::Toil(const int thread_idx, std::shared_ptr<Worker> worker) {
  while (true) {
    Job job;
    {
      std::unique_lock<std::mutex> lock(worker->mtx);
      if (worker->workq.empty()) {
        worker->cv.wait(lock);
      }

      // Check for spurious wakeup.
      if (worker->workq.empty()) {
        continue;
      }

      // There is a job in the queue now, so let's get to work.
      job.swap(worker->workq.front());
      worker->workq.pop();
    }
    job();
  }
}

std::shared_ptr<Threadpool::Worker> Threadpool::Select() {
  return workers_[distribution_(generator_)];
}

}  // namespace util
