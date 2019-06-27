#pragma once

#include <condition_variable>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

namespace util {

class Threadpool {
 public:
  typedef std::function<void()> Job;

  explicit Threadpool(const int num_threads);
  ~Threadpool();

  // Queue up work for execution.
  void Enqueue(Job&& fn);

  int num_threads() const { return num_threads_; }

 private:
  typedef struct Worker {
    // Thread object tied to this worker.
    std::thread wthread;

    // Mutex to control the worker thread.
    std::mutex mtx;

    // Condition variable used to wake up the worker.
    std::condition_variable cv;

    // The work queue for this worker.
    std::queue<Job> workq;

    // If true, the thread will stop toiling.
    std::atomic<bool> rage_quit;
  } Worker;
  std::vector<std::shared_ptr<Worker>> workers_;

  // The number of threads in this pool.
  const int num_threads_;

  std::default_random_engine generator_;
  std::uniform_int_distribution<int> distribution_;

 private:
  // Choose a thread to execute a job.
  std::shared_ptr<Worker> Select();

  // Life of a worker thread. It takes the index of the worker thread in the
  // worker thread vector.
  static void Toil(const int thread_idx, std::shared_ptr<Worker> worker);
};

}  // namespace util
