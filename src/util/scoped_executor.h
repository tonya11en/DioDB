#pragma once

namespace util {

// The ScopedExecutor takes a function and executes it upon destruction of the
// object. It's essentially a cute mimic of Golang's "defer" functionality.
class ScopedExecutor {
 public:
  ScopedExecutor(const std::function<void()>& fn) : fn_(fn) {}
  ~ScopedExecutor() {
    fn_();
  }

 private:
  std::function<void()> fn_;
};

} // namespace util
