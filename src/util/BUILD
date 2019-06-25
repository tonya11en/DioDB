cc_library(
  name = "util_lib",
  srcs = ["threadpool.cc",
          "scoped_executor.h"],
  hdrs = ["threadpool.h"],
  deps = [
    "@glog//:glog",
  ],
  copts = ["-std=c++17"],
  visibility = ["//src:__pkg__"],
)
