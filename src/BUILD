cc_binary(
  name = "diodb",
  srcs = ["main.cc"],
  deps = [
    "@glog//:glog",
    "@com_github_gflags_gflags//:gflags",
    "@com_github_grpc_grpc//:grpc++",
    ":diodb_server_lib",
  ],
)

cc_library(
  name = "diodb_server_lib",
  srcs = ["server.cc"],
  hdrs = ["server.h"],
  deps = [
    "@glog//:glog",
    "@com_github_gflags_gflags//:gflags",
    "//proto:cc_server_grpc",
    "@com_github_grpc_grpc//:grpc++",
  ],
)

cc_library(
  name = "buffer_lib",
  hdrs = ["buffer.h"],
  deps = [],
)

cc_library(
  name = "memtable_lib",
  srcs = ["memtable.cc"],
  hdrs = ["memtable.h"],
  deps = [
    "@glog//:glog",
    ":buffer_lib",
  ],
  visibility = ["//test:__pkg__"],
)
