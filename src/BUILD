cc_binary(
  name = "diverdb",
  srcs = ["main.cc"],
  deps = [
    "@glog//:glog",
    "@com_github_gflags_gflags//:gflags",
    "@com_github_grpc_grpc//:grpc++",
    ":diverdb_server_lib",
  ],
)

cc_library(
  name = "diverdb_server_lib",
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
    "//proto:segment_proto",
    ":stats_lib",
  ],
  visibility = ["//test:__pkg__"],
)

cc_library(
  name = "sstable_lib",
  srcs = ["sstable.cc"],
  hdrs = ["sstable.h"],
  deps = [
    "@glog//:glog",
    ":memtable_lib",
    "@boost//:filesystem",
    "@com_google_protobuf//:protobuf",
    "//proto:segment_proto",
    ":stats_lib",
  ],
  visibility = ["//test:__pkg__"],
)

cc_library(
  name = "stats_lib",
  hdrs = ["table_stats.h"],
)
