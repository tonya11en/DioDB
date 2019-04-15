cc_binary(
    name = "diodb",
    srcs = ["main.cc"],
    deps = [
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags",
      "@com_github_grpc_grpc//:grpc++",
    ],
)

cc_library(
    name = "diodb_server",
    srcs = ["server.cc"],
    hdrs = ["server.h"],
    deps = [
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags",
      "@com_github_grpc_grpc//:grpc++",
    ],
)

proto_library(
  name = "server_proto",
  srcs = ["server.proto"],
)
