cc_binary(
    name = "diodb",
    srcs = ["main.cc"],
    deps = [
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags",
      "@grpc//:grpc++",
    ],
)

cc_library(
    name = "diodb_server",
    srcs = ["server.cc"],
    hdrs = ["server.h"],
    deps = [
      "@glog//:glog",
      "@com_github_gflags_gflags//:gflags",
      "@grpc//:grpc++",
    ],
)

proto_library(
  name = "server_proto",
  srcs = ["server.proto"],
  has_service = True,
)
