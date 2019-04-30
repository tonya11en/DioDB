load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

cc_grpc_library(
  name = "cc_server_grpc",
  deps = [],
  srcs = ["server.proto"],
  proto_only =  False,
  well_known_protos = True,
  generate_mocks = True,
  visibility = ["//visibility:public"],
)

proto_library(
  name = "sstable_proto",
  srcs = ["sstable.proto"],
  visibility = ["//visibility:public"],
)
