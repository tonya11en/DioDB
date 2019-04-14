workspace(name = "diodb")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Logging.
git_repository(
 name = "glog",
 remote = "git@github.com:google/glog.git",
 tag = "v0.4.0",
)

# Command line flags.
git_repository(
    name = "com_github_gflags_gflags",
    remote = "https://github.com/gflags/gflags.git",
    tag = "v2.2.2",
)

# Test framework.
git_repository(
    name = "googletest",
    remote = "https://github.com/google/googletest",
    tag = "release-1.8.1",
)

# RPC lib.
git_repository(
    name = "grpc",
    remote = "git@github.com:grpc/grpc.git",
    tag = "v1.19.1",
)
git_repository(
  name = "com_google_protobuf",
  remote = "https://github.com/google/protobuf.git",
  tag = "v3.6.1",
)
