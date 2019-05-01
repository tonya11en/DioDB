workspace(name = "diverdb")

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
    name = "com_github_grpc_grpc",
    remote = "git@github.com:grpc/grpc.git",
    tag = "v1.20.0",
)
git_repository(
  name = "com_google_protobuf",
  remote = "https://github.com/google/protobuf.git",
  tag = "v3.6.1.2",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()

# Boost C++ rules.
git_repository(
    name = "com_github_nelhage_rules_boost",
    commit = "6d6fd834281cb8f8e758dd9ad76df86304bf1869",
    remote = "https://github.com/nelhage/rules_boost",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
boost_deps()
