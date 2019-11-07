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

# Boost C++ rules.
git_repository(
    name = "com_github_nelhage_rules_boost",
    commit = "6d6fd834281cb8f8e758dd9ad76df86304bf1869",
    remote = "https://github.com/nelhage/rules_boost",
)

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
boost_deps()
