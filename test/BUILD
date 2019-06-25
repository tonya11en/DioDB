cc_test(
  name = "NoopTest",
  srcs = ["noop_test.cc"],
  deps = [
    "@googletest//:gtest_main",
  ],
  copts = ["-std=c++17"],
)

cc_test(
  name = "MemtableTest",
  srcs = ["memtable_test.cc"],
  deps = [
    "//src:memtable_lib",
    "@googletest//:gtest_main",
  ],
  copts = ["-std=c++17"],
)

cc_test(
  name = "SSTableTest",
  srcs = ["sstable_test.cc"],
  deps = [
    "//test/mocks:sstable_mock_lib",
    "@boost//:filesystem",
    "@com_google_protobuf//:protobuf",
    "@glog//:glog",
    "@googletest//:gtest_main",
  ],
  copts = ["-std=c++17"],
)

cc_test(
  name = "DBControllerIntegration",
  srcs = ["dbcontroller_integration_test.cc"],
  deps = [
    "//src:db_ctl_lib",
    "//test/mocks:sstable_mock_lib",
    "@boost//:filesystem",
    "@com_google_protobuf//:protobuf",
    "@glog//:glog",
    "@googletest//:gtest_main",
  ],
  copts = ["-std=c++17"],
)
