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
    "@googletest//:gtest_main",
    "//src:memtable_lib",
  ],
  copts = ["-std=c++17"],
)

cc_test(
  name = "SSTableTest",
  srcs = ["sstable_test.cc"],
  deps = [
    "@googletest//:gtest_main",
    "//test/mocks:sstable_mock_lib",
    "@boost//:filesystem",
    "@glog//:glog",
    "@com_google_protobuf//:protobuf",
  ],
  copts = ["-std=c++17"],
)
