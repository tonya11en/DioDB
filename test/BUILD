cc_test(
  name = "NoopTest",
  srcs = ["noop_test.cc"],
  deps = [
    "@googletest//:gtest_main",
  ],
)

cc_test(
  name = "MemtableTest",
  srcs = ["memtable_test.cc"],
  deps = [
    "@googletest//:gtest_main",
    "//src:memtable_lib",
  ],
)

cc_test(
  name = "SSTableTest",
  srcs = ["sstable_test.cc"],
  deps = [
    "@googletest//:gtest_main",
    "//src:sstable_lib",
    "@boost//:filesystem",
    "@glog//:glog",
    "@com_google_protobuf//:protobuf",
    "//proto:segment_proto",
  ],
  copts = ["--std=c++14"],
)
