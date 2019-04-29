cc_test(
  name = "NoopTest",
  srcs = ["noop_test.cc"],
  deps = [
    "@googletest//:gtest_main",
  ]
)

cc_test(
  name = "MemtableTest",
  srcs = ["memtable_test.cc"],
  deps = [
    "@googletest//:gtest_main",
    "//src:memtable_lib",
  ],
)
