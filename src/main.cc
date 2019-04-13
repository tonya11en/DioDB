#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

using namespace std;

static const string VERSION = "0.0.1";

void finish() {
  gflags::ShutDownCommandLineFlags();
}

int main(int argc, char *argv[]) {
  gflags::SetVersionString(VERSION);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(INFO) << "lol, sup";

  finish();
  return 0;
}
