#include <iostream>

#include <glog/logging.h>
#include <gflags/gflags.h>

using namespace std;

void finish() {
  gflags::ShutDownCommandLineFlags();
}

int main(int argc, char *argv[]) {
  gflags::SetVersionString("0.0.1");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  LOG(INFO) << "lol, sup";

  finish();
  return 0;
}
