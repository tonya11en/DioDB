#include <iostream>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/server.h"

using namespace std;

using diodb::Server::DiverDBServer;

DEFINE_string(
    root_dir, "/tmp/",
    "The location of the root directory containing all of the database files");
DEFINE_string(server_port, "6666",
              "The port that the DiverDB server listens on");

void initialize(int argc, char *argv[]) {
  gflags::SetVersionString("0.0");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_log_dir.empty()) {
    FLAGS_logtostderr = 1;
  }
  google::InitGoogleLogging(argv[0]);
}

int main(int argc, char *argv[]) {
  initialize(argc, argv);

  const string server_address("0.0.0.0:" + FLAGS_server_port);
  LOG(INFO) << "DiverDB server will listen on " << server_address;
  DiverDBServer diodb_server;

  LOG(INFO) << "lol, nvmd this is a no-op until RPCs are supported";

  return 0;
}
