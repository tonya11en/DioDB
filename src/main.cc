#include <iostream>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "src/server.h"

using namespace std;

using DioDB::Server::DioDBServer;

DEFINE_string(server_port, "6666", "The port that the DioDB server listens on");

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
  LOG(INFO) << "DioDB server will listen on " << server_address;
  DioDBServer diodb_server;

  // Fire up the DioDB server.
  LOG(INFO) << "initializing the service";
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&diodb_server);
  unique_ptr<grpc::Server> server(builder.BuildAndStart());
  server->Wait();

  gflags::ShutDownCommandLineFlags();
  return 0;
}
