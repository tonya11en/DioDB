#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "proto/server.grpc.pb.h"

using diverdbserver::DBInfoReply;
using diverdbserver::DBInfoRequest;
using diverdbserver::DiverDBServerService;
using diverdbserver::HolyDiverReply;
using diverdbserver::HolyDiverRequest;
using grpc::ServerContext;
using grpc::Status;

namespace diverdb {
namespace Server {

class DiverDBServer : public DiverDBServerService::Service {
 public:
  // Get information about this DiverDB server.
  Status GetDBInfo(ServerContext *context, const DBInfoRequest *request,
                   DBInfoReply *reply);

  // Get the lyrics to Dio's Holy Diver.
  Status HolyDiver(ServerContext *context, const HolyDiverRequest *request,
                   HolyDiverReply *lyrics);
};

}  // namespace Server
}  // namespace diverdb
