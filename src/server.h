#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "proto/server.grpc.pb.h"

using diodbserver::DBInfoReply;
using diodbserver::DBInfoRequest;
using diodbserver::DiverDBServerService;
using diodbserver::HolyDiverReply;
using diodbserver::HolyDiverRequest;
using grpc::ServerContext;
using grpc::Status;

namespace diodb {
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
}  // namespace diodb
