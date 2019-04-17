#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "proto/server.grpc.pb.h"

using diodbserver::DioDBServerService;
using diodbserver::HolyDiverReply;
using diodbserver::HolyDiverRequest;
using grpc::ServerContext;
using grpc::Status;

namespace DioDB {
namespace Server {

class DioDBServer : public DioDBServerService::Service {
public:
  // Get the lyrics to Dio's Holy Diver.
  Status HolyDiver(ServerContext *context,
                   const HolyDiverRequest *request,
                   HolyDiverReply *lyrics);
};

} // Server
} // DioDB
