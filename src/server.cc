#include <gflags/gflags.h>
#include <glog/logging.h>

#include "src/server.h"

using namespace std;

namespace diodb {
namespace Server {
Status DioDBServer::GetDBInfo(ServerContext *context,
                              const DBInfoRequest *request,
                              DBInfoReply *reply) {
  reply->set_db_version(gflags::VersionString());
  return Status::OK;
}

Status DioDBServer::HolyDiver(ServerContext *context,
                              const HolyDiverRequest *request,
                              HolyDiverReply *lyrics) {
  static const string holy_diver =
      "Mmmmhhh mhmm\n"
      "Yeah yeah\n"
      "Holy diver\n"
      "You\'ve been down too long in the midnight sea\n"
      "Oh what\'s becoming of me\n"
      "Ride the tiger\n"
      "You can see his stripes but you know he\'s clean\n"
      "Oh, don\'t you see what I mean\n"
      "Gotta get away\n"
      "Holy diver, yeah\n"
      "Shiny diamonds\n"
      "Like the eyes of a cat in the black and blue\n"
      "Something is coming for you\n"
      "Look out\n"
      "Race for the morning\n"
      "You can hide in the sun \'till you see the light\n"
      "Oh, we will pray it\'s all right\n"
      "Gotta get away, get away\n"
      "Between the velvet lies\n"
      "There\'s a truth that\'s hard as steel, yeah\n"
      "The vision never dies\n"
      "Life\'s a never ending wheel\n"
      "Holy diver\n"
      "You\'re the star of the masquerade\n"
      "No need to look so afraid\n"
      "Jump, jump\n"
      "Jump on the tiger\n"
      "You can feel his heart but you know he\'s mean\n"
      "Some light can never be seen, yeah\n"
      "Holy diver\n"
      "You\'ve been down too long in the midnight sea\n"
      "Oh what\'s becoming of me\n"
      "No, no\n"
      "Ride the tiger\n"
      "You can see his stripes but you know he\'s clean\n"
      "Oh, don\'t you see what I mean\n"
      "Gotta get away, get away\n"
      "Gotta get away, get away, yeah\n"
      "Holy diver, soul survivor\n"
      "You\'re the one who\'s clean\n"
      "Holy diver\n"
      "Never come to knew, coming after you\n"
      "Oh holy diver, yeah\n"
      "Alright, get away, get away, get away\n"
      "Holy diver\n"
      "Holy diver\n"
      "Oh holy diver, mmm";

  LOG(INFO) << "Ride the tiger";
  lyrics->set_lyrics(holy_diver);
  return Status::OK;
}

}  // namespace Server
}  // namespace diodb
