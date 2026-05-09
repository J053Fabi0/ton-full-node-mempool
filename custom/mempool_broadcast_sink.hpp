#pragma once

#include "td/actor/actor.h"
#include "td/utils/buffer.h"
#include "td/utils/port/detail/NativeFd.h"

#include <memory>
#include <string>

namespace ton {
namespace validator {
namespace fullnode {
namespace custom {

class MemPoolBroadcastSink {
 public:
  explicit MemPoolBroadcastSink(std::string socket_path = "/tmp/ton-mempool.sock");
  void start_up();
  void tear_down();

  // Called via td::actor::send_closure - non-blocking actor message
  void on_external_message(td::BufferSlice boc_data);

 private:
  std::string socket_path_;

  class SocketWriter;
  std::unique_ptr<SocketWriter> writer_;
};

}  // namespace custom
}  // namespace fullnode
}  // namespace validator
}  // namespace ton
