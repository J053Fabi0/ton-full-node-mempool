#pragma once

#include "td/actor/actor.h"
#include "td/utils/buffer.h"
#include "td/utils/port/detail/NativeFd.h"
#include "td/utils/BufferedFd.h"
#include "td/utils/port/SocketFd.h"
#include "td/utils/logging.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <memory>
#include <string>

namespace ton {
namespace validator {
namespace fullnode {
namespace custom {

class MemPoolBroadcastSink : public td::actor::Actor {
 public:
  explicit MemPoolBroadcastSink(std::string socket_path = "/tmp/ton-mempool.sock");
  void start_up() override;
  void tear_down() override;

  // Called via td::actor::send_closure - non-blocking actor message
  void on_external_message(td::BufferSlice boc_data);

 private:
  std::string socket_path_;

  class SocketWriter {
   public:
    explicit SocketWriter(const std::string &socket_path) : socket_path_(socket_path), connected_(false) {
    }

    void write_async(td::BufferSlice data) {
      if (!connected_) {
        if (!try_connect()) {
          VLOG(ERROR) << "MemPoolBroadcastSink: socket not connected: " << socket_path_;
          return;
        }
      }

      uint32_t len = static_cast<uint32_t>(data.size());
      uint32_t be_len = htonl(len);
      char lenbuf[4];
      memcpy(lenbuf, &be_len, 4);

      buffered_fd_.output_buffer().append(td::BufferSlice(lenbuf, 4));
      buffered_fd_.output_buffer().append(data.clone());

      auto res = buffered_fd_.flush_write();
      if (res.is_error()) {
        VLOG(ERROR) << "MemPoolBroadcastSink: write error: " << res.error();
        connected_ = false;
        buffered_fd_.close();
      }
    }

   private:
    bool try_connect() {
      int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
      if (fd < 0) {
        VLOG(ERROR) << "MemPoolBroadcastSink: socket() failed";
        return false;
      }

      struct sockaddr_un addr{};
      addr.sun_family = AF_UNIX;
      if (socket_path_.size() >= sizeof(addr.sun_path)) {
        ::close(fd);
        VLOG(ERROR) << "MemPoolBroadcastSink: socket path too long";
        return false;
      }
      strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

      if (::connect(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        VLOG(ERROR) << "MemPoolBroadcastSink: connect() failed to " << socket_path_;
        return false;
      }

      td::NativeFd native(fd);
      auto r = td::SocketFd::from_native_fd(std::move(native));
      if (r.is_error()) {
        ::close(fd);
        VLOG(ERROR) << "MemPoolBroadcastSink: from_native_fd failed: " << r.error();
        return false;
      }

      buffered_fd_ = td::BufferedFd<td::SocketFd>(r.move_as_ok());
      connected_ = true;
      VLOG(ERROR) << "MemPoolBroadcastSink: connected to " << socket_path_;
      return true;
    }

    std::string socket_path_;
    td::BufferedFd<td::SocketFd> buffered_fd_;
    bool connected_;
  };

  std::unique_ptr<SocketWriter> writer_;
};

// Inline method definitions to keep this header header-only and avoid linker errors.
inline MemPoolBroadcastSink::MemPoolBroadcastSink(std::string socket_path)
    : socket_path_(std::move(socket_path)), writer_(nullptr) {
}

inline void MemPoolBroadcastSink::start_up() {
  writer_.reset(new SocketWriter(socket_path_));
}

inline void MemPoolBroadcastSink::tear_down() {
  writer_.reset();
}

inline void MemPoolBroadcastSink::on_external_message(td::BufferSlice boc_data) {
  if (!writer_) {
    writer_.reset(new SocketWriter(socket_path_));
  }
  writer_->write_async(std::move(boc_data));
}

}  // namespace custom
}  // namespace fullnode
}  // namespace validator
}  // namespace ton
