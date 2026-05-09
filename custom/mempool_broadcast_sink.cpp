#include "custom/mempool_broadcast_sink.hpp"

#include "td/utils/BufferedFd.h"
#include "td/utils/port/detail/NativeFd.h"
#include "td/utils/port/SocketFd.h"
#include "td/utils/logging.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace ton {
namespace validator {
namespace fullnode {
namespace custom {

class MemPoolBroadcastSink::SocketWriter {
 public:
  explicit SocketWriter(const std::string &socket_path) : socket_path_(socket_path), connected_(false) {
  }

  void write_async(td::BufferSlice data) {
    if (!connected_) {
      if (!try_connect()) {
        VLOG(FULL_NODE_WARNING) << "MemPoolBroadcastSink: socket not connected: " << socket_path_;
        return;
      }
    }

    // Prefix with 4-byte big-endian length
    uint32_t len = static_cast<uint32_t>(data.size());
    uint32_t be_len = htonl(len);
    char lenbuf[4];
    memcpy(lenbuf, &be_len, 4);

    // Append to buffered fd output buffer
    buffered_fd_.output_buffer().append(td::BufferSlice(lenbuf, 4));
    buffered_fd_.output_buffer().append(data);

    auto res = buffered_fd_.flush_write();
    if (res.is_error()) {
      VLOG(FULL_NODE_WARNING) << "MemPoolBroadcastSink: write error: " << res.error();
      connected_ = false;
      buffered_fd_.close();
    }
  }

 private:
  bool try_connect() {
    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
      VLOG(FULL_NODE_WARNING) << "MemPoolBroadcastSink: socket() failed";
      return false;
    }

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    if (socket_path_.size() >= sizeof(addr.sun_path)) {
      ::close(fd);
      VLOG(FULL_NODE_WARNING) << "MemPoolBroadcastSink: socket path too long";
      return false;
    }
    strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

    if (::connect(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
      ::close(fd);
      VLOG(FULL_NODE_DEBUG) << "MemPoolBroadcastSink: connect() failed to " << socket_path_;
      return false;
    }

    // Wrap into td::SocketFd
    td::NativeFd native(fd);
    auto r = td::SocketFd::from_native_fd(std::move(native));
    if (r.is_error()) {
      ::close(fd);
      VLOG(FULL_NODE_WARNING) << "MemPoolBroadcastSink: from_native_fd failed: " << r.error();
      return false;
    }

    buffered_fd_ = td::BufferedFd<td::SocketFd>(r.move_as_ok());
    connected_ = true;
    VLOG(FULL_NODE_DEBUG) << "MemPoolBroadcastSink: connected to " << socket_path_;
    return true;
  }

  std::string socket_path_;
  td::BufferedFd<td::SocketFd> buffered_fd_;
  bool connected_;
};

MemPoolBroadcastSink::MemPoolBroadcastSink(std::string socket_path) : socket_path_(std::move(socket_path)) {
}

void MemPoolBroadcastSink::start_up() {
  writer_ = std::make_unique<SocketWriter>(socket_path_);
}

void MemPoolBroadcastSink::tear_down() {
  writer_.reset();
}

void MemPoolBroadcastSink::on_external_message(td::BufferSlice boc_data) {
  if (writer_) {
    writer_->write_async(std::move(boc_data));
  }
}

}  // namespace custom
}  // namespace fullnode
}  // namespace validator
}  // namespace ton
