#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/evp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>

#include "src/cli/serve/audio_stream.hpp"
#include "src/cli/serve/router/ws_relay.hpp"
#include "src/cli/serve/websocket.hpp"

namespace {

using gufo::router::RelayWebSocket;
using gufo::router::UpstreamTarget;
using gufo::server::EncodeAudioBase64;
using gufo::server::WebSocket;
using Clock = std::chrono::steady_clock;
using Milliseconds = std::chrono::milliseconds;

struct RelayOutcome {
  bool ok{false};
  std::string error;
  Clock::time_point returned_at;
};

std::string LowerCase(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}

/// The RFC 6455 accept token, computed with the same SHA-1 the server side of
/// the handshake uses.
std::string AcceptFor(std::string_view key) {
  const std::string input =
      std::string(key) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned size = 0;
  const int ok = EVP_Digest(input.data(), input.size(), digest.data(), &size,
                            EVP_sha1(), nullptr);
  assert(ok == 1);
  return EncodeAudioBase64(
      std::string_view(reinterpret_cast<const char*>(digest.data()), size));
}

void SendAll(int fd, std::string_view payload) {
  while (!payload.empty()) {
    const auto count = ::send(fd, payload.data(), payload.size(), MSG_NOSIGNAL);
    assert(count > 0);
    payload.remove_prefix(static_cast<std::size_t>(count));
  }
}

bool ReadExact(int fd, char* out, std::size_t size, Milliseconds budget) {
  const auto deadline = Clock::now() + budget;
  while (size != 0) {
    const auto remaining =
        std::chrono::duration_cast<Milliseconds>(deadline - Clock::now())
            .count();
    if (remaining <= 0) {
      return false;
    }
    pollfd slot{fd, POLLIN, 0};
    const int ready = ::poll(&slot, 1, static_cast<int>(remaining));
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (ready == 0) {
      return false;
    }
    const auto count = ::recv(fd, out, size, 0);
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (count == 0) {
      return false;
    }
    out += count;
    size -= static_cast<std::size_t>(count);
  }
  return true;
}

/// Read one frame header plus its extended length, mask and payload, without
/// interpreting any of it.
bool ReadRawFrame(int fd, std::string* frame, Milliseconds budget) {
  std::array<unsigned char, 2> head{};
  if (!ReadExact(fd, reinterpret_cast<char*>(head.data()), head.size(),
                 budget)) {
    return false;
  }
  frame->assign(reinterpret_cast<const char*>(head.data()), head.size());
  std::uint64_t length = head[1] & 127U;
  if (length == 126 || length == 127) {
    const unsigned extra = length == 126 ? 2U : 8U;
    std::array<unsigned char, 8> wide{};
    if (!ReadExact(fd, reinterpret_cast<char*>(wide.data()), extra, budget)) {
      return false;
    }
    frame->append(reinterpret_cast<const char*>(wide.data()), extra);
    length = 0;
    for (unsigned i = 0; i < extra; ++i) {
      length = (length << 8) | wide[i];
    }
  }
  if ((head[1] & 128U) != 0) {
    std::array<unsigned char, 4> mask{};
    if (!ReadExact(fd, reinterpret_cast<char*>(mask.data()), mask.size(),
                   budget)) {
      return false;
    }
    frame->append(reinterpret_cast<const char*>(mask.data()), mask.size());
  }
  std::string payload(static_cast<std::size_t>(length), '\0');
  if (!payload.empty() &&
      !ReadExact(fd, payload.data(), payload.size(), budget)) {
    return false;
  }
  frame->append(payload);
  return true;
}

/// One unmasked server frame, exactly as a worker sends to a client.
std::string ServerFrame(std::uint8_t opcode, std::string_view payload) {
  std::string frame(1, static_cast<char>(0x80U | opcode));
  frame.push_back(static_cast<char>(payload.size()));
  frame.append(payload);
  return frame;
}

/// One masked client frame, exactly as a browser (and therefore `HttpServer`)
/// delivers it.
std::string MaskedFrame(std::uint8_t opcode, std::string_view payload) {
  std::string frame(1, static_cast<char>(0x80U | opcode));
  frame.push_back(static_cast<char>(0x80U | payload.size()));
  const std::array<char, 4> mask{0x23, 0x48, 0x15, static_cast<char>(0x9a)};
  frame.append(mask.data(), mask.size());
  for (std::size_t i = 0; i < payload.size(); ++i) {
    frame.push_back(
        static_cast<char>(static_cast<unsigned char>(payload[i]) ^
                          static_cast<unsigned char>(mask[i % mask.size()])));
  }
  return frame;
}

std::string HeadValue(std::string_view head, std::string_view name) {
  const auto want = LowerCase(std::string(name));
  std::size_t position = head.find("\r\n");
  if (position == std::string_view::npos) {
    return {};
  }
  position += 2;
  while (position < head.size()) {
    const auto end = head.find("\r\n", position);
    const std::string_view line = head.substr(
        position,
        (end == std::string_view::npos ? head.size() : end) - position);
    const auto colon = line.find(':');
    if (colon != std::string_view::npos &&
        LowerCase(std::string(line.substr(0, colon))) == want) {
      std::string_view value = line.substr(colon + 1);
      while (!value.empty() && value.front() == ' ') {
        value.remove_prefix(1);
      }
      return std::string(value);
    }
    if (end == std::string_view::npos) {
      break;
    }
    position = end + 2;
  }
  return {};
}

/// In-process fake worker: completes the handshake in server role and echoes
/// every received frame verbatim, then closes on request.
class FakeWsWorker {
public:
  enum class Mode { kEchoThenClose, kWrongAccept, kPipelinedHeadFrame };

  explicit FakeWsWorker(Mode mode) : mode_(mode) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    assert(listen_fd_ >= 0);
    const int reuse = 1;
    assert(::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse,
                        sizeof(reuse)) == 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = 0;
    assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
    assert(::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) == 0);
    assert(::listen(listen_fd_, 4) == 0);
    socklen_t length = sizeof(address);
    assert(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                         &length) == 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { Run(); });
  }

  ~FakeWsWorker() {
    stop_ = true;
    if (thread_.joinable()) {
      thread_.join();
    }
    ::close(listen_fd_);
  }

  FakeWsWorker(const FakeWsWorker&) = delete;
  FakeWsWorker& operator=(const FakeWsWorker&) = delete;

  [[nodiscard]] int port() const { return port_; }
  [[nodiscard]] std::string seen_key() const { return seen_key_; }

  bool WaitForHandshake(Milliseconds budget) {
    const auto deadline = Clock::now() + budget;
    while (Clock::now() < deadline) {
      if (handshake_done_.load()) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return handshake_done_.load();
  }

  void RequestClose() { close_requested_ = true; }

private:
  void Run() {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      return;
    }
    const std::string head = ReadHeadBytes(fd);
    if (head.empty()) {
      ::close(fd);
      return;
    }
    seen_key_ = HeadValue(head, "Sec-WebSocket-Key");
    const std::string accept =
        mode_ == Mode::kWrongAccept ? "Zm9ndXM=" : AcceptFor(seen_key_);
    std::string reply =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " +
        accept + "\r\n\r\n";
    if (mode_ == Mode::kPipelinedHeadFrame) {
      // A frame written in the same buffer as the 101 head: the relay must
      // capture the bytes past the blank line and still deliver them.
      reply += ServerFrame(1, "pipelined-then-head");
    }
    SendAll(fd, reply);
    handshake_done_ = true;
    if (mode_ == Mode::kWrongAccept) {
      ::close(fd);
      return;
    }
    const auto deadline = Clock::now() + std::chrono::seconds(10);
    while (!stop_.load() && Clock::now() < deadline) {
      if (close_requested_.load()) {
        const char close_frame[] = {'\x88', '\x02', '\x03', '\xe8'};
        SendAll(fd, std::string_view(close_frame, sizeof(close_frame)));
        break;
      }
      pollfd slot{fd, POLLIN, 0};
      const int ready = ::poll(&slot, 1, 20);
      if (ready < 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }
      if (ready == 0) {
        continue;
      }
      std::string frame;
      if (!ReadRawFrame(fd, &frame, std::chrono::seconds(5))) {
        break;
      }
      SendAll(fd, frame);
    }
    ::close(fd);
  }

  std::string ReadHeadBytes(int fd) {
    std::string data;
    const auto deadline = Clock::now() + std::chrono::seconds(5);
    while (data.find("\r\n\r\n") == std::string::npos) {
      const auto remaining =
          std::chrono::duration_cast<Milliseconds>(deadline - Clock::now())
              .count();
      if (remaining <= 0) {
        return {};
      }
      pollfd slot{fd, POLLIN, 0};
      if (::poll(&slot, 1, static_cast<int>(remaining)) <= 0) {
        return {};
      }
      char buffer[1024];
      const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
      if (count <= 0) {
        return {};
      }
      data.append(buffer, static_cast<std::size_t>(count));
      if (data.size() > 64 * 1024) {
        return {};
      }
    }
    return data;
  }

  Mode mode_;
  int listen_fd_{-1};
  int port_{0};
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::atomic<bool> close_requested_{false};
  std::atomic<bool> handshake_done_{false};
  std::string seen_key_;
};

/// One connected front: a `server::WebSocket` over a socketpair, exactly the
/// object `HttpServer` hands to the `websocket` callback.
class FrontSocket {
public:
  FrontSocket() : front_(Open(&peer_fd_, &front_fd_), std::string()) {}
  ~FrontSocket() {
    ::close(peer_fd_);
    ::close(front_fd_);
  }

  FrontSocket(const FrontSocket&) = delete;
  FrontSocket& operator=(const FrontSocket&) = delete;

  [[nodiscard]] WebSocket& front() { return front_; }
  [[nodiscard]] int peer() const { return peer_fd_; }

private:
  /// The front descriptor stays owned by the test: `HttpServer` closes the
  /// connection fd, and `WebSocket` never does.
  static int Open(int* peer, int* front) {
    int fds[2];
    assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    *peer = fds[0];
    *front = fds[1];
    return fds[1];
  }

  int peer_fd_{-1};
  int front_fd_{-1};
  WebSocket front_;
};

void TestEchoRoundTripAndClose() {
  FrontSocket front;
  FakeWsWorker worker(FakeWsWorker::Mode::kEchoThenClose);
  const UpstreamTarget target{"127.0.0.1", worker.port()};

  auto relay = std::async(std::launch::async, [&target, &front] {
    RelayOutcome outcome;
    outcome.ok = RelayWebSocket(target, front.front(), &outcome.error);
    outcome.returned_at = Clock::now();
    return outcome;
  });

  assert(worker.WaitForHandshake(std::chrono::seconds(5)));
  const std::string frame = MaskedFrame(1, "hello-router");
  SendAll(front.peer(), frame);

  std::string echoed;
  assert(ReadRawFrame(front.peer(), &echoed, std::chrono::seconds(2)));
  assert(echoed == frame);

  const auto close_at = Clock::now();
  worker.RequestClose();
  assert(relay.wait_for(std::chrono::milliseconds(200)) ==
         std::future_status::ready);
  const RelayOutcome outcome = relay.get();
  assert(outcome.ok);
  assert(outcome.error.empty());
  assert(
      std::chrono::duration_cast<Milliseconds>(outcome.returned_at - close_at)
          .count() < 200);
  assert(front.front().cancelled());
}

void TestHandshakePipelinedFrameReachesFront() {
  FrontSocket front;
  FakeWsWorker worker(FakeWsWorker::Mode::kPipelinedHeadFrame);
  const UpstreamTarget target{"127.0.0.1", worker.port()};

  auto relay = std::async(std::launch::async, [&target, &front] {
    RelayOutcome outcome;
    outcome.ok = RelayWebSocket(target, front.front(), &outcome.error);
    outcome.returned_at = Clock::now();
    return outcome;
  });

  assert(worker.WaitForHandshake(std::chrono::seconds(5)));

  // The worker wrote a text frame in the same send as its 101 head. Losing it
  // would stall this read to the full budget and fail the test.
  std::string received;
  assert(ReadRawFrame(front.peer(), &received, std::chrono::seconds(2)));
  assert(received == ServerFrame(1, "pipelined-then-head"));

  worker.RequestClose();
  assert(relay.wait_for(std::chrono::milliseconds(200)) ==
         std::future_status::ready);
  const RelayOutcome outcome = relay.get();
  assert(outcome.ok);
  assert(front.front().cancelled());
}

void TestWrongAcceptFailsWithoutHang() {
  FrontSocket front;
  FakeWsWorker worker(FakeWsWorker::Mode::kWrongAccept);
  const UpstreamTarget target{"127.0.0.1", worker.port()};

  auto relay = std::async(std::launch::async, [&target, &front] {
    RelayOutcome outcome;
    outcome.ok = RelayWebSocket(target, front.front(), &outcome.error);
    outcome.returned_at = Clock::now();
    return outcome;
  });

  assert(relay.wait_for(std::chrono::seconds(5)) == std::future_status::ready);
  const RelayOutcome outcome = relay.get();
  assert(!outcome.ok);
  assert(!outcome.error.empty());
}

}  // namespace

int main() {
  TestEchoRoundTripAndClose();
  TestHandshakePipelinedFrameReachesFront();
  TestWrongAcceptFailsWithoutHang();
  std::cout << "All router websocket relay tests passed.\n";
  return 0;
}
