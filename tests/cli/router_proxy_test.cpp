#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "src/cli/serve/http_server.hpp"
#include "src/cli/serve/router/proxy.hpp"

namespace {

using gufo::router::ProxyToWorker;
using gufo::router::UpstreamTarget;
using gufo::server::HttpRequest;
using gufo::server::HttpResponse;

std::string LowerCase(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return value;
}

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::milliseconds budget) {
  const auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

bool HeaderPresent(
    const std::vector<std::pair<std::string, std::string>>& headers,
    std::string_view name, std::string* value = nullptr) {
  const auto want = LowerCase(std::string(name));
  bool found = false;
  for (const auto& [header_name, header_value] : headers) {
    if (LowerCase(header_name) == want) {
      if (found) {
        return false;  // duplicated header
      }
      found = true;
      if (value != nullptr) {
        *value = header_value;
      }
    }
  }
  return found;
}

struct CapturedRequest {
  std::string method;
  std::string target;  // path + "?" + query, as sent on the request line
  std::string body;
  std::vector<std::pair<std::string, std::string>> headers;

  std::string Header(std::string_view name) const {
    std::string value;
    HeaderPresent(headers, name, &value);
    return value;
  }
};

void SetRecvTimeout(int fd, int milliseconds) {
  const timeval timeout{milliseconds / 1000, (milliseconds % 1000) * 1000};
  assert(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) ==
         0);
}

void SendAll(int fd, std::string_view payload) {
  while (!payload.empty()) {
    const auto count = ::send(fd, payload.data(), payload.size(), MSG_NOSIGNAL);
    assert(count > 0);
    payload.remove_prefix(static_cast<std::size_t>(count));
  }
}

std::string Chunk(std::string_view payload) {
  std::ostringstream out;
  out << std::hex << payload.size() << "\r\n" << payload << "\r\n";
  return out.str();
}

/// Single-connection in-process fake worker on an ephemeral port. The
/// responder runs after the full front request has been captured.
class FakeWorker {
public:
  explicit FakeWorker(std::function<void(int)> responder)
      : responder_(std::move(responder)) {
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
    thread_ = std::thread([this] { AcceptOnce(); });
  }

  ~FakeWorker() {
    if (thread_.joinable()) {
      thread_.join();
    }
    ::close(listen_fd_);
  }

  FakeWorker(const FakeWorker&) = delete;
  FakeWorker& operator=(const FakeWorker&) = delete;

  [[nodiscard]] int port() const { return port_; }
  CapturedRequest Last() {
    const std::lock_guard lock(mutex_);
    return last_;
  }

private:
  void AcceptOnce() {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      return;
    }
    SetRecvTimeout(fd, 500);
    if (ReadRequest(fd)) {
      responder_(fd);
    }
    ::close(fd);
  }

  bool ReadRequest(int fd) {
    std::string data;
    std::size_t header_end = std::string::npos;
    while (header_end == std::string::npos) {
      char buffer[4096];
      const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
      if (count <= 0) {
        return false;
      }
      data.append(buffer, static_cast<std::size_t>(count));
      header_end = data.find("\r\n\r\n");
    }
    CapturedRequest request;
    std::istringstream head_stream(data.substr(0, header_end));
    if (!(head_stream >> request.method >> request.target)) {
      return false;
    }
    std::string line;
    std::getline(head_stream, line);  // remainder of the request line
    std::size_t content_length = 0;
    while (std::getline(head_stream, line)) {
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      const auto colon = line.find(':');
      if (colon == std::string::npos) {
        continue;
      }
      const std::string name = line.substr(0, colon);
      std::string value = line.substr(colon + 1);
      while (!value.empty() && value.front() == ' ') {
        value.erase(value.begin());
      }
      if (LowerCase(name) == "content-length") {
        content_length = static_cast<std::size_t>(std::stoul(value));
      }
      request.headers.emplace_back(name, value);
    }
    request.body = data.substr(header_end + 4);
    while (request.body.size() < content_length) {
      char buffer[4096];
      const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
      if (count <= 0) {
        break;
      }
      request.body.append(buffer, static_cast<std::size_t>(count));
    }
    if (request.body.size() > content_length) {
      request.body.resize(content_length);
    }
    {
      const std::lock_guard lock(mutex_);
      last_ = std::move(request);
    }
    return true;
  }

  std::function<void(int)> responder_;
  int listen_fd_{-1};
  int port_{0};
  std::thread thread_;
  std::mutex mutex_;
  CapturedRequest last_;
};

HttpRequest MakeRequest(std::string method, std::string path,
                        std::string body = {}) {
  HttpRequest request;
  request.method = std::move(method);
  request.path = std::move(path);
  request.body = std::move(body);
  request.headers = {{"Host", "front.example:9"},
                     {"Authorization", "Bearer secret-token"},
                     {"Connection", "keep-alive"},
                     {"X-Custom", "kept"}};
  return request;
}

std::string Drain(const HttpResponse& response) {
  std::string collected;
  assert(response.streaming_body);
  response.streaming_body([&collected](std::string_view chunk) {
    collected.append(chunk);
    return true;
  });
  return collected;
}

int ClosedPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  assert(fd >= 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
  assert(::bind(fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) == 0);
  socklen_t length = sizeof(address);
  assert(::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) ==
         0);
  const int port = ntohs(address.sin_port);
  ::close(fd);
  return port;
}

}  // namespace

void TestEchoRoundTrip() {
  FakeWorker fake([](int fd) {
    const std::string body = "echo:hi";
    SendAll(fd,
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " +
                std::to_string(body.size()) + "\r\n\r\n");
    SendAll(fd, body);
  });

  auto request = MakeRequest("POST", "/v1/x", "hi");
  request.query = "a=1";
  UpstreamTarget target;
  target.port = fake.port();
  const HttpResponse response = ProxyToWorker(target, request);

  assert(response.status == 200);
  assert(Drain(response) == "echo:hi");

  const CapturedRequest seen = fake.Last();
  assert(seen.method == "POST");
  assert(seen.target == "/v1/x?a=1");
  assert(seen.body == "hi");
  assert(seen.Header("authorization").empty());
  assert(seen.Header("host") == "127.0.0.1:" + std::to_string(fake.port()));
  assert(seen.Header("x-custom") == "kept");
  std::string content_length;
  assert(HeaderPresent(seen.headers, "content-length", &content_length));
  assert(content_length == "2");
  assert(!HeaderPresent(seen.headers, "connection") ||
         LowerCase(seen.Header("connection")) == "close");
  assert(!HeaderPresent(seen.headers, "transfer-encoding"));
}

void TestSsePassthrough() {
  FakeWorker fake([](int fd) {
    SendAll(fd,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
            "Transfer-Encoding: chunked\r\n\r\n");
    SendAll(fd, Chunk("data: 1\n\n"));
    SendAll(fd, Chunk("data: 2\n\n"));
    SendAll(fd, Chunk("data: [DONE]\n\n"));
    SendAll(fd, "0\r\n\r\n");
  });

  const UpstreamTarget target{"127.0.0.1", fake.port()};
  const HttpResponse response =
      ProxyToWorker(target, MakeRequest("GET", "/v1/stream"));

  assert(response.status == 200);
  assert(Drain(response) == "data: 1\n\ndata: 2\n\ndata: [DONE]\n\n");
  assert(!HeaderPresent(response.headers, "transfer-encoding"));
  assert(!HeaderPresent(response.headers, "connection"));
  assert(HeaderPresent(response.headers, "content-type"));
}

void TestConnectRefused() {
  const UpstreamTarget target{"127.0.0.1", ClosedPort()};
  const HttpResponse response = ProxyToWorker(target, MakeRequest("GET", "/"));
  assert(response.status == 502);
  assert(response.body.find("upstream_unavailable") != std::string::npos);
  assert(!response.streaming_body);
}

void TestCancellationClosesUpstream() {
  auto closed = std::make_shared<std::atomic<bool>>(false);
  FakeWorker fake([closed](int fd) {
    SendAll(fd,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
            "Transfer-Encoding: chunked\r\n\r\n");
    SendAll(fd, Chunk("data: 1\n\n"));
    // Never send more; wait until the front closes the upstream connection.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    char buffer[64];
    while (std::chrono::steady_clock::now() < deadline) {
      const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
      if (count == 0) {
        closed->store(true);
        return;
      }
    }
  });

  auto request = MakeRequest("GET", "/v1/slow");
  std::atomic<bool> cancelled{false};
  request.is_cancelled = [&cancelled] { return cancelled.load(); };

  const UpstreamTarget target{"127.0.0.1", fake.port()};
  const HttpResponse response = ProxyToWorker(target, request);
  assert(response.status == 200);
  assert(response.streaming_body);

  std::string collected;
  bool first_chunk_seen = false;
  std::chrono::steady_clock::time_point cancel_at;
  response.streaming_body([&](std::string_view chunk) {
    collected.append(chunk);
    if (!cancelled.load()) {
      assert(chunk == "data: 1\n\n");
      cancelled = true;
      first_chunk_seen = true;
      cancel_at = std::chrono::steady_clock::now();
    }
    return true;
  });
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - cancel_at)
                           .count();
  assert(first_chunk_seen);
  assert(collected == "data: 1\n\n");
  assert(elapsed < 200);
  assert(
      WaitUntil([&closed] { return closed->load(); }, std::chrono::seconds(2)));
}

void TestWorkerEofMidBody() {
  FakeWorker fake([](int fd) {
    SendAll(fd,
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
            "Content-Length: 100\r\n\r\n");
    SendAll(fd, "0123456789");
    // Responder returns; FakeWorker closes mid-body.
  });

  const UpstreamTarget target{"127.0.0.1", fake.port()};
  const HttpResponse response =
      ProxyToWorker(target, MakeRequest("GET", "/v1/partial"));
  assert(response.status == 200);
  assert(Drain(response) == "0123456789");
}

int main() {
  TestEchoRoundTrip();
  TestSsePassthrough();
  TestConnectRefused();
  TestCancellationClosesUpstream();
  TestWorkerEofMidBody();
  std::cout << "All router proxy tests passed.\n";
  return 0;
}
