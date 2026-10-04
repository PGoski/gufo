#include "src/cli/serve/router/proxy.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "src/core/json.hpp"

namespace gufo::router {
namespace {

using gufo::server::HttpRequest;
using gufo::server::HttpResponse;

using Milliseconds = std::chrono::milliseconds;

/// Receive timeout while streaming a body: bounds how long a cancelled
/// request takes to notice between upstream chunks.
constexpr Milliseconds kBodyPollInterval{50};
/// Upper bound for the worker to produce its status line and headers.
constexpr Milliseconds kHeaderBudget{std::chrono::seconds(30)};

bool IEquals(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(lhs[i])) !=
        std::tolower(static_cast<unsigned char>(rhs[i]))) {
      return false;
    }
  }
  return true;
}

bool IsHopByHop(std::string_view name) {
  return IEquals(name, "Connection") || IEquals(name, "Keep-Alive") ||
         IEquals(name, "Transfer-Encoding") || IEquals(name, "Upgrade");
}

bool IsDroppedRequestHeader(std::string_view name) {
  return IsHopByHop(name) || IEquals(name, "Authorization") ||
         IEquals(name, "Host") || IEquals(name, "Content-Length");
}

std::string Trim(std::string value) {
  const auto not_space = [](unsigned char c) { return c != ' ' && c != '\t'; };
  while (!value.empty() &&
         !not_space(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  std::size_t begin = 0;
  while (begin < value.size() &&
         !not_space(static_cast<unsigned char>(value[begin]))) {
    ++begin;
  }
  return value.substr(begin);
}

HttpResponse UpstreamUnavailable(std::string_view detail) {
  json::Value error = json::Value::object();
  error["message"] = "upstream worker unavailable: " + std::string(detail);
  error["type"] = "server_error";
  error["code"] = "upstream_unavailable";
  json::Value body = json::Value::object();
  body["error"] = std::move(error);
  HttpResponse response;
  response.status = 502;
  response.reason = "Bad Gateway";
  response.body = body.dump();
  response.log_details = "upstream_unavailable detail=" + std::string(detail);
  return response;
}

int ConnectTo(const UpstreamTarget& target, std::string* error) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  const std::string service = std::to_string(target.port);
  addrinfo* results = nullptr;
  const int rc =
      ::getaddrinfo(target.host.c_str(), service.c_str(), &hints, &results);
  if (rc != 0 || results == nullptr) {
    *error = "resolve failed";
    return -1;
  }
  int fd = -1;
  for (addrinfo* info = results; info != nullptr; info = info->ai_next) {
    fd = ::socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (::connect(fd, info->ai_addr, info->ai_addrlen) == 0) {
      break;
    }
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(results);
  if (fd < 0) {
    *error = std::strerror(errno);
  }
  return fd;
}

bool SendAll(int fd, std::string_view payload) {
  while (!payload.empty()) {
    const auto count = ::send(fd, payload.data(), payload.size(), MSG_NOSIGNAL);
    if (count <= 0) {
      return false;
    }
    payload.remove_prefix(static_cast<std::size_t>(count));
  }
  return true;
}

std::string BuildUpstreamRequest(const UpstreamTarget& target,
                                 const HttpRequest& request) {
  std::string out;
  out += request.method.empty() ? "GET" : request.method;
  out += ' ';
  out += request.path.empty() ? "/" : request.path;
  if (!request.query.empty()) {
    out += '?';
    out += request.query;
  }
  out += " HTTP/1.1\r\n";
  for (const auto& [name, value] : request.headers) {
    if (!IsDroppedRequestHeader(name)) {
      out += name;
      out += ": ";
      out += value;
      out += "\r\n";
    }
  }
  out += "Host: " + target.host + ":" + std::to_string(target.port) + "\r\n";
  out += "Connection: close\r\n";
  if (!request.body.empty()) {
    out += "Content-Length: " + std::to_string(request.body.size()) + "\r\n";
  }
  out += "\r\n";
  out += request.body;
  return out;
}

/// recv with a timeout. Returns >0 bytes or 0 on EOF; on -1, *timed_out
/// distinguishes a timeout from a hard error.
ssize_t Receive(int fd, Milliseconds timeout, char* buffer, std::size_t size,
                bool* timed_out) {
  const timeval slice{
      static_cast<time_t>(timeout.count() / 1000),
      static_cast<suseconds_t>((timeout.count() % 1000) * 1000)};
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &slice, sizeof(slice)) != 0) {
    *timed_out = false;
    return -1;
  }
  for (;;) {
    const auto count = ::recv(fd, buffer, size, 0);
    if (count >= 0) {
      *timed_out = false;
      return count;
    }
    if (errno == EINTR) {
      continue;
    }
    *timed_out = errno == EAGAIN || errno == EWOULDBLOCK;
    return -1;
  }
}

enum class Framing { kContentLength, kChunked, kToClose };

struct UpstreamHead {
  int status{0};
  std::string reason;
  std::vector<std::pair<std::string, std::string>> headers;
  Framing framing{Framing::kToClose};
  std::size_t content_length{0};
  std::string remainder;  // body bytes received together with the head
};

/// Read status line + headers. Returns false on EOF before headers, a
/// header-phase timeout, or a malformed head.
bool ReadHead(int fd, UpstreamHead* head, std::string* error) {
  std::string data;
  std::size_t header_end = std::string::npos;
  const auto start = std::chrono::steady_clock::now();
  char buffer[4096];
  while (header_end == std::string::npos) {
    bool timed_out = false;
    const auto elapsed = std::chrono::duration_cast<Milliseconds>(
        std::chrono::steady_clock::now() - start);
    const auto remaining =
        kHeaderBudget > elapsed ? kHeaderBudget - elapsed : Milliseconds(1);
    const auto count =
        Receive(fd, remaining, buffer, sizeof(buffer), &timed_out);
    if (count == 0) {
      *error = "connection closed before headers";
      return false;
    }
    if (count < 0) {
      *error =
          timed_out ? "timed out waiting for headers" : std::strerror(errno);
      return false;
    }
    data.append(buffer, static_cast<std::size_t>(count));
    if (data.size() > 1024 * 1024) {
      *error = "response headers too large";
      return false;
    }
    header_end = data.find("\r\n\r\n");
  }
  std::vector<std::string> lines;
  std::size_t pos = 0;
  while (pos < header_end) {
    const auto next = data.find("\r\n", pos);
    if (next == std::string::npos || next > header_end) {
      lines.push_back(data.substr(pos, header_end - pos));
      break;
    }
    lines.push_back(data.substr(pos, next - pos));
    pos = next + 2;
  }
  if (lines.empty() || lines[0].compare(0, 5, "HTTP/") != 0) {
    *error = "malformed status line";
    return false;
  }
  const auto first_space = lines[0].find(' ');
  const auto second_space = first_space == std::string::npos
                                ? std::string::npos
                                : lines[0].find(' ', first_space + 1);
  const std::string code =
      lines[0].substr(first_space + 1, second_space == std::string::npos
                                           ? std::string::npos
                                           : second_space - first_space - 1);
  head->status = std::atoi(code.c_str());
  if (head->status < 100 || head->status > 599) {
    *error = "invalid status code";
    return false;
  }
  if (second_space != std::string::npos) {
    head->reason = Trim(lines[0].substr(second_space + 1));
  }
  for (std::size_t i = 1; i < lines.size(); ++i) {
    const auto colon = lines[i].find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string name = Trim(lines[i].substr(0, colon));
    const std::string value = Trim(lines[i].substr(colon + 1));
    if (IEquals(name, "Transfer-Encoding") && IEquals(value, "chunked")) {
      head->framing = Framing::kChunked;
    }
    if (IEquals(name, "Content-Length")) {
      head->content_length =
          static_cast<std::size_t>(std::strtoull(value.c_str(), nullptr, 10));
    }
    if (!IsHopByHop(name)) {
      head->headers.emplace_back(name, value);
    }
  }
  if (head->framing != Framing::kChunked && head->content_length > 0) {
    head->framing = Framing::kContentLength;
  }
  head->remainder = data.substr(header_end + 4);
  return true;
}

/// Single close point for the upstream socket: closed explicitly when the
/// stream ends, otherwise when the last owner (streaming body or scope) is
/// dropped without streaming.
struct SocketCell {
  std::atomic<int> fd{-1};
  SocketCell() = default;
  SocketCell(const SocketCell&) = delete;
  SocketCell& operator=(const SocketCell&) = delete;
  ~SocketCell() { Close(); }
  void Close() {
    const int owned = fd.exchange(-1);
    if (owned >= 0) {
      ::close(owned);
    }
  }
};

bool Cancelled(const HttpRequest& request) {
  return request.is_cancelled && request.is_cancelled();
}

/// Read one more socket slice, honouring cancellation while the upstream is
/// silent. Returns false to stop streaming (EOF, error, cancel).
bool Refill(int fd, std::string& buffer, const HttpRequest& request) {
  char raw[8192];
  for (;;) {
    if (Cancelled(request)) {
      return false;
    }
    bool timed_out = false;
    const auto count =
        Receive(fd, kBodyPollInterval, raw, sizeof(raw), &timed_out);
    if (count > 0) {
      buffer.append(raw, static_cast<std::size_t>(count));
      return true;
    }
    if (count == 0 || !timed_out) {
      return false;
    }
  }
}

void StreamContentLength(int fd, std::string buffer, std::size_t remaining,
                         const HttpResponse::BodyWriter& write,
                         const HttpRequest& request) {
  if (buffer.size() > remaining) {
    buffer.resize(remaining);
  }
  if (!buffer.empty() && !write(buffer)) {
    return;
  }
  remaining -= buffer.size();
  while (remaining > 0) {
    buffer.clear();
    if (!Refill(fd, buffer, request)) {
      return;
    }
    if (buffer.size() > remaining) {
      buffer.resize(remaining);
    }
    if (!write(buffer)) {
      return;
    }
    remaining -= buffer.size();
  }
}

void StreamChunked(int fd, std::string buffer,
                   const HttpResponse::BodyWriter& write,
                   const HttpRequest& request) {
  for (;;) {
    if (Cancelled(request)) {
      return;
    }
    const auto line_end = buffer.find("\r\n");
    if (line_end == std::string::npos) {
      if (!Refill(fd, buffer, request)) {
        return;
      }
      continue;
    }
    std::string size_line = buffer.substr(0, line_end);
    const auto extension = size_line.find(';');
    if (extension != std::string::npos) {
      size_line.resize(extension);
    }
    char* parse_end = nullptr;
    const unsigned long long size =
        std::strtoull(size_line.c_str(), &parse_end, 16);
    if (parse_end == size_line.c_str()) {
      return;  // corrupt chunk framing
    }
    const std::size_t payload = static_cast<std::size_t>(size);
    const std::size_t consumed = line_end + 2;
    if (payload == 0) {
      return;  // final chunk; trailers are ignored
    }
    while (buffer.size() < consumed + payload + 2) {
      if (!Refill(fd, buffer, request)) {
        return;
      }
    }
    const std::string_view chunk(buffer.data() + consumed, payload);
    if (!write(chunk)) {
      return;
    }
    buffer.erase(0, consumed + payload + 2);
    if (Cancelled(request)) {
      return;
    }
  }
}

void StreamToClose(int fd, std::string buffer,
                   const HttpResponse::BodyWriter& write,
                   const HttpRequest& request) {
  if (!buffer.empty() && !write(buffer)) {
    return;
  }
  for (;;) {
    buffer.clear();
    if (!Refill(fd, buffer, request)) {
      return;
    }
    if (!write(buffer)) {
      return;
    }
  }
}

}  // namespace

gufo::server::HttpResponse ProxyToWorker(
    const UpstreamTarget& target, const gufo::server::HttpRequest& request) {
  std::string error;
  const int fd = ConnectTo(target, &error);
  if (fd < 0) {
    return UpstreamUnavailable(error);
  }
  auto cell = std::make_shared<SocketCell>();
  cell->fd.store(fd);
  const timeval send_timeout{kHeaderBudget.count() / 1000,
                             (kHeaderBudget.count() % 1000) * 1000};
  if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &send_timeout,
                   sizeof(send_timeout)) != 0) {
    return UpstreamUnavailable(std::strerror(errno));
  }
  if (!SendAll(fd, BuildUpstreamRequest(target, request))) {
    return UpstreamUnavailable("failed to forward request");
  }
  UpstreamHead head;
  if (!ReadHead(fd, &head, &error)) {
    return UpstreamUnavailable(error);
  }

  HttpResponse response;
  response.status = head.status;
  response.reason = head.reason.empty() ? "OK" : head.reason;
  response.headers = std::move(head.headers);
  const Framing framing = head.framing;
  const std::size_t content_length = head.content_length;
  std::string remainder = std::move(head.remainder);
  HttpRequest front = request;
  response.streaming_body =
      [cell, framing, content_length, remainder = std::move(remainder),
       front](const HttpResponse::BodyWriter& write) mutable {
        const int upstream = cell->fd.load();
        switch (framing) {
          case Framing::kContentLength:
            StreamContentLength(upstream, std::move(remainder), content_length,
                                write, front);
            break;
          case Framing::kChunked:
            StreamChunked(upstream, std::move(remainder), write, front);
            break;
          case Framing::kToClose:
            StreamToClose(upstream, std::move(remainder), write, front);
            break;
        }
        cell->Close();
      };
  return response;
}

}  // namespace gufo::router
