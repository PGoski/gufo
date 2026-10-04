#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "src/cli/serve/router/workers.hpp"

namespace {

using gufo::router::ReserveLoopbackPort;
using gufo::router::Worker;
using gufo::router::WorkerSpec;

using Clock = std::chrono::steady_clock;

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::milliseconds budget) {
  const auto deadline = Clock::now() + budget;
  while (Clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

std::filesystem::path TempPath(const std::string& name) {
  return std::filesystem::temp_directory_path() /
         ("gufo_router_workers_test_" + std::to_string(::getpid()) + "_" +
          name);
}

/// Writes an executable python3 script (shebang + chmod) for the
/// GUFO_ROUTER_WORKER_EXE seam and points the seam at it.
class WorkerScript {
public:
  WorkerScript(const std::string& name, const std::string& body) {
    path_ = TempPath(name + ".py");
    std::ofstream out(path_);
    out << "#!/usr/bin/env python3\n" << body;
    out.close();
    assert(out);
    ::chmod(path_.c_str(), 0755);
    ::setenv("GUFO_ROUTER_WORKER_EXE", path_.c_str(), 1);
  }
  ~WorkerScript() {
    ::unsetenv("GUFO_ROUTER_WORKER_EXE");
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

private:
  std::filesystem::path path_;
};

/// Captures everything Logger writes (std::clog -> fd 2) for the duration of
/// the scope.
class StderrCapture {
public:
  StderrCapture() : path_(TempPath("stderr.log")) {
    saved_fd_ = ::dup(STDERR_FILENO);
    assert(saved_fd_ >= 0);
    const int file_fd =
        ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert(file_fd >= 0);
    ::dup2(file_fd, STDERR_FILENO);
    ::close(file_fd);
  }
  ~StderrCapture() {
    std::fflush(nullptr);
    ::dup2(saved_fd_, STDERR_FILENO);
    ::close(saved_fd_);
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  [[nodiscard]] std::string Contents() {
    std::fflush(nullptr);
    std::ifstream in(path_);
    std::ostringstream buffer;
    buffer << in.rdbuf();
    return buffer.str();
  }

private:
  std::filesystem::path path_;
  int saved_fd_{-1};
};

/// One-shot in-process HTTP responder on an ephemeral port.
class FakeHealthServer {
public:
  explicit FakeHealthServer(std::string status_line)
      : status_line_(std::move(status_line)) {
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
    assert(::listen(listen_fd_, 1) == 0);
    socklen_t length = sizeof(address);
    assert(::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address),
                         &length) == 0);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { Serve(); });
  }
  ~FakeHealthServer() {
    if (thread_.joinable()) {
      thread_.join();
    }
    ::close(listen_fd_);
  }

  [[nodiscard]] int port() const { return port_; }

private:
  void Serve() {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      return;
    }
    char buffer[1024];
    std::string request;
    while (request.find("\r\n\r\n") == std::string::npos) {
      const auto count = ::recv(fd, buffer, sizeof(buffer), 0);
      if (count <= 0) {
        break;
      }
      request.append(buffer, static_cast<std::size_t>(count));
    }
    std::string path;
    const std::size_t first = request.find(' ');
    const std::size_t second = request.find(' ', first + 1);
    if (first != std::string::npos && second != std::string::npos) {
      path = request.substr(first + 1, second - first - 1);
    }
    if (path == "/health") {
      const std::string body = "{\"status\":\"ok\"}\n";
      const std::string response =
          status_line_ + "Content-Length: " + std::to_string(body.size()) +
          "\r\nConnection: close\r\n\r\n" + body;
      const auto sent =
          ::send(fd, response.data(), response.size(), MSG_NOSIGNAL);
      (void)sent;
    }
    ::close(fd);
  }

  std::string status_line_;
  int listen_fd_{-1};
  int port_{0};
  std::thread thread_;
};

void TestReserveLoopbackPort() {
  std::string error;
  const int first = ReserveLoopbackPort(&error);
  assert(first >= 1024 && first <= 65535);
  assert(error.empty());
  const int second = ReserveLoopbackPort(&error);
  assert(second >= 1024 && second <= 65535);
  assert(first != second);
  // The reservation was released, so rebinding the same port must work.
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  assert(fd >= 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<uint16_t>(first));
  assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
  assert(::bind(fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) == 0);
  ::close(fd);
}

void TestExecFailure() {
  ::unsetenv("GUFO_ROUTER_WORKER_EXE");
  WorkerSpec spec;
  spec.model_id = "test-1";
  spec.modality = "llm";
  spec.port = ReserveLoopbackPort(nullptr);
  const auto start = Clock::now();
  std::string error;
  std::unique_ptr<Worker> worker = Worker::Spawn(spec, "/nonexistent", &error);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      Clock::now() - start);
  assert(worker == nullptr);
  assert(!error.empty());
  assert(elapsed < std::chrono::milliseconds(500));
}

void TestWorkerStdoutToLogger() {
  WorkerScript script("print", R"PY(
import os
import sys

expected_port = sys.argv[1] if len(sys.argv) > 1 else ""
if (os.environ.get("GUFO_HOST") == "127.0.0.1"
        and os.environ.get("GUFO_PORT") == expected_port):
    print("event=ready_probe", flush=True)
else:
    print("event=env_wrong host=%s port=%s"
          % (os.environ.get("GUFO_HOST"), os.environ.get("GUFO_PORT")),
          flush=True)
)PY");
  const int port = ReserveLoopbackPort(nullptr);
  WorkerSpec spec;
  spec.model_id = "test-1";
  spec.modality = "llm";
  spec.args = {std::to_string(port)};
  spec.port = port;

  StderrCapture capture;
  std::string error;
  std::unique_ptr<Worker> worker = Worker::Spawn(spec, "/unused", &error);
  assert(worker != nullptr);
  assert(error.empty());
  assert(worker->port() == port);
  assert(worker->model_id() == "test-1");

  const bool logged = WaitUntil(
      [&capture] {
        return capture.Contents().find("model=test-1 event=ready_probe") !=
               std::string::npos;
      },
      std::chrono::seconds(4));
  const std::string contents = capture.Contents();
  worker->Stop(std::chrono::seconds(2));
  assert(logged);
  assert(contents.find("event=env_wrong") == std::string::npos);
}

void TestPollExit() {
  WorkerScript script("exit3", "import sys\nsys.exit(3)\n");
  WorkerSpec spec;
  spec.model_id = "test-1";
  spec.modality = "llm";
  spec.port = ReserveLoopbackPort(nullptr);
  std::string error;
  std::unique_ptr<Worker> worker = Worker::Spawn(spec, "/unused", &error);
  assert(worker != nullptr);

  int exit_status = -1;
  const bool reaped = WaitUntil([&] { return worker->PollExit(&exit_status); },
                                std::chrono::seconds(2));
  assert(reaped);
  assert(exit_status == 3);
  // Idempotent after the reap.
  int again = -1;
  assert(worker->PollExit(&again));
  assert(again == 3);
  worker->Stop(std::chrono::seconds(1));
}

void TestStopWithSigtermIgnored() {
  const std::filesystem::path pid_file = TempPath("stop.pid");
  WorkerScript script("ignore_term", R"PY(
import os
import signal
import sys
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], "w") as handle:
    handle.write(str(os.getpid()))
    handle.flush()
time.sleep(60)
)PY");
  WorkerSpec spec;
  spec.model_id = "test-1";
  spec.modality = "llm";
  spec.args = {pid_file.string()};
  spec.port = ReserveLoopbackPort(nullptr);
  std::string error;
  std::unique_ptr<Worker> worker = Worker::Spawn(spec, "/unused", &error);
  assert(worker != nullptr);

  int child_pid = -1;
  const bool announced = WaitUntil(
      [&child_pid, &pid_file] {
        std::ifstream in(pid_file);
        return static_cast<bool>(in >> child_pid) && child_pid > 0;
      },
      std::chrono::seconds(5));
  assert(announced);

  const auto start = Clock::now();
  worker->Stop(std::chrono::seconds(1));
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      Clock::now() - start);
  // The grace expired against an ignored SIGTERM, so the SIGKILL path ran:
  // roughly a second, never the full default grace.
  assert(elapsed >= std::chrono::milliseconds(900));
  assert(elapsed < std::chrono::seconds(5));
  assert(::kill(child_pid, 0) == -1);
  int exit_status = -1;
  assert(worker->PollExit(&exit_status));
  std::error_code ignored;
  std::filesystem::remove(pid_file, ignored);
}

void TestHealthy() {
  WorkerScript script("sleep", "import time\ntime.sleep(30)\n");

  auto spawn_on = [](int port) {
    WorkerSpec spec;
    spec.model_id = "test-1";
    spec.modality = "llm";
    spec.port = port;
    std::string error;
    std::unique_ptr<Worker> worker = Worker::Spawn(spec, "/unused", &error);
    assert(worker != nullptr);
    return worker;
  };

  {
    FakeHealthServer ok("HTTP/1.1 200 OK\r\n");
    std::unique_ptr<Worker> worker = spawn_on(ok.port());
    assert(worker->Healthy());
    worker->Stop(std::chrono::seconds(2));
  }
  {
    FakeHealthServer broken("HTTP/1.1 500 Internal Server Error\r\n");
    std::unique_ptr<Worker> worker = spawn_on(broken.port());
    assert(!worker->Healthy());
    worker->Stop(std::chrono::seconds(2));
  }
  {
    // Nothing listens: connect fails fast, still within the probe budget.
    std::unique_ptr<Worker> worker = spawn_on(ReserveLoopbackPort(nullptr));
    const auto start = Clock::now();
    assert(!worker->Healthy());
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - start);
    assert(elapsed < std::chrono::milliseconds(400));
    worker->Stop(std::chrono::seconds(2));
  }
}

}  // namespace

int main() {
  TestReserveLoopbackPort();
  TestExecFailure();
  TestWorkerStdoutToLogger();
  TestPollExit();
  TestStopWithSigtermIgnored();
  TestHealthy();
  std::cout << "All router workers tests passed.\n";
  return 0;
}
