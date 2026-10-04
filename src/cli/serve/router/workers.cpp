#include "src/cli/serve/router/workers.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string_view>
#include <thread>

#include "src/cli/serve/logging.hpp"

extern "C" {
extern char** environ;
}

namespace gufo::router {
namespace {

constexpr std::chrono::milliseconds kProbeBudget{300};
constexpr int kPumpPollMs = 100;

using Clock = std::chrono::steady_clock;

std::string ErrnoText(int err) {
  const char* text = std::strerror(err);
  return text != nullptr ? text : "unknown error";
}

int TerminationStatus(int raw) {
  if (WIFEXITED(raw)) {
    return WEXITSTATUS(raw);
  }
  if (WIFSIGNALED(raw)) {
    return 128 + WTERMSIG(raw);
  }
  return -1;
}

short RemainingMs(Clock::time_point deadline) {
  const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
      deadline - Clock::now());
  return static_cast<short>(
      std::max<std::chrono::milliseconds::rep>(left.count(), 0));
}

/// Drains one pipe end, logging each complete line. Exits at EOF or error;
/// a trailing partial line is still logged.
void PumpPipe(int fd, const std::string& model_id) {
  std::string pending;
  std::array<char, 4096> buffer{};
  bool closed = false;
  while (!closed) {
    pollfd descriptor{fd, POLLIN, 0};
    const int ready = ::poll(&descriptor, 1, kPumpPollMs);
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    if (ready == 0) {
      continue;
    }
    const ssize_t count = ::read(fd, buffer.data(), buffer.size());
    if (count == 0) {
      break;
    }
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    pending.append(buffer.data(), static_cast<std::size_t>(count));
    std::size_t newline = 0;
    while ((newline = pending.find('\n')) != std::string::npos) {
      std::string line = pending.substr(0, newline);
      pending.erase(0, newline + 1);
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      gufo::server::Logger::Info("router", "model=" + model_id + " " + line);
    }
  }
  if (!pending.empty()) {
    if (pending.back() == '\r') {
      pending.pop_back();
    }
    if (!pending.empty()) {
      gufo::server::Logger::Info("router", "model=" + model_id + " " + pending);
    }
  }
  ::close(fd);
}

/// Connect, send, and read the response status line under one shared
/// deadline. Returns the HTTP status code, or -1 on any failure.
int ProbeHealth(int port, Clock::time_point deadline) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    return -1;
  }
  ::fcntl(fd, F_SETFL, O_NONBLOCK);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<uint16_t>(port));
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

  if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0) {
    const int err = errno;
    if (err != EINPROGRESS && err != EINTR && err != EAGAIN) {
      ::close(fd);
      return -1;
    }
    pollfd descriptor{fd, POLLOUT | POLLERR | POLLHUP, RemainingMs(deadline)};
    if (::poll(&descriptor, 1, RemainingMs(deadline)) <= 0) {
      ::close(fd);
      return -1;
    }
    int so_error = 0;
    socklen_t length = sizeof(so_error);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &length) != 0 ||
        so_error != 0) {
      ::close(fd);
      return -1;
    }
  }

  static constexpr std::string_view kRequest =
      "GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\n"
      "Connection: close\r\n\r\n";
  std::size_t sent = 0;
  while (sent < kRequest.size()) {
    pollfd descriptor{fd, POLLOUT, RemainingMs(deadline)};
    if (::poll(&descriptor, 1, RemainingMs(deadline)) <= 0) {
      ::close(fd);
      return -1;
    }
    const ssize_t written = ::send(fd, kRequest.data() + sent,
                                   kRequest.size() - sent, MSG_NOSIGNAL);
    if (written > 0) {
      sent += static_cast<std::size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    ::close(fd);
    return -1;
  }

  std::string response;
  std::array<char, 256> buffer{};
  while (response.find("\r\n") == std::string::npos && response.size() < 80) {
    pollfd descriptor{fd, POLLIN, RemainingMs(deadline)};
    const int ready = ::poll(&descriptor, 1, RemainingMs(deadline));
    if (ready < 0 && errno == EINTR) {
      continue;
    }
    if (ready <= 0) {
      ::close(fd);
      return -1;
    }
    const ssize_t count = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count <= 0) {
      ::close(fd);
      return -1;
    }
    response.append(buffer.data(), static_cast<std::size_t>(count));
  }
  ::close(fd);

  // "HTTP/1.x NNN"
  if (!response.starts_with("HTTP/1.") || response.size() < 12 ||
      response[8] != ' ') {
    return -1;
  }
  const std::string code = response.substr(9, 3);
  if (code.find_first_not_of("0123456789") != std::string::npos) {
    return -1;
  }
  return std::atoi(code.c_str());
}

}  // namespace

struct Worker::Impl {
  pid_t pid{-1};
  int port{0};
  std::string model_id;
  int out_fd{-1};
  int err_fd{-1};
  std::thread out_pump;
  std::thread err_pump;
  std::mutex mutex;  // guards waitpid and the flags below
  bool reaped = false;
  bool stopped = false;
  int exit_status = -1;
};

Worker::Worker() : impl_(std::make_unique<Impl>()) {}

std::unique_ptr<Worker> Worker::Spawn(const WorkerSpec& spec,
                                      const std::filesystem::path& exe_path,
                                      std::string* error) {
  auto fail = [&](const std::string& message) {
    if (error != nullptr) {
      *error = message;
    }
    return nullptr;
  };

  // Test-only seam (see header comment): replaces the executable and drops
  // the "serve" + modality prefix so a script can stand in for a worker.
  const char* seam = std::getenv("GUFO_ROUTER_WORKER_EXE");
  const bool skip_mode =
      seam != nullptr && *seam != '\0' && std::filesystem::exists(seam);
  const std::filesystem::path exe = skip_mode ? seam : exe_path;

  std::vector<std::string> argv_strings;
  argv_strings.push_back(exe.string());
  if (!skip_mode) {
    argv_strings.push_back("serve");
    argv_strings.push_back(spec.modality);
  }
  for (const std::string& arg : spec.args) {
    argv_strings.push_back(arg);
  }

  std::vector<std::string> env_strings;
  for (char** entry = environ; entry != nullptr && *entry != nullptr; ++entry) {
    const std::string_view text(*entry);
    if (!text.starts_with("GUFO_HOST=") && !text.starts_with("GUFO_PORT=")) {
      env_strings.emplace_back(text);
    }
  }
  env_strings.push_back("GUFO_HOST=127.0.0.1");
  env_strings.push_back("GUFO_PORT=" + std::to_string(spec.port));

  // Null-terminated views. The vectors are stable while argv/envp are used:
  // everything here completes (or _exits) before any further mutation.
  std::vector<char*> argv;
  argv.reserve(argv_strings.size() + 1);
  for (std::string& value : argv_strings) {
    argv.push_back(value.data());
  }
  argv.push_back(nullptr);
  std::vector<char*> envp;
  envp.reserve(env_strings.size() + 1);
  for (std::string& value : env_strings) {
    envp.push_back(value.data());
  }
  envp.push_back(nullptr);

  int out_pipe[2] = {-1, -1};
  int err_pipe[2] = {-1, -1};
  int fail_pipe[2] = {-1, -1};
  if (::pipe2(out_pipe, O_CLOEXEC) != 0 || ::pipe2(err_pipe, O_CLOEXEC) != 0 ||
      ::pipe2(fail_pipe, O_CLOEXEC) != 0) {
    const int err = errno;
    for (int* pipe : {out_pipe, err_pipe, fail_pipe}) {
      if (pipe[0] >= 0) {
        ::close(pipe[0]);
      }
      if (pipe[1] >= 0) {
        ::close(pipe[1]);
      }
    }
    return fail("pipe2 failed: " + ErrnoText(err));
  }

  const std::string exe_cstr = exe.string();
  const pid_t pid = ::fork();
  if (pid < 0) {
    const int err = errno;
    ::close(out_pipe[0]);
    ::close(out_pipe[1]);
    ::close(err_pipe[0]);
    ::close(err_pipe[1]);
    ::close(fail_pipe[0]);
    ::close(fail_pipe[1]);
    return fail("fork failed: " + ErrnoText(err));
  }
  if (pid == 0) {
    // Async-signal-safe only until execve.
    ::dup2(out_pipe[1], STDOUT_FILENO);
    ::dup2(err_pipe[1], STDERR_FILENO);
    ::close(out_pipe[0]);
    ::close(err_pipe[0]);
    ::close(fail_pipe[0]);
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
    ::execve(exe_cstr.c_str(), argv.data(), envp.data());
    const int exec_errno = errno;
    // CLOEXEC would strip it on success; on failure report a single errno
    // byte so the parent never reports a false success.
    const unsigned char report = static_cast<unsigned char>(
        exec_errno > 0 && exec_errno < 256 ? exec_errno : 1);
    const ssize_t ignored = ::write(fail_pipe[1], &report, 1);
    (void)ignored;
    ::_exit(127);
  }

  // Parent.
  ::close(out_pipe[1]);
  ::close(err_pipe[1]);
  ::close(fail_pipe[1]);
  unsigned char report = 0;
  std::string exec_error;
  for (;;) {
    const ssize_t count = ::read(fail_pipe[0], &report, 1);
    if (count == 1) {
      exec_error = ErrnoText(static_cast<int>(report));
      break;
    }
    if (count == 0) {
      break;  // CLOEXEC closed the pipe: execve succeeded.
    }
    if (errno != EINTR) {
      exec_error = "failure pipe read error: " + ErrnoText(errno);
      break;
    }
  }
  ::close(fail_pipe[0]);
  if (!exec_error.empty()) {
    ::close(out_pipe[0]);
    ::close(err_pipe[0]);
    int raw = 0;
    while (::waitpid(pid, &raw, 0) < 0 && errno == EINTR) {
    }
    return fail("exec " + exe_cstr + " failed: " + exec_error);
  }

  std::unique_ptr<Worker> worker(new Worker());
  Impl& impl = *worker->impl_;
  impl.pid = pid;
  impl.port = spec.port;
  impl.model_id = spec.model_id;
  impl.out_fd = out_pipe[0];
  impl.err_fd = err_pipe[0];
  impl.out_pump =
      std::thread(PumpPipe, impl.out_fd, std::string(impl.model_id));
  impl.err_pump =
      std::thread(PumpPipe, impl.err_fd, std::string(impl.model_id));
  if (error != nullptr) {
    error->clear();
  }
  return worker;
}

int Worker::port() const {
  return impl_->port;
}

const std::string& Worker::model_id() const {
  return impl_->model_id;
}

bool Worker::Healthy() const {
  return ProbeHealth(impl_->port, Clock::now() + kProbeBudget) == 200;
}

bool Worker::PollExit(int* exit_status) {
  std::lock_guard lock(impl_->mutex);
  Impl& impl = *impl_;
  if (impl.reaped) {
    if (exit_status != nullptr) {
      *exit_status = impl.exit_status;
    }
    return true;
  }
  int raw = 0;
  const pid_t rc = ::waitpid(impl.pid, &raw, WNOHANG);
  if (rc == impl.pid) {
    impl.reaped = true;
    impl.exit_status = TerminationStatus(raw);
    if (exit_status != nullptr) {
      *exit_status = impl.exit_status;
    }
    return true;
  }
  if (rc < 0 && errno == ECHILD) {
    impl.reaped = true;
    impl.exit_status = -1;
    if (exit_status != nullptr) {
      *exit_status = impl.exit_status;
    }
    return true;
  }
  return false;
}

void Worker::Stop(std::chrono::seconds grace) {
  std::lock_guard lock(impl_->mutex);
  Impl& impl = *impl_;
  if (impl.stopped) {
    return;
  }
  impl.stopped = true;
  if (!impl.reaped) {
    if (::kill(impl.pid, SIGTERM) != 0 && errno != ESRCH) {
      // Fall through: the bounded wait below still ends in SIGKILL/reap.
    }
    const auto deadline = Clock::now() + grace;
    for (;;) {
      int raw = 0;
      const pid_t rc = ::waitpid(impl.pid, &raw, WNOHANG);
      if (rc == impl.pid) {
        impl.reaped = true;
        impl.exit_status = TerminationStatus(raw);
        break;
      }
      if (rc < 0 && errno == ECHILD) {
        impl.reaped = true;
        break;
      }
      if (Clock::now() >= deadline) {
        ::kill(impl.pid, SIGKILL);
        for (;;) {
          int killed = 0;
          const pid_t final = ::waitpid(impl.pid, &killed, 0);
          if (final == impl.pid) {
            impl.exit_status = TerminationStatus(killed);
            break;
          }
          if (final < 0 && errno != EINTR) {
            break;
          }
        }
        impl.reaped = true;
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }
  if (impl.out_pump.joinable()) {
    impl.out_pump.join();
  }
  if (impl.err_pump.joinable()) {
    impl.err_pump.join();
  }
}

Worker::~Worker() {
  Stop(std::chrono::seconds(0));
}

int ReserveLoopbackPort(std::string* error) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    if (error != nullptr) {
      *error = "socket failed: " + ErrnoText(errno);
    }
    return -1;
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    const int err = errno;
    ::close(fd);
    if (error != nullptr) {
      *error = "bind failed: " + ErrnoText(err);
    }
    return -1;
  }
  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    const int err = errno;
    ::close(fd);
    if (error != nullptr) {
      *error = "getsockname failed: " + ErrnoText(err);
    }
    return -1;
  }
  ::close(fd);
  if (error != nullptr) {
    error->clear();
  }
  return ntohs(address.sin_port);
}

}  // namespace gufo::router
