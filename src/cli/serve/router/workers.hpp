#ifndef GUFO_CLI_SERVE_ROUTER_WORKERS_HPP_
#define GUFO_CLI_SERVE_ROUTER_WORKERS_HPP_

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace gufo::router {

struct WorkerSpec {
  std::string model_id;
  std::string modality;  // appended after "serve"
  std::vector<std::string> args;
  int port;  // via GUFO_HOST/GUFO_PORT env
};

class Worker {
public:
  /// fork()+execv(exe_path) with argv {exe, "serve", modality, args...};
  /// PR_SET_PDEATHSIG(SIGKILL); stdout/stderr pipes pumped line-wise into
  /// Logger::Info("router", "model=<id> <line>"). nullptr + `error` on fork
  /// or exec failure (exec failure detected via pipe, no false success).
  ///
  /// Test-only seam: when the environment variable GUFO_ROUTER_WORKER_EXE
  /// names an existing file, it replaces `exe_path` and the "serve" +
  /// modality prefix is skipped, so argv becomes {exe, args...}. Production
  /// must never set it.
  static std::unique_ptr<Worker> Spawn(const WorkerSpec& spec,
                                       const std::filesystem::path& exe_path,
                                       std::string* error);

  [[nodiscard]] int port() const;
  [[nodiscard]] const std::string& model_id() const;

  /// GET /health on 127.0.0.1:port() within 300 ms; true only on HTTP 200.
  [[nodiscard]] bool Healthy() const;

  /// Non-blocking waitpid(WNOHANG); true + status when reaped (cached after
  /// the first reap). `exit_status` is the exit code, or 128+signal when the
  /// worker died from a signal.
  bool PollExit(int* exit_status);

  /// SIGTERM, wait grace, SIGKILL; idempotent; reaps and joins pump threads.
  void Stop(std::chrono::seconds grace = std::chrono::seconds(10));

  ~Worker();  // Stop(0s) when still running

private:
  Worker();

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

/// bind(127.0.0.1:0) → getsockname → close. Returns -1 + error.
int ReserveLoopbackPort(std::string* error);

}  // namespace gufo::router

#endif  // GUFO_CLI_SERVE_ROUTER_WORKERS_HPP_
