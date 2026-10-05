#include "src/cli/serve/router/router.hpp"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "src/cli/arg_parser.hpp"
#include "src/cli/serve/http_server.hpp"
#include "src/cli/serve/logging.hpp"
#include "src/cli/serve/router/preset.hpp"
#include "src/cli/serve/router/proxy.hpp"
#include "src/cli/serve/router/registry.hpp"
#include "src/cli/serve/router/workers.hpp"
#include "src/cli/serve/serve.hpp"
#include "src/cli/serve/serve_options.hpp"
#include "src/cli/serve/video_api.hpp"
#include "src/core/json.hpp"

namespace gufo::cli {
namespace {

namespace json = gufo::json;
namespace router = gufo::router;
using server::HttpRequest;
using server::HttpResponse;
using server::HttpServer;
using server::HttpServerOptions;
using server::Logger;

constexpr std::string_view kPresetGrammar =
    "Preset file format:\n"
    "  Plain UTF-8 text. Lines starting with '#' are comments; blank lines\n"
    "  are ignored. Each model is one section:\n"
    "\n"
    "    [llm/<model-id>]      modality is one of llm|image|video|tts|asr;\n"
    "                          <model-id> is the routing key served under\n"
    "                          /v1/models and must be unique per file.\n"
    "    key = value           maps to the worker argv '--key value'. Keys\n"
    "                          are exactly the long options of\n"
    "                          'gufo serve <modality>' shown below; boolean\n"
    "                          options take true|false.\n"
    "    host, port, api-key   are router-managed and rejected in "
    "sections.\n\n";

constexpr std::chrono::milliseconds kAdmissionPoll{50};
constexpr std::chrono::milliseconds kSupervisionTick{250};
constexpr int kSpawnAttempts = 3;
constexpr std::chrono::seconds kShutdownDrain{10};

HttpResponse Err(int status, std::string_view reason, std::string_view message,
                 std::string_view type, std::string_view code) {
  json::Value detail = json::Value::object();
  detail["message"] = std::string(message);
  detail["type"] = std::string(type);
  detail["code"] = std::string(code);
  json::Value body = json::Value::object();
  body["error"] = std::move(detail);
  return HttpResponse{
      .status = status, .reason = std::string(reason), .body = body.dump()};
}

/// Router-managed paths answered by the front itself. Returns nullopt when
/// the path belongs to the model-routing table instead.
std::optional<HttpResponse> RouteFront(const router::Registry& registry,
                                       const HttpRequest& req) {
  if (req.method == "GET" && (req.path == "/health" || req.path == "/healthz" ||
                              req.path == "/v1/health")) {
    json::Value body = json::Value::object();
    body["status"] = "ok";
    return HttpResponse{.status = 200, .reason = "OK", .body = body.dump()};
  }
  // Preset validation finished before the listener started, so answering is
  // itself the readiness condition for the front.
  if (req.method == "GET" && (req.path == "/ready" || req.path == "/readyz" ||
                              req.path == "/v1/ready")) {
    json::Value body = json::Value::object();
    body["status"] = "ready";
    return HttpResponse{.status = 200, .reason = "OK", .body = body.dump()};
  }
  if (req.method == "GET" &&
      (req.path == "/v1/models" || req.path == "/models")) {
    json::Value resp = json::Value::object();
    resp["object"] = "list";
    json::Value data = json::Value::array();
    for (const auto& [id, loaded] : registry.Listing()) {
      json::Value model = json::Value::object();
      model["id"] = id;
      model["object"] = "model";
      model["owned_by"] = "gufo";
      model["loaded"] = loaded;
      data.push_back(std::move(model));
    }
    resp["data"] = std::move(data);
    return HttpResponse{.status = 200, .reason = "OK", .body = resp.dump()};
  }
  return std::nullopt;
}

std::optional<int> ParseEnvPort(const char* value) {
  if (value == nullptr || *value == '\0') {
    return std::nullopt;
  }
  const std::string_view text(value);
  int parsed = 0;
  const auto [ptr, ec] =
      std::from_chars(text.data(), text.data() + text.size(), parsed);
  if (ec != std::errc{} || ptr != text.data() + text.size()) {
    return std::nullopt;
  }
  return parsed;
}

void PrintRouterHelp(const ArgParser& parser) {
  std::cout << parser.FormatHelp() << "\n" << kPresetGrammar;
  for (const std::string_view modality :
       {"llm", "image", "video", "tts", "asr"}) {
    PrintServeHelp("gufo", modality);
  }
}

std::string MultipartModel(const std::string& body) {
  const std::string needle = "name=\"model\"";
  const auto field = body.find(needle);
  if (field == std::string::npos) {
    return "";
  }
  const auto start = body.find("\r\n\r\n", field);
  if (start == std::string::npos) {
    return "";
  }
  const auto end = body.find("\r\n", start + 4);
  return body.substr(start + 4, end == std::string::npos ? std::string::npos
                                                         : end - (start + 4));
}

bool IsJsonModelPost(const HttpRequest& req) {
  if (req.method != "POST") {
    return false;
  }
  static const std::vector<std::string> kPaths = {
      "/v1/chat/completions", "/v1/completions",        "/v1/responses",
      "/v1/messages",         "/v1/images/generations", "/v1/images/edits",
      "/v1/audio/speech"};
  if (std::find(kPaths.begin(), kPaths.end(), req.path) != kPaths.end()) {
    return true;
  }
  return server::IsVideoApiPath(req.path);
}

std::string JsonBodyModel(const HttpRequest& req) {
  try {
    const json::Value parsed = json::parse(req.body);
    if (const auto* value = parsed.find("model");
        value != nullptr && value->is_string()) {
      return value->str();
    }
  } catch (const std::exception&) {
    // A malformed body is a missing model as far as routing is concerned.
  }
  return "";
}

/// Model resolution table from the design spec. `missing` marks a family
/// endpoint whose model source (JSON field, form field, query param) is
/// absent, which is a 400 rather than an unresolvable path.
struct Resolution {
  std::string model_id;
  bool missing{false};
};

bool IsIntrospectionPath(const std::string& path) {
  return path == "/v1/slots" || path == "/slots" || path == "/v1/metrics" ||
         path == "/metrics";
}

/// Job id from `/v1/videos/<id>` or `/v1/videos/<id>/content`, matching the
/// shape video_api.cpp serves (ParseVideoPath).
std::string VideoJobId(const std::string& path) {
  constexpr std::string_view prefix = "/v1/videos/";
  std::string_view remainder(path);
  if (!remainder.starts_with(prefix)) {
    return "";
  }
  remainder.remove_prefix(prefix.size());
  constexpr std::string_view suffix = "/content";
  if (remainder.ends_with(suffix)) {
    remainder.remove_suffix(suffix.size());
  }
  if (remainder.empty() || remainder.find('/') != std::string_view::npos) {
    return "";
  }
  return std::string(remainder);
}

Resolution ResolveModel(const HttpRequest& req) {
  Resolution resolution;
  if (IsJsonModelPost(req)) {
    resolution.model_id = JsonBodyModel(req);
    resolution.missing = resolution.model_id.empty();
    return resolution;
  }
  if (req.method == "POST" && req.path == "/v1/audio/transcriptions") {
    resolution.model_id = MultipartModel(req.body);
    resolution.missing = resolution.model_id.empty();
    return resolution;
  }
  if (req.method == "GET" &&
      (req.path == "/v1/realtime" || req.path == "/v1/audio/speech/stream")) {
    resolution.model_id = req.query_param("model");
    resolution.missing = resolution.model_id.empty();
    return resolution;
  }
  if (req.method == "GET" && IsIntrospectionPath(req.path)) {
    resolution.model_id = req.query_param("model");
    resolution.missing = resolution.model_id.empty();
    return resolution;
  }
  return resolution;
}

/// Holds the front-request busy count until released exactly once; releasing
/// from the streaming relay keeps the model busy across the whole stream.
/// `Arm` runs with the router mutex already held; `Release` takes it itself
/// because the stream completion (or response destruction) happens on a
/// request thread with the lock free.
class HeldBusy : public std::enable_shared_from_this<HeldBusy> {
public:
  HeldBusy(std::mutex& mutex, router::Registry& registry, std::string model_id)
      : mutex_(mutex), registry_(registry), model_id_(std::move(model_id)) {}
  HeldBusy(const HeldBusy&) = delete;
  HeldBusy& operator=(const HeldBusy&) = delete;
  ~HeldBusy() { Release(); }

  std::shared_ptr<HeldBusy> Arm() {
    registry_.RequestStarted(model_id_);
    armed_ = true;
    return shared_from_this();
  }

  void Release() {
    if (!armed_) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    armed_ = false;
    registry_.RequestFinished(model_id_);
  }

private:
  std::mutex& mutex_;
  router::Registry& registry_;
  std::string model_id_;
  bool armed_{false};
};

/// Routes requests, owns live workers and the shared admission state.
class RouterState {
public:
  RouterState(std::vector<router::PresetModel> presets,
              router::RegistryOptions options)
      : registry(std::move(presets), options), options(options) {}

  router::Registry registry;
  router::RegistryOptions options;
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<router::Worker>> workers;
  std::filesystem::path exe_path;
  std::atomic<bool> stop_requested{false};

  /// Worker::Spawn must run on the long-lived spawner thread: PR_SET_PDEATHSIG
  /// binds the worker to the thread that forked it, and request-handling
  /// connection threads exit as soon as their client disconnects. The spawner
  /// lives for the whole routing lifetime, so workers die only with the
  /// router.
  void StartSpawner() {
    spawner_ = std::thread([this] { SpawnThreadMain(); });
  }

  void StopSpawner() {
    {
      const std::lock_guard<std::mutex> lock(spawn_mutex_);
      spawn_shutdown_ = true;
    }
    spawn_cv_.notify_all();
    if (spawner_.joinable()) {
      spawner_.join();
    }
  }

  /// Runs one Worker::Spawn on the spawner thread and waits for its result.
  /// The caller holds the state mutex; the spawner never touches it.
  std::unique_ptr<router::Worker> SpawnViaSpawnerLocked(
      const router::WorkerSpec& spec, std::string* error) {
    SpawnJob job;
    job.spec = spec;
    {
      const std::lock_guard<std::mutex> lock(spawn_mutex_);
      pending_ = &job;
    }
    spawn_cv_.notify_all();
    {
      std::unique_lock<std::mutex> done_lock(job.done_mutex);
      job.done_cv.wait(done_lock, [&job] { return job.done; });
    }
    *error = job.error;
    return std::move(job.result);
  }

  HttpResponse Dispatch(const HttpRequest& req) {
    // Every registry touch, including RouteFront's Listing, is serialized on
    // the same mutex the supervision tick holds.
    std::unique_lock<std::mutex> lock(mutex);
    if (const auto front = RouteFront(registry, req)) {
      return *front;
    }
    const Resolution resolution = ResolveModel(req);
    if (resolution.missing) {
      return Err(400, "Bad Request", "missing 'model' field",
                 "invalid_request_error", "missing_model");
    }
    if (stop_requested.load()) {
      return Err(502, "Bad Gateway", "router is shutting down", "server_error",
                 "upstream_unavailable");
    }
    Resolution resolved = resolution;
    if (resolved.model_id.empty() && server::IsVideoApiPath(req.path) &&
        (req.method == "GET" || req.method == "DELETE")) {
      // Status/content reads for a recorded job route to its owner model and
      // reload that worker if it died (persisted job state answers again).
      resolved.model_id = registry.JobOwner(VideoJobId(req.path));
    }
    if (resolved.model_id.empty()) {
      // Nothing resolved the path to a model: an unrecorded video job (404
      // listing preset ids) or an unknown route (404 not_found).
      if (server::IsVideoApiPath(req.path) &&
          (req.method == "GET" || req.method == "DELETE")) {
        return ModelNotFoundError();
      }
      return Err(404, "Not Found", "no route for this path",
                 "invalid_request_error", "not_found");
    }
    const std::string& model_id = resolved.model_id;
    const router::PresetModel* preset = registry.Find(model_id);
    if (preset == nullptr) {
      return ModelNotFoundError();
    }
    if (IsIntrospectionPath(req.path) && req.method == "GET" &&
        !LoadedLocked(model_id)) {
      return Err(409, "Conflict", "model '" + model_id + "' is not loaded",
                 "invalid_request_error", "model_not_loaded");
    }
    auto admitted = AdmitLocked(model_id, *preset);
    if (const auto* failed = std::get_if<HttpResponse>(&admitted)) {
      return std::move(*failed);
    }
    const int worker_port = std::get<int>(admitted);
    auto busy = std::make_shared<HeldBusy>(mutex, registry, model_id)->Arm();
    router::UpstreamTarget target;
    target.host = "127.0.0.1";
    target.port = worker_port;
    // The relay blocks while connecting and reading upstream headers, so it
    // runs with the lock free; the busy count keeps the worker resident.
    lock.unlock();
    HttpResponse response = router::ProxyToWorker(target, req);
    {
      const std::lock_guard<std::mutex> relock(mutex);
      if (response.status == 502 && !response.streaming_body) {
        ReapIfExitedLocked(model_id);
      }
    }
    if (response.streaming_body) {
      // The busy count lives until the streamed body completes or cancels.
      HttpResponse streamed = std::move(response);
      // The front closes after one response, but a stream needs the client
      // socket open until the last chunk so a disconnect can be observed.
      streamed.headers.emplace_back("Connection", "keep-alive");
      streamed.streaming_body = [inner = std::move(streamed.streaming_body),
                                 busy](const HttpResponse::BodyWriter& writer) {
        try {
          inner(writer);
        } catch (...) {
          busy->Release();
          throw;
        }
        busy->Release();
      };
      return streamed;
    }
    busy->Release();
    return response;
  }

  /// One supervision tick: reap dead workers, unload idle ones, free a slot
  /// for queued loads, and kill wedged loads past their deadline.
  void SuperviseOnce() {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto it = workers.begin(); it != workers.end();) {
      int exit_status = 0;
      if (it->second->PollExit(&exit_status)) {
        Logger::Warn("router", "event=worker_exit model=" + it->first +
                                   " status=" + std::to_string(exit_status));
        registry.MarkUnloaded(it->first);
        it = workers.erase(it);
      } else {
        ++it;
      }
    }
    for (const auto& id : registry.DueIdleUnloads()) {
      Logger::Info("router", "event=idle_unload model=" + id);
      StopLocked(id);
    }
    if (registry.HasQueued()) {
      const std::string candidate = registry.EvictionCandidate();
      if (!candidate.empty()) {
        Logger::Info("router", "event=evict model=" + candidate);
        StopLocked(candidate);
      }
    }
    for (const auto& id : registry.DueLoadTimeouts()) {
      Logger::Warn("router", "event=load_timeout_expired model=" + id);
      StopLocked(id);
    }
  }

private:
  using AdmissionResult = std::variant<int, HttpResponse>;

  struct SpawnJob {
    router::WorkerSpec spec;
    std::unique_ptr<router::Worker> result;
    std::string error;
    bool done{false};
    std::mutex done_mutex;
    std::condition_variable done_cv;
  };

  void SpawnThreadMain() {
    for (;;) {
      SpawnJob* job = nullptr;
      {
        std::unique_lock<std::mutex> lock(spawn_mutex_);
        spawn_cv_.wait(
            lock, [this] { return spawn_shutdown_ || pending_ != nullptr; });
        if (spawn_shutdown_) {
          return;
        }
        job = pending_;
        pending_ = nullptr;
      }
      job->result = router::Worker::Spawn(job->spec, exe_path, &job->error);
      {
        const std::lock_guard<std::mutex> done_lock(job->done_mutex);
        job->done = true;
      }
      job->done_cv.notify_all();
    }
  }

  std::thread spawner_;
  std::mutex spawn_mutex_;
  std::condition_variable spawn_cv_;
  SpawnJob* pending_{nullptr};
  bool spawn_shutdown_{false};

  bool LoadedLocked(const std::string& model_id) {
    for (const auto& [id, loaded] : registry.Listing()) {
      if (id == model_id) {
        return loaded;
      }
    }
    return false;
  }

  HttpResponse ModelNotFoundError() {
    std::string ids;
    for (const auto& [id, loaded] : registry.Listing()) {
      (void)loaded;
      if (!ids.empty()) {
        ids += ", ";
      }
      ids += id;
    }
    return Err(404, "Not Found", "unknown model; preset ids: " + ids,
               "invalid_request_error", "model_not_found");
  }

  std::shared_ptr<router::Worker> FindWorkerLocked(const std::string& id) {
    const auto it = workers.find(id);
    return it == workers.end() ? nullptr : it->second;
  }

  /// Stops and unloads `id`. The mutex is released around Stop(); the local
  /// shared_ptr keeps the worker object alive until it is reaped.
  void StopLocked(const std::string& id) {
    auto worker = FindWorkerLocked(id);
    registry.MarkUnloaded(id);
    workers.erase(id);
    if (worker == nullptr) {
      return;
    }
    mutex.unlock();
    worker->Stop();
    mutex.lock();
  }

  /// Marks the model unloaded only when its worker has actually exited; a
  /// live-but-refusing worker keeps its state for the next-request retry.
  void ReapIfExitedLocked(const std::string& id) {
    auto worker = FindWorkerLocked(id);
    if (worker == nullptr) {
      return;
    }
    int exit_status = 0;
    if (worker->PollExit(&exit_status)) {
      Logger::Warn("router", "event=worker_exit model=" + id +
                                 " status=" + std::to_string(exit_status));
      registry.MarkUnloaded(id);
      workers.erase(id);
    }
  }

  /// Arms a fresh spawn with a freshly reserved port. Returns the fatal 502
  /// (port exhaustion / spawn failure) or nullopt once the worker is spawned.
  std::optional<HttpResponse> TrySpawnLocked(const std::string& model_id,
                                             const router::PresetModel& preset,
                                             int attempt) {
    std::string spawn_error;
    const int worker_port = router::ReserveLoopbackPort(&spawn_error);
    if (worker_port < 0) {
      registry.MarkUnloaded(model_id);
      return Err(502, "Bad Gateway", "no loopback port: " + spawn_error,
                 "server_error", "upstream_unavailable");
    }
    registry.MarkLoading(model_id);
    router::WorkerSpec spec;
    spec.model_id = model_id;
    spec.modality = preset.modality;
    spec.args = preset.argv;
    spec.port = worker_port;
    auto worker = SpawnViaSpawnerLocked(spec, &spawn_error);
    if (worker == nullptr) {
      Logger::Warn("router", "event=spawn_failed model=" + model_id +
                                 " attempt=" + std::to_string(attempt) +
                                 " error=" + spawn_error);
      registry.MarkUnloaded(model_id);
      return Err(
          502, "Bad Gateway",
          "failed to spawn worker for '" + model_id + "': " + spawn_error,
          "server_error", "upstream_unavailable");
    }
    Logger::Info("router", "event=spawn model=" + model_id +
                               " port=" + std::to_string(worker_port) +
                               " attempt=" + std::to_string(attempt));
    workers[model_id] = std::move(worker);
    return std::nullopt;
  }

  /// Polls `registry.RequestLoad` until the model is ready, following the
  /// spawn/await/queue decisions. State transitions happen in short critical
  /// sections; the mutex stays free while waiting for worker readiness so
  /// supervision ticks and concurrent waiters are never starved.
  AdmissionResult AdmitLocked(const std::string& model_id,
                              const router::PresetModel& preset) {
    int attempt = 0;
    // The whole held request (queue + spawn + readiness) is bound by
    // load_timeout even if supervision or a spawn retry clears the registry
    // deadline state mid-flight. MarkUnloaded disarms the registry clock, so
    // the front tracks the outer bound itself.
    const auto began = std::chrono::steady_clock::now();
    for (;;) {
      if (options.load_timeout > std::chrono::seconds(0) &&
          std::chrono::steady_clock::now() - began >= options.load_timeout) {
        return LoadTimeoutLocked(model_id);
      }
      std::shared_ptr<router::Worker> worker;
      int port = 0;
      {
        const router::LoadDecision decision = registry.RequestLoad(model_id);
        if (decision == router::LoadDecision::kLoadTimeout) {
          return LoadTimeoutLocked(model_id);
        }
        if (decision == router::LoadDecision::kSpawn) {
          ++attempt;
          if (attempt > kSpawnAttempts) {
            registry.MarkUnloaded(model_id);
            return Err(502, "Bad Gateway",
                       "worker for '" + model_id + "' failed to start",
                       "server_error", "upstream_unavailable");
          }
          if (const auto failure = TrySpawnLocked(model_id, preset, attempt)) {
            return *failure;
          }
        }
        worker = FindWorkerLocked(model_id);
        if (worker == nullptr) {
          // A load is supposedly underway but its worker vanished; the next
          // RequestLoad re-decides.
          port = -1;
        } else {
          port = worker->port();
        }
      }
      if (port < 0) {
        SleepUnlocked();
        continue;
      }
      // One admission iteration against `worker` with the lock released
      // around both the health probe and the poll sleep. The mutex is held
      // at each loop entry and on every exit path.
      bool redecide = false;
      while (!redecide) {
        int exit_status = 0;
        const bool exited = worker->PollExit(&exit_status);
        mutex.unlock();
        const bool healthy = worker->Healthy();
        mutex.lock();
        if (exited) {
          Logger::Warn("router", "event=worker_exit model=" + model_id +
                                     " status=" + std::to_string(exit_status) +
                                     " phase=load");
          registry.MarkUnloaded(model_id);
          workers.erase(model_id);
          redecide = true;
          continue;
        }
        if (healthy) {
          registry.MarkReady(model_id);
          return port;
        }
        if (stop_requested.load()) {
          return Err(502, "Bad Gateway", "router is shutting down",
                     "server_error", "upstream_unavailable");
        }
        const router::LoadDecision recheck = registry.RequestLoad(model_id);
        if (recheck == router::LoadDecision::kLoadTimeout) {
          return LoadTimeoutLocked(model_id);
        }
        SleepUnlocked();
      }
    }
  }

  /// Sleeps for one admission poll with the state mutex released (the caller
  /// must hold it; it is held again on return).
  void SleepUnlocked() {
    mutex.unlock();
    std::this_thread::sleep_for(kAdmissionPoll);
    mutex.lock();
  }

  /// Turns an expired load deadline into the 504 the held request receives,
  /// killing the wedged worker first.
  HttpResponse LoadTimeoutLocked(const std::string& model_id) {
    Logger::Warn("router", "event=load_timeout model=" + model_id);
    auto worker = FindWorkerLocked(model_id);
    registry.MarkUnloaded(model_id);
    workers.erase(model_id);
    if (worker != nullptr) {
      mutex.unlock();
      worker->Stop();
      mutex.lock();
    }
    return Err(504, "Gateway Timeout",
               "model '" + model_id + "' did not load before the timeout",
               "server_error", "load_timeout");
  }
};

}  // namespace

int RunRouter(std::span<const char* const> args) {
  (void)std::setvbuf(stdout, nullptr, _IONBF, 0);
  (void)std::setvbuf(stderr, nullptr, _IONBF, 0);

  std::string host = "127.0.0.1";
  if (const char* env_host = std::getenv("HOST");
      env_host != nullptr && *env_host != '\0') {
    host = env_host;
  } else if (const char* env_host = std::getenv("GUFO_HOST");
             env_host != nullptr && *env_host != '\0') {
    host = env_host;
  }
  int port = 8080;
  if (const auto env_port = ParseEnvPort(std::getenv("PORT"));
      env_port.has_value()) {
    port = *env_port;
  } else if (const auto env_port = ParseEnvPort(std::getenv("GUFO_PORT"));
             env_port.has_value()) {
    port = *env_port;
  }

  std::filesystem::path preset_path;
  std::filesystem::path models_dir;
  std::size_t models_max = 2;
  std::size_t sleep_idle_seconds = 900;
  std::size_t load_timeout_seconds = 600;
  bool autoload = false;
  std::size_t max_connections = 16;
  std::size_t max_request_body_bytes =
      static_cast<std::size_t>(8) * 1024 * 1024;
  std::string api_key;
  ServerLogOptions log_options;

  ArgParser parser("gufo router",
                   "Serve multiple preset models with on-demand loading");
  parser.AddOption("", "--models-preset", "FILE",
                   "Preset file listing the models to multiplex (required)",
                   "Router", &preset_path);
  parser.AddOption("", "--models-dir", "DIR",
                   "Base directory that resolves relative model paths",
                   "Router", &models_dir);
  parser.AddOption("", "--models-max", "N",
                   "Maximum concurrently loaded workers (default: 2; "
                   "0 = unlimited)",
                   "Router", &models_max);
  parser.AddOption("", "--sleep-idle-seconds", "SEC",
                   "Unload a worker idle for this long (default: 900; "
                   "0 disables)",
                   "Router", &sleep_idle_seconds);
  parser.AddOption("", "--load-timeout-seconds", "SEC",
                   "Bound for one full cold load (default: 600; "
                   "0 = unlimited)",
                   "Router", &load_timeout_seconds);
  parser.AddFlag("", "--autoload",
                 "Preload preset models at startup in section order", "Router",
                 &autoload);
  AddServerOptions(parser, &host, &port, nullptr, &max_connections,
                   &max_request_body_bytes, &api_key, &log_options);

  std::string parse_error;
  if (!parser.Parse(args, &parse_error)) {
    std::cerr << "Error: " << parse_error << "\n";
    parser.PrintHelp(std::cerr);
    return 2;
  }
  if (parser.IsHelpRequested()) {
    PrintRouterHelp(parser);
    return 0;
  }
  if (log_options.verbose && log_options.level.has_value()) {
    std::cerr << "Error: -v cannot combine with --log-level\n";
    return 2;
  }
  if (log_options.verbose) {
    Logger::SetLevel(server::LogLevel::kDebug);
  } else if (log_options.level.has_value()) {
    Logger::SetLevel(*log_options.level);
  }

  if (preset_path.empty()) {
    std::cerr << "Error: --models-preset <FILE> is required\n";
    return 2;
  }
  std::vector<router::PresetModel> models;
  std::string preset_error;
  if (!router::LoadPreset(preset_path, models_dir, &models, &preset_error)) {
    std::cerr << "Error: " << preset_error << "\n";
    return 2;
  }

  router::RegistryOptions registry_options;
  registry_options.max_active = models_max;
  registry_options.idle_timeout = std::chrono::seconds(sleep_idle_seconds);
  registry_options.load_timeout = std::chrono::seconds(load_timeout_seconds);
  auto state = std::make_shared<RouterState>(models, registry_options);
  state->exe_path = "/proc/self/exe";

  // --autoload preloads in section order up to --models-max, waiting for each
  // worker's health endpoint within --load-timeout-seconds.
  if (autoload) {
    for (const auto& model : models) {
      if (registry_options.max_active > 0 &&
          state->workers.size() >= registry_options.max_active) {
        break;
      }
      std::string spawn_error;
      const int worker_port = router::ReserveLoopbackPort(&spawn_error);
      if (worker_port < 0) {
        std::cerr << "Error: model '" << model.id << "': " << spawn_error
                  << "\n";
        return 2;
      }
      const router::WorkerSpec spec{.model_id = model.id,
                                    .modality = model.modality,
                                    .args = model.argv,
                                    .port = worker_port};
      auto worker = router::Worker::Spawn(spec, state->exe_path, &spawn_error);
      if (worker == nullptr) {
        std::cerr << "Error: model '" << model.id << "': " << spawn_error
                  << "\n";
        return 2;
      }
      Logger::Info("router", "event=autoload_spawn model=" + model.id +
                                 " port=" + std::to_string(worker_port));
      const auto started = std::chrono::steady_clock::now();
      while (!worker->Healthy()) {
        int exit_status = 0;
        if (worker->PollExit(&exit_status)) {
          std::cerr << "Error: model '" << model.id
                    << "' worker exited with status " << exit_status << "\n";
          return 2;
        }
        if (load_timeout_seconds > 0 &&
            std::chrono::steady_clock::now() - started >=
                std::chrono::seconds(load_timeout_seconds)) {
          std::cerr << "Error: model '" << model.id << "' load timed out\n";
          return 2;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
      }
      state->registry.MarkReady(model.id);
      Logger::Info("router", "event=autoload_ready model=" + model.id);
      state->workers[model.id] = std::move(worker);
    }
  }

  HttpServerOptions options;
  options.max_request_body_bytes = max_request_body_bytes;
  options.max_connections = max_connections;
  options.api_key = api_key;
  options.dispatcher = [state](const HttpRequest& request) {
    return state->Dispatch(request);
  };
  HttpServer server(host, port, nullptr, nullptr, nullptr, nullptr,
                    std::move(options));
  std::string start_error;
  if (!server.start(&start_error)) {
    std::cerr << "Error: " << start_error << "\n";
    return 2;
  }
  state->StartSpawner();
  std::thread supervision([&state] {
    while (!state->stop_requested.load()) {
      state->SuperviseOnce();
      std::this_thread::sleep_for(kSupervisionTick);
    }
  });
  server.run(true);

  // Graceful shutdown: stop accepting, refuse new loads, drain in-flight
  // work for up to 10 s, then terminate every worker.
  state->stop_requested.store(true);
  const auto drain_deadline = std::chrono::steady_clock::now() + kShutdownDrain;
  while (std::chrono::steady_clock::now() < drain_deadline) {
    std::lock_guard<std::mutex> lock(state->mutex);
    bool any_busy = false;
    for (const auto& [id, worker] : state->workers) {
      (void)worker;
      any_busy = any_busy || state->registry.Busy(id);
    }
    if (!any_busy) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  {
    std::vector<std::shared_ptr<router::Worker>> live;
    std::lock_guard<std::mutex> lock(state->mutex);
    for (auto& [id, worker] : state->workers) {
      (void)id;
      live.push_back(std::move(worker));
    }
    state->workers.clear();
    for (auto& worker : live) {
      worker->Stop();
    }
  }
  supervision.join();
  state->StopSpawner();
  return 0;
}

}  // namespace gufo::cli