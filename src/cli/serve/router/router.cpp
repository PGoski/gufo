#include "src/cli/serve/router/router.hpp"

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "src/cli/arg_parser.hpp"
#include "src/cli/serve/http_server.hpp"
#include "src/cli/serve/logging.hpp"
#include "src/cli/serve/router/preset.hpp"
#include "src/cli/serve/router/registry.hpp"
#include "src/cli/serve/router/workers.hpp"
#include "src/cli/serve/serve.hpp"
#include "src/cli/serve/serve_options.hpp"
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

HttpResponse RouteFront(const router::Registry& registry,
                        const HttpRequest& req) {
  if (req.method == "GET" && (req.path == "/health" || req.path == "/healthz" ||
                              req.path == "/v1/health")) {
    json::Value body = json::Value::object();
    body["status"] = "ok";
    return {.status = 200, .reason = "OK", .body = body.dump()};
  }
  // Preset validation finished before the listener started, so answering is
  // itself the readiness condition for the front.
  if (req.method == "GET" && (req.path == "/ready" || req.path == "/readyz" ||
                              req.path == "/v1/ready")) {
    json::Value body = json::Value::object();
    body["status"] = "ready";
    return {.status = 200, .reason = "OK", .body = body.dump()};
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
    return {.status = 200, .reason = "OK", .body = resp.dump()};
  }
  json::Value detail = json::Value::object();
  detail["message"] = "no route for this path";
  detail["type"] = "invalid_request_error";
  detail["code"] = "not_found";
  json::Value body = json::Value::object();
  body["error"] = std::move(detail);
  return {.status = 404, .reason = "Not Found", .body = body.dump()};
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
  router::Registry registry(models, registry_options);

  // --autoload preloads in section order up to --models-max, waiting for each
  // worker's health endpoint within --load-timeout-seconds.
  std::vector<std::unique_ptr<router::Worker>> workers;
  if (autoload) {
    const std::filesystem::path exe_path = "/proc/self/exe";
    for (const auto& model : models) {
      if (registry_options.max_active > 0 &&
          workers.size() >= registry_options.max_active) {
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
      auto worker = router::Worker::Spawn(spec, exe_path, &spawn_error);
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
      registry.MarkReady(model.id);
      Logger::Info("router", "event=autoload_ready model=" + model.id);
      workers.push_back(std::move(worker));
    }
  }

  HttpServerOptions options;
  options.max_request_body_bytes = max_request_body_bytes;
  options.max_connections = max_connections;
  options.api_key = api_key;
  options.dispatcher = [&registry](const HttpRequest& request) {
    return RouteFront(registry, request);
  };
  HttpServer server(host, port, nullptr, nullptr, nullptr, nullptr,
                    std::move(options));
  std::string start_error;
  if (!server.start(&start_error)) {
    std::cerr << "Error: " << start_error << "\n";
    return 2;
  }
  server.run(true);
  for (auto& worker : workers) {
    worker->Stop();
  }
  return 0;
}

}  // namespace gufo::cli
