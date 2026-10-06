#include "src/cli/serve/serve_options.hpp"

#include <functional>
#include <span>

namespace gufo::cli {
namespace {

// Split a `NAME=VALUE` CLI spec. Returns false when either side is empty.
bool SplitNameValue(std::string_view spec, std::string_view flag,
                    std::string* name, std::string* value, std::string* error) {
  const std::size_t separator = spec.find('=');
  if (separator == std::string_view::npos || separator == 0 ||
      separator + 1 >= spec.size()) {
    *error = std::string(flag) + " expects NAME=VALUE";
    return false;
  }
  *name = std::string(spec.substr(0, separator));
  *value = std::string(spec.substr(separator + 1));
  return true;
}

}  // namespace

void AddServerOptions(ArgParser& parser, std::string* host, int* port,
                      std::size_t* sessions_or_null,
                      std::size_t* max_connections,
                      std::size_t* max_request_bytes, std::string* api_key,
                      ServerLogOptions* log) {
  parser.AddOption("-i", "--host", "IP", "Bind address", "Server", host);
  parser.AddOption("-p", "--port", "N", "Port to listen on", "Server", port);
  if (sessions_or_null != nullptr)
    parser.AddOption("-j", "--sessions", "N",
                     "Preallocated GPU request sessions", "Server",
                     sessions_or_null);
  parser.AddOption("", "--max-connections", "N",
                   "Maximum simultaneous HTTP connections", "Server",
                   max_connections);
  parser.AddOption("", "--max-request-bytes", "N",
                   "Maximum HTTP request body bytes", "Server",
                   max_request_bytes);
  parser.AddOption("", "--api-key", "KEY", "API key", "Server", api_key);
  parser.AddCustomOption(
      "", "--log-level", "LEVEL", kLogLevelHelp, "Logging",
      [log](std::string_view flag, std::string_view value, std::string* error) {
        const auto level = server::LogLevelFromName(value);
        if (!level.has_value()) {
          *error = std::string(flag) + " must be error, warn, info or debug";
          return false;
        }
        log->level = *level;
        return true;
      });
  parser.AddFlag("-v", "--verbose", kVerboseHelp, "Logging", &log->verbose);
}

void AddServerOptionsForHelp(ArgParser& parser,
                             ServerOptionHelpTargets* targets,
                             bool include_sessions) {
  AddServerOptions(parser, &targets->host, &targets->port,
                   include_sessions ? &targets->session_count : nullptr,
                   &targets->max_connections, &targets->max_request_body_bytes,
                   &targets->api_key, &targets->log);
}

void RegisterSpeechServeOptions(ArgParser& parser, bool tts,
                                SpeechServeOptions* options) {
  parser.AddOption("-m", "--model", "DIR",
                   tts ? "Qwen3-TTS 12Hz 1.7B model directory"
                       : "Qwen3-ASR 1.7B model directory",
                   "Model", &options->model);
  parser.AddOption("-c", "--context", "N",
                   tts ? "Context capacity (default: 4096)"
                       : "Context capacity per audio chunk (default: 1024)",
                   "Model", &options->context);
  parser.AddOption("", "--served-model-name", "NAME", "Public API model ID",
                   "Model", &options->served_model_name);
  if (!tts)
    return;
  parser.AddCustomOption(
      "", "--voice", "NAME=PATH",
      "Register a named Qwen3-TTS Base voice from a reference WAV "
      "(repeatable)",
      "Model",
      [options](std::string_view, std::string_view value, std::string* err) {
        std::string name;
        std::string path;
        if (!SplitNameValue(value, "--voice", &name, &path, err)) {
          return false;
        }
        options->voice_specs.emplace_back(std::move(name), std::move(path));
        return true;
      });
  parser.AddCustomOption(
      "", "--voice-lang", "NAME=LANGUAGE",
      "Language a --voice speaks; used when a request omits 'language' "
      "(repeatable)",
      "Model",
      [options](std::string_view, std::string_view value, std::string* err) {
        std::string name;
        std::string language;
        if (!SplitNameValue(value, "--voice-lang", &name, &language, err)) {
          return false;
        }
        if (!options->voice_lang_specs
                 .emplace(std::move(name), std::move(language))
                 .second) {
          *err = "duplicate --voice-lang name";
          return false;
        }
        return true;
      });
  parser.AddCustomOption(
      "", "--voice-text", "NAME=TEXT|PATH",
      "Reference transcript for a --voice, given inline or as a file path; "
      "defaults to a .txt sidecar beside the WAV (repeatable)",
      "Model",
      [options](std::string_view, std::string_view value, std::string* err) {
        std::string name;
        std::string text;
        if (!SplitNameValue(value, "--voice-text", &name, &text, err)) {
          return false;
        }
        if (!options->voice_text_specs.emplace(std::move(name), std::move(text))
                 .second) {
          *err = "duplicate --voice-text name";
          return false;
        }
        return true;
      });
}

void RegisterVideoServeOptions(ArgParser& parser, VideoServeOptions* options) {
  parser.AddOption("-m", "--model", "DIR",
                   "Operator-supplied MiniMax H3 directory", "Model",
                   &options->model);
  parser.AddOption(
      "", "--root", "DIR",
      "Storage root for persistent video jobs (default: video-jobs)", "Storage",
      &options->root);
  parser.AddOption("", "--manifest", "PATH", "Pinned H3 manifest override",
                   "Storage", &options->manifest);
  parser.AddOption("", "--ttl", "SEC",
                   "Completed-artifact TTL in seconds (default: 3600)",
                   "Storage", &options->ttl_seconds);
}

void RegisterImageServeOptions(ArgParser& parser, ImageServeOptions* options) {
  parser.AddOption("-m", "--model", "DIR",
                   "Qwen-Image-2.1 safetensors directory", "Model",
                   &options->model);
  parser.AddOption("", "--served-model-name", "NAME", "Public API model ID",
                   "Model", &options->served_model_name);
}

void RegisterLlmServeOptions(ArgParser& parser, LlmServeOptions* options) {
  // Model & Context
  parser.AddOption("-m", "--model", "PATH",
                   "Path to GGUF model file (required)", "Model",
                   &options->model);
  parser.AddOption("", "--mmproj", "PATH",
                   "Qwen BF16 vision sidecar (auto-discovered beside model)",
                   "Model", &options->vision_model_path);
  parser.AddOption("", "--served-model-name", "ID",
                   "Model identifier exposed by the OpenAI API", "Model",
                   &options->served_model_name);
  parser.AddOption(
      "-c", "--context", "N",
      "Context tokens per session (default: 0 = model native context)", "Model",
      &options->max_context);

  // Sampling Defaults
  parser.AddOption(
      "-n", "--max-tokens", "N",
      "Default new-token limit (default: -1 = until EOS or context full)",
      "Sampling Defaults", &options->max_tokens);
  RegisterSamplingOptions(parser, &options->sampling_config,
                          "Sampling Defaults", true, true);

  // Reasoning Defaults
  parser.AddOption("", "--think", "MODE",
                   "Default reasoning mode: on, off, or auto (default: model)",
                   "Reasoning Defaults", &options->reasoning_mode);
  parser.AddOption(
      "", "--reasoning-effort", "LEVEL",
      "Default effort: auto, minimal, low, medium, high, xhigh, or max",
      "Reasoning Defaults", &options->reasoning_effort);
  parser.AddOption("", "--preserve-thinking", "MODE",
                   "Replay prior reasoning: on, off, or auto",
                   "Reasoning Defaults", &options->preserve_thinking);

  // Speculative & Hardware
  parser.AddOption("", "--speculative", "MODE",
                   "HTTP draft backend: dspark, dflash2, mtp, or off",
                   "Speculative", &options->speculative_backend);
  parser.AddOption("", "--dflash-model", "PATH",
                   "Path to Qwen DFlash2 GGUF file", "Speculative",
                   &options->dflash_model_path);
  parser.AddOption(
      "", "--draft-policy", "POLICY",
      "DFlash2 block length: fixed or adaptive (default: adaptive)",
      "Speculative", &options->draft_policy);
  parser.AddOption("", "--dspark-model", "PATH",
                   "Path to DeepSeek V4 Flash DSpark support GGUF file",
                   "Speculative", &options->dspark_model_path);
  parser.AddOption("", "--mtp-model", "PATH",
                   "Path to the Qwen MTP draft GGUF (Qwen3.8-Flash-Next: the "
                   "mtp-...-shared-*.gguf sidecar)",
                   "Speculative", &options->mtp_model_path);
  parser.AddOption(
      "-d", "--draft-tokens", "N",
      "Maximum speculative draft tokens evaluated per step (default: 7)",
      "Speculative", &options->draft_tokens);

  parser.AddOption("", "--min-draft-tokens", "N",
                   "Adaptive draft floor (default: 1)", "Speculative",
                   &options->min_draft_tokens);
  parser.AddOption(
      "", "--prefill-chunk", "N",
      "Maximum prompt tokens between active decode rounds (default: 512)",
      "Scheduling", &options->prefill_chunk_tokens);
  parser.AddOption("", "--max-pending", "N",
                   "Maximum queued generation requests (default: 16)",
                   "Scheduling", &options->max_pending_requests);
  parser.AddOption("", "--max-pending-per-client", "N",
                   "Maximum queued requests per client IP (default: 4)",
                   "Scheduling", &options->max_pending_requests_per_client);
  parser.AddOption("", "--request-timeout-ms", "MS",
                   "Queue plus generation timeout; 0 disables it (default: 0)",
                   "Scheduling", &options->request_timeout_ms);
  parser.AddOption("", "--max-output-bytes", "N",
                   "Maximum generated bytes per request (default: 1048576)",
                   "Scheduling", &options->max_output_bytes);
  parser.AddOption("", "--max-buffered-output-bytes", "N",
                   "Maximum queued stream bytes per request (default: 65536)",
                   "Scheduling", &options->max_buffered_output_bytes);
  parser.AddOption(
      "", "--max-buffered-output-total", "N",
      "Maximum queued stream bytes across requests (default: 262144)",
      "Scheduling", &options->max_buffered_output_bytes_total);
  parser.AddOption(
      "", "--cache-ram-bytes", "N",
      "Retained RAM-cache byte budget (default: 0 = auto: half of free "
      "RAM, at most 32 GiB; explicit values may use free RAM minus 4 GiB)",
      "Cache", &options->cache_ram_bytes);
  parser.AddOption("", "--cache-disk", "DIR",
                   "Opt-in restart-safe continuation cache directory", "Cache",
                   &options->cache_disk_directory);
  parser.AddOption("", "--cache-disk-bytes", "N",
                   "Retained disk-cache byte budget (default: " +
                       std::to_string(options->cache_disk_bytes) + ")",
                   "Cache", &options->cache_disk_bytes);
  parser.AddOption("", "--cache-disk-staging-bytes", "N",
                   "RAM limit for queued snapshots and each disk read "
                   "(default: 0 = auto, at most 1 GiB and 1/8 available RAM)",
                   "Cache", &options->cache_disk_staging_bytes);
  parser.AddFlag("", "--log-progress",
                 "Log live prefill and decode progress (needs "
                 "--log-level=info or debug)",
                 "Logging", &options->log_progress);
}

std::string ValidateServeArgv(std::string_view modality,
                              const std::vector<std::string>& argv) {
  ArgParser parser("gufo serve " + std::string(modality));
  std::string host = "127.0.0.1";
  int port = 8080;
  std::size_t sessions = 1;
  std::size_t max_connections = 16;
  std::size_t max_request_bytes = static_cast<std::size_t>(8) * 1024 * 1024;
  std::string api_key;
  ServerLogOptions log;

  // The parsed `--model` value is read only after `Parse` has armed the sinks.
  // `--model` and `--sessions` follow the modality's own parser: llm and video
  // accept sessions, the other modalities do not.
  const auto run = [&](const std::function<std::string()>& model,
                       std::string_view value_hint) -> std::string {
    std::vector<const char*> args;
    args.reserve(argv.size());
    for (const auto& value : argv) {
      args.push_back(value.c_str());
    }
    std::string error;
    if (!parser.Parse(std::span<const char* const>(args), &error)) {
      return error;
    }
    if (model().empty()) {
      return "--model <" + std::string(value_hint) + "> is required for " +
             std::string(modality) + " server";
    }
    return std::string();
  };

  if (modality == "llm") {
    LlmServeOptions options;
    RegisterLlmServeOptions(parser, &options);
    AddServerOptions(parser, &host, &port, &sessions, &max_connections,
                     &max_request_bytes, &api_key, &log);
    return run([&] { return options.model; }, "PATH");
  }
  if (modality == "video") {
    VideoServeOptions options;
    options.root = "video-jobs";
    options.ttl_seconds = 3600;
    RegisterVideoServeOptions(parser, &options);
    AddServerOptions(parser, &host, &port, &sessions, &max_connections,
                     &max_request_bytes, &api_key, &log);
    return run([&] { return options.model.string(); }, "DIR");
  }
  if (modality == "image") {
    ImageServeOptions options;
    RegisterImageServeOptions(parser, &options);
    AddServerOptions(parser, &host, &port, nullptr, &max_connections,
                     &max_request_bytes, &api_key, &log);
    return run([&] { return options.model.string(); }, "DIR");
  }
  if (modality == "tts" || modality == "asr") {
    const bool tts = modality == "tts";
    SpeechServeOptions options;
    options.context = tts ? 4096U : 1024U;
    RegisterSpeechServeOptions(parser, tts, &options);
    AddServerOptions(parser, &host, &port, nullptr, &max_connections,
                     &max_request_bytes, &api_key, &log);
    return run([&] { return options.model.string(); }, "DIR");
  }
  return "unknown serve modality '" + std::string(modality) + "'";
}

}  // namespace gufo::cli
