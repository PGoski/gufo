#ifndef GUFO_CLI_SERVE_SERVE_OPTIONS_HPP_
#define GUFO_CLI_SERVE_SERVE_OPTIONS_HPP_

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "src/cli/arg_parser.hpp"
#include "src/cli/sampling_options.hpp"
#include "src/cli/serve/logging.hpp"
#include "src/cli/serve/text_generation_scheduler.hpp"
#include "src/cli/serve/text_model_runner.hpp"

namespace gufo::cli {

// Server-level options are accepted on either side of the modality
// subcommand. Registering them from one place keeps `gufo serve --help`, every
// `gufo serve <modality> --help`, and the parser that actually consumes them
// from drifting apart.

/// `--log-level` and its `-v/--verbose` shorthand share one sink. `level`
/// holds the canonical spelling when given and stays empty otherwise; the two
/// spellings together are a conflict, reported where the level is armed.
struct ServerLogOptions {
  bool verbose = false;
  std::optional<server::LogLevel> level;
};

// Verbosity help text. `gufo serve --help` lists these options by hand while
// every modality help registers them through the parser, so both readings share
// one source for the wording.
inline constexpr std::string_view kLogLevelHelp =
    "Log verbosity: error, warn, info or debug (default: info)";
inline constexpr std::string_view kVerboseHelp =
    "Shorthand for --log-level=debug";

/// Help-only sinks for AddServerOptions, so subcommand help can list the
/// server options it shares with `gufo serve` without parsing into them.
struct ServerOptionHelpTargets {
  std::string host = "127.0.0.1";
  int port = 8080;
  std::size_t session_count = 1;
  std::size_t max_connections = 16;
  std::size_t max_request_body_bytes =
      static_cast<std::size_t>(8) * 1024 * 1024;
  std::string api_key;
  ServerLogOptions log;
};

/// Registers the server options shared by every modality. `sessions_or_null`
/// is `nullptr` for the modalities that do not accept `--sessions`.
void AddServerOptions(ArgParser& parser, std::string* host, int* port,
                      std::size_t* sessions_or_null,
                      std::size_t* max_connections,
                      std::size_t* max_request_bytes, std::string* api_key,
                      ServerLogOptions* log);

void AddServerOptionsForHelp(ArgParser& parser,
                             ServerOptionHelpTargets* targets,
                             bool include_sessions = true);

struct SpeechServeOptions {
  std::filesystem::path model;
  std::size_t context;
  std::string served_model_name;
  std::vector<std::pair<std::string, std::string>> voice_specs;
  std::map<std::string, std::string> voice_text_specs;
  std::map<std::string, std::string> voice_lang_specs;
};

struct VideoServeOptions {
  std::filesystem::path model, root, manifest;
  std::uint64_t ttl_seconds;
};

struct ImageServeOptions {
  std::filesystem::path model;
  std::string served_model_name = "Qwen-Image-2.1";
};

struct LlmServeOptions {
  std::string model;
  std::string served_model_name;
  std::uint32_t max_context = 0;
  std::int64_t max_tokens = -1;
  sampling::SamplingConfig sampling_config;
  std::string reasoning_mode = "auto";
  std::string reasoning_effort = "auto";
  std::string preserve_thinking = "auto";
  std::string speculative_backend;
  std::string dflash_model_path;
  std::string draft_policy;
  std::string dspark_model_path;
  std::string mtp_model_path;
  std::string vision_model_path;
  std::size_t draft_tokens = 7;
  std::size_t min_draft_tokens = 1;
  std::size_t prefill_chunk_tokens = server::kDefaultDecodeActivePrefillTokens;
  std::size_t max_pending_requests = 16;
  std::size_t max_pending_requests_per_client = 4;
  std::uint64_t request_timeout_ms = 0;
  std::size_t max_output_bytes = server::kDefaultMaxOutputBytes;
  std::size_t max_buffered_output_bytes =
      server::kDefaultMaxBufferedOutputBytes;
  std::size_t max_buffered_output_bytes_total =
      server::kDefaultMaxBufferedOutputBytesTotal;
  std::size_t cache_ram_bytes = 0;
  std::filesystem::path cache_disk_directory;
  std::size_t cache_disk_bytes =
      server::TextRunnerDiskCacheOptions::kDefaultCapacityBytes;
  std::size_t cache_disk_staging_bytes = 0;
  bool log_progress = false;
};

/// Registers one modality's own options. Defaults that depend on the calling
/// context (`--context 4096|1024` for speech, `--manifest`, `--root`,
/// `--ttl`) are set on the struct by the caller before registering.
void RegisterSpeechServeOptions(ArgParser& parser, bool tts,
                                SpeechServeOptions* options);
void RegisterVideoServeOptions(ArgParser& parser, VideoServeOptions* options);
void RegisterImageServeOptions(ArgParser& parser, ImageServeOptions* options);
void RegisterLlmServeOptions(ArgParser& parser, LlmServeOptions* options);

/// Validate one preset section's argv against a modality's registered long
/// options plus the per-modality required-option rule (--model non-empty).
/// Returns "" when valid; otherwise the parser's own message.
[[nodiscard]] std::string ValidateServeArgv(
    std::string_view modality, const std::vector<std::string>& argv);

}  // namespace gufo::cli

#endif  // GUFO_CLI_SERVE_SERVE_OPTIONS_HPP_
