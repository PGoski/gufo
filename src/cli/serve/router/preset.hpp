#ifndef GUFO_CLI_SERVE_ROUTER_PRESET_HPP_
#define GUFO_CLI_SERVE_ROUTER_PRESET_HPP_

#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace gufo::router {

struct PresetModel {
  std::string modality;           // "llm"|"video"|"image"|"tts"|"asr"
  std::string id;                 // routing key from the section header
  std::vector<std::string> argv;  // worker args, e.g. {"--model", "/x.gguf"}
};

/// Parse + validate a preset file. `models_dir` (may be empty) resolves
/// relative path values; when empty, values are passed through unchanged.
/// Returns false and sets `error` (section-prefixed, e.g.
/// "[llm/a] Unknown option: --bogus") on any problem.
[[nodiscard]] bool LoadPreset(const std::filesystem::path& file,
                              const std::filesystem::path& models_dir,
                              std::vector<PresetModel>* models,
                              std::string* error);

/// True when `key` is a router-managed option rejected in sections.
[[nodiscard]] bool IsReservedKey(std::string_view key);

}  // namespace gufo::router

#endif  // GUFO_CLI_SERVE_ROUTER_PRESET_HPP_
