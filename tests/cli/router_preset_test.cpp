#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "src/cli/serve/router/preset.hpp"

using gufo::router::IsReservedKey;
using gufo::router::LoadPreset;
using gufo::router::PresetModel;

namespace {

std::filesystem::path WriteFixture(const std::string& name,
                                   const std::string& contents) {
  const auto path = std::filesystem::temp_directory_path() / name;
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << contents;
  out.close();
  return path;
}

bool SameModels(const std::vector<PresetModel>& lhs,
                const std::vector<PresetModel>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    if (lhs[i].modality != rhs[i].modality || lhs[i].id != rhs[i].id ||
        lhs[i].argv != rhs[i].argv) {
      return false;
    }
  }
  return true;
}

constexpr const char* kHappyBody =
    "# comment\n"
    "\n"
    "[llm/a]\n"
    "model = rel/x.gguf\n"
    "sessions = 2\n"
    "log-progress = true\n"
    "vite = false\n"
    "\n"
    "[asr/b]\n"
    "model = rel/asr\n";

}  // namespace

void TestHappyPath() {
  const auto file = WriteFixture("gufo_router_preset_happy.conf", kHappyBody);
  std::vector<PresetModel> models;
  std::string error;
  assert(LoadPreset(file, "/models", &models, &error));
  assert(error.empty());
  assert(models.size() == 2);
  const std::vector<std::string> expected_a = {
      "--model",        "/models/rel/x.gguf",  "--sessions", "2",
      "--log-progress", "--served-model-name", "a"};
  assert(models[0].modality == "llm");
  assert(models[0].id == "a");
  assert(models[0].argv == expected_a);
  const std::vector<std::string> expected_b = {"--model", "/models/rel/asr",
                                               "--served-model-name", "b"};
  assert(models[1].modality == "asr");
  assert(models[1].id == "b");
  assert(models[1].argv == expected_b);
}

void TestErrorMentionsSectionAndToken(const char* name, const char* body,
                                      const std::string& section_token,
                                      const std::string& offending_token) {
  const auto file = WriteFixture(name, body);
  std::vector<PresetModel> models;
  std::string error;
  assert(!LoadPreset(file, "/models", &models, &error));
  assert(error.find(section_token) != std::string::npos);
  assert(error.find(offending_token) != std::string::npos);
}

void TestErrors() {
  TestErrorMentionsSectionAndToken("gufo_router_preset_bad_modality.conf",
                                   "[sr/x]\nmodel = m\n", "sr/x", "sr");
  TestErrorMentionsSectionAndToken("gufo_router_preset_dup_id.conf",
                                   "[llm/a]\nmodel = m\n[asr/a]\nmodel = m\n",
                                   "asr/a", "duplicate");
  TestErrorMentionsSectionAndToken("gufo_router_preset_reserved.conf",
                                   "[llm/a]\nport = 1\n", "llm/a", "port");
  TestErrorMentionsSectionAndToken("gufo_router_preset_unknown_key.conf",
                                   "[llm/a]\nmodel = m\nbogus = 1\n", "llm/a",
                                   "bogus");
  TestErrorMentionsSectionAndToken("gufo_router_preset_missing_model.conf",
                                   "[llm/a]\nsessions = 2\n", "llm/a",
                                   "--model");
  TestErrorMentionsSectionAndToken("gufo_router_preset_empty_body.conf",
                                   "[llm/a]\n[llm/b]\nmodel = m\n", "llm/a",
                                   "empty");
  TestErrorMentionsSectionAndToken("gufo_router_preset_no_slash.conf",
                                   "[llmx]\nmodel = m\n", "llmx", "section");
}

void TestCrlfAndBomMatchLf() {
  const auto lf = WriteFixture("gufo_router_preset_lf.conf", kHappyBody);
  std::string crlf = "\xEF\xBB\xBF";
  for (const char* p = kHappyBody; *p != '\0'; ++p) {
    if (*p == '\n') {
      crlf.push_back('\r');
    }
    crlf.push_back(*p);
  }
  const auto crlf_file = WriteFixture("gufo_router_preset_crlf_bom.conf", crlf);
  std::vector<PresetModel> lf_models;
  std::vector<PresetModel> crlf_models;
  std::string error;
  assert(LoadPreset(lf, "/models", &lf_models, &error));
  assert(LoadPreset(crlf_file, "/models", &crlf_models, &error));
  assert(SameModels(lf_models, crlf_models));
}

void TestEmptyModelsDirPassesValuesThrough() {
  const auto file = WriteFixture("gufo_router_preset_happy.conf", kHappyBody);
  std::vector<PresetModel> models;
  std::string error;
  assert(LoadPreset(file, "", &models, &error));
  assert(models.size() == 2);
  assert(models[0].argv.size() >= 2);
  assert(models[0].argv[1] == "rel/x.gguf");
  assert(models[1].argv[1] == "rel/asr");
}

void TestIsReservedKey() {
  assert(IsReservedKey("host"));
  assert(IsReservedKey("port"));
  assert(IsReservedKey("api-key"));
  assert(!IsReservedKey("model"));
  assert(!IsReservedKey("sessions"));
  assert(!IsReservedKey("hostname"));
}

int main() {
  TestHappyPath();
  TestErrors();
  TestCrlfAndBomMatchLf();
  TestEmptyModelsDirPassesValuesThrough();
  TestIsReservedKey();
  std::cout << "All router preset tests passed.\n";
  return 0;
}
