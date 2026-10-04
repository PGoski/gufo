#include "src/cli/serve/serve_options.hpp"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

using gufo::cli::ValidateServeArgv;

void TestLlmPresetSection() {
  assert(ValidateServeArgv("llm", {"--model", "/tmp/m.gguf"}).empty());
  assert(ValidateServeArgv("llm", {}).find("--model") != std::string::npos);
  assert(ValidateServeArgv("llm", {"--model", "x", "--bogus", "1"})
             .find("bogus") != std::string::npos);
}

void TestSpeechPresetSections() {
  assert(
      ValidateServeArgv("tts", {"--model", "/tmp/d", "--voice", "a=/tmp/w.wav"})
          .empty());
  assert(ValidateServeArgv("asr", {"--model", "/tmp/d", "--voice", "a=b"})
             .find("Unknown option") != std::string::npos);
}

void TestImageAndVideoPresetSections() {
  assert(ValidateServeArgv("image", {"--model", "/tmp/d"}).empty());
  assert(ValidateServeArgv("video", {}).find("--model") != std::string::npos);
}

void TestUnknownModality() {
  assert(
      ValidateServeArgv("embedding", {"--model", "/tmp/d"}).find("embedding") !=
      std::string::npos);
}

int main() {
  TestLlmPresetSection();
  TestSpeechPresetSections();
  TestImageAndVideoPresetSections();
  TestUnknownModality();
  std::cout << "All serve option tests passed.\n";
  return 0;
}
