#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "src/cli/serve/router/preset.hpp"
#include "src/cli/serve/router/registry.hpp"

using gufo::router::LoadDecision;
using gufo::router::PresetModel;
using gufo::router::Registry;
using gufo::router::RegistryOptions;

namespace {

using Clock = Registry::Clock;
using TimePoint = Clock::time_point;
using Seconds = std::chrono::seconds;

std::vector<PresetModel> Models(
    const std::vector<std::pair<std::string, std::string>>& modality_ids) {
  std::vector<PresetModel> presets;
  for (const auto& [modality, id] : modality_ids) {
    PresetModel model;
    model.modality = modality;
    model.id = id;
    model.argv = {"--model", "/models/" + id + ".gguf"};
    presets.push_back(std::move(model));
  }
  return presets;
}

struct FakeClock {
  std::shared_ptr<TimePoint> now = std::make_shared<TimePoint>();
  Registry::Clock::time_point operator()() const { return *now; }
  void Advance(std::int64_t seconds) { *now += std::chrono::seconds(seconds); }
};

bool Contains(const std::vector<std::string>& ids, const std::string& id) {
  for (const auto& value : ids) {
    if (value == id) {
      return true;
    }
  }
  return false;
}

}  // namespace

void TestSpawnAwaitQueue() {
  FakeClock clock;
  RegistryOptions options;
  options.max_active = 2;
  Registry registry(Models({{"llm", "a"}, {"llm", "b"}, {"llm", "c"}}), options,
                    clock);

  assert(registry.RequestLoad("a") == LoadDecision::kSpawn);
  registry.MarkLoading("a");
  assert(registry.ActiveCount() == 1);
  assert(registry.RequestLoad("a") == LoadDecision::kAwaitLoad);
  clock.Advance(1);
  registry.MarkReady("a");
  assert(registry.RequestLoad("a") == LoadDecision::kAwaitLoad);

  clock.Advance(1);
  registry.RequestStarted("a");
  assert(registry.Busy("a"));
  clock.Advance(1);
  registry.RequestFinished("a");
  assert(!registry.Busy("a"));

  clock.Advance(1);
  assert(registry.RequestLoad("b") == LoadDecision::kSpawn);
  registry.MarkLoading("b");
  clock.Advance(1);
  registry.MarkReady("b");
  assert(registry.ActiveCount() == 2);

  clock.Advance(1);
  assert(registry.RequestLoad("c") == LoadDecision::kQueue);
  assert(registry.HasQueued());
  assert(registry.EvictionCandidate() == "a");

  registry.MarkUnloaded("a");
  assert(registry.ActiveCount() == 1);
  assert(registry.RequestLoad("c") == LoadDecision::kSpawn);
  registry.MarkLoading("c");
  assert(!registry.HasQueued());

  auto listing = registry.Listing();
  assert(listing.size() == 3);
  assert(listing[0].first == "a" && !listing[0].second);
  assert(listing[1].first == "b" && listing[1].second);
  assert(listing[2].first == "c" && !listing[2].second);
}

void TestLoadTimeout() {
  FakeClock clock;
  RegistryOptions options;
  options.max_active = 2;
  options.load_timeout = Seconds(600);
  Registry registry(Models({{"llm", "a"}, {"llm", "b"}, {"llm", "c"}}), options,
                    clock);

  assert(registry.RequestLoad("a") == LoadDecision::kSpawn);
  registry.MarkLoading("a");
  registry.MarkReady("a");
  assert(registry.RequestLoad("b") == LoadDecision::kSpawn);
  registry.MarkLoading("b");
  registry.MarkReady("b");
  assert(registry.RequestLoad("c") == LoadDecision::kQueue);

  clock.Advance(601);
  auto due = registry.DueLoadTimeouts();
  assert(due.size() == 1);
  assert(due[0] == "c");
  assert(registry.RequestLoad("c") == LoadDecision::kLoadTimeout);
  assert(registry.RequestLoad("c") == LoadDecision::kLoadTimeout);

  registry.MarkUnloaded("c");
  assert(!Contains(registry.DueLoadTimeouts(), "c"));
  assert(registry.RequestLoad("c") == LoadDecision::kQueue);

  FakeClock wedged_clock;
  Registry wedged(Models({{"llm", "a"}, {"llm", "b"}, {"llm", "c"}}), options,
                  wedged_clock);
  assert(wedged.RequestLoad("a") == LoadDecision::kSpawn);
  wedged.MarkLoading("a");
  wedged.MarkReady("a");
  assert(wedged.RequestLoad("b") == LoadDecision::kSpawn);
  wedged.MarkLoading("b");
  assert(wedged.RequestLoad("c") == LoadDecision::kQueue);
  wedged_clock.Advance(601);
  auto wedged_due = wedged.DueLoadTimeouts();
  assert(wedged_due.size() == 2);
  assert(wedged_due[0] == "b" && wedged_due[1] == "c");
  assert(wedged.RequestLoad("b") == LoadDecision::kLoadTimeout);

  FakeClock forever_clock;
  RegistryOptions forever;
  forever.max_active = 2;
  forever.load_timeout = Seconds(0);
  Registry patient(Models({{"llm", "a"}, {"llm", "b"}, {"llm", "c"}}), forever,
                   forever_clock);
  assert(patient.RequestLoad("a") == LoadDecision::kSpawn);
  patient.MarkLoading("a");
  assert(patient.RequestLoad("b") == LoadDecision::kSpawn);
  patient.MarkLoading("b");
  assert(patient.RequestLoad("c") == LoadDecision::kQueue);
  forever_clock.Advance(1000000);
  assert(patient.RequestLoad("c") == LoadDecision::kQueue);
  assert(patient.DueLoadTimeouts().empty());
}

void TestIdleUnload() {
  FakeClock clock;
  RegistryOptions options;
  options.max_active = 2;
  options.idle_timeout = Seconds(900);
  Registry registry(Models({{"llm", "a"}, {"llm", "b"}}), options, clock);

  assert(registry.RequestLoad("a") == LoadDecision::kSpawn);
  registry.MarkLoading("a");
  registry.MarkReady("a");
  registry.RequestStarted("a");
  clock.Advance(5);
  registry.RequestFinished("a");

  clock.Advance(795);
  assert(registry.DueIdleUnloads().empty());

  clock.Advance(500);
  registry.MarkLoading("b");
  registry.MarkReady("b");

  clock.Advance(106);
  auto due = registry.DueIdleUnloads();
  assert(due.size() == 1);
  assert(due[0] == "a");
}

void TestBusyExemption() {
  FakeClock clock;
  RegistryOptions options;
  options.max_active = 2;
  options.idle_timeout = Seconds(900);
  Registry registry(Models({{"tts", "a"}, {"asr", "b"}}), options, clock);

  registry.MarkLoading("a");
  registry.MarkReady("a");
  registry.MarkLoading("b");
  registry.MarkReady("b");

  registry.WebSocketOpened("a");
  registry.TrackVideoJob("job1", "b");
  assert(registry.Busy("a") && registry.Busy("b"));

  clock.Advance(100000);
  assert(registry.DueIdleUnloads().empty());
  assert(registry.EvictionCandidate().empty());

  registry.RequestStarted("a");
  registry.RequestStarted("a");
  registry.WebSocketClosed("a");
  clock.Advance(1);
  registry.RequestFinished("a");
  assert(registry.Busy("a"));
  registry.RequestFinished("a");
  assert(!registry.Busy("a"));
  clock.Advance(1);
  registry.CompleteVideoJob("job1");
  assert(!registry.Busy("b"));

  clock.Advance(901);
  auto due = registry.DueIdleUnloads();
  assert(due.size() == 2);
  assert(due[0] == "a" && due[1] == "b");
  assert(registry.EvictionCandidate() == "a");
}

void TestLruAcrossModalities() {
  FakeClock clock;
  RegistryOptions options;
  options.max_active = 3;
  Registry registry(Models({{"llm", "l"}, {"tts", "t"}, {"asr", "s"}}), options,
                    clock);

  registry.MarkLoading("l");
  registry.MarkReady("l");
  clock.Advance(1);
  registry.MarkLoading("t");
  registry.MarkReady("t");
  clock.Advance(1);
  registry.MarkLoading("s");
  registry.MarkReady("s");

  clock.Advance(1);
  registry.RequestStarted("l");

  clock.Advance(100);
  assert(registry.EvictionCandidate() == "t");
}

void TestMultiModalityResidency() {
  FakeClock clock;
  RegistryOptions options;
  options.max_active = 6;
  Registry registry(Models({{"asr", "a1"},
                            {"asr", "a2"},
                            {"tts", "t1"},
                            {"tts", "t2"},
                            {"llm", "l1"},
                            {"llm", "l2"}}),
                    options, clock);

  for (const char* id : {"a1", "a2", "t1", "t2", "l1", "l2"}) {
    assert(registry.RequestLoad(id) == LoadDecision::kSpawn);
    registry.MarkLoading(id);
    clock.Advance(1);
    registry.MarkReady(id);
  }
  assert(registry.ActiveCount() == 6);

  auto listing = registry.Listing();
  assert(listing.size() == 6);
  const std::vector<std::string> expected_order = {"a1", "a2", "t1",
                                                   "t2", "l1", "l2"};
  for (std::size_t i = 0; i < listing.size(); ++i) {
    assert(listing[i].first == expected_order[i]);
    assert(listing[i].second);
  }

  registry.RequestStarted("l1");
  registry.RequestStarted("l2");
  registry.RequestStarted("a1");
  registry.RequestStarted("a2");
  clock.Advance(1);
  registry.RequestFinished("l1");

  assert(registry.Busy("a1") && registry.Busy("a2"));
  assert(!registry.Busy("l1"));
  assert(registry.Busy("l2"));
  assert(!registry.Busy("t1") && !registry.Busy("t2"));
  assert(registry.EvictionCandidate() == "t1");

  registry.RequestFinished("l2");
  registry.RequestFinished("a1");
  registry.RequestFinished("a2");
  assert(registry.EvictionCandidate() == "t1");
}

void TestJobOwnerRoundTrip() {
  FakeClock clock;
  RegistryOptions options;
  Registry registry(Models({{"video", "v"}}), options, clock);

  assert(registry.JobOwner("missing").empty());
  registry.MarkLoading("v");
  registry.MarkReady("v");
  registry.TrackVideoJob("job1", "v");
  assert(registry.JobOwner("job1") == "v");
  assert(registry.Busy("v"));
  assert(registry.EvictionCandidate().empty());
  registry.CompleteVideoJob("job1");
  assert(registry.JobOwner("job1").empty());
  assert(!registry.Busy("v"));
  registry.CompleteVideoJob("job1");
  assert(registry.EvictionCandidate() == "v");
}

int main() {
  TestSpawnAwaitQueue();
  TestLoadTimeout();
  TestIdleUnload();
  TestBusyExemption();
  TestLruAcrossModalities();
  TestMultiModalityResidency();
  TestJobOwnerRoundTrip();
  std::cout << "All router registry tests passed.\n";
  return 0;
}
