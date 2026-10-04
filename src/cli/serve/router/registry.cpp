#include "src/cli/serve/router/registry.hpp"

#include <chrono>
#include <utility>

namespace gufo::router {

Registry::Registry(std::vector<PresetModel> presets, RegistryOptions options,
                   std::function<Clock::time_point()> now)
    : options_(options), now_(std::move(now)) {
  entries_.reserve(presets.size());
  for (auto& preset : presets) {
    Entry entry;
    entry.preset = std::move(preset);
    entries_.push_back(std::move(entry));
  }
}

std::chrono::steady_clock::time_point Registry::Now() const {
  if (now_) {
    return now_();
  }
  return Clock::now();
}

Registry::Entry* Registry::Lookup(std::string_view model_id) {
  for (auto& entry : entries_) {
    if (entry.preset.id == model_id) {
      return &entry;
    }
  }
  return nullptr;
}

const Registry::Entry* Registry::Lookup(std::string_view model_id) const {
  for (const auto& entry : entries_) {
    if (entry.preset.id == model_id) {
      return &entry;
    }
  }
  return nullptr;
}

bool Registry::BusyEntry(const Entry& entry) {
  return entry.open_requests > 0 || entry.open_sockets > 0 ||
         entry.open_jobs > 0;
}

void Registry::Touch(Entry& entry) {
  entry.last_active = Now();
}

bool Registry::DeadlineExpired(const Entry& entry) const {
  if (options_.load_timeout == std::chrono::seconds(0) ||
      !entry.deadline_armed) {
    return false;
  }
  return Now() > entry.deadline;
}

const PresetModel* Registry::Find(std::string_view model_id) const {
  const Entry* entry = Lookup(model_id);
  return entry == nullptr ? nullptr : &entry->preset;
}

std::vector<std::pair<std::string, bool>> Registry::Listing() const {
  std::vector<std::pair<std::string, bool>> listing;
  listing.reserve(entries_.size());
  for (const auto& entry : entries_) {
    listing.emplace_back(entry.preset.id, entry.state == State::kReady);
  }
  return listing;
}

LoadDecision Registry::RequestLoad(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return LoadDecision::kQueue;
  }
  switch (entry->state) {
    case State::kReady:
      return LoadDecision::kAwaitLoad;
    case State::kLoading:
      return DeadlineExpired(*entry) ? LoadDecision::kLoadTimeout
                                     : LoadDecision::kAwaitLoad;
    case State::kUnloaded:
      break;
  }
  if (entry->deadline_armed && DeadlineExpired(*entry)) {
    return LoadDecision::kLoadTimeout;
  }
  if (!entry->deadline_armed &&
      options_.load_timeout != std::chrono::seconds(0)) {
    entry->deadline_armed = true;
    entry->deadline = Now() + options_.load_timeout;
  }
  if (options_.max_active == 0 || ActiveCount() < options_.max_active) {
    return LoadDecision::kSpawn;
  }
  entry->queued = true;
  return LoadDecision::kQueue;
}

std::string Registry::EvictionCandidate() {
  Entry* best = nullptr;
  for (auto& entry : entries_) {
    if (entry.state == State::kUnloaded || BusyEntry(entry)) {
      continue;
    }
    if (best == nullptr || entry.last_active < best->last_active) {
      best = &entry;
    }
  }
  return best == nullptr ? std::string() : best->preset.id;
}

bool Registry::HasQueued() const {
  for (const auto& entry : entries_) {
    if (entry.queued) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> Registry::DueIdleUnloads() {
  std::vector<std::string> due;
  if (options_.idle_timeout == std::chrono::seconds(0)) {
    return due;
  }
  const Clock::time_point now = Now();
  for (const auto& entry : entries_) {
    if (entry.state != State::kReady || BusyEntry(entry)) {
      continue;
    }
    if (now - entry.last_active >= options_.idle_timeout) {
      due.push_back(entry.preset.id);
    }
  }
  return due;
}

std::vector<std::string> Registry::DueLoadTimeouts() {
  std::vector<std::string> due;
  if (options_.load_timeout == std::chrono::seconds(0)) {
    return due;
  }
  for (const auto& entry : entries_) {
    if (entry.state != State::kReady && DeadlineExpired(entry)) {
      due.push_back(entry.preset.id);
    }
  }
  return due;
}

void Registry::MarkLoading(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr || entry->state == State::kReady) {
    return;
  }
  entry->state = State::kLoading;
  entry->queued = false;
  Touch(*entry);
}

void Registry::MarkReady(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  entry->state = State::kReady;
  entry->deadline_armed = false;
  entry->queued = false;
  Touch(*entry);
}

void Registry::MarkUnloaded(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  entry->state = State::kUnloaded;
  entry->deadline_armed = false;
  entry->queued = false;
  Touch(*entry);
}

void Registry::RequestStarted(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  ++entry->open_requests;
  Touch(*entry);
}

void Registry::RequestFinished(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  if (entry->open_requests > 0) {
    --entry->open_requests;
  }
  Touch(*entry);
}

void Registry::WebSocketOpened(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  ++entry->open_sockets;
  Touch(*entry);
}

void Registry::WebSocketClosed(std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  if (entry->open_sockets > 0) {
    --entry->open_sockets;
  }
  Touch(*entry);
}

void Registry::TrackVideoJob(std::string_view job_id,
                             std::string_view model_id) {
  Entry* entry = Lookup(model_id);
  if (entry == nullptr) {
    return;
  }
  const auto previous = job_owners_.find(std::string(job_id));
  if (previous != job_owners_.end()) {
    Entry* old_owner = Lookup(previous->second);
    if (old_owner != nullptr && old_owner->open_jobs > 0) {
      --old_owner->open_jobs;
    }
    job_owners_.erase(previous);
  }
  job_owners_.emplace(std::string(job_id), std::string(model_id));
  ++entry->open_jobs;
  Touch(*entry);
}

void Registry::CompleteVideoJob(std::string_view job_id) {
  const auto it = job_owners_.find(std::string(job_id));
  if (it == job_owners_.end()) {
    return;
  }
  Entry* entry = Lookup(it->second);
  job_owners_.erase(it);
  if (entry == nullptr) {
    return;
  }
  if (entry->open_jobs > 0) {
    --entry->open_jobs;
  }
  Touch(*entry);
}

std::string Registry::JobOwner(std::string_view job_id) const {
  const auto it = job_owners_.find(std::string(job_id));
  return it == job_owners_.end() ? std::string() : it->second;
}

bool Registry::Busy(std::string_view model_id) const {
  const Entry* entry = Lookup(model_id);
  return entry != nullptr && BusyEntry(*entry);
}

std::size_t Registry::ActiveCount() const {
  std::size_t count = 0;
  for (const auto& entry : entries_) {
    if (entry.state != State::kUnloaded) {
      ++count;
    }
  }
  return count;
}

}  // namespace gufo::router
