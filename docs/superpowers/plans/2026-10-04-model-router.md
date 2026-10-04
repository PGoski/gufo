# Model Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `gufo router` top-level subcommand that serves multiple preset models across modalities through one OpenAI/Anthropic-compatible front door, spawning `gufo serve` workers on demand with `--models-max` admission, idle unload, and a bounded cold-load timeout.

**Architecture:** Gateway design: the router is a front `server::HttpServer` (extended with a request-dispatcher hook) that reverse-proxies HTTP/SSE/WebSocket to loopback `gufo serve <modality>` child processes it spawns and supervises. Preset sections are validated and turned into worker argv by the same `ArgParser` registrations that `gufo serve` uses, extracted into shared code first.

**Tech Stack:** C++20, POSIX sockets/fork/exec (no new dependencies), existing `gufo_http`/`gufo_logging` libraries, CTest, Python 3 stdlib for process-level contract tests.

**Spec:** `docs/superpowers/specs/2026-10-04-model-router-design.md`

## Global Constraints

- C++20, Linux x86-64 only; no new third-party dependencies (POSIX + existing libs only).
- One canonical long option per behavior; no aliases.
- `gufo serve` and its five modality subcommands keep byte-identical help and parse behavior; they are the worker command.
- No Python in the serving path; Python stdlib only in tests.
- Defaults verbatim from the spec: `--models-max 2` (0 = unlimited), `--sleep-idle-seconds 900` (0 disables idle unload), `--load-timeout-seconds 600` (0 = unlimited wait), front server defaults same as `serve` (host `127.0.0.1`, port 8080, env fallbacks `HOST`, `PORT`, `GUFO_HOST`, `GUFO_PORT`).
- Front error codes: 400 `invalid_request_error` missing model, 404 `model_not_found` unknown id, 404 `not_found` unknown path, 409 `model_not_loaded`, 502 `upstream_unavailable` worker died/failed, 504 `load_timeout`.
- Worker env injection only: `GUFO_HOST=127.0.0.1`, `GUFO_PORT=<assigned>`; never forward the client `Authorization` header; workers run keyless.
- Children always die with the router: `PR_SET_PDEATHSIG(SIGKILL)`.
- Before committing C++ changes run: `nix shell --inputs-from . nixpkgs#clang-tools -c python3 tools/ci/check-format.py` (add `--fix` if needed).
- Iteration build (host, no GPU): `cmake --preset cpu-test` once, then `cmake --build --preset cpu-test --target <target>`; run one test with `ctest --test-dir build/cpu-test -R '^<name>$' --output-on-failure`.
- Conventional Commits, single-line message; stage only task-owned paths; never edit CHANGELOG.
- Reserved preset keys rejected with an explicit error: `host`, `port`, `api-key`.

## Review Focus

- Client disconnect mid-SSE/WS stream: the upstream worker connection must close promptly and the model's busy count must drop — pinned in Task 8 (`router_test.py` cancel case) and Task 6 (relay unit test).
- Worker dies mid-relay (after headers were already forwarded): the front stream ends and the model is marked unloaded without crashing the router — pinned in Task 9.
- Preset file with CRLF line endings, UTF-8 BOM, or trailing whitespace on keys: parses identically to clean LF; unknown keys still rejected — pinned in Task 2.
- N concurrent first requests for one unloaded model: exactly one spawn; all requests are held and served — pinned in Task 8.
- Worker answers `/health` 200 but then refuses the proxied request (port stolen, instant crash): front answers 502 `upstream_unavailable`, marks unloaded, next request retries — pinned in Task 8.

---

### Task 1: Shared serve-option registration

Extract per-modality `gufo serve` option registration into shared code so both `serve`, its help output, and (later) the router consume one registration. Behavior-preserving only.

**Files:**
- Create: `src/cli/serve/serve_options.hpp`
- Create: `src/cli/serve/serve_options.cpp`
- Modify: `src/cli/serve/serve.cpp` (remove moved code ~lines 226–514; replace inline registration in `PrintServeHelp` lines 518–721 and in the `RunServe` parse branches for asr/video/llm with calls to the shared functions)
- Modify: `CMakeLists.txt` (add `src/cli/serve/serve_options.cpp` to the `gufo_http` library)

**Interfaces:**
- Consumes: `gufo::cli::ArgParser` (`src/cli/arg_parser.hpp`), `gufo::server::LogLevelFromName` (`src/cli/serve/logging.hpp`), `RegisterSamplingOptions` (`src/cli/sampling_options.hpp`).
- Produces (namespace `gufo::cli`, declared in `src/cli/serve/serve_options.hpp`):

```cpp
struct ServerLogOptions { /* move verbatim from serve.cpp:226 */ };
struct ServerOptionHelpTargets { /* move verbatim */ };

void AddServerOptions(ArgParser& parser, std::string* host, int* port,
                      std::size_t* sessions_or_null,
                      std::size_t* max_connections,
                      std::size_t* max_request_bytes, std::string* api_key,
                      ServerLogOptions* log);                 // serve.cpp:238
void AddServerOptionsForHelp(ArgParser& parser,
                             ServerOptionHelpTargets* targets,
                             bool include_sessions = true);   // serve.cpp:283

struct SpeechServeOptions { /* fields moved from SpeechOptions */ };
struct VideoServeOptions { std::filesystem::path model, root, manifest;
                           std::uint64_t ttl_seconds; };
struct ImageServeOptions { std::filesystem::path model;
                           std::string served_model_name; };
struct LlmServeOptions { /* one field per local in serve.cpp:576-607, same
                            types and default values */ };

void RegisterSpeechServeOptions(ArgParser&, bool tts,
                                SpeechServeOptions*);   // serve.cpp:450 body
void RegisterVideoServeOptions(ArgParser&, VideoServeOptions*);
void RegisterImageServeOptions(ArgParser&, ImageServeOptions*);
void RegisterLlmServeOptions(ArgParser&, LlmServeOptions*);

/// Validate one preset section's argv against a modality's registered long
/// options plus the per-modality required-option rule (--model non-empty).
/// Returns "" when valid; otherwise the parser's own message.
[[nodiscard]] std::string ValidateServeArgv(std::string_view modality,
                                            const std::vector<std::string>& argv);
```

`RegisterXServeOptions` bodies are the current `AddOption`/`AddFlag`/`AddCustomOption` calls moved verbatim, in the exact current order, so help layout and group ordering cannot drift. Caller-side defaults that depend on context (`--context 4096|1024` for speech, `--manifest DefaultH3SourceManifest()`, `--root video-jobs`) stay at the call site: the caller sets struct fields before registering, and `ValidateServeArgv` sets the same context defaults.

- [ ] **Step 1: Capture baseline help text for all five modalities**

Build the current tree, then save the exact help bytes:

```sh
cmake --preset cpu-test && cmake --build --preset cpu-test --target gufo --parallel 4
for m in llm video image tts asr; do
  ./build/cpu-test/gufo serve $m --help > /tmp/router-baseline-$m.txt
done
```

Expected: five non-empty files.

- [ ] **Step 2: Move the registration code**

Create `serve_options.{hpp,cpp}` with the declarations above and the moved bodies. Keep every description string, value hint, group name, short flag, and registration order byte-identical. `DefaultH3SourceManifest()` is not moved with the struct default — `RegisterVideoServeOptions` registers `--manifest` with whatever value `options->manifest` holds at call time, as today.

- [ ] **Step 3: Rewire `serve.cpp` to call the shared functions**

`PrintServeHelp` builds a default-populated options struct per modality (mirroring today's local defaults), calls `RegisterXServeOptions`, then `AddServerOptionsForHelp`, then `PrintHelp`. Each `RunServe` branch replaces its inline registration with the same call on its existing locals grouped in the struct, reading struct fields afterwards. Delete the moved code and the now-duplicated help registrations.

- [ ] **Step 4: Verify help is byte-identical and parse behavior unchanged**

```sh
cmake --build --preset cpu-test --target gufo --parallel 4
for m in llm video image tts asr; do
  ./build/cpu-test/gufo serve $m --help | diff - /tmp/router-baseline-$m.txt
done
ctest --test-dir build/cpu-test -R 'serve_cli_test|gufo_serve_asr_help|gufo_serve_tts_help|strix_serve_help' --output-on-failure
```

Expected: every `diff` empty, every test PASS.

- [ ] **Step 5: Add a unit test pinning `ValidateServeArgv`**

In `tests/cli/arg_parser_test.cpp` (or a new `serve_options_test.cpp` registered in `CMakeLists.txt` like `arg_parser_test`, linking `gufo_http`), assert:

```cpp
assert(ValidateServeArgv("llm", {"--model", "/tmp/m.gguf"}).empty());
assert(ValidateServeArgv("llm", {}).find("--model") != std::string::npos);
assert(ValidateServeArgv("llm", {"--model", "x", "--bogus", "1"})
           .find("bogus") != std::string::npos);
assert(ValidateServeArgv("tts", {"--model", "/tmp/d", "--voice", "a=/tmp/w.wav"}).empty());
assert(ValidateServeArgv("asr", {"--model", "/tmp/d", "--voice", "a=b"})
           .find("Unknown option") != std::string::npos);
assert(ValidateServeArgv("image", {"--model", "/tmp/d"}).empty());
assert(ValidateServeArgv("video", {}).find("--model") != std::string::npos);
```

Run: `cmake --build --preset cpu-test --target serve_options_test && ctest --test-dir build/cpu-test -R '^serve_options_test$' --output-on-failure`. Expected: PASS.

- [ ] **Step 6: Format check and commit**

```sh
nix shell --inputs-from . nixpkgs#clang-tools -c python3 tools/ci/check-format.py
git add src/cli/serve/serve_options.hpp src/cli/serve/serve_options.cpp src/cli/serve/serve.cpp CMakeLists.txt tests/cli/serve_options_test.cpp
git commit -m "refactor(serve): extract per-modality option registration into serve_options"
```

---

### Task 2: Preset file parser

**Files:**
- Create: `src/cli/serve/router/preset.hpp`, `src/cli/serve/router/preset.cpp`
- Create: `tests/cli/router_preset_test.cpp`
- Modify: `CMakeLists.txt` (`gufo_http` sources += the two router files; new `add_executable(router_preset_test ...)` linking `gufo_http`, `add_test(NAME router_preset_test ...)`, `LABELS "cpu;router"`, `TIMEOUT 30`)

**Interfaces:**
- Consumes: `ValidateServeArgv` (Task 1).
- Produces (namespace `gufo::router`, `src/cli/serve/router/preset.hpp`):

```cpp
struct PresetModel {
  std::string modality;  // "llm"|"video"|"image"|"tts"|"asr"
  std::string id;        // routing key from the section header
  std::vector<std::string> argv;  // worker args, e.g. {"--model", "/x.gguf"}
};

/// Parse + validate a preset file. `models_dir` (may be empty) resolves
/// relative path values; when empty, values are passed through unchanged.
/// Returns false and sets `error` (section-prefixed, e.g.
/// "[llm/a] Unknown option --bogus") on any problem.
[[nodiscard]] bool LoadPreset(const std::filesystem::path& file,
                              const std::filesystem::path& models_dir,
                              std::vector<PresetModel>* models,
                              std::string* error);

/// True when `key` is a router-managed option rejected in sections.
[[nodiscard]] bool IsReservedKey(std::string_view key);
```

Grammar (from the spec): full-line `#` comments; blank lines ignored; header `[<modality>/<id>]`; body `key = value`, value runs to end of line (no inline comments); `key = true` → bare `--key` appended, `key = false` → omitted, any other value on a flag-like key is an error from `ValidateServeArgv`; normal keys → `--key`, `value`. Strip a UTF-8 BOM and `\r` line endings; trim spaces around key and around the value's leading edge (a value keeps interior and trailing text as-is). Reject: unknown modality, duplicate `id` across sections, empty section body, reserved keys (`host`, `port`, `api-key` — `IsReservedKey` compares the bare key name), and anything `ValidateServeArgv` rejects for that modality. If the section has no `served-model-name`, append `--served-model-name <id>` to `argv`.

- [ ] **Step 1: Write the failing unit test**

`tests/cli/router_preset_test.cpp` writes preset fixtures to `std::filesystem::temp_directory_path()` and asserts:

```cpp
// Happy path: two sections, comment + blank lines, boolean true/false,
// relative path resolution with models_dir, auto served-model-name.
// "[llm/a]\nmodel = rel/x.gguf\nsessions = 2\nlog-progress = true\nvite = false\n"
// → argv {"--model", "<models_dir>/rel/x.gguf", "--sessions", "2",
//         "--log-progress", "--served-model-name", "a"}
// Errors, each asserting `error` mentions the section and the offending token:
//   unknown modality "[sr/x]"; duplicate id; reserved key "port = 1";
//   unknown key "--bogus" → contains "bogus"; missing required model;
//   empty body; "[llm/x]" header without modality slash.
// CRLF + BOM fixture parses to the same PresetModel list as the LF fixture.
// models_dir empty → relative value passed through unchanged.
```

- [ ] **Step 2: Run to verify failure**

`cmake --build --preset cpu-test --target router_preset_test` → compile error (header missing). That is the failure.

- [ ] **Step 3: Implement `preset.cpp`**

Line loop building the section list; per section run the grammar above then `ValidateServeArgv(modality, argv)` and prefix any returned message with `"[<modality>/<id>] "`.

- [ ] **Step 4: Run to verify pass**

`cmake --build --preset cpu-test --target router_preset_test && ctest --test-dir build/cpu-test -R '^router_preset_test$' --output-on-failure` → PASS.

- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/preset.hpp src/cli/serve/router/preset.cpp tests/cli/router_preset_test.cpp CMakeLists.txt
git commit -m "feat(router): parse and validate multi-model preset files"
```

---

### Task 3: Registry state machine

Pure logic, fake-clock unit tests, no processes or sockets.

**Files:**
- Create: `src/cli/serve/router/registry.hpp`, `src/cli/serve/router/registry.cpp`
- Create: `tests/cli/router_registry_test.cpp`
- Modify: `CMakeLists.txt` (same pattern: `gufo_http` += registry.cpp; `router_registry_test` linking `gufo_http`)

**Interfaces:**
- Consumes: `PresetModel` (Task 2).
- Produces (namespace `gufo::router`, `src/cli/serve/router/registry.hpp`):

```cpp
struct RegistryOptions {
  std::size_t max_active{2};                 // 0 = unlimited
  std::chrono::seconds idle_timeout{900};    // 0 disables idle unload
  std::chrono::seconds load_timeout{600};    // 0 = unlimited wait
};

enum class LoadDecision { kSpawn, kAwaitLoad, kQueue, kLoadTimeout };

class Registry {
public:
  using Clock = std::chrono::steady_clock;
  explicit Registry(std::vector<PresetModel> presets, RegistryOptions options,
                    std::function<Clock::time_point()> now = nullptr);

  [[nodiscard]] const PresetModel* Find(std::string_view model_id) const;
  [[nodiscard]] std::vector<std::pair<std::string, bool>> Listing() const;  // section order, loaded flag

  // Called by the front for every request needing the model. Tracks the
  // caller's deadline on first call: spawn + queue + readiness together may
  // not exceed load_timeout, else returns kLoadTimeout.
  LoadDecision RequestLoad(std::string_view model_id);

  // Supervision-thread queries.
  [[nodiscard]] std::string EvictionCandidate();       // LRU unloadable id, "" if none
  [[nodiscard]] bool HasQueued() const;
  [[nodiscard]] std::vector<std::string> DueIdleUnloads();
  [[nodiscard]] std::vector<std::string> DueLoadTimeouts();  // loading or queued past deadline

  // State transitions driven by the supervisor / proxy.
  void MarkLoading(std::string_view model_id);
  void MarkReady(std::string_view model_id);
  void MarkUnloaded(std::string_view model_id);

  // Activity accounting (all nestable per connection).
  void RequestStarted(std::string_view model_id);
  void RequestFinished(std::string_view model_id);
  void WebSocketOpened(std::string_view model_id);
  void WebSocketClosed(std::string_view model_id);
  void TrackVideoJob(std::string_view job_id, std::string_view model_id);
  void CompleteVideoJob(std::string_view job_id);
  [[nodiscard]] std::string JobOwner(std::string_view job_id) const;  // "" unknown

  [[nodiscard]] bool Busy(std::string_view model_id) const;   // any open activity
  [[nodiscard]] std::size_t ActiveCount() const;              // loading + ready
};
```

Semantics (from the spec): a model is unloadable only when not `Busy` (in-flight HTTP request counted from request start to stream end, open WebSocket, or unfinished tracked video job). `EvictionCandidate` picks the least-recently-active unloadable model among ready+loading; video-job owners are never candidates. The idle timer starts at the later of last request start and last request end (`RequestFinished`/`WebSocketClosed`). `RequestLoad` decision per call: ready → `kAwaitLoad` (already satisfied; the caller's poll loop proceeds once state is ready); loading → `kAwaitLoad`; unloaded with a free slot → `kSpawn`, and the caller must `MarkLoading` before spawning; unloaded with all slots used → `kQueue` until the supervision thread evicts (via `EvictionCandidate`) and frees a slot, then `kSpawn`; past the deadline armed on the first `RequestLoad` for that id → `kLoadTimeout`, and the entry is cleared by `MarkUnloaded`. The deadline covers queue plus spawn plus readiness together (spec `--load-timeout-seconds`); `DueLoadTimeouts()` reports still-loading ids past deadline so the supervisor can kill a wedged worker.

- [ ] **Step 1: Write the failing unit tests**

`tests/cli/router_registry_test.cpp` with a `std::shared_ptr<Clock::time_point>` fake clock, covering:

```cpp
// 1. Spawn/await/queue: max_active 2; A,B spawn (kSpawn twice); C → kQueue;
//    A.RequestStarted/Finished, then A unloadable → next C RequestLoad = kSpawn.
// 2. Load timeout: queued C; advance clock 601 s → kLoadTimeout;
//    with load_timeout 0 → still kQueue forever.
// 3. Idle: RequestStarted at t0, RequestFinished t0+5; idle_timeout 900 →
//    DueIdleUnloads empty at t0+800, contains id at t0+906.
// 4. Busy exemption: open WebSocket or tracked video job → model absent from
//    DueIdleUnloads and from EvictionCandidate.
// 5. LRU across modalities: three ready workers (llm busy streaming, tts
//    idle-oldest, asr idle-newer) → EvictionCandidate = tts id.
// 6. Simultaneous multi-modality residency: presets asr×2 + tts×2 + llm×2,
//    max_active 6: all reach ready; busy counts stay independent per id;
//    Listing() reports loaded=true for exactly the resident ids in section
//    order; completing one llm request while both asr stream keeps tts
//    unloadable but not the streaming llm.
// 7. JobOwner round-trip: TrackVideoJob/CompleteVideoJob/JobOwner("").
```

- [ ] **Step 2: Run to verify failure** — build target fails (missing symbols). Expected.
- [ ] **Step 3: Implement `registry.cpp`** — one entry per preset: state enum, busy counter, last-active timestamp, queue/deadline flags; `JobOwner` a `std::map<std::string, std::string>`.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build/cpu-test -R '^router_registry_test$' --output-on-failure` → PASS.
- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/registry.hpp src/cli/serve/router/registry.cpp tests/cli/router_registry_test.cpp CMakeLists.txt
git commit -m "feat(router): registry admission, LRU eviction and idle state machine"
```

---

### Task 4: Upstream HTTP proxy relay

**Files:**
- Create: `src/cli/serve/router/proxy.hpp`, `src/cli/serve/router/proxy.cpp`
- Create: `tests/cli/router_proxy_test.cpp`
- Modify: `CMakeLists.txt` (same pattern, links `gufo_http`)

**Interfaces:**
- Consumes: `server::HttpRequest`, `server::HttpResponse` (`src/cli/serve/http_server.hpp`).
- Produces (namespace `gufo::router`, `src/cli/serve/router/proxy.hpp`):

```cpp
struct UpstreamTarget { std::string host{"127.0.0.1"}; int port{0}; };

/// Forward one buffered front request to the worker and produce a front
/// response. The returned response streams the worker body through
/// `HttpResponse::streaming_body`, checking `request.is_cancelled` between
/// chunks. Connect failure / EOF before headers → 502 `upstream_unavailable`
/// JSON error. Hop-by-hop headers (Connection, Keep-Alive, Transfer-Encoding,
/// Upgrade) are stripped both ways; `Authorization`, `Host`, `Connection`
/// are not forwarded.
gufo::server::HttpResponse ProxyToWorker(const UpstreamTarget& target,
                                         const gufo::server::HttpRequest& request);
```

Implementation shape: blocking POSIX socket; send request line `METHOD path?query HTTP/1.1`, headers, body; parse status + headers; if `Content-Length` read exactly; if `Transfer-Encoding: chunked` decode chunks; each chunk goes to the `streaming_body` writer (for SSE this preserves event boundaries as they arrive). Worker responses without either framing are read to connection close.

- [ ] **Step 1: Write the failing test**

`tests/cli/router_proxy_test.cpp` runs an in-process fake worker on a raw socket thread (pattern copied from `tests/cli/http_server_test.cpp` socket helpers) asserting:

```cpp
// echo: POST /v1/x?a=1 body "hi" → fake replies 200 + body "echo:hi"; front
//   sees status 200 and exact body; Host header seen by fake is not "127.0.0.1:<client>",
//   no Authorization header reaches the fake.
// sse: fake replies chunked "data: 1\n\ndata: 2\n\ndata: [DONE]\n\n";
//   collected streaming chunks equal those bytes in order.
// connect refused (closed port) → status 502, body contains "upstream_unavailable".
// cancel: after headers, fake blocks; set is_cancelled → ProxyToWorker
//   returns (streaming ends) within 200 ms and fake observes the upstream
//   connection closed.
// worker EOF mid-body (fake closes after half the bytes) → streaming ends,
//   no crash, no exception escaping.
```

- [ ] **Step 2: Verify it fails to build** — expected.
- [ ] **Step 3: Implement `proxy.cpp`.**
- [ ] **Step 4: Run to verify pass** — `ctest -R '^router_proxy_test$'` PASS.
- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/proxy.hpp src/cli/serve/router/proxy.cpp tests/cli/router_proxy_test.cpp CMakeLists.txt
git commit -m "feat(router): upstream HTTP relay with SSE passthrough and cancellation"
```

---

### Task 5: Worker supervisor

**Files:**
- Create: `src/cli/serve/router/workers.hpp`, `src/cli/serve/router/workers.cpp`
- Create: `tests/cli/router_workers_test.cpp`
- Modify: `CMakeLists.txt` (same pattern)

**Interfaces:**
- Consumes: `PresetModel` (Task 2), `server::Logger` (`src/cli/serve/logging.hpp`).
- Produces (namespace `gufo::router`, `src/cli/serve/router/workers.hpp`):

```cpp
struct WorkerSpec {
  std::string model_id;
  std::string modality;                      // appended after "serve"
  std::vector<std::string> args;             // preset argv
  int port;                                  // via GUFO_HOST/GUFO_PORT env
};

class Worker {
public:
  /// fork()+execv(exe_path) with argv {exe, "serve", modality, args...};
  /// PR_SET_PDEATHSIG(SIGKILL); stdout/stderr pipes pumped line-wise into
  /// Logger::Info("router", "model=<id> <line>"). nullptr + `error` on fork
  /// or exec failure (exec failure detected via pipe, no false success).
  static std::unique_ptr<Worker> Spawn(const WorkerSpec& spec,
                                       const std::filesystem::path& exe_path,
                                       std::string* error);
  [[nodiscard]] int port() const;
  [[nodiscard]] const std::string& model_id() const;
  [[nodiscard]] bool Healthy() const;   // GET /health, 300 ms timeout, 200
  /// Non-blocking waitpid(WNOHANG); true + status when reaped.
  bool PollExit(int* exit_status);
  /// SIGTERM, wait grace, SIGKILL; idempotent; reaps and joins pump threads.
  void Stop(std::chrono::seconds grace = std::chrono::seconds(10));
  ~Worker();  // Stop(0s) when still running
};

/// bind(127.0.0.1:0) → getsockname → close. Returns -1 + error.
int ReserveLoopbackPort(std::string* error);
```

- [ ] **Step 1: Write the failing test**

```cpp
// ReserveLoopbackPort returns a port in [1024,65535]; two calls differ.
// Spawn with exe "/bin/sh", modality ignored (test passes args that override:
// use spec.args and set exe to a script? -> instead: exe_path may be any
// binary; the test uses exe_path="/bin/sh" with spec.modality/args crafted
// as {"-c", "..."} is NOT possible because argv is serve+modality first — so
// Spawn takes an additional hidden override: when the file named by env
// GUFO_ROUTER_WORKER_EXE exists it replaces exe_path AND argv after "serve"
// is skipped: argv becomes {exe, args...}. Test sets that env var to a
// python3 script that sleeps / exits / prints.
// - print test: worker script writes "event=ready_probe\n" → Logger capture
//   (reroute Logger or grep the log file Logger writes) contains
//   "model=test-1 event=ready_probe".
// - PollExit: script exits 3 → PollExit true, exit status 3, within 2 s.
// - Stop: script traps SIGTERM ignores; Stop(grace=1s) still leaves no live
//   pid after Stop returns (kill -0 fails).
// - exec failure: exe_path=/nonexistent → Spawn returns nullptr, error
//   non-empty, within 500 ms.
```

- [ ] **Step 2: Verify it fails** — expected build failure.
- [ ] **Step 3: Implement `workers.cpp`** — `execv` in the child with a copied `environ` plus `GUFO_HOST=127.0.0.1` and `GUFO_PORT=<port>`; parent sets `PR_SET_PDEATHSIG` before `execv`; two pump threads read pipes with `poll()`, line-buffer, log, EOF-tolerant. Apply the `GUFO_ROUTER_WORKER_EXE` override exactly as described (it is the contract-test seam; document it in the header comment as test-only).
- [ ] **Step 4: Run to verify pass** — `ctest -R '^router_workers_test$'` PASS.
- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/workers.hpp src/cli/serve/router/workers.cpp tests/cli/router_workers_test.cpp CMakeLists.txt
git commit -m "feat(router): worker process spawn, health probe and supervised stop"
```

---

### Task 6: WebSocket relay

**Files:**
- Create: `src/cli/serve/router/ws_relay.hpp`, `src/cli/serve/router/ws_relay.cpp`
- Create: `tests/cli/router_ws_relay_test.cpp`
- Modify: `CMakeLists.txt` (same pattern)

**Interfaces:**
- Consumes: `server::WebSocket` + `IsWebSocketUpgrade` (`src/cli/serve/websocket.hpp`), `UpstreamTarget` (Task 4). Gufo's `WebSocket::Send` does not mask (server-side); client→worker frames that are already masked pass through valid untouched, so the relay is a raw byte pump after the handshake — no re-masking anywhere.
- Produces (namespace `gufo::router`, `src/cli/serve/router/ws_relay.hpp`):

```cpp
/// Perform the RFC 6455 client handshake against the worker (fresh
/// Sec-WebSocket-Key, require 101 + correct Sec-WebSocket-Accept computed via
/// the SHA1 already vendored under src/core/crypto), then pump raw bytes in
/// both directions until either side closes or front.cancelled(). Any
/// handshake failure → returns false without throwing.
bool RelayWebSocket(const UpstreamTarget& target, gufo::server::WebSocket& front,
                    std::string* error);
```

- [ ] **Step 1: Write the failing test**

In-process: a fake worker thread accepts a socket, completes the WS handshake in server role (hash the client key with the same SHA1 helper), and echoes each received frame verbatim. The front side is a `server::WebSocket` constructed over one end of a `socketpair`; the test peer on the other end sends masked client frames exactly as `HttpServer` would deliver them to the `websocket` callback. Assert: a text frame sent by the front peer arrives echoed back; a close frame from the worker ends the relay (return) within 200 ms; a worker that answers the handshake with a wrong `Sec-WebSocket-Accept` makes `RelayWebSocket` return false with a non-empty error and no hang.

- [ ] **Step 2: Verify it fails** — expected build failure.
- [ ] **Step 3: Implement `ws_relay.cpp`** — `poll()` loop over the two fds, `read`/`write` raw bytes (no frame parsing), symmetric; on `POLLHUP`/error from either side, `front.MarkClosed()` and return true.
- [ ] **Step 4: Run to verify pass** — `ctest -R '^router_ws_relay_test$'` PASS.
- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/ws_relay.hpp src/cli/serve/router/ws_relay.cpp tests/cli/router_ws_relay_test.cpp CMakeLists.txt
git commit -m "feat(router): websocket handshake and raw frame relay to workers"
```

---

### Task 7: Dispatcher hook and `gufo router` front

**Files:**
- Modify: `src/cli/serve/http_server.hpp` (add field to `HttpServerOptions`), `src/cli/serve/http_server.cpp` (`handle_request` around line 1531; constructor around line 1305)
- Create: `src/cli/serve/router/router.hpp`, `src/cli/serve/router/router.cpp`
- Modify: `src/cli/main.cpp` (dispatch + help line), `CMakeLists.txt` (gufo_http sources += router.cpp; ctest `gufo_router_help` contract like line 343 pattern)

**Interfaces:**
- Consumes: everything from Tasks 1–3 (preset, registry, worker spawn for `--autoload`), `PrintServeHelp` (`src/cli/serve/serve.hpp`).
- Produces:

```cpp
// http_server.hpp HttpServerOptions addition:
/// When set, handle_request returns dispatcher(request) immediately after
/// authentication; built-in health/model routes are skipped and the
/// constructor does not register model routes (null backend is then legal).
std::function<HttpResponse(const HttpRequest&)> dispatcher{};

// router.hpp
namespace gufo::cli {
int RunRouter(std::span<const char* const> args);
}
```

`RunRouter` this task: parse router options (`--models-preset` required, `--models-dir`, `--models-max`, `--sleep-idle-seconds`, `--load-timeout-seconds`, `--autoload`, `--host/--port/--api-key/--max-connections/--max-request-bytes/--log-level/-v` via `AddServerOptions`); `LoadPreset` (abort exit 2 with its error); build `Registry`; `--autoload` spawns workers in section order until `--models-max`; front `HttpServer` with `dispatcher` set to a member that answers the router-managed paths only: `GET /health|/healthz|/v1/health` → 200 `{"status":"ok"}`; `GET /ready*` → 200 `{"status":"ready"}` (preset validation runs before the listener starts, so both spec conditions hold whenever the dispatcher answers); `GET /v1/models|/models` → OpenAI list from `Registry::Listing()` with each entry `{"id", "object":"model", "owned_by":"gufo", "loaded": <bool>}`; everything else → 404 `{"error":{...,"code":"not_found"}}` shaped like serve's `Err`. `gufo router help` prints the preset grammar block then `PrintServeHelp("gufo", m)` for all five modalities. SIGINT/SIGTERM → stop accepting, `Stop()` all live workers, exit 0.

- [ ] **Step 1: Add the dispatcher hook with a failing http_server_test case**

In `tests/cli/http_server_test.cpp`: construct `HttpServer` with null backend, `options.dispatcher = [](const HttpRequest& r){ return response with body "dispatched" + r.path; }`; raw-socket GET `/anything` → 200 body `dispatched/anything`; with `api_key` set, missing bearer still 401 before the dispatcher runs. Run `ctest -R '^http_server_test$'` → FAIL (field missing), implement the two-line hook (`if (options_.dispatcher) return options_.dispatcher(req);` after the `IsAuthorized` check; guard `register_routes()` behind `!options_.dispatcher`), rerun → PASS. Also rerun the full existing `http_server_test` to prove no regression for backend servers.

- [ ] **Step 2: Write the failing CLI contract additions**

Extend `tests/cli/serve_test.py` with (run() helper already present):

```python
check(["router"], 2, "--models-preset")                     # missing preset
check(["router", "--models-preset", "/nonexistent.preset"], 2, "nonexistent")
check(["router", "--help"], 0, "[llm/<model-id>]")          # grammar shown
check(["help", "router"], 0, "models-max")
out = check(["--help"], 0, "router")                         # main listing
# bad preset aborts before binding: file with "[llm/a]" only (missing model)
check(["router", "--models-preset", bad_file], 2, "[llm/a]")
```

Run `ctest --test-dir build/cpu-test -R '^serve_cli_test$'` → FAIL.

- [ ] **Step 3: Implement `router.cpp` front + main.cpp wiring**

Add the `router` subcommand to `main.cpp` dispatch and the `Commands:` help block (`  router         Serve multiple preset models with on-demand loading`), plus `gufo help router` topic routing. Use the worker exe override from Task 5 (`/proc/self/exe` default). Front `HttpServer` gets `backend = nullptr` and the dispatcher described above.

- [ ] **Step 4: Verify**

`cmake --build --preset cpu-test --target gufo --parallel 4 && ctest --test-dir build/cpu-test -R '^serve_cli_test$|^gufo_help$|^gufo_router_help$' --output-on-failure` → PASS. Then a live probe: `./build/cpu-test/gufo router --models-preset /tmp/two-sections.preset --port 18081 &` → `curl /health` 200, `curl /v1/models` lists both ids with `"loaded": false` (or true for autoloaded), kill clean.

- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/http_server.hpp src/cli/serve/http_server.cpp tests/cli/http_server_test.cpp src/cli/serve/router/router.hpp src/cli/serve/router/router.cpp src/cli/main.cpp CMakeLists.txt tests/cli/serve_test.py
git commit -m "feat(router): gufo router front door with health, ready and model listing"
```

---

### Task 8: Request routing, admission and proxy dispatch

**Files:**
- Modify: `src/cli/serve/router/router.cpp`
- Create: `tests/cli/router_test.py`
- Modify: `CMakeLists.txt` (`add_test(NAME router_contract_test COMMAND ${Python3_EXECUTABLE} tests/cli/router_test.py $<TARGET_FILE:gufo>)`, `LABELS "cpu;server;router"`, `TIMEOUT 120`)

**Interfaces:**
- Consumes: `ProxyToWorker` (Task 4), `Registry` API exactly as Task 3, `Worker::Spawn`/`Healthy`/`PollExit` (Task 5), `json::Value` (`src/core/json.hpp`), `gufo::server::IsVideoApiPath` (`src/cli/serve/video_api.hpp`).
- Produces: the completed dispatcher (model resolution table from the spec), plus a reusable fake worker in the Python test.

Dispatch resolution inside the router dispatcher:
- `POST /v1/chat/completions|/v1/completions|/v1/responses|/v1/messages|/v1/images/generations|/v1/images/edits|/v1/audio/speech|/v1/video/generations` (use `IsVideoApiPath` for the video family) → `model` string from the JSON body; missing → 400 `invalid_request_error` "missing 'model' field".
- `POST /v1/audio/transcriptions` (multipart) → `name="model"` form field.
- `GET /v1/realtime`, `GET /v1/audio/speech/stream` → `?model=` query param.
- `GET/DELETE` under `IsVideoApiPath` with a recorded job id → route to `Registry::JobOwner(job)`; job id extraction must follow the URL shape `video_api.cpp` actually serves (read it; the front recorded ids by parsing the POST response JSON `id` field and called `TrackVideoJob`).
- Unknown id → 404 `model_not_found` whose message lists preset ids.
- `GET /v1/slots`, `GET /v1/metrics` (and `/slots`,`/metrics`) → require `?model=`; if loaded, `ProxyToWorker`; else 409 `model_not_loaded`.

Admission loop per request (front thread): poll `registry.RequestLoad(id)` every 50 ms: `kAwaitLoad` → poll `worker.Healthy()`; when registry flipped ready, proceed. `kQueue` → keep waiting (supervision thread runs `EvictionCandidate()` + `Worker::Stop()` when `HasQueued()`). `kLoadTimeout` → 504 `{"error":{"type":"server_error","code":"load_timeout"}}`. Spawn path: `MarkLoading`, `Worker::Spawn`, then wait healthy under the same deadline; if the worker reports bind failure (`EADDRINUSE` startup crash or never turns healthy because the port got stolen), respawn with a fresh `ReserveLoopbackPort()` up to 3 attempts before failing 502 + `MarkUnloaded` (spec: retry spawn up to 3 times). `PollExit` during load → 502 + `MarkUnloaded`. Count activity: `RequestStarted` before proxying, `RequestFinished` in a scope guard when the streamed response completes or cancels. A supervision thread (250 ms tick) runs `DueIdleUnloads()` → drain-then-`Stop()`, then `DueLoadTimeouts()` → kill wedged loading workers + `MarkUnloaded`.

- [ ] **Step 1: Write the failing process-level contract test (part 1)**

`tests/cli/router_test.py` defines `FakeWorker` — a Python stdlib `http.server.ThreadingHTTPServer` on a reserved port that answers `/health` 200 after `GUFO_FAKE_READY_DELAY` seconds, `/v1/chat/completions` (and siblings) with JSON echoing its id from `argv`, chunked `/v1/chat/completions?stream=true` SSE, and `/v1/slots`. It is used via `GUFO_ROUTER_WORKER_EXE` (Task 5 seam) — the fake ignores `serve <modality>` argv and reads its id from `--served-model-name`. Cases:

```python
# cold load held: request for unloaded model waits (fake ready delay 1 s)
#   and returns 200, exactly one worker process spawned
# concurrent first requests x5 → one spawn, all five succeed
# model resolution: chat JSON model, transcriptions multipart model field,
#   ?model= query — each reaches the right fake id
# missing model field → 400 invalid_request_error; unknown id → 404 lists ids
# saturated queue: max 2, worker A held busy (slow SSE); request for C waits,
#   then completes after A's stream ends
# load timeout: ready delay 5 s, --load-timeout-seconds 1 → 504 load_timeout,
#   worker killed, next request retries and succeeds
# disconnect mid-SSE: kill the client socket → busy count drops (next
#   saturation request now admits without waiting for a full stream)
# connect-refused after health (fake exits right after ready probe) → 502
#   upstream_unavailable, reload on next request
```

- [ ] **Step 2: Run to verify failure** — `ctest --test-dir build/cpu-test -R '^router_contract_test$'` fails on the first waiting case (dispatcher still 404s).
- [ ] **Step 3: Implement the dispatch + admission wiring in `router.cpp`** as specified above.
- [ ] **Step 4: Run to verify pass** — `router_contract_test` PASS; rerun `serve_cli_test`. The test also records wall-clock latency of the cold-start request and the following hot request for the same model and prints both (`event=router_timing cold_ms=... hot_ms=...`) — recorded and retained per AGENTS.md per-request timing rules, never asserted against absolute values.
- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/router.cpp tests/cli/router_test.py CMakeLists.txt
git commit -m "feat(router): request routing, held loads, admission queue and timeouts"
```

---

### Task 9: WebSocket, video jobs and lifecycle contract

**Files:**
- Modify: `src/cli/serve/router/router.cpp`
- Modify: `tests/cli/router_test.py` (part 2)

**Interfaces:**
- Consumes: `RelayWebSocket` (Task 6), `Registry::WebSocketOpened/Closed`, `TrackVideoJob`/`JobOwner` (Task 3), `Worker::Stop`.
- Produces: complete feature parity with the spec's "Routing and proxying" section.

WS: `IsWebSocketUpgrade(request)` paths resolve the model (query param), run the admission loop, then return an `HttpResponse` whose `websocket` callback calls `RelayWebSocket(target, front, &err)` wrapped in `WebSocketOpened/Closed` guards. Video POST: after proxying a `POST` under `IsVideoApiPath`, parse the buffered response JSON, and on 2xx `TrackVideoJob(id, model)`; the video job exempts its worker from eviction and idle unload until the job reaches a terminal state (front re-reads `GET <job path>` responses: terminal status field per `video_api.cpp` → `CompleteVideoJob`). Job read for a dead worker: reload that model first, then proxy (worker restores state from `--root`). Shutdown: SIGTERM → stop accepting, refuse new loads, wait up to 10 s for in-flight, `Stop()` workers (10 s grace), exit 0.

- [ ] **Step 1: Write the failing contract additions**

Extend `tests/cli/router_test.py` (fake worker gains a minimal WS echo endpoint):

```python
# ws relay: connect /v1/realtime?model=<id>, send text, receive echo,
#   close cleanly; busy count released (saturation check after close)
# ws keeps worker eviction-exempt while open (max 2, open ws, third model
#   waits until ws closes)
# worker crash mid-SSE → client stream ends, router alive, model unloaded,
#   next request reloads (log grep "model=<id>" prefixed lines present)
# idle unload: --sleep-idle-seconds 1, one hot request, worker pid gone
#   within 3 s, /v1/models loaded=false
# eviction: 3-section max 2 preset, idle A → request B then C evicts A
# PDEATHSIG: router killed -9, worker pid dead within 2 s
# graceful shutdown: SIGTERM during in-flight SSE → stream completes,
#   exit code 0, no surviving workers
```

- [ ] **Step 2: Verify failure** — WS case fails (dispatcher returns 404 for upgrades).
- [ ] **Step 3: Implement** the wiring above in `router.cpp`.
- [ ] **Step 4: Run to verify pass** — `ctest --test-dir build/cpu-test -R '^router_contract_test$'` PASS.
- [ ] **Step 5: Format, commit**

```sh
git add src/cli/serve/router/router.cpp tests/cli/router_test.py
git commit -m "feat(router): websocket relay, video job tracking and lifecycle rules"
```

---

### Task 10: Docs, GPU smoke and full checks

**Files:**
- Modify: `docs/SERVER.md`, `docs/CLI.md`, `README.md`
- Create: `tests/cli/router_gpu_smoke.py`
- Modify: `CMakeLists.txt` (register `router_gpu_smoke` with `LABELS "gpu"` so hosted CPU CI skips it, following how existing gpu-labeled tests are registered)

- [ ] **Step 1: Write the docs**

`docs/SERVER.md`: "Model router" section — subcommand synopsis with the spec's exact defaults, preset grammar with the spec's example, lifecycle table mirroring the spec's error-handling summary. `docs/CLI.md`: `gufo router` entry with every option and default. `README.md`: one feature bullet under the serving list. No CHANGELOG edit.

- [ ] **Step 2: GPU smoke test (runs only under gpu-test)**

`tests/cli/router_gpu_smoke.py <gufo-binary>`: build a 2-section preset (tts + asr, small dirs resolved from the model paths `docs/SERVER.md` documents for those modalities; skip with a loud "SKIPPED: model dir missing" and exit 0 like existing model-dependent tests), start `gufo router --models-max 2`, one TTS request then one ASR request, assert both 200 and `/v1/models` loaded flags, idle-unload both with `--sleep-idle-seconds 2`, assert worker pids gone.

- [ ] **Step 3: Run the full affected checks**

```sh
nix shell --inputs-from . nixpkgs#clang-tools -c python3 tools/ci/check-format.py
cmake --build --preset cpu-test --target gufo --parallel 4
ctest --test-dir build/cpu-test -L router --output-on-failure
ctest --test-dir build/cpu-test -R 'serve_cli_test|http_server_test' --output-on-failure
nix build .#checks.x86_64-linux.pr    # hosted contract suite
```

Expected: all PASS. On a GPU dev box also: `nix develop -c cmake --preset gpu-test && nix develop -c ctest --preset gpu-full -R '^router_gpu_smoke$' --output-on-failure` — a missing-model SKIP is not a quality pass; report it as such.

- [ ] **Step 4: Commit**

```sh
git add docs/SERVER.md docs/CLI.md README.md tests/cli/router_gpu_smoke.py CMakeLists.txt
git commit -m "docs(router): document gufo router and add gpu smoke check"
```

---