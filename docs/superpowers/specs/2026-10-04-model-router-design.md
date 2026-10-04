# Model Router: dynamic multi-model serving for Gufo

Status: draft for review, 2026-10-04

## Purpose

Strix Halo cannot hold every supported model resident at once. `gufo router` is a
top-level subcommand that exposes one OpenAI/Anthropic-compatible front door for
several preset models across modalities (`llm`, `image`, `video`, `tts`, `asr`).
It starts real `gufo serve <modality>` workers on loopback on demand, reverse-
proxies HTTP, SSE, WebSocket, and multipart traffic to them, and unloads workers
that have been idle or are displaced by `--models-max`.

This mirrors the llama.cpp router server feature set
(`--models-preset`, `--models-dir`, `--models-max`, `--sleep-idle-seconds`)
while keeping Gufo's own option names as the single configuration contract.

## Goals

- One listener for several model instances spanning modalities.
- Dynamic load on first request for a preset model; unload on idle timeout or
  when `--models-max` forces eviction.
- Configuration keys are the same long options as `gufo serve <modality>`, 1:1,
  validated and interpreted by the same `ArgParser` registrations. Adding a
  `gufo serve` option makes it available in the preset file with zero router
  code changes.
- Crash isolation: a worker death (including the existing `device_lost` exit)
  unloads only that model.

## Non-goals

- In-process dynamic model load/unload (HIP queue budgets are fixed per
  process; see `src/cli/serve/serve.cpp` queue-plan comment).
- Memory-aware admission control; `--models-max` and preset authoring are the
  contract.
- Accepting verbatim llama.cpp INI files (a key-shim loader can be added later
  in front of this parser).
- Auto-respawn of crashed workers outside normal load-on-request.
- HTTP/2, keep-alive pooling, web UI, per-model authentication.

## Command interface

New top-level subcommand, listed in `gufo --help` next to `serve`, `chat`,
`prompt`, `video`, `transcribe`:

```text
gufo router --models-preset <FILE>
            [--models-dir <DIR>]
            [--models-max N]            default 2; 0 = unlimited
            [--sleep-idle-seconds SEC]  default 900; 0 disables idle unload
            [--autoload]                preload preset models at startup
            [--host IP] [--port N] [--api-key KEY]
            [--max-connections N] [--max-request-bytes N]
            [--log-level LEVEL] [-v]
```

- Front server options use the same names, defaults, and env fallbacks as
  `serve` (`GUFO_HOST`, `GUFO_PORT`, `HOST`, `PORT`) by reusing the shared
  server-option registration.
- `gufo router help` prints the preset syntax plus, per modality, the accepted
  keys generated from the same `ArgParser` registrations that render
  `gufo serve <modality> --help`. No option list is hardcoded in the router.
- `serve` and its five modality subcommands keep their exact current behavior;
  they are the worker command the router spawns.

## Preset file format

Plain UTF-8 text. No shell quoting rules, no escapes.

```ini
# Full-line comments only (leading '#'). Blank lines ignored.

[llm/qwen3.8-27b]
model        = /models/llm/Qwen3.8-27B-UD-Q4_K_XL.gguf
speculative  = dflash2
dflash-model = /models/llm/Qwen3.8-27B-DFlash2-Q4_K_M.gguf
sessions     = 2
cache-disk   = /var/cache/gufo/qwen27b

[asr/qwen3-asr]
model = /models/qwen3-asr
```

- Section header: `[<modality>/<model-id>]` with modality in
  `llm|image|video|tts|asr`. `model-id` is the routing key; it must be unique
  across the file (startup error otherwise) and becomes the worker's
  `--served-model-name` default.
- Body line: `key = value` maps to worker argv `--key value` verbatim.
  Boolean flags use `key = true|false`; `false` omits the flag, `true` passes
  the bare `--key`. A value runs to end of line (no inline comments). Keys are
  matched against that modality's registered long options only.
- Router-managed keys rejected in sections with an explicit error:
  `host`, `port`, `api-key`. The router instead injects per-child environment
  defaults `GUFO_HOST=127.0.0.1` and `GUFO_PORT=<assigned>`, which every
  modality already honors as defaults, so no flag injection is needed.
- Relative path values resolve against `--models-dir` when it is set,
  otherwise against the router's current working directory.
- Startup validation: every section is fed through the modality's real
  `ArgParser`; unknown keys, bad values, and missing required options abort
  with the same message `gufo serve <modality>` would print, prefixed with the
  section name. `--models-max` must be 0 or `>= 1`; with `--autoload`, presets
  beyond the limit are not an error (they load on demand).

## Worker supervision

- Worker argv: `/proc/self/exe serve <modality> <config argv...>`.
- Port: the router binds 127.0.0.1:0, reads `getsockname`, closes, and passes
  the port via env. On `EADDRINUSE` at worker start, the router retries spawn
  with a fresh port up to 3 attempts.
- Load: first request for a model spawns the worker; requests for the same
  model queue behind readiness. Readiness is `GET /health` returning 200 (503
  means still loading). Pending requests are held, not rejected, matching
  today's pre-ready `serve` behavior; there is no separate load timeout. A
  worker that exits with a non-zero status during load fails all held
  requests with 502 and marks the model unloaded; the next request retries
  the load.
- Child output: worker stdout/stderr are captured line-wise and re-emitted to
  the router log prefixed with the `model-id`, so `event=...` lines from
  functional tests remain greppable.
- Idle unload: a worker becomes unloadable `--sleep-idle-seconds` after its
  last activity, where activity is measured from request start AND request
  end (including full SSE stream and open WebSocket sessions). Unload =
  SIGTERM, SIGKILL after a 10 s grace.
- Admission at `--models-max`: the least-recently-active unloadable worker is
  evicted for a new load. If every worker is busy (in-flight request, open
  WebSocket, unfinished tracked video job), the new-model request receives
  503 with `Retry-After: 5` and is not queued.
- Worker death while in flight: the affected client streams/connections close
  as they do today when `serve` dies; the model is marked unloaded and the
  next request reloads it.
- `--autoload` starts preset models at startup in section order until the
  active limit; the rest load on demand. Default is lazy-only: the router
  exists precisely because presets do not all fit, so preloading everything is
  opt-in (deviation from llama.cpp's default is deliberate).

## Routing and proxying

Model resolution per endpoint family:

| Endpoint | Model source |
| --- | --- |
| `/v1/chat/completions`, `/v1/completions`, `/v1/responses`, `/v1/messages`, `/v1/images/*`, `/v1/audio/speech`, `/v1/videos` | JSON `model` field |
| `/v1/audio/transcriptions` (multipart) | `model` form field |
| `/v1/realtime`, `/v1/audio/speech/stream` (WebSocket) | `model` query parameter |
| `/v1/videos/<id>` status/content | router job table, below |

- Missing `model` field or query parameter: 400 with an
  `invalid_request_error` naming the missing field. Unknown id: 404
  `model_not_found` listing preset ids.
- Relay semantics: response bodies are forwarded without re-serialization.
  SSE bytes pass through verbatim. WebSocket upgrade requests are completed
  against the worker and frames are relayed in both directions until either
  side closes. Multipart bodies are streamed to the worker (no full buffering
  beyond the front's existing `--max-request-bytes` bound).
- Cancellation: front connection close closes the worker connection, which
  already propagates cancellation into the Gufo scheduler.
- Auth: `--api-key` is enforced at the front only; workers run keyless on
  loopback and the router never forwards a client `Authorization` header to a
  worker.
- Video jobs: job ids returned by `POST /v1/videos` are recorded with their
  model. While any tracked job for a model is non-terminal, that worker is
  eviction-exempt. A status/content read for a known job id after the worker
  died reloads that worker (persisted job state under the section's `--root`
  answers the request). Unknown job id: 404.
- Router-managed paths handled by the router itself:
  - `GET /health` — front alive (200) once the listener is up, independent of
    workers.
  - `GET /ready` — 200 once preset validation passed and the listener is up.
  - `GET /v1/models` — every preset model in OpenAI list shape with the
    extension field `"loaded": true|false`.
  - `GET /v1/slots` and `GET /v1/metrics` — require `?model=<id>`; proxied to
    that worker when loaded, 409 `model_not_loaded` otherwise.
- Unknown paths: 404, matching `serve`.

## Shutdown

On SIGINT/SIGTERM: stop accepting, refuse new loads, wait up to 10 s for
in-flight requests to finish, then SIGTERM all workers, SIGKILL after a
further 10 s, exit 0. Children are always killed even on router crash via
`PR_SET_PDEATHSIG`.

## Code layout

- New `src/cli/serve/router/`: `preset.{hpp,cpp}` (file → per-section argv +
  validation), `registry.{hpp,cpp}` (ids, LRU/idle/drain state machine),
  `workers.{hpp,cpp}` (spawn/health/kill, output capture), `proxy.{hpp,cpp}`
  (HTTP client, SSE relay), `ws_relay.{hpp,cpp}` (WebSocket frame relay),
  `router.cpp` (front wiring into the existing `server::HttpServer` dispatch).
- Only existing-code change: extract the per-modality option registration from
  `RunServe` (`src/cli/serve/serve.cpp`) into shared `RegisterServeOptions`
  functions (e.g. `src/cli/serve/serve_options.{hpp,cpp}`) used by both
  `serve` and `router`. Help output and parsing behavior must stay byte-identical.

## Error handling summary

| Condition | Behavior |
| --- | --- |
| Bad preset (unknown key, bad value, duplicate id, reserved key) | Startup abort, exit 2, section-prefixed parser message |
| Worker load fails (non-zero exit) | Held requests 502, model unloaded, next request retries |
| Worker dies mid-request | Streams close as with today's `serve` crash; model unloaded |
| `--models-max` reached, nothing unloadable | New-model requests 503 + `Retry-After: 5` |
| Idle timeout / eviction | Drain-then-kill; no in-flight request is ever cut |
| GPU `device_lost` in a worker | Existing worker exit path; router treats it as death |

## Testing

- C++ unit tests: preset parsing (comments, flags, reserved keys, duplicate
  ids, `--models-dir` resolution), registry policy (LRU order, idle timer from
  stream end, busy exemption incl. video jobs, retry-after on saturation).
- Router contract tests on hosted CPU CI (`nix build .#checks.x86_64-linux.pr`)
  with scripted fake workers (a small stdlib HTTP/SSE/WS server in the
  existing Python test tooling): load-on-request with held requests, readiness
  gating, SSE passthrough, WebSocket relay and close propagation, multipart
  passthrough, `/v1/models` loaded flags, 503 saturation path, graceful
  eviction, worker-crash handling, `PR_SET_PDEATHSIG` cleanup.
- GPU smoke check under `gpu-test` with one small preset (asr + tts, two
  sections) proving cross-modality routing and idle unload on real HIP
  contexts; run only the affected ctest name.
- Functional suite: router profile replaying one text request cold-start and
  one hot; retain per-request timings per AGENTS.md rules.
- Docs: `docs/SERVER.md` router section, `docs/CLI.md` `gufo router` entry,
  README feature bullet. No CHANGELOG edits (release workflow).

## Success criteria

1. With a 3-section preset and `--models-max 2`, requests to all three models
   succeed; at most 2 workers are alive at any time; no request is ever cut by
   eviction.
2. After `--sleep-idle-seconds` with no traffic, RSS of the router tree drops
   to front-only size (weights unmapped).
3. A cold request completes with correct output and logs `event=worker_spawn`
   and the worker's own startup lines prefixed by model id.
4. Adding a new long option to `gufo serve llm` requires no router source
   change for that option to be accepted in `[llm/*]` sections (verified by a
   unit test against a synthetic registered option).
