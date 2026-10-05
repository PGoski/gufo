"""Process-level contract tests for `gufo router` with scripted fake workers.

Run through ctest as router_contract_test with the gufo binary as argv[1].
The fake worker reuses this module through the GUFO_ROUTER_WORKER_EXE seam
(Task 5): a launcher script with this interpreter's shebang imports
serve_fake_worker() from tests/cli/router_test.py.

Timings are recorded and printed (event=router_timing), never asserted
against absolute values, per the repository timing rules.
"""

import http.client
import itertools
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))

# Fake-worker environment (inherited through the router into every worker):
#   GUFO_FAKE_READY_DELAY  default /health ready delay in seconds
#   GUFO_FAKE_READY_DIR    per-model override file <dir>/<model>.delay
#   GUFO_FAKE_SPAWN_LOG    append "<model> <pid>" once per process start
#   GUFO_FAKE_EXIT_DIR     one-shot: on the first 200 /health remove
#                          <dir>/<model>.exit and exit(0) right after
#   GUFO_FAKE_REFUSE_DIR   per-model: while <dir>/<model>.refuse exists,
#                          non-/health requests close the connection with no
#                          response bytes, as a dead upstream would


def multipart_field(body, name):
    needle = 'name="' + name + '"'
    pos = body.find(needle)
    if pos < 0:
        return ""
    pos = body.find("\r\n\r\n", pos)
    if pos < 0:
        return ""
    start = pos + 4
    end = body.find("\r\n", start)
    return body[start:end if end >= 0 else None]


class FakeWorker:
    """Stdlib HTTP worker standing in for `gufo serve` behind the router.

    Answers /health (200 after the ready delay), echoes its id from
    --served-model-name on the JSON endpoints, streams SSE for
    /v1/chat/completions?stream=true and serves /v1/slots and /v1/metrics.
    """

    def __init__(self, model, port, ready_delay=0.0):
        self.model = model
        self.port = port
        self.ready_after = time.monotonic() + ready_delay

    def ready(self):
        return time.monotonic() >= self.ready_after

    def json_payload(self, path, extra=None):
        payload = {"id": "chatcmpl-fake", "object": "chat.completion",
                   "model": self.model, "path": path}
        if extra:
            payload.update(extra)
        return payload


def serve_fake_worker():
    """Entry point for the fake worker process spawned by the router."""
    argv = sys.argv[1:]
    model = ""
    for index, arg in enumerate(argv):
        if arg == "--served-model-name" and index + 1 < len(argv):
            model = argv[index + 1]
    port = int(os.environ["GUFO_PORT"])
    started = time.monotonic()
    refuse_dir = os.environ.get("GUFO_FAKE_REFUSE_DIR")
    job_seq = itertools.count(1)

    def ready_delay():
        delay = float(os.environ.get("GUFO_FAKE_READY_DELAY") or "0")
        ready_dir = os.environ.get("GUFO_FAKE_READY_DIR")
        if ready_dir:
            try:
                with open(os.path.join(ready_dir, model + ".delay")) as handle:
                    delay = float(handle.read().strip() or "0")
            except OSError:
                pass
        return delay

    spawn_log = os.environ.get("GUFO_FAKE_SPAWN_LOG")
    if spawn_log:
        with open(spawn_log, "a") as handle:
            handle.write("{} {}\n".format(model, os.getpid()))

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def split_path(self):
            path, _, query = self.path.partition("?")
            return path, urllib.parse.parse_qs(query)

        def refusing(self):
            return bool(refuse_dir) and os.path.exists(
                os.path.join(refuse_dir, model + ".refuse"))

        def send_refused(self):
            # Fail the proxied request the way a dead upstream does: hang up
            # without a single response byte.
            self.close_connection = True
            self.connection.close()

        def send_json(self, status, payload):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path, _ = self.split_path()
            if path == "/health":
                if time.monotonic() - started < ready_delay():
                    self.send_json(503, {"status": "loading"})
                    return
                self.send_json(200, {"status": "ok"})
                self.wfile.flush()
                exit_dir = os.environ.get("GUFO_FAKE_EXIT_DIR")
                marker = (os.path.join(exit_dir, model + ".exit")
                          if exit_dir else None)
                if marker and os.path.exists(marker):
                    try:
                        os.remove(marker)
                    except OSError:
                        pass
                    os._exit(0)
                return
            if self.refusing():
                self.send_refused()
                return
            if path in ("/v1/slots", "/slots", "/v1/metrics", "/metrics",
                        "/v1/realtime",
                        "/v1/audio/speech/stream"):
                self.send_json(200, {"model": model, "path": path})
                return
            if path.startswith("/v1/videos/"):
                self.send_json(200, {"id": path.rsplit("/", 1)[-1],
                                     "object": "video",
                                     "status": "completed", "model": model})
                return
            self.send_json(404, {"error": {"message": "not found",
                                           "type": "invalid_request_error",
                                           "code": "not_found"}})

        def serve_sse(self, params):
            hold = float(params.get("hold", ["0"])[0])
            chunks = max(int(params.get("chunks", ["4"])[0]), 1)
            period = hold / chunks
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.close_connection = True
            self.end_headers()
            try:
                for index in range(chunks):
                    event = "data: {}\n\n".format(
                        json.dumps({"model": model, "n": index}))
                    self.wfile.write(event.encode())
                    self.wfile.flush()
                    if period:
                        time.sleep(period)
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
            except OSError:
                pass

        def do_DELETE(self):
            path, _ = self.split_path()
            if self.refusing():
                self.send_refused()
                return
            if path.startswith("/v1/videos/"):
                self.send_json(200, {"id": path.rsplit("/", 1)[-1],
                                     "object": "video", "deleted": True,
                                     "model": model})
                return
            self.send_json(404, {"error": {"message": "not found",
                                           "type": "invalid_request_error",
                                           "code": "not_found"}})

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            path, params = self.split_path()
            if self.refusing():
                self.send_refused()
                return
            if path == "/v1/videos":
                self.send_json(200, {"id": "job-{}-{}".format(
                    model, next(job_seq)),
                    "object": "video", "status": "queued", "model": model})
                return
            if path == "/v1/audio/transcriptions":
                self.send_json(200, {"model": model,
                                     "field": multipart_field(
                                         raw.decode("latin-1"), "model")})
                return
            if (path == "/v1/chat/completions"
                    and params.get("stream", ["false"])[0] == "true"):
                self.serve_sse(params)
                return
            self.send_json(200, {"id": "chatcmpl-fake",
                                 "object": "chat.completion",
                                 "model": model, "path": path})

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.serve_forever()


def free_port():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def write_fake_launcher(tmp_root):
    path = os.path.join(tmp_root, "fake_worker.py")
    with open(path, "w") as handle:
        handle.write("#!" + sys.executable + "\n")
        handle.write("import sys\n")
        handle.write("sys.path.insert(0, {!r})\n".format(HERE))
        handle.write("import router_test\n")
        handle.write("router_test.serve_fake_worker()\n")
    os.chmod(path, 0o755)
    return path


class RouterProcess:
    def __init__(self, binary, tmp, sections, extra_args=(), fake_env=None,
                 name="router"):
        self.name = name
        self.tmp = tmp
        self.port = free_port()
        preset = os.path.join(tmp, name + ".preset")
        with open(preset, "w") as handle:
            handle.write(sections)
        self.spawn_log = os.path.join(tmp, name + ".spawns")
        self.log_path = os.path.join(tmp, name + ".log")
        env = {key: value for key, value in os.environ.items()
               if key not in {"HOST", "PORT", "GUFO_HOST", "GUFO_PORT"}}
        env["GUFO_ROUTER_WORKER_EXE"] = FAKE_EXE
        env["GUFO_FAKE_SPAWN_LOG"] = self.spawn_log
        env.update(fake_env or {})
        self.log = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            [binary, "router", "--models-preset", preset,
             "--port", str(self.port), *extra_args],
            env=env, stdout=self.log, stderr=subprocess.STDOUT)
        self._wait_ready()

    def _wait_ready(self):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                status, _ = self.request("GET", "/health")
                if status == 200:
                    return
            except OSError:
                pass
            time.sleep(0.05)
        raise AssertionError("router did not answer /health: "
                             + self.tail_log())

    def request(self, method, path, body=None, headers=None, conn=None):
        own = conn is None
        if own:
            conn = http.client.HTTPConnection("127.0.0.1", self.port,
                                              timeout=30)
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            headers = dict(headers or {}, **{"Content-Type":
                                              "application/json"})
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        payload = response.read()
        status = response.status
        if own:
            conn.close()
        return status, payload

    def open_stream(self, path, body):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        conn.request("POST", path, body=json.dumps(body).encode(),
                     headers={"Content-Type": "application/json"})
        return conn, conn.getresponse()

    def loaded_map(self):
        _, payload = self.request("GET", "/v1/models")
        listing = json.loads(payload)
        return {entry["id"]: entry["loaded"] for entry in listing["data"]}

    def spawns(self, model):
        try:
            with open(self.spawn_log) as handle:
                lines = handle.read().splitlines()
        except OSError:
            lines = []
        return [int(line.split()[1]) for line in lines
                if line.split() and line.split()[0] == model]

    def tail_log(self):
        try:
            with open(self.log_path, "rb") as handle:
                data = handle.read()
        except OSError:
            return "<no log>"
        return data[-2000:].decode("utf-8", "replace")

    def close(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def poll_until(predicate, timeout, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def pid_gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


FAKE_EXE = None


def main():
    global FAKE_EXE
    binary = sys.argv[1]
    tmp_root = tempfile.mkdtemp(prefix="gufo-router-")
    FAKE_EXE = write_fake_launcher(tmp_root)
    ready_dir = os.path.join(tmp_root, "ready")
    os.mkdir(ready_dir)
    try:
        run_cases(binary, tmp_root, ready_dir)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
    print("router_contract_test: all cases passed")


def run_cases(binary, tmp_root, ready_dir):
    # Case 1+2: cold load held, exactly one spawn; concurrent first requests.
    with open(os.path.join(ready_dir, "alpha.delay"), "w") as handle:
        handle.write("1")
    with open(os.path.join(ready_dir, "beta.delay"), "w") as handle:
        handle.write("1")
    sections = ("[llm/alpha]\nmodel = fake.gguf\n"
                "[llm/beta]\nmodel = fake.gguf\n"
                "[llm/gamma]\nmodel = fake.gguf\n")
    with RouterProcess(binary, tmp_root, sections,
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="basic") as router:
        cold_started = time.monotonic()
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "alpha", "messages": []})
        cold_ms = (time.monotonic() - cold_started) * 1000.0
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "alpha", payload
        hot_started = time.monotonic()
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "alpha", "messages": []})
        hot_ms = (time.monotonic() - hot_started) * 1000.0
        assert status == 200, (status, payload, router.tail_log())
        print("event=router_timing cold_ms={:.1f} hot_ms={:.1f}".format(
            cold_ms, hot_ms))
        assert len(router.spawns("alpha")) == 1, router.spawns("alpha")

        results = {}

        def hammer(index):
            results[index] = router.request(
                "POST", "/v1/chat/completions",
                {"model": "beta", "messages": []})

        threads = [threading.Thread(target=hammer, args=(i,))
                   for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert len(results) == 5, results
        for index, (status, payload) in results.items():
            assert status == 200, (index, status, payload)
            assert json.loads(payload)["model"] == "beta", payload
        assert len(router.spawns("beta")) == 1, router.spawns("beta")

        # Case 3: model resolution per endpoint family.
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "gamma", "messages": []})
        assert status == 200 and json.loads(payload)["model"] == "gamma"
        boundary = "gufofakeboundary"
        form = (
            "--{}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\n"
            "gamma\r\n"
            "--{}\r\nContent-Disposition: form-data; name=\"file\"; "
            "filename=\"a.wav\"\r\nContent-Type: audio/wav\r\n\r\n"
            "RIFFxxxx\r\n--{}--\r\n").format(boundary, boundary, boundary)
        status, payload = router.request(
            "POST", "/v1/audio/transcriptions", body=form.encode(),
            headers={"Content-Type":
                     "multipart/form-data; boundary=" + boundary})
        assert status == 200, (status, payload, router.tail_log())
        echoed = json.loads(payload)
        assert echoed["model"] == "gamma", payload
        assert echoed["field"] == "gamma", payload
        for slots_path in ("/v1/slots", "/slots"):
            status, payload = router.request(
                "GET", slots_path + "?model=gamma")
            assert status == 200, (slots_path, status, payload)
            assert json.loads(payload)["model"] == "gamma", payload
        status, payload = router.request(
            "GET", "/v1/realtime?model=gamma")
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "gamma", payload

        # Case 4: missing/unknown model errors.
        status, payload = router.request("POST", "/v1/chat/completions",
                                         {"messages": []})
        assert status == 400, (status, payload, router.tail_log())
        error = json.loads(payload)["error"]
        assert error["type"] == "invalid_request_error", payload
        assert "missing 'model' field" in error["message"], payload
        status, payload = router.request("GET", "/v1/slots")
        assert status == 400, (status, payload)
        status, payload = router.request("GET", "/v1/slots?model=ghost")
        assert status == 404, (status, payload)
        assert json.loads(payload)["error"]["code"] == "model_not_found", \
            payload
        status, payload = router.request(
            "POST", "/v1/chat/completions", {"model": "ghost", "messages": []})
        assert status == 404, (status, payload, router.tail_log())
        error = json.loads(payload)["error"]
        assert error["code"] == "model_not_found", payload
        for known in ("alpha", "beta", "gamma"):
            assert known in error["message"], payload

    # Case 5: saturated queue evicts an idle worker for a queued model.
    abc = ("[llm/aa]\nmodel = fake.gguf\n"
           "[llm/bb]\nmodel = fake.gguf\n"
           "[llm/cc]\nmodel = fake.gguf\n")
    with RouterProcess(binary, tmp_root, abc, extra_args=["--models-max", "2"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="queue") as router:
        for model in ("aa", "bb"):
            status, payload = router.request(
                "POST", "/v1/chat/completions",
                {"model": model, "messages": []})
            assert status == 200, (model, status, payload)

        stream_done = threading.Event()
        stream_status = {}

        def hold_busy():
            conn, response = router.open_stream(
                "/v1/chat/completions?stream=true&hold=3&chunks=6",
                {"model": "aa", "messages": []})
            stream_status["status"] = response.status
            response.readline()
            response.read()
            conn.close()
            stream_done.set()

        busy = threading.Thread(target=hold_busy)
        busy.start()
        admitted = threading.Event()
        queue_result = {}

        def queued_request():
            status, payload = router.request(
                "POST", "/v1/chat/completions",
                {"model": "cc", "messages": []})
            queue_result["result"] = (status, payload)
            admitted.set()

        waiter = threading.Thread(target=queued_request)
        waiter.start()
        # The queued load may finish before or after the held stream ends;
        # correctness is that it finishes with the right worker and freed
        # slot, without asserting absolute timing.
        assert admitted.wait(timeout=30), (router.tail_log(),
                                           "queued request never admitted")
        status, payload = queue_result["result"]
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "cc", payload
        loaded = router.loaded_map()
        assert loaded["cc"] is True, loaded
        assert loaded["bb"] is False, loaded
        busy.join(timeout=30)
        assert stream_status.get("status") == 200

    # Case 6: load timeout returns 504, kills the worker, next request works.
    with open(os.path.join(ready_dir, "slow.delay"), "w") as handle:
        handle.write("5")
    with RouterProcess(binary, tmp_root, "[llm/slow]\nmodel = fake.gguf\n",
                       extra_args=["--load-timeout-seconds", "1"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="timeout") as router:
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "slow", "messages": []})
        assert status == 504, (status, payload, router.tail_log())
        error = json.loads(payload)["error"]
        assert error["code"] == "load_timeout", payload
        assert error["type"] == "server_error", payload
        pids = router.spawns("slow")
        assert pids, router.tail_log()
        assert poll_until(lambda: all(pid_gone(pid) for pid in pids), 8.0), \
            ("worker survived the load timeout", pids, router.tail_log())
        with open(os.path.join(ready_dir, "slow.delay"), "w") as handle:
            handle.write("0")
        # A supervision tick may already be mid-respawn; retry the request
        # until a fresh worker answers.
        outcomes = []

        def retry():
            status, payload = router.request(
                "POST", "/v1/chat/completions",
                {"model": "slow", "messages": []})
            outcomes.append((status, payload))
            return status == 200

        assert poll_until(retry, 20.0, interval=0.25), \
            (outcomes, router.tail_log())
        assert json.loads(outcomes[-1][1])["model"] == "slow"

    # Case 7: client disconnect mid-SSE releases the busy count so the
    # queued model admits without waiting for the full held stream.
    ab2 = ("[llm/da]\nmodel = fake.gguf\n"
           "[llm/db]\nmodel = fake.gguf\n"
           "[llm/dc]\nmodel = fake.gguf\n")
    with RouterProcess(binary, tmp_root, ab2,
                       extra_args=["--models-max", "2"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="disconnect") as router:
        held = {}

        def hold(model, result_key):
            try:
                conn, response = router.open_stream(
                    "/v1/chat/completions?stream=true&hold=12&chunks=12",
                    {"model": model, "messages": []})
                held[result_key] = (conn, response)
                response.readline()
                held[result_key + "_seen"] = True
            except (OSError, http.client.HTTPException):
                held[result_key + "_error"] = True

        first = threading.Thread(target=hold, args=("da", "a"), daemon=True)
        second = threading.Thread(target=hold, args=("db", "b"), daemon=True)
        first.start()
        second.start()
        assert poll_until(lambda: "a_seen" in held and "b_seen" in held, 30.0),\
            router.tail_log()
        # Both slots are busy; the third model queues behind them.
        queued = {}

        def queue_third():
            started = time.monotonic()
            status, payload = router.request(
                "POST", "/v1/chat/completions",
                {"model": "dc", "messages": []})
            queued["result"] = (status, payload)
            queued["elapsed"] = time.monotonic() - started

        third = threading.Thread(target=queue_third, daemon=True)
        third.start()
        # Kill the first client socket; the relay must notice, release the
        # busy count and let the queued load proceed long before the held
        # stream's natural end (~12 s). A leaked busy count would keep this
        # request queued past the join deadline or past the other stream.
        # The relayed stream is close-delimited, so getresponse() already
        # marked the connection closed and handed the socket to the
        # response; releasing the response's buffered file is what sends
        # the FIN (conn.sock.close() could not: getresponse nulled it).
        held["a"][1].fp.close()
        third.join(timeout=9.0)
        assert not third.is_alive(), \
            ("queued request stuck after client disconnect", router.tail_log())
        status, payload = queued["result"]
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "dc", payload
        print("event=router_timing disconnect_release_ms={:.1f}".format(
            queued["elapsed"] * 1000.0))
        assert router.loaded_map().get("dc") is True
        held["b"][1].fp.close()

    # Case 8: worker that exits right after the health probe: the held
    # request gets 502 upstream_unavailable and the next request reloads.
    exit_dir = os.path.join(tmp_root, "exitmarks")
    os.mkdir(exit_dir)
    with open(os.path.join(exit_dir, "blink.exit"), "w") as handle:
        handle.write("1")
    with RouterProcess(binary, tmp_root, "[llm/blink]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir,
                                 "GUFO_FAKE_EXIT_DIR": exit_dir},
                       name="blink") as router:
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "blink", "messages": []})
        assert status == 502, (status, payload, router.tail_log())
        assert json.loads(payload)["error"]["code"] == "upstream_unavailable",\
            payload
        assert poll_until(
            lambda: router.loaded_map().get("blink") is False, 8.0), \
            ("dead worker still reported loaded", router.tail_log())
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "blink", "messages": []})
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "blink", payload
        assert len(router.spawns("blink")) == 2, router.spawns("blink")

    # Case 9: a video job recorded from the worker's create response stays
    # routable by job id alone (reloading a dead owner) and is released by a
    # terminal status read or a DELETE.
    # The fake answers the video endpoints regardless of modality; an llm
    # section keeps the --served-model-name injection preset.cpp skips for
    # real video workers.
    with RouterProcess(binary, tmp_root, "[llm/vee]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="video") as router:
        status, payload = router.request(
            "POST", "/v1/videos", {"model": "vee", "prompt": "x"})
        assert status == 200, (status, payload, router.tail_log())
        created = json.loads(payload)
        assert created["model"] == "vee", payload
        job_id = created["id"]
        assert router.loaded_map().get("vee") is True
        for pid in router.spawns("vee"):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        assert poll_until(
            lambda: router.loaded_map().get("vee") is False, 8.0), \
            ("killed worker still reported loaded", router.tail_log())
        # Only the recorded job id can resolve this request to its owner.
        status, payload = router.request("GET", "/v1/videos/" + job_id)
        assert status == 200, (status, payload, router.tail_log())
        echoed = json.loads(payload)
        assert echoed["model"] == "vee", payload
        assert echoed["id"] == job_id, payload
        assert len(router.spawns("vee")) == 2, router.spawns("vee")
        # The worker reports the job completed, so the router forgets it.
        assert poll_until(
            lambda: router.request("GET", "/v1/videos/" + job_id)[0] == 404,
            8.0), ("terminal status did not release the job",
                   router.tail_log())
        # A DELETE releases the job the same way.
        status, payload = router.request(
            "POST", "/v1/videos", {"model": "vee", "prompt": "y"})
        job_two = json.loads(payload)["id"]
        status, payload = router.request("DELETE", "/v1/videos/" + job_two)
        assert status == 200, (status, payload, router.tail_log())
        assert poll_until(
            lambda: router.request("GET", "/v1/videos/" + job_two)[0] == 404,
            8.0), ("DELETE did not release the job", router.tail_log())

    # Case 10: a live worker that answers /health but closes proxied
    # connections is killed and unloaded; the next request gets a fresh
    # worker instead of retrying the same dead backend forever.
    refuse_dir = os.path.join(tmp_root, "refuse")
    os.mkdir(refuse_dir)
    with RouterProcess(binary, tmp_root, "[llm/wuss]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir,
                                 "GUFO_FAKE_REFUSE_DIR": refuse_dir},
                       name="refusing") as router:
        with open(os.path.join(refuse_dir, "wuss.refuse"), "w") as handle:
            handle.write("1")
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "wuss", "messages": []})
        assert status == 502, (status, payload, router.tail_log())
        assert json.loads(payload)["error"]["code"] == "upstream_unavailable",\
            payload
        dead = router.spawns("wuss")
        assert dead, router.tail_log()
        assert poll_until(lambda: all(pid_gone(pid) for pid in dead), 8.0), \
            ("refusing worker survived the 502", dead, router.tail_log())
        os.remove(os.path.join(refuse_dir, "wuss.refuse"))
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "wuss", "messages": []})
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "wuss", payload
        assert len(router.spawns("wuss")) == 2, router.spawns("wuss")


if __name__ == "__main__":
    main()
