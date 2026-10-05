"""Process-level contract tests for `gufo router` with scripted fake workers.

Run through ctest as router_contract_test with the gufo binary as argv[1].
The fake worker reuses this module through the GUFO_ROUTER_WORKER_EXE seam
(Task 5): a launcher script with this interpreter's shebang imports
serve_fake_worker() from tests/cli/router_test.py.

Timings are recorded and printed (event=router_timing), never asserted
against absolute values, per the repository timing rules.
"""

import base64
import hashlib
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

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

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

        def ws_read(self, count):
            data = b""
            while len(data) < count:
                chunk = self.rfile.read(count - len(data))
                if not chunk:
                    raise ConnectionError("websocket peer went away")
                data += chunk
            return data

        def ws_recv_frame(self):
            head = self.ws_read(2)
            opcode = head[0] & 0x0F
            masked = head[1] & 0x80
            length = head[1] & 0x7F
            if length == 126:
                length = int.from_bytes(self.ws_read(2), "big")
            elif length == 127:
                length = int.from_bytes(self.ws_read(8), "big")
            key = self.ws_read(4) if masked else b""
            payload = self.ws_read(length) if length else b""
            if masked:
                payload = bytes(b ^ key[i % 4]
                                for i, b in enumerate(payload))
            return opcode, payload

        def ws_send(self, opcode, payload):
            out = bytearray([0x80 | opcode])
            size = len(payload)
            if size < 126:
                out.append(size)
            elif size < 0x10000:
                out.append(126)
                out += size.to_bytes(2, "big")
            else:
                out.append(127)
                out += size.to_bytes(8, "big")
            out += payload
            self.wfile.write(bytes(out))
            self.wfile.flush()

        def handle_ws(self):
            # Minimal RFC 6455 echo: complete the upgrade against whatever
            # key the front relayed, then echo text/binary frames back and
            # close cleanly on a close frame. Server frames stay unmasked;
            # 7-bit, 16-bit and 64-bit lengths are read, only 7/16-bit are
            # produced (echo payloads here are short).
            key = self.headers.get("Sec-WebSocket-Key", "")
            accept = base64.b64encode(hashlib.sha1(
                (key + WS_GUID).encode()).digest()).decode()
            self.send_response(101, "Switching Protocols")
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            try:
                while True:
                    opcode, payload = self.ws_recv_frame()
                    if opcode == 0x8:
                        self.ws_send(0x8, b"")
                        break
                    if opcode in (0x1, 0x2):
                        self.ws_send(opcode, payload)
            except (OSError, ConnectionError):
                pass
            # The connection is no longer HTTP; keep the request handler
            # from looping and let finish() close the socket.
            self.close_connection = True

        def do_GET(self):
            if (self.headers.get("Upgrade", "").lower() == "websocket"
                    and "upgrade" in
                    self.headers.get("Connection", "").lower()):
                self.handle_ws()
                return
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

    def log_contains(self, needle):
        try:
            with open(self.log_path, "rb") as handle:
                return needle in handle.read().decode("utf-8", "replace")
        except OSError:
            return False

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


def proc_dead(pid):
    """True when the pid is gone or only a zombie. After the router itself
    is SIGKILLed nobody can wait() its workers, so the PDEATHSIG-killed
    worker may linger as a zombie until the container's init collects it.
    A zombie runs no code, holds no socket and maps no weights, so it
    satisfies the PR_SET_PDEATHSIG contract even though kill(pid, 0)
    still succeeds."""
    if pid_gone(pid):
        return True
    try:
        with open("/proc/{}/stat".format(pid)) as handle:
            remainder = handle.read().rsplit(") ", 1)
    except OSError:
        return True
    return len(remainder) == 2 and remainder[1].split()[0] == "Z"


class WsClient:
    """Minimal raw RFC 6455 client: handshake, masked frames, unmasked in."""

    def __init__(self, port, path):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.f = self.sock.makefile("rb")
        self.key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((
            "GET {} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            "Sec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n"
            ).format(path, self.key).encode())
        head = b""
        while not head.endswith(b"\r\n\r\n"):
            line = self.f.readline()
            assert line, "websocket handshake closed early"
            head += line
        self.head = head
        assert b" 101" in head.split(b"\r\n", 1)[0], head
        expect = base64.b64encode(hashlib.sha1(
            (self.key + WS_GUID).encode()).digest()).decode()
        assert expect.encode() in head, (head, expect)

    def send_text(self, text):
        payload = text.encode()
        assert len(payload) < 126, "test frames stay in the 7-bit form"
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(bytearray([0x81, 0x80 | len(payload)])
                                + mask + masked))

    def _read(self, count):
        data = b""
        while len(data) < count:
            chunk = self.f.read(count - len(data))
            assert chunk, "websocket stream ended mid-frame"
            data += chunk
        return data

    def recv_frame(self):
        head = self._read(2)
        opcode = head[0] & 0x0F
        length = head[1] & 0x7F
        if length == 126:
            length = int.from_bytes(self._read(2), "big")
        elif length == 127:
            length = int.from_bytes(self._read(8), "big")
        return opcode, self._read(length) if length else b""

    def close(self):
        mask = os.urandom(4)
        try:
            self.sock.sendall(bytes(bytearray([0x88, 0x80]) + mask))
            self.recv_frame()  # close echo
        except (AssertionError, OSError):
            pass
        self.sock.close()


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

    # Case 11 (case_11_ws_relay): WebSocket relay end to end. The upgrade on
    # /v1/realtime?model=<id> completes, a masked text frame echoes back, the
    # close handshake is clean, and the open-socket count is released: with
    # --models-max 1 a following probe for another model queues, evicts the
    # now-idle ws worker and admits long before any join deadline.
    ws_two = ("[llm/wa]\nmodel = fake.gguf\n"
              "[llm/wb]\nmodel = fake.gguf\n")
    with RouterProcess(binary, tmp_root, ws_two,
                       extra_args=["--models-max", "1"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="wsrelay") as router:
        try:
            client = WsClient(router.port, "/v1/realtime?model=wa")
            client.send_text("ping-9")
            opcode, payload = client.recv_frame()
        except AssertionError as exc:
            raise AssertionError("case_11_ws_relay: {}: {}".format(
                exc, router.tail_log())) from exc
        assert opcode == 0x1 and payload == b"ping-9", \
            (opcode, payload, router.tail_log())
        assert router.loaded_map().get("wa") is True, router.tail_log()
        client.close()
        probe = {}

        def probe_wb():
            probe["result"] = router.request(
                "POST", "/v1/chat/completions",
                {"model": "wb", "messages": []})

        probe_thread = threading.Thread(target=probe_wb, daemon=True)
        probe_started = time.monotonic()
        probe_thread.start()
        probe_thread.join(timeout=30)
        assert not probe_thread.is_alive(), \
            ("probe stuck: ws open-socket count never released",
             router.tail_log())
        status, payload = probe["result"]
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "wb", payload
        print("event=router_timing ws_probe_admit_ms={:.1f}".format(
            (time.monotonic() - probe_started) * 1000.0))
        assert poll_until(
            lambda: router.log_contains("event=evict model=wa"), 8.0), \
            ("ws worker was never freed for the queued probe",
             router.tail_log())

    # Case 12 (case_12_ws_eviction_exempt): an open WebSocket keeps its
    # worker eviction-exempt (registry: open_sockets -> Busy; spec "Routing
    # and proxying" busy workers are never cut). max 2: ws on xa, a held SSE
    # on xb, so xc queues behind two exempt workers while the ws stays
    # open, and admits only after the ws closes (eviction then takes the
    # ws worker itself, now idle, never the still-busy xb).
    ws_three = ("[llm/xa]\nmodel = fake.gguf\n"
                "[llm/xb]\nmodel = fake.gguf\n"
                "[llm/xc]\nmodel = fake.gguf\n")
    with RouterProcess(binary, tmp_root, ws_three,
                       extra_args=["--models-max", "2"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="wsexempt") as router:
        try:
            client = WsClient(router.port,
                              "/v1/audio/speech/stream?model=xa")
            client.send_text("hold")
            echoed = client.recv_frame()
        except AssertionError as exc:
            raise AssertionError("case_12_ws_eviction_exempt: {}: {}".format(
                exc, router.tail_log())) from exc
        assert echoed == (0x1, b"hold"), (echoed, router.tail_log())
        held = {}
        release = threading.Event()

        def hold_stream():
            conn, response = router.open_stream(
                "/v1/chat/completions?stream=true&hold=40&chunks=40",
                {"model": "xb", "messages": []})
            response.readline()
            held["conn"] = conn
            held["response"] = response
            release.wait()
            try:
                response.read()
            except (OSError, ValueError, http.client.HTTPException):
                # fp.close() from the main thread makes read() raise on the
                # closed buffered file; the disconnect itself is the point.
                pass
            conn.close()

        holder = threading.Thread(target=hold_stream, daemon=True)
        holder.start()
        assert poll_until(lambda: "response" in held, 30.0), \
            ("held stream never started", router.tail_log())
        queued = {}

        def queue_third():
            queued["result"] = router.request(
                "POST", "/v1/chat/completions",
                {"model": "xc", "messages": []})

        waiter = threading.Thread(target=queue_third, daemon=True)
        waiter.start()
        # While the ws is open nothing is unloadable, so xc must stay
        # queued and xa must never be evicted (a broken exemption would
        # have admitted xc within a supervision tick).
        observe_until = time.monotonic() + 2.0
        while time.monotonic() < observe_until:
            assert waiter.is_alive(), \
                ("xc admitted while the ws worker was exempt",
                 queued, router.tail_log())
            assert not router.log_contains("event=evict model=xa"), \
                ("ws worker was evicted while the socket was open",
                 router.tail_log())
            time.sleep(0.1)
        assert router.loaded_map().get("xc") is False, router.tail_log()
        client.close()
        waiter.join(timeout=30)
        assert not waiter.is_alive(), \
            ("xc never admitted after the ws closed", router.tail_log())
        status, payload = queued["result"]
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "xc", payload
        xa_pids = router.spawns("xa")
        assert poll_until(lambda: all(pid_gone(pid) for pid in xa_pids),
                          10.0), ("eviction did not take the freed ws "
                                  "worker", xa_pids, router.tail_log())
        loaded = router.loaded_map()
        assert loaded == {"xa": False, "xb": True, "xc": True}, loaded
        release.set()
        # End the held stream the way a disconnect would (case 7 mechanics).
        try:
            held["response"].fp.close()
        except OSError:
            pass
        holder.join(timeout=30)
        assert not holder.is_alive(), router.tail_log()

    # Case 13 (case_13_worker_crash_mid_sse): the worker dies mid-relay
    # after headers were forwarded. The client stream ends, the router
    # stays alive, the model is marked unloaded, the exit is logged, and
    # the next request reloads a fresh worker.
    with RouterProcess(binary, tmp_root, "[llm/cx]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="crash") as router:
        conn, response = router.open_stream(
            "/v1/chat/completions?stream=true&hold=15&chunks=15",
            {"model": "cx", "messages": []})
        first = response.readline()
        assert first.startswith(b"data:"), (first, router.tail_log())
        pids = router.spawns("cx")
        assert pids, router.tail_log()
        os.kill(pids[-1], signal.SIGKILL)
        body = response.read()
        conn.close()
        assert b"[DONE]" not in body, \
            ("stream completed although its worker was killed", body,
             router.tail_log())
        status, payload = router.request("GET", "/v1/models")
        assert status == 200, (status, payload, router.tail_log())
        assert poll_until(
            lambda: router.log_contains("event=worker_exit model=cx"),
            10.0), ("no worker_exit line for the crashed worker",
                    router.tail_log())
        assert poll_until(
            lambda: router.loaded_map().get("cx") is False, 10.0), \
            ("crashed model still reported loaded", router.tail_log())
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "cx", "messages": []})
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "cx", payload
        assert len(router.spawns("cx")) == 2, router.spawns("cx")

    # Case 14 (case_14_idle_unload): --sleep-idle-seconds 1 unloads the
    # worker after one hot request; the pid dies and /v1/models reports
    # loaded=false. The bound is generous; the elapsed value is only
    # printed (event=router_timing), never asserted.
    with RouterProcess(binary, tmp_root, "[llm/zi]\nmodel = fake.gguf\n",
                       extra_args=["--sleep-idle-seconds", "1"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="idle") as router:
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "zi", "messages": []})
        assert status == 200, (status, payload, router.tail_log())
        pid = router.spawns("zi")[-1]
        unload_started = time.monotonic()
        assert poll_until(lambda: pid_gone(pid), 10.0), \
            ("idle worker survived --sleep-idle-seconds 1", pid,
             router.tail_log())
        print("event=router_timing idle_unload_ms={:.1f}".format(
            (time.monotonic() - unload_started) * 1000.0))
        assert poll_until(
            lambda: router.loaded_map().get("zi") is False, 10.0), \
            ("idle-unloaded model still reported loaded", router.tail_log())

    # Case 15 (case_15_evicts_idle_worker): 3 models, max 2, idle A (no
    # held streams, default 900 s idle timeout so eviction is the only
    # unload path). Hot A, hot B fills the slots; C queues and the
    # supervision thread evicts the LRU idle worker A, never B.
    evict_three = ("[llm/ea]\nmodel = fake.gguf\n"
                   "[llm/eb]\nmodel = fake.gguf\n"
                   "[llm/ec]\nmodel = fake.gguf\n")
    with RouterProcess(binary, tmp_root, evict_three,
                       extra_args=["--models-max", "2"],
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="evict") as router:
        for model in ("ea", "eb"):
            status, payload = router.request(
                "POST", "/v1/chat/completions",
                {"model": model, "messages": []})
            assert status == 200, (model, status, payload,
                                   router.tail_log())
        ea_pids = router.spawns("ea")
        assert ea_pids, router.tail_log()
        queued = {}

        def request_third():
            queued["result"] = router.request(
                "POST", "/v1/chat/completions",
                {"model": "ec", "messages": []})

        waiter = threading.Thread(target=request_third, daemon=True)
        waiter.start()
        waiter.join(timeout=30)
        assert not waiter.is_alive(), \
            ("queued C never admitted behind two idle workers",
             router.tail_log())
        status, payload = queued["result"]
        assert status == 200, (status, payload, router.tail_log())
        assert json.loads(payload)["model"] == "ec", payload
        assert poll_until(
            lambda: all(pid_gone(pid) for pid in ea_pids), 10.0), \
            ("idle A was not evicted for C", ea_pids, router.tail_log())
        assert poll_until(
            lambda: router.log_contains("event=evict model=ea"), 10.0), \
            router.tail_log()
        loaded = router.loaded_map()
        assert loaded == {"ea": False, "eb": True, "ec": True}, loaded

    # Case 16 (case_16_pdeathsig): killing the router with SIGKILL must
    # take its workers with it via PR_SET_PDEATHSIG.
    with RouterProcess(binary, tmp_root, "[llm/pd]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="pdeath") as router:
        status, payload = router.request(
            "POST", "/v1/chat/completions",
            {"model": "pd", "messages": []})
        assert status == 200, (status, payload, router.tail_log())
        pid = router.spawns("pd")[-1]
        router.proc.kill()
        router.proc.wait(timeout=15)
        death_started = time.monotonic()
        assert poll_until(lambda: proc_dead(pid), 3.0), \
            ("worker survived router SIGKILL (PR_SET_PDEATHSIG)", pid,
             router.tail_log())
        print("event=router_timing pdeathsig_ms={:.1f}".format(
            (time.monotonic() - death_started) * 1000.0))

    # Case 17 (case_17_graceful_shutdown): SIGTERM during an in-flight SSE
    # stream. The stream completes fully (terminal [DONE] chunk), the
    # router exits 0 within its drain window, and no worker survives. The
    # drain runs with the state lock free and keeps proxying the loaded
    # model, so this must not be weakened to a kill-then-check case.
    with RouterProcess(binary, tmp_root, "[llm/gs]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="shutdown") as router:
        conn, response = router.open_stream(
            "/v1/chat/completions?stream=true&hold=3&chunks=6",
            {"model": "gs", "messages": []})
        first = response.readline()
        assert first.startswith(b"data:"), (first, router.tail_log())
        pid = router.spawns("gs")[-1]
        router.proc.terminate()
        body = response.read()
        conn.close()
        assert b"[DONE]" in body, \
            ("in-flight stream was cut by SIGTERM", body,
             router.tail_log())
        exit_started = time.monotonic()
        exited = router.proc.wait(timeout=15)
        print("event=router_timing graceful_exit_ms={:.1f}".format(
            (time.monotonic() - exit_started) * 1000.0))
        assert exited == 0, (exited, router.tail_log())
        assert poll_until(lambda: pid_gone(pid), 10.0), \
            ("worker survived graceful shutdown", pid, router.tail_log())

    # M-5 video error-code parity: unrecorded and malformed job reads
    # answer exactly like video_api.cpp (404 video_not_found), a
    # collection read is 405 method_not_allowed, and a genuinely unknown
    # model on a non-video route keeps 404 model_not_found listing the
    # preset ids.
    with RouterProcess(binary, tmp_root, "[llm/mv]\nmodel = fake.gguf\n",
                       fake_env={"GUFO_FAKE_READY_DIR": ready_dir},
                       name="videoerr") as router:
        status, payload = router.request("GET", "/v1/videos")
        assert status == 405, (status, payload, router.tail_log())
        error = json.loads(payload)["error"]
        assert error["code"] == "method_not_allowed", payload
        assert error["type"] == "invalid_request_error", payload
        for method in ("GET", "DELETE"):
            status, payload = router.request(method, "/v1/videos/nope")
            assert status == 404, (method, status, payload,
                                   router.tail_log())
            error = json.loads(payload)["error"]
            assert error["code"] == "video_not_found", payload
            assert error["type"] == "invalid_request_error", payload
            assert "mv" not in error["message"], \
                ("job 404 must not list preset ids", payload)
        status, payload = router.request("GET", "/v1/videos/a/b")
        assert status == 404, (status, payload, router.tail_log())
        assert json.loads(payload)["error"]["code"] == "video_not_found", \
            payload
        # A recorded job on a live worker still resolves by job id.
        status, payload = router.request(
            "POST", "/v1/videos", {"model": "mv", "prompt": "z"})
        assert status == 200, (status, payload, router.tail_log())
        job_id = json.loads(payload)["id"]
        status, payload = router.request("GET", "/v1/videos/" + job_id)
        assert status == 200, (status, payload, router.tail_log())
        # Unknown model on a non-video route keeps the listing behavior.
        status, payload = router.request("GET", "/v1/slots?model=ghost")
        assert status == 404, (status, payload, router.tail_log())
        error = json.loads(payload)["error"]
        assert error["code"] == "model_not_found", payload
        assert "mv" in error["message"], payload


if __name__ == "__main__":
    main()
