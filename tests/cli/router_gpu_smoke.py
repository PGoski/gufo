"""GPU smoke check for `gufo router` with real TTS and ASR workers.

Run through ctest as router_gpu_smoke (gpu label only) with the gufo binary
as argv[1]. Two preset sections (tts + asr) resolve from the model paths
documented in docs/SERVER.md for those modalities; when either model
directory is missing the check skips loudly with exit 0, like the other
model-dependent checks in this repository. On a machine with the models it
proves cross-modality routing on real HIP contexts and idle unload.

No absolute timings are asserted; the idle-unload bound is a poll deadline.
"""

import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import wave

# Model locations documented in docs/SERVER.md for each modality.
TTS_DIR = "/persist/models/audio/Qwen3-TTS-12Hz-1.7B-CustomVoice"
ASR_SNAPSHOTS = "/var/llms/huggingface/hub/models--Qwen--Qwen3-ASR-1.7B/snapshots"

TTS_ID = "smoke-tts"
ASR_ID = "smoke-asr"

REQUEST_TIMEOUT = 300
IDLE_UNLOAD_BOUND = 120


def resolve_asr_dir():
    if not os.path.isdir(ASR_SNAPSHOTS):
        return None
    for entry in sorted(os.listdir(ASR_SNAPSHOTS)):
        path = os.path.join(ASR_SNAPSHOTS, entry)
        if os.path.isdir(path):
            return path
    return None


def custom_voice(tts_dir):
    with open(os.path.join(tts_dir, "config.json")) as handle:
        config = json.load(handle)
    speakers = config.get("talker_config", {}).get("spk_id", {})
    return sorted(speakers)[0] if speakers else None


def free_port():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


def write_silence_wav(path, seconds=1.0, rate=16000):
    with wave.open(path, "wb") as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(rate)
        handle.writeframes(b"\x00\x00" * int(seconds * rate))


class RouterProcess:
    def __init__(self, binary, tmp, sections, extra_args, name):
        self.name = name
        self.port = free_port()
        preset = os.path.join(tmp, name + ".preset")
        with open(preset, "w") as handle:
            handle.write(sections)
        self.log_path = os.path.join(tmp, name + ".log")
        env = {key: value for key, value in os.environ.items()
               if key not in {"HOST", "PORT", "GUFO_HOST", "GUFO_PORT",
                              "GUFO_ROUTER_WORKER_EXE"}}
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

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port,
                                          timeout=REQUEST_TIMEOUT)
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            headers = dict(headers or {},
                           **{"Content-Type": "application/json"})
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        payload = response.read()
        status = response.status
        conn.close()
        return status, payload

    def loaded_map(self):
        _, payload = self.request("GET", "/v1/models")
        listing = json.loads(payload)
        return {entry["id"]: entry["loaded"] for entry in listing["data"]}

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
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def router_children(pid):
    live = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open("/proc/{}/stat".format(entry)) as handle:
                fields = handle.read().rsplit(") ", 1)[1].split()
        except (OSError, IndexError):
            continue
        # fields[0] is the state, fields[1] the parent pid. A zombie holds no
        # weights and no socket, so only live children count against unload.
        if len(fields) > 1 and fields[1] == str(pid) and fields[0] != "Z":
            live.append(int(entry))
    return live


def poll_until(predicate, timeout, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def transcribe(router, wav_path):
    boundary = "gufosmokeboundary"
    with open(wav_path, "rb") as handle:
        audio = handle.read()
    form = (
        "--{}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\n"
        "{}\r\n"
        "--{}\r\nContent-Disposition: form-data; name=\"file\"; "
        "filename=\"smoke.wav\"\r\nContent-Type: audio/wav\r\n\r\n").format(
            boundary, ASR_ID, boundary).encode() + audio + (
        "\r\n--{}--\r\n").format(boundary).encode()
    return router.request("POST", "/v1/audio/transcriptions", body=form,
                          headers={"Content-Type":
                                   "multipart/form-data; boundary=" + boundary})


def synthesize(router, voice):
    return router.request("POST", "/v1/audio/speech",
                          {"model": TTS_ID, "voice": voice,
                           "input": "Hello from the Gufo router smoke test."})


def run_loaded_checks(router, voice, wav_path):
    status, payload = synthesize(router, voice)
    assert status == 200, (status, payload[:200], router.tail_log())
    assert len(payload) > 44, (len(payload), router.tail_log())
    loaded = router.loaded_map()
    assert loaded.get(TTS_ID) is True, (loaded, router.tail_log())
    assert loaded.get(ASR_ID) is False, (loaded, router.tail_log())
    status, payload = transcribe(router, wav_path)
    assert status == 200, (status, payload[:200], router.tail_log())
    loaded = router.loaded_map()
    assert loaded.get(TTS_ID) is True, (loaded, router.tail_log())
    assert loaded.get(ASR_ID) is True, (loaded, router.tail_log())


def run_idle_unload_checks(router, voice, wav_path):
    status, payload = synthesize(router, voice)
    assert status == 200, (status, payload[:200], router.tail_log())
    status, payload = transcribe(router, wav_path)
    assert status == 200, (status, payload[:200], router.tail_log())
    pid = router.proc.pid
    gone = poll_until(lambda: not router_children(pid), IDLE_UNLOAD_BOUND)
    assert gone, ("workers still alive after idle bound: "
                  + str(router_children(pid)) + " " + router.tail_log())
    loaded = router.loaded_map()
    assert loaded.get(TTS_ID) is False, (loaded, router.tail_log())
    assert loaded.get(ASR_ID) is False, (loaded, router.tail_log())


def main():
    if len(sys.argv) != 2:
        print("usage: router_gpu_smoke.py <gufo-binary>")
        return 2
    binary = sys.argv[1]
    asr_dir = resolve_asr_dir()
    if not os.path.isdir(TTS_DIR) or asr_dir is None:
        missing = [] if os.path.isdir(TTS_DIR) else [TTS_DIR]
        if asr_dir is None:
            missing.append(ASR_SNAPSHOTS + "/*")
        print("SKIPPED: model dir missing: " + ", ".join(missing), flush=True)
        return 0
    voice = custom_voice(TTS_DIR)
    assert voice is not None, "no spk_id voices in " + TTS_DIR + "/config.json"
    sections = ("[tts/{}]\nmodel = {}\n\n[asr/{}]\nmodel = {}\n".format(
        TTS_ID, TTS_DIR, ASR_ID, asr_dir))
    tmp = tempfile.mkdtemp(prefix="gufo-router-gpu-smoke-")
    wav_path = os.path.join(tmp, "smoke.wav")
    write_silence_wav(wav_path)
    try:
        with RouterProcess(binary, tmp, sections, ["--models-max", "2"],
                           "loaded") as router:
            run_loaded_checks(router, voice, wav_path)
        with RouterProcess(binary, tmp, sections,
                           ["--models-max", "2", "--sleep-idle-seconds", "2"],
                           "idle") as router:
            run_idle_unload_checks(router, voice, wav_path)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("router_gpu_smoke: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())