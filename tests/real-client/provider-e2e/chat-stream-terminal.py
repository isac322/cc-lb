#!/usr/bin/env python3
"""Offline Chat terminal-state proof with raw HTTP and installed OMP 18.18.7."""

import argparse
import datetime
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.parse
import uuid


ADMIN_TOKEN = "chat-terminal-synthetic-admin"
UPSTREAM_KEY = "chat-terminal-synthetic-upstream"
DONE = b"data: [DONE]\n\n"
MODEL = "gpt-4o"
MODES = ("done-fault", "partial-eof", "healthy", "finish-eof", "finish-fault")
USAGE_FIELDS = (
    "input_tokens", "output_tokens", "thinking_tokens", "cache_creation_input_tokens",
    "cache_read_input_tokens", "cache_write_input_tokens", "list_cost_usd_micros",
    "list_cost_input_micros", "list_cost_output_micros", "list_cost_cache_read_micros",
    "list_cost_cache_write_micros",
)
DIAGNOSTIC_FIELDS = (
    "upstream_error_type", "upstream_error_message", "upstream_body_end",
    "upstream_stream_warning_type", "upstream_stream_warning_message",
)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def fixture(mode):
    def chunk(choices, usage=None):
        value = {"id": "chatcmpl-terminal-loopback", "object": "chat.completion.chunk",
                 "created": 1, "model": MODEL, "choices": choices, "usage": usage}
        return ("data: " + json.dumps(value, separators=(",", ":")) + "\n\n").encode()
    data = chunk([{"index": 0, "delta": {"role": "assistant", "content": "hello"},
                   "finish_reason": None}])
    if mode != "partial-eof":
        data += chunk([{"index": 0, "delta": {}, "finish_reason": "stop"}])
    if mode in ("done-fault", "healthy"):
        data += chunk([], {"prompt_tokens": 7, "completion_tokens": 2, "total_tokens": 9,
                           "prompt_tokens_details": {"cached_tokens": 0},
                           "completion_tokens_details": {"reasoning_tokens": 0}})
        data += DONE
    return data


class Ledger:
    """Append records; never remove the parent's retained cleanup evidence."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()

    def record(self, kind, identifier, delete_command, state="active"):
        value = {"kind": kind, "identifier": str(identifier), "created_at": stamp(),
                 "delete_command": delete_command, "state": state}
        # JSONL records can also be appended to a parent's Markdown ledger.
        with self.lock, self.path.open("a") as stream:
            stream.write(json.dumps(value, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def stopped(self, kind, identifier):
        self.record(kind, identifier, "already stopped by this harness", "stopped")


def request(port, path, body=None, token=None, timeout=2):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    headers = {}
    if token:
        headers["Authorization"] = "Bearer " + token
    if body is not None:
        headers["Content-Type"] = "application/json"
    try:
        connection.request("POST" if body is not None else "GET", path,
                           json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = False

    def handle_error(self, _request, _address):
        self.run.errors.append("HTTP fixture handler failed; inspect per-attempt evidence")


class QuietHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def finish_body(self, data):
        self.wfile.write(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        self.wfile.flush()

    def close_socket(self):
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()


class Upstream(QuietHandler):
    def do_GET(self):
        if self.path != "/catalog.json":
            self.send_error(404)
            return
        body = json.dumps({MODEL: {"litellm_provider": "openai", "mode": "chat",
                                  "input_cost_per_token": 0.000002,
                                  "output_cost_per_token": 0.00001,
                                  "max_input_tokens": 128000, "max_output_tokens": 4096,
                                  "max_tokens": 128000}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        run = self.server.run
        attempt = run.active_attempt
        try:
            require(attempt is not None, "upstream request outside an active tap forwarding operation")
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            value = json.loads(body)
            dispatch = {"request_id": attempt["request_id"], "path": self.path,
                        "body_sha256": hashlib.sha256(body).hexdigest(), "stream": value.get("stream"),
                        "started_at": stamp(), "fault_released_by": None}
            run.dispatches.append(dispatch)
            require(self.path == "/v1/chat/completions", "unexpected upstream path")
            require(value.get("stream") is True and value.get("model") == MODEL,
                    "non-streaming or unexpected-model upstream call")
            require(body == attempt["request_body"], "tap changed the client's request body")
            data = fixture(attempt["mode"])
            dispatch["body_hex"] = data.hex()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Connection", "close")
            self.end_headers()
            self.finish_body(data)
            if attempt["mode"] in ("done-fault", "finish-fault"):
                require(attempt["fault_ready"].wait(5), "tap did not observe terminal bytes before fault deadline")
                dispatch["fault_released_by"] = "tap-observed-complete-DONE" if attempt["mode"] == "done-fault" else "tap-observed-finished-choice"
                self.close_socket()  # Deliberately omit the HTTP zero chunk.
            else:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
                self.close_connection = True
        except Exception as error:
            run.errors.append(f"upstream: {type(error).__name__}: {error}")
            self.close_socket()


class Tap(QuietHandler):
    def do_POST(self):
        run = self.server.run
        self.connection.settimeout(20)
        # The serial forwarding lock also gives identical OMP retries an exact
        # dispatch identity, independent of any upstream header filtering.
        with run.forward_lock:
            identifier = "chat-terminal-" + uuid.uuid4().hex
            attempt = {"request_id": identifier, "client": run.client, "mode": run.mode,
                       "path": self.path, "request_body": b"", "body": bytearray(),
                       "delivered_body": bytearray(), "status": None, "headers": {},
                       "fault_ready": threading.Event(), "complete": threading.Event(),
                       "transport_complete": False, "transport_error": None,
                       "client_disconnected": False, "started_at": stamp()}
            run.attempts.append(attempt)
            run.active_attempt = attempt
            connection = None
            try:
                require(self.path == "/openai/v1/chat/completions", "tap rejected a non-Chat route")
                require(self.headers.get("Transfer-Encoding") is None, "unsupported chunked client request")
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                attempt["request_body"] = body
                require(json.loads(body).get("stream") is True, "client requested a non-streaming response")
                excluded = {"host", "connection", "content-length", "request-id", "x-request-id", "accept-encoding"}
                headers = {key: value for key, value in self.headers.items() if key.lower() not in excluded}
                headers.update({"request-id": identifier, "Accept-Encoding": "identity", "Connection": "close"})
                connection = http.client.HTTPConnection("127.0.0.1", run.proxy_port, timeout=15)
                connection.request("POST", self.path, body, headers)
                response = connection.getresponse()
                attempt.update(status=response.status, headers={key.lower(): value for key, value in response.getheaders()})
                require(attempt["headers"].get("request-id") == identifier, "proxy did not preserve injected request-id")
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() not in ("transfer-encoding", "content-length", "connection", "server", "date"):
                        self.send_header(key, value)
                self.send_header("Transfer-Encoding", "chunked")
                self.send_header("Connection", "close")
                self.end_headers()
                held = bytearray()
                holding_tail = False
                while True:
                    data = response.read1(65536)
                    if not data:
                        attempt["transport_complete"] = True
                        break
                    attempt["body"].extend(data)
                    observed = bytes(attempt["body"])
                    terminal = (DONE in observed if attempt["mode"] == "done-fault"
                                else b'"finish_reason":"stop"' in observed)
                    if attempt["mode"] in ("done-fault", "finish-fault") and terminal:
                        attempt["fault_ready"].set()
                        # Hold only the terminal read and subsequent tail until
                        # cc-lb finishes. OMP may otherwise close immediately on
                        # DONE and turn the intended upstream fault into a race
                        # with downstream cancellation. Earlier reads stream.
                        holding_tail = True
                    if holding_tail:
                        held.extend(data)
                    else:
                        self.forward(attempt, data)
                if held:
                    self.forward(attempt, bytes(held))
                if not attempt["client_disconnected"]:
                    try:
                        self.wfile.write(b"0\r\n\r\n")
                        self.wfile.flush()
                    except OSError:
                        attempt["client_disconnected"] = True
            except Exception as error:
                attempt["transport_error"] = f"{type(error).__name__}: {error}"
                run.errors.append("tap: " + attempt["transport_error"])
            finally:
                if connection:
                    connection.close()
                self.close_connection = True
                run.active_attempt = None
                attempt["complete"].set()

    def forward(self, attempt, data):
        if attempt["client_disconnected"]:
            return  # Still drain and capture the proxy's complete response.
        try:
            self.finish_body(data)
            attempt["delivered_body"].extend(data)
        except OSError:
            attempt["client_disconnected"] = True


class Run:
    def __init__(self, args, binary, omp):
        self.args, self.binary, self.omp = args, binary, omp
        self.root = args.output
        self.ledger = Ledger(args.ledger)
        self.processes, self.servers, self.reservations = [], [], []
        self.process_listeners = []
        self.attempts, self.dispatches, self.errors = [], [], []
        self.forward_lock = threading.Lock()
        self.active_attempt = None
        self.mode = self.client = None
        self.secrets = [ADMIN_TOKEN, UPSTREAM_KEY, "01" * 32]
        self.report = {"schema_version": 1, "expected_contract": args.expect, "result": "FAIL",
                       "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                       "binary_source_sha": args.source_sha, "source_sha_provenance": "caller" if args.source_sha else "not supplied",
                       "omp_binary": str(omp), "cases": [], "errors": self.errors,
                       "tap_policy": "untouched request/body bytes; terminal read held until proxy EOF for deterministic upstream faults"}

    def environment(self, directory):
        directory.mkdir(parents=True, exist_ok=True)
        env = {key: value for key, value in os.environ.items()
               if key in ("PATH", "LANG", "LC_ALL", "TZ", "__CF_USER_TEXT_ENCODING")}
        for key, suffix in (("HOME", "home"), ("TMPDIR", "tmp"), ("XDG_CACHE_HOME", "cache"),
                            ("XDG_CONFIG_HOME", "config"), ("XDG_DATA_HOME", "data"),
                            ("PI_CODING_AGENT_DIR", "agent")):
            path = directory / suffix
            path.mkdir(exist_ok=True)
            env[key] = str(path)
        env["NO_PROXY"] = "127.0.0.1,localhost"
        return env

    def spawn(self, command, env, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
        proc = subprocess.Popen(command, cwd=self.root, env=env, stdin=subprocess.DEVNULL,
                                stdout=stdout, stderr=stderr, start_new_session=True)
        self.processes.append(proc)
        self.ledger.record("process-group", proc.pid,
                           f"kill -TERM -- -{proc.pid} # verify process start/command ownership first")
        return proc

    def communicate(self, proc, timeout):
        try:
            return proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.stop_process(proc)
            stdout, stderr = proc.communicate()
            raise AssertionError(f"process {proc.pid} exceeded {timeout}s; output retained where available") from None
        finally:
            if proc.poll() is not None:
                self.ledger.stopped("process-group", proc.pid)

    def stop_process(self, proc):
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=3)
        self.ledger.stopped("process-group", proc.pid)

    def start_server(self, handler, label):
        server = Server(("127.0.0.1", 0), handler)
        server.run = self
        identifier = f"pid={os.getpid()} 127.0.0.1:{server.server_port} {label}"
        self.servers.append((server, identifier))
        self.ledger.record("listener", identifier, f"stop owning harness PID {os.getpid()} after ownership verification")
        thread = threading.Thread(target=server.serve_forever, name=label, daemon=True)
        thread.start()
        return server

    def admin(self, path, body=None):
        status, data = request(self.admin_port, "/admin/v1/" + path, body, ADMIN_TOKEN)
        require(status in (200, 201), f"admin {path.split('?')[0]} returned {status}: {data[:200]!r}")
        return json.loads(data)

    def until(self, description, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while True:
            require(self.proc.poll() is None, "cc-lb exited; inspect server.log")
            value = predicate()
            if value:
                return value
            remaining = deadline - time.monotonic()
            require(remaining > 0, description + " timed out")
            threading.Event().wait(min(0.05, remaining))

    def start(self):
        env = self.environment(self.root / "server-environment")
        for name, command in (("binary_version", [str(self.binary), "--version"]),
                              ("omp_version", [str(self.omp), "--version"])):
            proc = self.spawn(command, env)
            stdout, stderr = self.communicate(proc, 15)
            self.report[name] = stdout.decode(errors="replace").strip()
            require(proc.returncode == 0, f"{name} failed: {stderr.decode(errors='replace')}")
        require("18.18.7" in self.report["omp_version"], "QA-10 requires installed OMP 18.18.7")
        upstream = self.start_server(Upstream, "chat-terminal-upstream")
        ports = []
        for label in ("proxy", "admin", "metrics"):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
            self.reservations.append((sock, label, port))
            self.ledger.record("port-reservation", f"pid={os.getpid()} 127.0.0.1:{port} {label}",
                               f"stop owning harness PID {os.getpid()} after ownership verification")
            ports.append(port)
        self.proxy_port, self.admin_port, self.metrics_port = ports
        self.db = self.root / "storage.sqlite"
        config = self.root / "config.toml"
        config.write_text(f'''[runtime]
data_dir = {json.dumps(str(self.root / "runtime"))}
[listener]
proxy_addr = "127.0.0.1:{self.proxy_port}"
admin_addr = "127.0.0.1:{self.admin_port}"
metrics_addr = "127.0.0.1:{self.metrics_port}"
[storage]
kind = "sqlite"
path = {json.dumps(str(self.db))}
[aead]
key_env = "CC_LB_MASTER_KEY"
[price_catalog]
cache_path = {json.dumps(str(self.root / "prices.json"))}
url = "http://127.0.0.1:{upstream.server_port}/catalog.json"
[[admin.auth.providers]]
kind = "static_token"
id = "chat-terminal"
token_env = "CC_LB_ADMIN_TOKEN"
''')
        for sock, label, port in self.reservations:
            sock.close()
            self.ledger.stopped("port-reservation", f"pid={os.getpid()} 127.0.0.1:{port} {label}")
        env.update(CC_LB_MASTER_KEY="01" * 32, CC_LB_ADMIN_TOKEN=ADMIN_TOKEN)
        with (self.root / "server.log").open("wb") as log:
            self.proc = self.spawn([str(self.binary), "serve", "--config", str(config)], env, log, log)
        for label, port in zip(("proxy", "admin", "metrics"), ports):
            identifier = f"pid={self.proc.pid} 127.0.0.1:{port} {label}"
            self.process_listeners.append(identifier)
            self.ledger.record("process-listener", identifier,
                               f"kill -TERM -- -{self.proc.pid} # verify process ownership first")

        def ready():
            try:
                return (request(self.proxy_port, "/healthz", timeout=0.3)[0] == 200
                        and request(self.admin_port, "/admin/v1/auth/session", token=ADMIN_TOKEN, timeout=0.3)[0] == 200)
            except OSError:
                return False
        self.until("server readiness", ready)
        self.upstream_id = self.admin("upstreams", {"name": "chat-terminal-loopback", "kind": "openai_api_key",
            "api_key_value": UPSTREAM_KEY, "base_url": f"http://127.0.0.1:{upstream.server_port}",
            "skip_preflight": True, "warmup_enabled": False})["id"]
        self.principal = self.admin("principals", {"name": "chat-terminal-loopback", "kind": "machine",
            "allowed_models": [MODEL], "allowed_upstreams": [self.upstream_id], "default_limits": []})["id"]
        self.key = self.admin(f"principals/{self.principal}/keys", {"label": "chat-terminal"})["plaintext_key"]
        self.secrets.append(self.key)

        def route_ready():
            status, data = request(self.admin_port, "/admin/v1/router/preview", {
                "principal_id": self.principal, "provider": "openai", "path": "/v1/chat/completions",
                "body": {"model": MODEL, "messages": [], "stream": True}}, ADMIN_TOKEN)
            require(status in (200, 404), f"unexpected route preview status {status}")
            return status == 200 and (json.loads(data).get("winner") or {}).get("upstream_id") == self.upstream_id
        self.until("Chat routing readiness", route_ready)
        require(not self.dispatches, "readiness dispatched a provider request")
        self.tap = self.start_server(Tap, "chat-terminal-tap")
        self.report.update(upstream_id=self.upstream_id, principal_id=self.principal,
                           proxy_port=self.proxy_port, tap_port=self.tap.server_port)

    def raw(self):
        status, body = request(self.tap.server_port, "/openai/v1/chat/completions", {
            "model": MODEL, "messages": [{"role": "user", "content": "Say hello"}],
            "stream": True, "stream_options": {"include_usage": True}}, self.key, timeout=20)
        require(status == 200, "raw HTTP status is not 200")
        return {"status": status, "body_hex": body.hex()}

    def actual_omp(self, name):
        env = self.environment(self.root / name)
        config = Path(env["PI_CODING_AGENT_DIR"]) / "models.yml"
        config.write_text(f'''providers:
  loopback:
    baseUrl: http://127.0.0.1:{self.tap.server_port}/openai/v1
    api: openai-completions
    apiKey: {self.key}
    models:
      - id: {MODEL}
        name: Loopback GPT
        api: openai-completions
        reasoning: false
        input: [text]
        contextWindow: 128000
        maxTokens: 32
''')
        command = [str(self.omp), "--model", "loopback/" + MODEL, "--no-tools", "--no-lsp",
                   "--no-extensions", "--no-skills", "--no-rules", "--no-context-files",
                   "--no-session", "--no-title", "--max-time", "12s", "-p", "Say hello"]
        proc = self.spawn(command, env)
        started = time.monotonic()
        timed_out = False
        try:
            stdout, stderr = proc.communicate(timeout=25)
        except subprocess.TimeoutExpired:
            timed_out = True
            self.stop_process(proc)
            stdout, stderr = proc.communicate()
        self.ledger.stopped("process-group", proc.pid)
        result = {"command": command, "returncode": proc.returncode, "wall_timeout": timed_out,
                  "elapsed_seconds": time.monotonic() - started,
                  "stdout": stdout.decode(errors="replace"), "stderr": stderr.decode(errors="replace")}
        self.save_json(self.root / (name + ".client.json"), result)
        return result

    def rows(self):
        fields = ("request_id", "upstream_id", "error_code", "list_status") + USAGE_FIELDS + DIAGNOSTIC_FIELDS
        with sqlite3.connect(f"file:{self.db}?mode=ro", uri=True, timeout=1) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT " + ",".join(fields) + " FROM request_events_v1")]

    def evidence(self, attempts):
        identifiers = {attempt["request_id"] for attempt in attempts}

        def committed():
            rows = self.rows()
            selected = [row for row in rows if row["request_id"] in identifiers]
            require(len(selected) <= len(identifiers), "duplicate durable request rows")
            return selected if len(selected) == len(identifiers) else None
        rows = self.until("every tap attempt's durable row", committed)
        by_id = {row["request_id"]: row for row in rows}
        projections = {}
        cursor = 0

        def projected():
            nonlocal cursor, rows, by_id
            page = self.admin("events/delta?" + urllib.parse.urlencode({"since_cursor": cursor, "limit": 500,
                              "principal_id": self.principal, "upstream_id": self.upstream_id}))
            for event in page.get("events", []):
                projections[event["request_id"]] = event
            cursor = page["next_cursor"]
            rows = [row for row in self.rows() if row["request_id"] in identifiers]
            by_id = {row["request_id"]: row for row in rows}
            return page["exhausted"] and len(rows) == len(identifiers) and all(
                identifier in projections
                and projections[identifier].get("error_code") == by_id[identifier]["error_code"]
                and projections[identifier].get("upstream_error_type") == by_id[identifier]["upstream_error_type"]
                and projections[identifier].get("upstream_stream_warning_type") == by_id[identifier]["upstream_stream_warning_type"]
                and projections[identifier].get("status") == by_id[identifier]["list_status"]
                for identifier in identifiers)
        self.until("matching admin event projections", projected)
        return by_id, projections

    def check_attempt(self, attempt, row):
        mode, old = attempt["mode"], self.args.expect == "old"
        body, upstream = bytes(attempt["body"]), fixture(mode)
        suffix = body[len(upstream):] if body.startswith(upstream) else b""
        errors = []
        for block in suffix.split(b"\n\n"):
            for line in block.splitlines():
                if line.startswith(b"data:"):
                    errors.append(json.loads(line[5:].strip()))
        native = (len(errors) == 1 and errors[0].get("error", {}).get("type") == "api_error"
                  and errors[0].get("error", {}).get("code") == "api_error"
                  and errors[0].get("error", {}).get("param") is None)
        fails = mode == "finish-fault" or (mode == "done-fault" and old) or (mode == "partial-eof" and not old)
        incomplete = mode == "partial-eof" and not old
        expected_type = "upstream_response_incomplete" if incomplete else "upstream_response_body_error" if fails else None
        warning = mode == "done-fault" and not old
        checks = {
            "http_200": attempt["status"] == row["list_status"] == 200,
            "sse_content_type": "text/event-stream" in attempt["headers"].get("content-type", ""),
            "clean_proxy_transport": attempt["transport_complete"] and attempt["transport_error"] is None,
            "original_prefix": body.startswith(upstream),
            "one_native_error_or_unchanged_body": native if fails else body == upstream,
            "durable_error_code": row["error_code"] == ("upstream_stream_error" if fails else None),
            "durable_error_type": row["upstream_error_type"] == expected_type,
            "durable_warning": (row["upstream_stream_warning_type"] == "upstream_response_body_error"
                                and bool(row["upstream_stream_warning_message"])) if warning
                               else row["upstream_stream_warning_type"] is None,
            "correct_upstream": row["upstream_id"] == self.upstream_id,
        }
        if mode in ("healthy", "done-fault"):
            checks["provider_usage_preserved"] = row["input_tokens"] == 7 and row["output_tokens"] == 2
        else:
            checks["unknown_usage_and_cost_not_invented"] = all(row[field] is None for field in USAGE_FIELDS)
        return checks

    def case(self, client, mode):
        self.client, self.mode = client, mode
        name = client + "-" + mode
        begin, dispatch_begin = len(self.attempts), len(self.dispatches)
        result = self.raw() if client == "raw" else self.actual_omp(name)
        attempts = self.attempts[begin:]
        require(attempts, name + ": no Chat request reached the tap")
        for attempt in attempts:
            require(attempt["complete"].wait(20), name + ": tap did not finish")
        dispatches = self.dispatches[dispatch_begin:]
        rows, projections = self.evidence(attempts)
        case = {"name": name, "client": client, "mode": mode, "client_result": result,
                "dispatch_count": len(dispatches), "attempts": [], "checks": {}}
        self.report["cases"].append(case)
        case["checks"]["one_dispatch_per_attempt"] = len(dispatches) == len(attempts) and all(
            sum(dispatch["request_id"] == attempt["request_id"] for dispatch in dispatches) == 1 for attempt in attempts)
        case["checks"]["unique_request_ids"] = len(rows) == len(attempts)
        if client == "raw":
            case["checks"]["one_raw_attempt"] = len(attempts) == 1
            case["checks"]["raw_body_matches_tap"] = result["body_hex"] == bytes(attempts[0]["body"]).hex()
        else:
            succeeds = mode in ("healthy", "done-fault", "finish-eof")
            case["checks"]["bounded_client_exit"] = not result["wall_timeout"]
            case["checks"]["omp_result"] = ((result["returncode"] == 0 and "hello" in result["stdout"])
                                                  if succeeds else result["returncode"] != 0)
            case["checks"]["successful_client_did_not_retry"] = not succeeds or len(attempts) == 1
            if mode == "partial-eof" and self.args.expect == "old":
                case["checks"]["baseline_omp_rejected_premature_eof"] = "before a finish_reason was received" in (result["stdout"] + result["stderr"])
                case["checks"]["baseline_omp_retried"] = len(attempts) > 1
        for index, attempt in enumerate(attempts, 1):
            identifier = attempt["request_id"]
            checks = self.check_attempt(attempt, rows[identifier])
            downstream_file = f"{name}.{index}.downstream.sse"
            delivered_file = f"{name}.{index}.delivered.sse"
            upstream_file = f"{name}.{index}.upstream.sse"
            (self.root / downstream_file).write_bytes(attempt["body"])
            (self.root / delivered_file).write_bytes(attempt["delivered_body"])
            (self.root / upstream_file).write_bytes(fixture(mode))
            case["attempts"].append({"request_id": identifier, "path": attempt["path"],
                "request_body_sha256": hashlib.sha256(attempt["request_body"]).hexdigest(),
                "status": attempt["status"], "headers": attempt["headers"],
                "response_bytes": len(attempt["body"]), "delivered_bytes": len(attempt["delivered_body"]),
                "downstream_file": downstream_file, "delivered_file": delivered_file, "upstream_file": upstream_file,
                "client_disconnected": attempt["client_disconnected"], "transport_complete": attempt["transport_complete"],
                "transport_error": attempt["transport_error"], "durable": rows[identifier],
                "admin_event": projections[identifier], "checks": checks,
                "dispatches": [dispatch for dispatch in dispatches if dispatch["request_id"] == identifier]})
        case["passed"] = all(case["checks"].values()) and all(all(attempt["checks"].values()) for attempt in case["attempts"])
        self.save_json(self.root / "report.json", self.report)

    def save_json(self, path, value):
        text = json.dumps(value, indent=2, sort_keys=True)
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        path.write_text(text + "\n")

    def execute(self):
        self.start()
        for client in ("raw", "omp"):
            for mode in MODES:
                self.case(client, mode)
        rows = self.rows()
        expected = {attempt["request_id"] for attempt in self.attempts}
        self.report["exactly_once"] = (len(rows) == len(expected) == len(self.dispatches)
                                          and {row["request_id"] for row in rows} == expected)
        require(not self.errors, "fixture errors: " + "; ".join(self.errors))
        require(self.report["exactly_once"], "final table-wide durable/dispatch count mismatch")
        require(all(case["passed"] for case in self.report["cases"]), "contract mismatch; inspect report.json checks")
        self.report["result"] = "OLD_DEFECTS_REPRODUCED" if self.args.expect == "old" else "PASS"

    def close(self):
        for proc in reversed(self.processes):
            self.stop_process(proc)
        for identifier in self.process_listeners:
            self.ledger.stopped("process-listener", identifier)
        for server, identifier in reversed(self.servers):
            server.shutdown()
            server.server_close()
            self.ledger.stopped("listener", identifier)
        for sock, _label, _port in self.reservations:
            sock.close()
        self.save_json(self.root / "report.json", self.report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="Existing absolute cc-lb binary")
    parser.add_argument("--output", required=True, type=Path, help="New evidence directory; never reused")
    parser.add_argument("--expect", required=True, choices=("old", "new"))
    parser.add_argument("--ledger", required=True, type=Path, help="External append-only cleanup ledger")
    parser.add_argument("--source-sha", default=os.environ.get("CC_LB_SOURCE_SHA"),
                        help="Caller-attested source commit used to build --binary (also CC_LB_SOURCE_SHA)")
    args = parser.parse_args()
    require(args.binary.is_absolute(), "--binary must be absolute")
    binary = args.binary.resolve(strict=True)
    require(binary.is_file() and os.access(binary, os.X_OK), "--binary must be executable")
    omp_path = shutil.which("omp")
    require(omp_path is not None, "installed omp is required; the harness installs nothing")
    args.output = args.output.expanduser().absolute()
    args.ledger = args.ledger.expanduser().absolute()
    require(not args.output.exists(), "--output must be a new directory")
    require(args.ledger.parent.is_dir(), "--ledger parent must already exist")
    require(not args.ledger.is_relative_to(args.output), "--ledger must be outside --output")
    args.output.mkdir()  # Parent scratch directory must already exist.
    ledger = Ledger(args.ledger)
    ledger.record("scratch-directory", args.output, "rm -rf -- " + shlex.quote(str(args.output)) + " # verify ownership first", "retained")
    print("Cleanup ledger:", args.ledger, flush=True)
    run = Run(args, binary, Path(omp_path).absolute())
    try:
        run.execute()
    except Exception as error:
        run.report["failure"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        run.close()
    print(json.dumps({"result": run.report["result"], "cases": len(run.report["cases"]),
                      "dispatch_count": len(run.dispatches), "report": str(args.output / "report.json"),
                      "retained_resources": [str(args.output)]}, sort_keys=True))


if __name__ == "__main__":
    main()
