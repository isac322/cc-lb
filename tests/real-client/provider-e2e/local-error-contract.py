#!/usr/bin/env python3
"""Offline #1165 integration proof; use installed SDKs and an existing cc-lb binary."""
import argparse
import contextlib
import datetime
import http.client
import http.server
import importlib.metadata
import json
import os
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.parse

STREAM_MESSAGE = "upstream SSE response could not be safely processed"
BODY_MESSAGE = "upstream response body could not be read"
RESPONSE_ID = "resp_loopback_1165"
PROVIDER_MESSAGE = "loopback provider rejected the request"
PROVIDER_RATE_LIMIT_MESSAGE = "loopback provider rate limit exceeded"
CONTENT_LENGTH_FAILURE_MODE = "provider-failed-content-length-malformed"
SYNTHETIC_KEY = "loopback-no-provider-secret"
MALFORMED_COMPLETE_SUFFIX = b"event: response.completed\ndata: {invalid-json}\n\n"
ADMIN_TOKEN = "loopback-admin-only"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def frame(value):
    return (f"event: {value['type']}\ndata: " + json.dumps(value, separators=(",", ":")) + "\n\n").encode()


def response_value():
    return {
        "id": RESPONSE_ID, "object": "response", "created_at": 1,
        "status": "in_progress", "error": None, "incomplete_details": None,
        "instructions": None, "max_output_tokens": None, "model": "gpt-5.2",
        "output": [], "parallel_tool_calls": True, "previous_response_id": None,
        "reasoning": {"effort": None, "summary": None}, "store": False,
        "temperature": 1, "text": {"format": {"type": "text"}},
        "tool_choice": "auto", "tools": [], "top_p": 1,
        "truncation": "disabled", "usage": None, "user": None, "metadata": {},
    }


def stream_frames(mode):
    response = response_value()
    item = {"id": "msg_loopback", "type": "message", "role": "assistant", "status": "in_progress", "content": []}
    part = {"type": "output_text", "text": "", "annotations": [], "logprobs": []}
    values = [
        {"type": "response.created", "sequence_number": 0, "response": response},
        {"type": "response.output_item.added", "sequence_number": 1, "output_index": 0, "item": item},
        {"type": "response.content_part.added", "sequence_number": 2, "item_id": item["id"], "output_index": 0, "content_index": 0, "part": part},
        {"type": "response.output_text.delta", "sequence_number": 3, "item_id": item["id"], "output_index": 0, "content_index": 0, "delta": "partial", "logprobs": []},
    ]
    if mode == "healthy":
        part = dict(part, text="partial")
        item = dict(item, status="completed", content=[part])
        response = dict(response, status="completed", output=[item], usage={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2})
        values += [
            {"type": "response.output_text.done", "sequence_number": 4, "item_id": item["id"], "output_index": 0, "content_index": 0, "text": "partial", "logprobs": []},
            {"type": "response.content_part.done", "sequence_number": 5, "item_id": item["id"], "output_index": 0, "content_index": 0, "part": part},
            {"type": "response.output_item.done", "sequence_number": 6, "output_index": 0, "item": item},
            {"type": "response.completed", "sequence_number": 7, "response": response},
        ]
    elif mode in ("provider-failed", CONTENT_LENGTH_FAILURE_MODE):
        code, message = ("rate_limit_exceeded", PROVIDER_RATE_LIMIT_MESSAGE) if mode == CONTENT_LENGTH_FAILURE_MODE else ("server_error", PROVIDER_MESSAGE)
        values.append({"type": "response.failed", "sequence_number": 4, "response": dict(response, status="failed", error={"code": code, "message": message})})
    data = b"".join(map(frame, values))
    return data + MALFORMED_COMPLETE_SUFFIX if mode == CONTENT_LENGTH_FAILURE_MODE else data


class Upstream(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_POST(self):
        try:
            path = urllib.parse.urlsplit(self.path).path
            require(path in ("/v1/responses", "/v1beta/models/gemini-2.5-flash:generateContent"), "unexpected upstream path")
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            mode = self.server.mode
            streaming = body.get("stream", False)
            self.server.calls.append({"path": path, "stream": streaming, "mode": mode})
            if streaming:
                require(path == "/v1/responses", "only Responses SSE is exercised here")
                data = stream_frames(mode)
                self.server.last_body = data
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                if mode == CONTENT_LENGTH_FAILURE_MODE:
                    # One complete upstream body, with an exact length including
                    # the malformed suffix: only proxy suppression changes it.
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    self.wfile.flush()
                    self.server.calls[-1].update(content_length=len(data), body_bytes=len(data), body_writes=1)
                    return
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                # Flush normal deltas before the transport fault, as in the issue repro.
                for event in data.split(b"\n\n"):
                    if not event:
                        continue
                    event += b"\n\n"
                    self.wfile.write(f"{len(event):x}\r\n".encode() + event + b"\r\n")
                    self.wfile.flush()
                    time.sleep(0.05)
                if mode != "fault":
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                    return
            else:
                openai = path == "/v1/responses"
                status = 200
                if mode == "fault":
                    data = b'{"partial_upstream_body":'
                elif mode == "provider-failed":
                    status = 400
                    data = json.dumps({"error": {"message": PROVIDER_MESSAGE, "type": "invalid_request_error", "param": None, "code": "invalid_request"}} if openai else {"error": {"code": 400, "message": PROVIDER_MESSAGE, "status": "INVALID_ARGUMENT"}}).encode()
                else:
                    data = json.dumps(dict(response_value(), status="completed", usage={"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}) if openai else {"candidates": [{"content": {"role": "model", "parts": [{"text": "hello"}]}, "finishReason": "STOP", "index": 0}], "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2}}).encode()
                self.server.last_body = data
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data) + (1000 if mode == "fault" else 0)))
                self.end_headers()
                self.wfile.write(data)
                self.wfile.flush()
                if mode != "fault":
                    return
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            # Do not include headers or bodies in diagnostics.
            self.server.failure = type(error).__name__ + ": " + str(error)
            self.close_connection = True


def http_request(port, path, body=None, token=None, timeout=20):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        connection.request("POST" if body is not None else "GET", path, json.dumps(body) if body is not None else None, headers)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


class Ledger:
    def __init__(self, root):
        self.path = root / "cleanup-ledger.json"
        self.entries = []
        self.add("scratch-directory", str(root), "rm -rf -- " + shlex.quote(str(root)))

    def add(self, kind, identifier, delete_command):
        self.entries.append({"kind": kind, "identifier": identifier, "delete_command": delete_command, "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "state": "retained"})
        dump(self.path, self.entries)

    def stopped(self, identifier):
        for entry in self.entries:
            if entry["identifier"] == identifier:
                entry["state"] = "stopped"
        dump(self.path, self.entries)


class Run:
    def __init__(self, root, binary, expect, httpx, openai, genai, types):
        self.root, self.binary, self.expect = root, binary, expect
        self.httpx, self.openai, self.genai, self.types = httpx, openai, genai, types
        self.ledger = Ledger(root)
        self.report = {"expected_contract": expect, "sdk_versions": {name: importlib.metadata.version(name) for name in ("openai", "google-genai", "httpx2")}, "cases": [], "contract_failures": []}
        self.secrets = [SYNTHETIC_KEY, ADMIN_TOKEN]
        self.upstreams = {}
        self.captures = []
        self.proc = None
        self.server = None

    def admin(self, path, body=None, timeout=20):
        status, data = http_request(self.admin_port, "/admin/v1/" + path, body, ADMIN_TOKEN, timeout=timeout)
        require(status in (200, 201), f"admin {path.split('?')[0]} status {status}")
        return json.loads(data)

    def start(self):
        # Keep sockets bound until configuration is ready, minimizing allocation races.
        with contextlib.ExitStack() as stack:
            sockets = [stack.enter_context(socket.socket()) for _ in range(3)]
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            self.proxy_port, self.admin_port, self.metrics_port = [sock.getsockname()[1] for sock in sockets]
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Upstream)
        self.server.mode, self.server.calls, self.server.failure = "healthy", [], None
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.ledger.add("loopback-upstream", str(self.server.server_address), "owned by this harness; stopped in finally")
        self.db = self.root / "storage.sqlite"
        config = self.root / "config.toml"
        config.write_text(f'''[runtime]
data_dir = {json.dumps(str(self.root / "data"))}
[listener]
proxy_addr = "127.0.0.1:{self.proxy_port}"
admin_addr = "127.0.0.1:{self.admin_port}"
metrics_addr = "127.0.0.1:{self.metrics_port}"
[storage]
kind = "sqlite"
path = {json.dumps(str(self.db))}
[aead]
key_env = "CC_LB_MASTER_KEY"
[[admin.auth.providers]]
kind = "static_token"
id = "local"
token_env = "CC_LB_ADMIN_TOKEN"
''')
        # No provider or ambient SDK credentials are used by the child.
        env = {key: value for key, value in os.environ.items() if not key.startswith(("OPENAI_", "GEMINI_", "GOOGLE_", "OTEL_", "CC_LB_"))}
        env.update(CC_LB_MASTER_KEY="01" * 32, CC_LB_ADMIN_TOKEN=ADMIN_TOKEN)
        with (self.root / "server.log").open("w") as log:
            self.proc = subprocess.Popen([self.binary, "serve", "--config", str(config)], cwd=self.root, env=env, stdout=log, stderr=log)
        self.ledger.add("process", str(self.proc.pid), f"kill -TERM {self.proc.pid} # verify command and ownership first")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            require(self.proc.poll() is None, "cc-lb exited during startup; inspect retained server.log")
            try:
                proxy_ready = http_request(self.proxy_port, "/healthz", timeout=0.5)[0] == 200
                admin_ready = http_request(self.admin_port, "/admin/v1/auth/session", token=ADMIN_TOKEN, timeout=0.5)[0] == 200
                if proxy_ready and admin_ready:
                    break
            except OSError:
                pass
            time.sleep(0.05)
        else:
            raise AssertionError("cc-lb startup timed out")
        for provider in ("openai", "gemini"):
            value = self.admin("upstreams", {"name": "loopback-" + provider, "kind": provider + "_api_key", "api_key_value": SYNTHETIC_KEY, "base_url": f"http://127.0.0.1:{self.server.server_port}", "skip_preflight": True, "warmup_enabled": False})
            self.upstreams[provider] = value["id"]
        principal = self.admin("principals", {"name": "local-error-contract", "kind": "machine", "allowed_models": [], "allowed_upstreams": list(self.upstreams.values()), "default_limits": []})
        self.principal = principal["id"]
        self.key = self.admin(f"principals/{self.principal}/keys", {"label": "local-error-contract"})["plaintext_key"]
        self.secrets.append(self.key)
        self.base_url = f"http://127.0.0.1:{self.proxy_port}"
        self.report["upstreams"] = self.upstreams
        self.wait_for_routes()

    def wait_for_routes(self):
        pending = set(self.upstreams)
        deadline = time.monotonic() + 20
        while pending and time.monotonic() < deadline:
            require(self.proc.poll() is None, "cc-lb exited while waiting for routing readiness")
            for provider in tuple(pending):
                path = "/v1/responses" if provider == "openai" else "/v1beta/models/gemini-2.5-flash:generateContent"
                body = {"model": "gpt-5.2", "input": [], "store": False} if provider == "openai" else {"contents": []}
                status, data = http_request(self.admin_port, "/admin/v1/router/preview", {
                    "principal_id": self.principal, "provider": provider, "path": path, "body": body,
                }, ADMIN_TOKEN)
                require(status in (200, 404), f"routing readiness returned unexpected status {status}")
                preview = json.loads(data)
                if status == 200 and (preview.get("winner") or {}).get("upstream_id") == self.upstreams[provider]:
                    pending.remove(provider)
            if pending:
                time.sleep(0.05)
        require(not pending, "routing preview did not observe configured upstreams before deadline")
        require(not self.server.calls, "routing readiness unexpectedly dispatched an upstream request")

    def capture_client(self):
        httpx = self.httpx
        captures = self.captures
        port = self.proxy_port

        class CaptureStream(httpx.SyncByteStream):
            def __init__(self, original, capture):
                self.original, self.capture = original, capture

            def __iter__(self):
                try:
                    for chunk in self.original:
                        self.capture["body"].extend(chunk)
                        yield chunk
                    self.capture["body_complete"] = True
                except httpx.HTTPError as error:
                    self.capture["transport_error"] = {"exception": type(error).__name__, "message": str(error)}
                    raise

            def close(self):
                self.original.close()

        class LoopbackTransport(httpx.BaseTransport):
            def __init__(self):
                self.transport = httpx.HTTPTransport(retries=0, trust_env=False)

            def handle_request(self, request):
                require(request.url.host == "127.0.0.1" and request.url.port == port and request.url.scheme == "http", "SDK attempted a non-loopback request")
                response = self.transport.handle_request(request)
                id_headers = {name: response.headers.get(name) for name in ("request-id", "x-request-id")}
                id_source = next((name for name, value in id_headers.items() if value), None)
                capture = {"status": response.status_code, "request_id": id_headers[id_source] if id_source else None, "request_id_source": id_source, "request_id_headers": id_headers, "content_type": response.headers.get("content-type", ""), "headers": dict(response.headers), "body": bytearray(), "body_complete": False, "transport_error": None}
                captures.append(capture)
                response.stream = CaptureStream(response.stream, capture)
                return response

            def close(self):
                self.transport.close()

        return httpx.Client(transport=LoopbackTransport(), timeout=20, trust_env=False)

    def sdk_request(self, provider, streaming, observe_transport_error=False):
        before = len(self.captures)
        result = {"events": [], "message": None, "exception": None, "native_error": None}
        with self.capture_client() as http_client:
            if provider == "openai":
                with self.openai.OpenAI(api_key=self.key, organization="", project="", base_url=self.base_url + "/openai/v1", max_retries=0, http_client=http_client) as client:
                    try:
                        value = client.responses.create(model="gpt-5.2", input=[{"role": "user", "content": [{"type": "input_text", "text": "Say partial"}]}], store=False, stream=streaming)
                        if streaming:
                            with value:
                                for event in value:
                                    event_type = getattr(event, "type", None)
                                    result["events"].append(event_type)
                                    if event_type == "response.failed":
                                        result["native_error"] = event.response.error.model_dump()
                                        result["message"] = event.response.error.message
                        else:
                            require(value.status == "completed", "healthy OpenAI SDK did not parse completion")
                    except self.openai.APIError as error:
                        result["exception"] = type(error).__name__
                        result["message"] = error.message
                        result["native_error"] = error.body
                    except self.httpx.HTTPError as error:
                        if not observe_transport_error:
                            raise
                        result["exception"] = type(error).__name__
                        result["transport_error"] = {"exception": type(error).__name__, "message": str(error)}
            else:
                options = self.types.HttpOptions(base_url=self.base_url + "/gemini", api_version="v1beta", headers={"Authorization": "Bearer " + self.key}, httpx_client=http_client, retry_options=self.types.HttpRetryOptions(attempts=1))
                with self.genai.Client(api_key=self.key, vertexai=False, http_options=options) as client:
                    try:
                        value = client.models.generate_content(model="gemini-2.5-flash", contents="Say hello")
                        require(value.text == "hello", "healthy Gemini SDK did not parse output")
                    except self.genai.errors.APIError as error:
                        result.update(exception=type(error).__name__, message=error.message, native_error={"code": error.code, "status": error.status, "message": error.message})
        require(len(self.captures) == before + 1, "SDK retried or did not issue exactly one request")
        return result, self.captures[-1]

    def durable_event(self, request_id, provider, baseline):
        deadline = time.monotonic() + 20
        rows = []
        while time.monotonic() < deadline:
            with sqlite3.connect(f"file:{self.db}?mode=ro", uri=True) as db:
                if request_id:
                    rows = db.execute("SELECT request_id, upstream_id, error_code, list_status FROM request_events_v1 WHERE id > ? AND request_id = ?", (baseline, request_id)).fetchall()
                else:
                    # The process/database are isolated and requests run serially.
                    # Correlate only when exactly one new committed row exists.
                    rows = db.execute("SELECT request_id, upstream_id, error_code, list_status FROM request_events_v1 WHERE id > ?", (baseline,)).fetchall()
            if rows:
                break
            time.sleep(0.1)
        require(len(rows) == 1, f"expected one durable row, found {len(rows)}")
        request_id, upstream_id, error_code, status = rows[0]
        require(upstream_id == self.upstreams[provider], "durable row selected wrong upstream")
        # Match qa-runtime.mjs::waitForRequestEvents: admin projection visibility
        # is observed with a deadline, not inferred from a SQLite commit.
        deadline = time.monotonic() + 30
        cursor = 0
        by_id = {}
        update_count = 0
        expected_ids = {case["request_id"] for case in self.report["cases"] if case["durable"]["upstream_id"] == self.upstreams[provider]} | {request_id}
        while time.monotonic() < deadline:
            page = self.admin("events/delta?" + urllib.parse.urlencode({"since_cursor": cursor, "limit": 500, "principal_id": self.principal, "upstream_id": self.upstreams[provider]}), timeout=max(0.001, min(2, deadline - time.monotonic())))
            for event in page.get("events", []):
                require(event["request_id"] in expected_ids, "admin delta exposed an unexpected distinct request ID")
                by_id[event["request_id"]] = event
                if event["request_id"] == request_id:
                    update_count += 1
            cursor = page["next_cursor"]
            # A row can receive final lifecycle fields after its initial insert.
            # Compare against the current durable state, not a stale first read.
            with sqlite3.connect(f"file:{self.db}?mode=ro", uri=True) as db:
                rows = db.execute("SELECT request_id, upstream_id, error_code, list_status FROM request_events_v1 WHERE id > ? AND request_id = ?", (baseline, request_id)).fetchall()
            require(len(rows) == 1, "durable request disappeared or became duplicated")
            request_id, upstream_id, error_code, status = rows[0]
            require(upstream_id == self.upstreams[provider], "durable row selected wrong upstream")
            visible = by_id.get(request_id)
            if page["exhausted"] and visible and visible["upstream_id"] == upstream_id and visible.get("error_code") == error_code and visible["status"] == status:
                return {"request_id": request_id, "upstream_id": upstream_id, "error_code": error_code, "status": status, "admin_event": visible, "admin_matching_updates": update_count}
            if not page["exhausted"]:
                continue
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(0.1, remaining))
        raise AssertionError("admin delta did not expose one request consistent with durable upstream/error/status before deadline")

    def contract(self, label, valid):
        if not valid:
            self.report["contract_failures"].append(label)

    def case(self, provider, streaming, mode):
        name = f"{provider}-{'stream' if streaming else 'buffered'}-{mode}"
        self.server.mode = mode
        before = len(self.server.calls)
        with sqlite3.connect(f"file:{self.db}?mode=ro", uri=True) as db:
            baseline = db.execute("SELECT COALESCE(MAX(id), 0) FROM request_events_v1").fetchone()[0]
        sdk, capture = self.sdk_request(provider, streaming, observe_transport_error=mode == CONTENT_LENGTH_FAILURE_MODE)
        require(self.server.failure is None, self.server.failure or "upstream failed")
        require(len(self.server.calls) == before + 1, name + ": expected exactly one upstream request")
        body = bytes(capture.pop("body"))
        (self.root / (name + (".sse" if streaming else ".json"))).write_bytes(body)
        for secret in self.secrets:
            require(secret.encode() not in body, name + ": credential leaked in response")
        event = self.durable_event(capture["request_id"], provider, baseline)
        if capture["request_id"] is None:
            capture["request_id"] = event["request_id"]
            capture["request_id_source"] = "durable/admin-single-dispatch"
        evidence = {"name": name, **capture, "sdk": sdk, "durable": event}
        self.report["cases"].append(evidence)
        if streaming:
            require("text/event-stream" in capture["content_type"], name + ": wrong SSE content type")
        if mode == CONTENT_LENGTH_FAILURE_MODE:
            expected_body = self.server.last_body[:-len(MALFORMED_COMPLETE_SUFFIX)]
            call = self.server.calls[-1]
            require(call["content_length"] == call["body_bytes"] == len(self.server.last_body) and call["body_writes"] == 1, name + ": upstream did not send one exactly framed body")
            (self.root / (name + ".upstream.sse")).write_bytes(self.server.last_body)
            evidence["upstream"] = call
            evidence["body_bytes"] = len(body)
            content_length = capture["headers"].get("content-length")
            checks = {
                "http_status": capture["status"] == 200,
                "transport_complete": capture["body_complete"] and capture["transport_error"] is None and sdk["exception"] is None,
                "downstream_framing": content_length is None or content_length == str(len(body)),
                "unchanged_provider_prefix": body == expected_body,
                "single_provider_terminal": sdk["events"].count("response.failed") == 1 and "response.completed" not in sdk["events"] and "error" not in sdk["events"],
                "provider_error": (sdk["native_error"] or {}).get("code") == "rate_limit_exceeded" and sdk["message"] == PROVIDER_RATE_LIMIT_MESSAGE,
                "durable_failure": event["error_code"] == "upstream_stream_error" and event["status"] == 200,
                "durable_provider_error": event["admin_event"].get("upstream_error_type") == "rate_limit_exceeded" and event["admin_event"].get("upstream_error_message") == PROVIDER_RATE_LIMIT_MESSAGE,
            }
            evidence["wire_contract_checks"] = checks
            evidence["wire_contract_passed"] = all(checks.values())
            self.contract(name, evidence["wire_contract_passed"])
            if self.expect == "old":
                # Baseline runs record actual diagnostics without pinning the
                # oracle to one SDK exception class or transport message.
                evidence["baseline_observation"] = "PASS" if evidence["wire_contract_passed"] else "FAIL"
            return
        if mode == "healthy":
            require(capture["status"] == 200, name + ": wrong HTTP status")
            require(sdk["exception"] is None, name + ": SDK rejected healthy control")
            require(body == self.server.last_body, name + ": healthy bytes changed")
            require(not event["error_code"], name + ": healthy control has error accounting")
            if streaming:
                require("response.completed" in sdk["events"], "SDK lost healthy completion")
        elif mode == "provider-failed":
            require(body == self.server.last_body, name + ": provider failure was rewritten")
            require(PROVIDER_MESSAGE in (sdk["message"] or ""), name + ": SDK lost provider message")
            require(event["error_code"], name + ": missing durable provider failure")
        elif streaming:
            require(capture["status"] == 200, name + ": streaming status changed after start")
            require(event["error_code"] == "upstream_stream_error", name + ": wrong durable stream classification")
            require(body.endswith(b"\n\n"), name + ": broken SSE boundary")
            blocks = body.decode().split("\n\n")
            frames = []
            for block in filter(None, blocks):
                lines = block.splitlines()
                values = [line[6:] for line in lines if line.startswith("data: ")]
                require(len(values) == 1, name + ": malformed SSE data boundary")
                value = json.loads(values[0])
                frames.append((next((line[7:] for line in lines if line.startswith("event: ")), None), value))
            require(any(value.get("type") == "response.output_text.delta" for _, value in frames), "missing normal output before fault")
            require(not any(value.get("type") == "response.completed" for _, value in frames), "fault emitted completion")
            terminal = [(kind, value) for kind, value in frames if kind in ("response.failed", "error")]
            require(len(terminal) == 1, "expected exactly one stream terminal error")
            kind, value = terminal[0]
            native = kind == "response.failed" and value.get("type") == "response.failed"
            if native:
                response = value.get("response", {})
                sequence = value.get("sequence_number")
                native = (type(sequence) is int and sequence > 3 and response.get("id") == RESPONSE_ID and response.get("status") == "failed" and response.get("error", {}).get("code") == "api_error" and response.get("error", {}).get("message") == STREAM_MESSAGE and sdk["message"] == STREAM_MESSAGE)
            self.contract(name, native)
            if self.expect == "old":
                require(kind == "error" and "type" not in value and value["error"]["message"] == STREAM_MESSAGE, "old mode no longer reproduces the old HTTP-envelope defect")
            (self.root / "responses-fault.sse").write_bytes(body)
        else:
            require(capture["status"] == 502 and "application/json" in capture["content_type"], name + ": wrong buffered HTTP contract")
            require(b"partial_upstream_body" not in body, name + ": partial body leaked")
            value = json.loads(body)
            error = value.get("error", {})
            require(error.get("message") == BODY_MESSAGE and BODY_MESSAGE in (sdk["message"] or ""), name + ": SDK lost safe error message")
            require(event["error_code"] == "upstream_stream_error", name + ": wrong durable body classification")
            native = "type" not in value
            if provider == "openai":
                native = native and error.get("type") == "api_error" and error.get("code") == "api_error" and "param" in error
            else:
                native = native and error.get("code") == 502 and error.get("status") == "UNAVAILABLE" and sdk["native_error"]["code"] == 502 and sdk["native_error"]["status"] == "UNAVAILABLE"
            self.contract(name, native)
            if self.expect == "old":
                require(value.get("type") == "error" and error.get("type") == "api_error" and "code" not in error, "old mode no longer reproduces the Anthropic-wrapper defect")

    def execute(self):
        self.report["binary_version"] = subprocess.check_output([self.binary, "--version"], text=True).strip()
        self.start()
        for mode in ("healthy", "fault", "provider-failed"):
            self.case("openai", True, mode)
            self.case("openai", False, mode)
            self.case("gemini", False, mode)
        self.case("openai", True, CONTENT_LENGTH_FAILURE_MODE)
        # Re-read all durable rows after every request, catching delayed duplicates.
        with sqlite3.connect(f"file:{self.db}?mode=ro", uri=True) as db:
            counts = db.execute("SELECT request_id, upstream_id, COUNT(*) FROM request_events_v1 GROUP BY request_id, upstream_id").fetchall()
        require(len(counts) == 10 and all(count == 1 for _, _, count in counts), "durable accounting is not exactly once across all ten request/upstream identities")
        expected_failures = ["openai-stream-fault", "openai-buffered-fault", "gemini-buffered-fault"] if self.expect == "old" else []
        if self.expect == "old":
            expected_failures.append("openai-stream-" + CONTENT_LENGTH_FAILURE_MODE)
        require(self.report["contract_failures"] == expected_failures, "native contracts differ from --expect; inspect report.json")
        self.report["result"] = "PASS" if self.expect == "new" else "OLD_DEFECTS_REPRODUCED"

    def close(self):
        if self.proc is not None:
            if self.proc.poll() is None:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait()
            self.ledger.stopped(str(self.proc.pid))
        if self.server is not None:
            identifier = str(self.server.server_address)
            self.server.shutdown()
            self.server.server_close()
            self.ledger.stopped(identifier)
        # Evidence is deliberately retained; cleanup requires the caller's signal.
        text = json.dumps(self.report, indent=2, sort_keys=True)
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        (self.root / "report.json").write_text(text + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expect", choices=("old", "new"), default="new", help="new is the regression gate; old proves the three original defects and the Content-Length framing defect")
    parser.add_argument("--output-parent", type=Path, default=Path(tempfile.gettempdir()), help="existing scratch parent outside the repository; a fresh run directory is always created")
    args = parser.parse_args()
    binary = os.environ.get("CC_LB_BIN")
    require(binary and Path(binary).is_file(), "set CC_LB_BIN to an existing cc-lb executable")
    binary = str(Path(binary).resolve())
    # Fail before creating resources if SDK dependencies are unavailable.
    import httpx2 as httpx
    import openai
    from google import genai
    from google.genai import types
    require("httpx_client" in types.HttpOptions.model_fields, "google-genai must support HttpOptions.httpx_client")
    parent = args.output_parent.resolve()
    repo = Path(__file__).resolve().parents[3]
    require(not parent.is_relative_to(repo), "--output-parent must be outside the repository")
    root = Path(tempfile.mkdtemp(prefix="cc-lb-1165-", dir=parent))
    run = Run(root, binary, args.expect, httpx, openai, genai, types)
    print("Evidence:", root)
    print("Cleanup ledger:", run.ledger.path)
    try:
        run.execute()
    finally:
        run.close()
    print(run.report["result"], f"{len(run.report['cases'])} loopback requests; no provider calls")
    print("Replay fixture:", root / "responses-fault.sse")


if __name__ == "__main__":
    main()
