#!/usr/bin/env python3
"""Exercise installed provider SDKs against cc-lb and a loopback-only provider."""

import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.metadata
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


MODELS = {"openai": "gpt-5.6", "gemini": "gemini-2.5-flash"}
RATES = {
    "input_cost_per_token": 2e-6,
    "output_cost_per_token": 1e-5,
    "cache_read_input_token_cost": 1e-7,
}
LEDGER = Path(tempfile.gettempdir()) / "cc-lb-cached-input-sdk-resources.jsonl"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def record(ledger, kind, identifier, delete, **details):
    entry = {"created_at": datetime.now(timezone.utc).isoformat(), "kind": kind,
             "id": str(identifier), "delete": delete, **details}
    # Append only: this ledger can contain resources owned by other workers.
    with ledger.open("a", encoding="utf-8") as output:
        output.write(json.dumps(entry, sort_keys=True) + "\n")


def loopback_only(event, args):
    if event == "socket.connect":
        address = args[1]
        require(isinstance(address, tuple) and ipaddress.ip_address(address[0]).is_loopback,
                "SDK attempted a non-loopback connection")
    elif event == "socket.getaddrinfo":
        require(args[0] in ("127.0.0.1", "::1", "localhost"),
                "SDK attempted non-loopback DNS resolution")


def wait_until(description, timeout, process, check):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"cc-lb exited ({process.returncode}) while waiting for {description}")
        try:
            result = check()
            if result:
                return result
        except (OSError, sqlite3.OperationalError) as error:
            last_error = str(error)
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for {description}; last error: {last_error}")


def catalog():
    return {model: {**RATES, "litellm_provider": provider, "mode": "chat",
                    "max_input_tokens": 131072, "max_output_tokens": 1024,
                    "max_tokens": 131072}
            for provider, model in MODELS.items()}


class Fixture(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), ProviderHandler)
        self.calls = []
        self.errors = []
        self.lock = threading.Lock()

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        return connection, address


class ProviderHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def send_payload(self, payload, content_type="application/json", status=200):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)
        self.close_connection = True

    def do_GET(self):
        if self.path == "/catalog.json":
            self.send_payload(json.dumps(catalog()).encode())
        else:
            self.send_payload(b'{"error":"unexpected fixture GET"}', status=404)

    def do_POST(self):
        try:
            self.handle_generation()
        except Exception as error:
            with self.server.lock:
                self.server.errors.append(str(error))
            self.send_payload(b'{"error":"invalid fixture request"}', status=400)

    def handle_generation(self):
        self.connection.settimeout(5)
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        path = urlsplit(self.path).path
        is_openai = path == "/v1/responses"
        is_gemini = path in (
            f"/v1beta/models/{MODELS['gemini']}:generateContent",
            f"/v1beta/models/{MODELS['gemini']}:streamGenerateContent",
        )
        require(is_openai or is_gemini, f"Unexpected fixture generation path: {path}")
        provider = "openai" if is_openai else "gemini"
        require(self.headers.get("Authorization") == "Bearer sk-loopback-openai-fixture"
                if is_openai else self.headers.get("x-goog-api-key") == "AIza-loopback-gemini-fixture",
                f"{provider} did not use the synthetic upstream credential")
        text = json.dumps(body)
        require(("pricing:hit" in text) != ("pricing:miss" in text), "Missing SDK scenario marker")
        cached = 20000 if "pricing:hit" in text else 0
        stream = bool(body.get("stream")) if is_openai else path.endswith(":streamGenerateContent")
        with self.server.lock:
            response_id = f"resp_sdk_{len(self.server.calls) + 1}"
            self.server.calls.append({"provider": provider, "path": path, "stream": stream,
                                      "cached": cached, "response_id": response_id})
        if is_openai:
            require(body.get("model") == MODELS[provider], "SDK OpenAI model mismatch")
            response = {
                "id": response_id, "object": "response", "created_at": 1700000000,
                "status": "completed", "model": MODELS[provider], "error": None,
                "incomplete_details": None,
                "output": [{"type": "message", "id": f"msg_{response_id}", "role": "assistant",
                            "status": "completed", "content": [
                                {"type": "output_text", "text": "ok", "annotations": []}]}],
                "usage": {"input_tokens": 21000, "input_tokens_details": {
                              "cached_tokens": cached, "cache_write_tokens": 0,
                              "cache_write_tokens_details": {
                                  "ttl_30m_tokens": 0, "ttl_12h_tokens": 0}},
                          "output_tokens": 1, "output_tokens_details": {"reasoning_tokens": 0},
                          "total_tokens": 21001},
            }
            if stream:
                events = [
                    {"type": "response.created", "sequence_number": 0,
                     "response": {**response, "status": "in_progress", "output": [], "usage": None}},
                    {"type": "response.output_text.delta", "sequence_number": 1,
                     "item_id": f"msg_{response_id}", "output_index": 0, "content_index": 0,
                     "delta": "ok", "logprobs": []},
                    {"type": "response.completed", "sequence_number": 2, "response": response},
                ]
                payload = "".join(f"event: {event['type']}\ndata: {json.dumps(event)}\n\n"
                                  for event in events).encode()
            else:
                payload = json.dumps(response).encode()
        else:
            response = {
                "responseId": response_id, "modelVersion": MODELS[provider],
                "candidates": [{"content": {"role": "model", "parts": [{"text": "ok"}]},
                                "finishReason": "STOP", "index": 0}],
                "usageMetadata": {"promptTokenCount": 21000, "cachedContentTokenCount": cached,
                                  "candidatesTokenCount": 1, "totalTokenCount": 21001},
            }
            payload = (f"data: {json.dumps(response)}\n\n" if stream
                       else json.dumps(response)).encode()
        self.send_payload(payload, "text/event-stream" if stream else "application/json")


def admin(opener, base, token, path, body=None):
    request = Request(base + path, data=None if body is None else json.dumps(body).encode(),
                      headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with opener.open(request, timeout=5) as response:
        return json.load(response)


def sdk_call(provider, stream, plaintext_key, proxy, cached):
    marker = "pricing:hit" if cached else "pricing:miss"
    if provider == "openai":
        from openai import OpenAI
        with OpenAI(api_key=plaintext_key, base_url=proxy + "/openai/v1",
                    organization="", project="", max_retries=0, timeout=15) as client:
            result = client.responses.create(
                model=MODELS[provider], input=[{"role": "user", "content": marker}],
                max_output_tokens=16, store=False, stream=stream)
            if stream:
                completed = []
                deltas = []
                with result:
                    for event in result:
                        if event.type == "response.output_text.delta":
                            deltas.append(event.delta)
                        if event.type == "response.completed":
                            completed.append(event.response)
                require("".join(deltas) == "ok" and len(completed) == 1,
                        "Responses SDK did not consume the actual SSE terminal event")
                result = completed[0]
            else:
                require(result.output_text == "ok", "Responses SDK output mismatch")
            usage = result.usage
            require((usage.input_tokens, usage.input_tokens_details.cached_tokens,
                     usage.output_tokens) == (21000, cached, 1), "Responses SDK usage mismatch")
    else:
        from google import genai
        from google.genai import types
        with genai.Client(vertexai=False, enterprise=False, api_key=plaintext_key,
                          http_options=types.HttpOptions(base_url=proxy + "/gemini",
                                                        api_version="v1beta", timeout=15000)) as client:
            config = types.GenerateContentConfig(max_output_tokens=16)
            if stream:
                with closing(client.models.generate_content_stream(
                        model=MODELS[provider], contents=marker, config=config)) as chunks:
                    results = list(chunks)
                require(len(results) == 1, "Gemini SDK did not consume the actual SSE response")
                result = results[0]
            else:
                result = client.models.generate_content(model=MODELS[provider], contents=marker,
                                                        config=config)
            require(result.text == "ok", "Gemini SDK output mismatch")
            usage = result.usage_metadata
            require((usage.prompt_token_count, usage.cached_content_token_count,
                     usage.candidates_token_count) == (21000, cached, 1), "Gemini SDK usage mismatch")
    return result.id if provider == "openai" else result.response_id


def read_evidence(db_path, key_id):
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1)) as db:
        db.row_factory = sqlite3.Row
        events = [dict(row) for row in db.execute(
            "SELECT request_id, key_id, principal_id, upstream_id, model, "
            "list_status AS status, input_tokens, cache_read_input_tokens, output_tokens, "
            "list_cost_input_micros AS cost_input_micros, "
            "list_cost_cache_read_micros AS cost_cache_read_micros, "
            "list_cost_output_micros AS cost_output_micros, "
            "list_cost_usd_micros AS cost_usd_micros "
            "FROM request_events_v1 WHERE key_id = ?", (key_id,))]
        buckets = [dict(zip(("width", "requests", "input", "output", "cost"), row))
                   for row in db.execute(
                       "SELECT bucket_width_secs, SUM(requests), SUM(input_tokens), "
                       "SUM(output_tokens), SUM(cost_usd_micros) FROM api_key_usage_buckets_v1 "
                       "WHERE key_id = ? GROUP BY bucket_width_secs", (key_id,))]
    if not events or not buckets:
        return None
    # Wait for final pricing and both configured durable cost windows, not an arbitrary delay.
    if any(event.get("cost_usd_micros") is None for event in events):
        return None
    if {bucket["width"] for bucket in buckets} != {60, 3600}:
        return None
    return events, buckets


def verify_evidence(events, buckets, key_id, principal_id, upstream_id, model, cached, total):
    require(len(events) == 1, f"Expected one terminal event, got {len(events)}")
    event = events[0]
    expected = {"key_id": key_id, "principal_id": principal_id, "upstream_id": upstream_id, "model": model,
                "status": 200, "input_tokens": 21000 - cached, "cache_read_input_tokens": cached,
                "output_tokens": 1, "cost_input_micros": (21000 - cached) * 2,
                "cost_cache_read_micros": total - (21000 - cached) * 2 - 10,
                "cost_output_micros": 10, "cost_usd_micros": total}
    for name, value in expected.items():
        require(event.get(name) == value,
                f"Event {name}: expected {value}, observed {event.get(name)!r}")
    for bucket in buckets:
        expected_bucket = {"width": bucket["width"], "requests": 1, "input": 21000 - cached,
                           "output": 1, "cost": total}
        require(bucket == expected_bucket,
                f"Durable per-key settlement: expected {expected_bucket}, observed {bucket}")
    return {name: event.get(name) for name in ("request_id", *expected)}


def stop_server(process, ledger):
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    record(ledger, "closed-process", process.pid, "none; process reaped", returncode=process.returncode)


def run(args):
    binary = Path(args.cc_lb_bin).expanduser().resolve()
    require(binary.is_file() and os.access(binary, os.X_OK), "CC_LB_BIN must be an executable real server")
    versions = {package: importlib.metadata.version(package) for package in ("openai", "google-genai")}
    ledger = args.ledger.expanduser().resolve()
    repository = Path(__file__).resolve().parents[3]
    require(not ledger.is_relative_to(repository), "Resource ledger must be outside the repository")
    require(ledger.parent.is_dir(), "Resource ledger parent directory must already exist")
    scratch = Path(tempfile.mkdtemp(prefix="cc-lb-cached-input-sdk-"))
    owner = secrets.token_hex(16)
    (scratch / ".sdk-harness-owner").write_text(owner)
    inode = scratch.stat().st_ino
    record(ledger, "scratch-directory", scratch, f"rm -rf -- {shlex.quote(str(scratch))}",
           owner=owner, inode=inode)
    print(f"Resource ledger: {ledger}; scratch: {scratch}", flush=True)
    # Neither SDK nor the child inherits a real credential, proxy, or cloud environment.
    environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(scratch),
                   "TMPDIR": str(scratch), "NO_PROXY": "127.0.0.1,localhost,::1"}
    os.environ.clear()
    os.environ.update(environment)
    sys.addaudithook(loopback_only)
    summary = {"sdk_versions": versions, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
               "expected_cached_total_micros": args.expected_cached_total, "cases": []}
    try:
        with ExitStack() as stack:
            fixture = Fixture()
            fixture_id = f"127.0.0.1:{fixture.server_port} (in-process provider fixture)"
            record(ledger, "loopback-fixture", fixture_id, "owned Fixture.shutdown/server_close + thread.join")
            fixture_thread = threading.Thread(target=fixture.serve_forever, daemon=True)
            fixture_thread.start()

            def close_fixture():
                fixture.shutdown()
                fixture.server_close()
                fixture_thread.join(timeout=5)
                require(not fixture_thread.is_alive(), "Fixture thread failed to stop")
                record(ledger, "closed-fixture", fixture_id, "none; socket closed and thread joined")

            stack.callback(close_fixture)
            # Keep all three port claims until immediately before exec; never use fixed ports.
            ports = []
            claims = []
            for _ in range(3):
                claim = socket.socket()
                stack.callback(claim.close)
                claim.bind(("127.0.0.1", 0))
                claims.append(claim)
                ports.append(claim.getsockname()[1])
                record(ledger, "port-claim", f"127.0.0.1:{ports[-1]} (pid {os.getpid()})",
                       "owned socket.close before cc-lb exec")
            proxy, admin_base = (f"http://127.0.0.1:{port}" for port in ports[:2])
            db_path = scratch / "storage.sqlite"
            price_path = scratch / "prices.json"
            price_path.write_text(json.dumps(catalog()))
            config = scratch / "config.toml"
            config.write_text(
                f'[runtime]\ndata_dir = {json.dumps(str(scratch / "data"))}\n'
                f'[listener]\nproxy_addr = "127.0.0.1:{ports[0]}"\n'
                f'admin_addr = "127.0.0.1:{ports[1]}"\nmetrics_addr = "127.0.0.1:{ports[2]}"\n'
                f'[storage]\nkind = "sqlite"\npath = {json.dumps(str(db_path))}\n'
                '[aead]\nkey_env = "CC_LB_MASTER_KEY"\n'
                f'[price_catalog]\ncache_path = {json.dumps(str(price_path))}\n'
                f'url = "http://127.0.0.1:{fixture.server_port}/catalog.json"\n'
                '[observability]\ntracing_level = "info"\n'
                '[[admin.auth.providers]]\nkind = "static_token"\nid = "sdk-fixture"\n'
                'token_env = "CC_LB_ADMIN_TOKEN"\n')
            token = secrets.token_hex(32)
            log = stack.enter_context((scratch / "server.log").open("w"))
            for claim in claims:
                record(ledger, "closed-port-claim",
                       f"127.0.0.1:{claim.getsockname()[1]} (pid {os.getpid()})",
                       "none; reservation socket closed")
                claim.close()
            process = subprocess.Popen([str(binary), "serve", "--config", str(config)], cwd=scratch,
                                       env={**environment, "CC_LB_MASTER_KEY": secrets.token_hex(32),
                                            "CC_LB_ADMIN_TOKEN": token}, stdin=subprocess.DEVNULL,
                                       stdout=log, stderr=subprocess.STDOUT)
            stack.callback(stop_server, process, ledger)
            record(ledger, "process", process.pid,
                   f"kill -TERM {process.pid} # only if command still matches {binary} serve --config {config}",
                   command=[str(binary), "serve", "--config", str(config)])
            opener = build_opener(ProxyHandler({}))

            def ready():
                with opener.open(admin_base + "/admin/health", timeout=1) as response:
                    health = response.status == 200
                log.flush()
                return health and "installed latest local LiteLLM price catalog" in (scratch / "server.log").read_text()

            wait_until("healthy real server with installed frozen catalog", args.timeout, process, ready)
            upstreams = {}
            for provider in MODELS:
                upstream = admin(opener, admin_base, token, "/admin/v1/upstreams", {
                    "name": f"sdk-loopback-{provider}", "kind": f"{provider}_api_key",
                    "base_url": f"http://127.0.0.1:{fixture.server_port}",
                    "api_key_value": "sk-loopback-openai-fixture" if provider == "openai"
                                     else "AIza-loopback-gemini-fixture",
                    "skip_preflight": True, "warmup_enabled": False})
                upstreams[provider] = upstream["id"]
            for provider, model in MODELS.items():
                for stream in (False, True):
                    for cached in (20000, 0):
                        label = f"{provider}-{'sse' if stream else 'json'}-{'hit' if cached else 'miss'}"
                        principal = admin(opener, admin_base, token, "/admin/v1/principals", {
                            "name": label, "kind": "machine", "allowed_models": [model],
                            "allowed_upstreams": [upstreams[provider]],
                            "default_limits": [{"kind": "cost_usd", "window_secs": window,
                                                "cap_micros": 100000000} for window in (3600, 604800)]})
                        key = admin(opener, admin_base, token,
                                    f"/admin/v1/principals/{principal['id']}/keys", {"label": label})
                        response_id = sdk_call(provider, stream, key["plaintext_key"], proxy, cached)
                        evidence = wait_until(label + " priced event and durable usage", args.timeout,
                                              process, lambda: read_evidence(db_path, key["key_id"]))
                        total = args.expected_cached_total if cached else 42010
                        event = verify_evidence(*evidence, key["key_id"], principal["id"],
                                                upstreams[provider], model, cached, total)
                        with fixture.lock:
                            require(not fixture.errors, f"Provider fixture errors: {fixture.errors}")
                            call = dict(fixture.calls[-1])
                        require(call["response_id"] == response_id and call["provider"] == provider
                                and call["stream"] == stream and call["cached"] == cached,
                                f"{label}: SDK response did not match the loopback provider call")
                        summary["cases"].append({"case": label, "sdk_response_id": response_id,
                                                 "event": event, "buckets": evidence[1], "wire": call})
                        print(f"PASS {label}: event and each per-key window = {total} micros", flush=True)
            require(len(fixture.calls) == 8, f"Expected eight SDK generations, got {len(fixture.calls)}")
            summary["coverage"] = {"openai_responses_json": True, "openai_responses_sse": True,
                                   "gemini_generate_content_json": True, "gemini_generate_content_sse": True,
                                   "responses_websocket": "not covered; requires a WebSocket provider fixture"}
            summary["verdict"] = "PASS"
    except BaseException as error:
        summary["verdict"] = "FAIL"
        summary["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (scratch / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2), flush=True)
        if args.cleanup:
            require(scratch.stat().st_ino == inode and (scratch / ".sdk-harness-owner").read_text() == owner,
                    "Refusing cleanup: scratch ownership changed")
            shutil.rmtree(scratch)
            record(ledger, "deleted-scratch-directory", scratch, "none; exact owned directory removed",
                   owner=owner, inode=inode)
        else:
            print(f"Retained evidence: {scratch}; cleanup entry: {ledger}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cc-lb-bin", default=os.environ.get("CC_LB_BIN"),
                        help="Existing real cc-lb binary (or set CC_LB_BIN)")
    parser.add_argument("--expected-cached-total", type=int, choices=(2010, 4010), default=4010,
                        help="4010 verifies the fix; 2010 records the pre-fix zero-cache-charge failure")
    parser.add_argument("--timeout", type=float, default=40,
                        help="Deadline in seconds for each readiness/persistence condition")
    parser.add_argument("--ledger", type=Path, default=LEDGER,
                        help="Append-only resource ledger outside the repository")
    parser.add_argument("--cleanup", action="store_true",
                        help="Explicitly delete only this run's owned scratch after closing all processes")
    args = parser.parse_args()
    if not args.cc_lb_bin:
        parser.error("--cc-lb-bin or CC_LB_BIN is required")
    require(args.timeout > 0, "--timeout must be positive")
    run(args)


if __name__ == "__main__":
    main()
