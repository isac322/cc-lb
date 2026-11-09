#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["openai==3.46.0"]
# ///
"""Local-only SDK regression. Run with CC_LB_BIN=/path/to/cc-lb uv run this-file.

CC_LB_QA_RUNROOT and CC_LB_QA_LEDGER optionally select retained evidence and the
parent's cleanup ledger. The script stops its server/mock but retains evidence.
Native cached-token usage is a fixed provider fixture, not a tokenizer claim.
"""

from __future__ import annotations

import json
import os
import shlex
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from openai import OpenAI

TOKEN = "sdk-admin-token"
PROVIDER_KEY = "sk-openai-sdk-cache-test"
INPUT = [
    {"role": "user", "content": [{"type": "input_text", "text": "stable prefix " * 1400}]},
    {"role": "user", "content": [{"type": "input_text", "text": "separate follow-up turn"}]},
]


def record_resource(ledger: Path, kind: str, identifier: str, delete: str) -> None:
    with ledger.open("a", encoding="utf-8") as out:
        out.write(json.dumps({"kind": kind, "identifier": identifier, "delete_command": delete,
                              "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}) + "\n")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def tool_input(ttl: str | None) -> list[dict]:
    marker = {"mode": "explicit"}
    if ttl is not None:
        marker["ttl"] = ttl
    return [
        INPUT[0],
        {"type": "function_call", "call_id": "call_owned_1", "name": "lookup", "arguments": "{}"},
        {"type": "function_call_output", "call_id": "call_owned_1", "output": [
            {"type": "input_text", "text": "stable tool result", "prompt_cache_breakpoint": marker},
            {"type": "input_text", "text": "uncached suffix"},
        ]},
        INPUT[1],
    ]


class Mock(BaseHTTPRequestHandler):
    lock = threading.Lock()
    wires: list[dict] = []
    errors: list[str] = []

    def do_POST(self) -> None:  # noqa: N802
        try:
            assert self.path == "/v1/responses", self.path
            assert self.headers.get("Authorization") == f"Bearer {PROVIDER_KEY}"
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            body = json.loads(raw)
            assert body["model"] == "gpt-5.6"
            assert body["input"] in (INPUT, tool_input("30m"), tool_input("12h"), tool_input(None))
            assert type(body["stream"]) is bool
            assert body["max_output_tokens"] == 16
            assert body["store"] is False
            assert "prompt_cache_retention" not in body
            if "prompt_cache_options" in body:
                assert body["prompt_cache_options"] in (
                    {"ttl": "30m"},
                    {"mode": "explicit", "ttl": "12h"},
                    {"mode": "implicit", "ttl": "30m"},
                    {"mode": "explicit", "ttl": "30m"},
                )
            with self.lock:
                self.wires.append(body)
            response = {
                "id": "resp_sdk_cache", "object": "response", "created_at": 1800000000,
                "status": "completed", "model": "gpt-5.6", "error": None,
                "incomplete_details": None, "store": False,
                "output": [{"type": "message", "id": "msg_sdk_cache", "role": "assistant",
                            "status": "completed", "content": [{"type": "output_text",
                            "text": "sdk-ok", "annotations": []}]}],
                "usage": {"input_tokens": 4096, "output_tokens": 1, "total_tokens": 4097,
                          "input_tokens_details": {"cached_tokens": 2048},
                          "output_tokens_details": {"reasoning_tokens": 0}},
            }
            if body["stream"]:
                payload = ("event: response.completed\ndata: " + json.dumps({
                    "type": "response.completed", "sequence_number": 0, "response": response
                }) + "\n\n").encode()
                content_type = "text/event-stream"
            else:
                payload = json.dumps(response).encode()
                content_type = "application/json"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        except Exception as error:
            with self.lock:
                self.errors.append(repr(error))
            self.send_error(400, "Responses wire contract rejected")

    def log_message(self, *_args: object) -> None:
        pass


def admin(port: int, method: str, path: str, body: dict | None = None) -> dict:
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method,
                                     data=None if body is None else json.dumps(body).encode())
    request.add_header("Authorization", f"Bearer {TOKEN}")
    request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def eventually(probe, description: str, timeout: int = 20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = probe()
            if last:
                return last
        except (OSError, urllib.error.URLError, sqlite3.OperationalError) as error:
            last = str(error)
        time.sleep(0.02)  # bounded polling of an exact readiness/durable-state predicate
    raise AssertionError(f"timed out: {description}; last={last}")


def main() -> None:
    binary = os.environ.get("CC_LB_BIN")
    if not binary:
        raise SystemExit("CC_LB_BIN is required")
    supplied_root = os.environ.get("CC_LB_QA_RUNROOT")
    root = Path(supplied_root) if supplied_root else Path(tempfile.mkdtemp(prefix="cc-lb-openai-sdk-"))
    root.mkdir(parents=True, exist_ok=True)
    ledger = Path(os.environ.get("CC_LB_QA_LEDGER", str(root / "cleanup-ledger.jsonl")))
    record_resource(ledger, "scratch", str(root), f"rm -rf -- {shlex.quote(str(root))}")
    print(f"evidence={root} cleanup_ledger={ledger}", flush=True)
    provider = ThreadingHTTPServer(("127.0.0.1", 0), Mock)
    thread = threading.Thread(target=provider.serve_forever, daemon=True)
    thread.start()
    record_resource(ledger, "listener", f"this-process:127.0.0.1:{provider.server_port}", "closed in script finally")
    proxy, admin_port, metrics = free_port(), free_port(), free_port()
    db = root / "cc-lb.sqlite"
    config = root / "cc-lb.toml"
    price_catalog = root / "price-catalog.json"
    config.write_text(f'''[listener]
proxy_addr = "127.0.0.1:{proxy}"
admin_addr = "127.0.0.1:{admin_port}"
metrics_addr = "127.0.0.1:{metrics}"
[runtime]
data_dir = "{root}"
[storage]
kind = "sqlite"
path = "{db}"
[aead]
key_env = "CC_LB_MASTER_KEY"
[price_catalog]
url = "http://127.0.0.1:9/price-catalog.json"
cache_path = "{price_catalog}"
[[admin.auth.providers]]
kind = "static_token"
id = "sdk-test"
token_env = "CC_LB_ADMIN_TOKEN"
''')
    process = None
    client = None
    stdout = (root / "server.stdout").open("w")
    stderr = (root / "server.stderr").open("w")
    try:
        process = subprocess.Popen([binary, "serve", "--config", str(config)], stdout=stdout,
            stderr=stderr, env={**os.environ, "CC_LB_MASTER_KEY": "0" * 64, "CC_LB_ADMIN_TOKEN": TOKEN})
        record_resource(ledger, "process", str(process.pid), f"kill {process.pid}")
        eventually(lambda: admin(admin_port, "GET", "/admin/health"), "admin ready", 30)
        upstream = admin(admin_port, "POST", "/admin/v1/upstreams", {
            "name": "sdk-openai", "kind": "openai_api_key",
            "base_url": f"http://127.0.0.1:{provider.server_port}/", "api_key_value": PROVIDER_KEY,
            "skip_preflight": True})
        principal = admin(admin_port, "POST", "/admin/v1/principals", {
            "name": "sdk-cache", "kind": "machine", "allowed_upstreams": [upstream["id"]],
            "cache_keepalive": {"enabled": True, "max_refreshes_per_session": 20,
                "max_total_duration_secs": 7200, "min_cache_tokens": 1024,
                "refresh_lead_time_5m_secs": 30,
                "classifier": {"openai_responses_enabled": True}}})
        principal_id = principal["id"]
        key = admin(admin_port, "POST", f"/admin/v1/principals/{principal_id}/keys", {"label": "sdk"})
        client = OpenAI(api_key=key["plaintext_key"], base_url=f"http://127.0.0.1:{proxy}/v1",
                        max_retries=0, timeout=15)
        with sqlite3.connect(db) as connection:
            cases = [
                (INPUT, None, False, "30m"),
                (INPUT, {"ttl": "30m"}, False, "30m"),
                (INPUT, None, True, "30m"),
                (INPUT, {"ttl": "30m"}, True, "30m"),
                (tool_input("30m"), {"mode": "explicit", "ttl": "12h"}, False, "30m"),
                (tool_input("12h"), {"mode": "implicit", "ttl": "30m"}, False, "12h"),
                (tool_input(None), {"mode": "explicit", "ttl": "12h"}, True, "12h"),
                (tool_input(None), {"mode": "explicit", "ttl": "30m"}, True, "30m"),
            ]
            for index, (input_value, options, stream, expected_ttl) in enumerate(cases):
                session = f"00000000-0000-7000-8000-{index + 400:012}"
                expected = {"model": "gpt-5.6", "input": input_value, "stream": stream,
                            "max_output_tokens": 16, "store": False}
                if options is not None:
                    expected["prompt_cache_options"] = options
                response = client.responses.create(**expected, extra_headers={"x-session-id": session})
                if stream:
                    completed = [event.response for event in response if event.type == "response.completed"]
                    assert len(completed) == 1
                    result = completed[0]
                else:
                    result = response
                assert result.output_text == "sdk-ok"
                assert result.usage.input_tokens_details.cached_tokens == 2048
                assert Mock.wires[index] == expected, (Mock.wires[index], expected)
                if expected_ttl == "12h":
                    def decision_row():
                        rows = connection.execute(
                            "SELECT session_key_hash, ttl FROM cache_keepalive_decisions "
                            "WHERE principal_id = ? AND decision = 'not_tracked'",
                            (principal_id,),
                        ).fetchall()
                        return next((row for row in rows if session in row[0]), None)
                    decision = eventually(decision_row, f"SDK case {index} 12h not-tracked decision")
                    assert decision[1] == "12h", decision
                    active = connection.execute(
                        "SELECT COUNT(*) FROM cache_keepalive_sessions "
                        "WHERE session_key_hash = ? AND status = 'active'", (decision[0],)
                    ).fetchone()[0]
                    assert active == 0, "12h tool-output boundary must not schedule renewal"
                    with sqlite3.connect(db.with_suffix(".scheduler.sqlite")) as scheduler:
                        jobs = scheduler.execute(
                            "SELECT COUNT(*) FROM Jobs WHERE job_type = 'cache_keepalive' "
                            "AND idempotency_key LIKE ?", (f"%{decision[0]}%",)
                        ).fetchone()[0]
                        assert jobs == 0, "12h tool-output boundary must not enqueue renewal jobs"
                    rows = admin(admin_port, "GET", f"/admin/v1/principals/{principal_id}/cache-keepalive")["rows"]
                    assert any(item["session_key_hash"] == decision[0]
                               and item["state"] == "not_tracked" and item["ttl"] == "12h" for item in rows)
                    continue
                # SessionKey uses principal UUID plus normalized thread id; query this
                # case by its unique durable session suffix, never a prior case's row.
                def session_row():
                    rows = connection.execute(
                        "SELECT session_key_hash, ttl, encrypted_payload, run_at, cache_anchor_at, expires_at "
                        "FROM cache_keepalive_sessions "
                        "WHERE principal_id = ? AND status = 'active' AND enqueue_state = 'enqueued'",
                        (principal_id,),
                    ).fetchall()
                    return next((row for row in rows if session in row[0]), None)
                row = eventually(session_row, f"SDK case {index} active 30m encrypted snapshot")
                assert row[1] == "30m" and row[2]
                assert b"stable prefix" not in row[2]
                assert row[5] - row[4] == 1800, row
                assert row[3] - row[4] in (1769, 1770), row
                def scheduled_row():
                    rows = admin(admin_port, "GET", f"/admin/v1/principals/{principal_id}/cache-keepalive")["rows"]
                    return next((item for item in rows if item["session_key_hash"] == row[0]
                                 and item["state"] == "scheduled" and item["ttl"] == "30m"), None)
                eventually(scheduled_row, f"SDK case {index} admin scheduled row")
        assert not Mock.errors, Mock.errors
        assert len(Mock.wires) == len(cases)
        print("PASS OpenAI 3.46.0 default/30m and tool-output TTL buffered+SSE listener-to-SQLite/admin", flush=True)
    finally:
        (root / "provider-wire.json").write_text(json.dumps(Mock.wires, indent=2))
        if client:
            client.close()
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        stdout.close()
        stderr.close()
        provider.shutdown()
        provider.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
