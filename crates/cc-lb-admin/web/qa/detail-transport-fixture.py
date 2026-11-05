#!/usr/bin/env python3
"""Run the real-server detail transport browser fixture for issue #1052.

The fixture seeds a migrated SQLite database, starts the supplied cc-lb binary,
and exposes three browser origins. Each origin serves a supplied static SPA and
forwards only /admin requests to the real server; path normalization is applied
only by the two proxy modes. No upstream calls are made.
"""
from __future__ import annotations

import argparse
import fcntl
import base64
import http.client
import json
import mimetypes
import os
import pathlib
import re
import signal
import socket
import sqlite3
import uuid
import tempfile
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

SQLITE_CONTAINER_IMAGE = "oven/bun:1.4.4-alpine"
SQLITE_CONTAINER_WORKDIR = "/var/lib/cc-lb"
SQLITE_HELPER_PATH = "/opt/detail-transport-sqlite.ts"
_SQLITE_CONTAINER_COUNTER = 0
LEDGER_PATH = pathlib.Path(
    os.environ.get(
        "DETAIL_CLEANUP_LEDGER",
        pathlib.Path(tempfile.gettempdir()) / "cc-lb-detail-transport-cleanup-ledger.json",
    )
)

PID = "00000000-0000-0000-0000-000000000001"
UPSTREAM = "00000000-0000-0000-0000-000000000101"
SESSION_PREFIX = PID + "/thread:"
IDS = {
    "renewed": SESSION_PREFIX + "thread-a",
    "scheduled": SESSION_PREFIX + "thread-b/agent:agent-b",
    "capped": SESSION_PREFIX + "literal%252Fbare%41",
    "expired": SESSION_PREFIX + "special+&=?# space 한글",
    "error": SESSION_PREFIX + "thread-e",
    "not_tracked": "decision:not-tracked",
}
RENEWAL_EVENT = "renewal:" + IDS["renewed"] + ":1"
ORDINARY_EVENT = "proxy:ordinary-event-1"
CONTROL_SESSION = "slash-free-session"

class FixtureError(RuntimeError):
    pass


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def ledger(kind: str, identifier: str, delete_command: str) -> None:
    path = LEDGER_PATH
    lock_path = pathlib.Path(str(path) + ".lock")
    temporary_path: pathlib.Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                if path.exists():
                    value = json.loads(path.read_text())
                    if not isinstance(value, dict):
                        raise FixtureError("cleanup ledger must contain a JSON object")
                else:
                    value = {"resources": []}
                resources = value.get("resources")
                if resources is None:
                    resources = []
                    value["resources"] = resources
                if not isinstance(resources, list) or any(not isinstance(item, dict) for item in resources):
                    raise FixtureError("cleanup ledger resources must be a JSON array of objects")
                if not any(item.get("identifier") == identifier for item in resources):
                    resources.append({
                        "kind": kind,
                        "identifier": identifier,
                        "delete_command": delete_command,
                        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    })
                value["cleanup_requires_user_finished_signal"] = True
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=path.parent,
                    prefix=f".{path.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as temporary:
                    temporary_path = pathlib.Path(temporary.name)
                    json.dump(value, temporary, indent=2)
                    temporary.write("\n")
                    temporary.flush()
                    os.fsync(temporary.fileno())
                os.replace(temporary_path, path)
                temporary_path = None
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    except FixtureError:
        raise
    except (OSError, ValueError) as exc:
        raise FixtureError("could not update cleanup ledger") from exc
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except OSError:
                pass

def sqlite_executor(kind: str, work_dir: pathlib.Path) -> dict[str, str]:
    if kind == "native":
        return {"kind": "native"}
    if kind != "docker":
        raise FixtureError(f"unsupported SQLite executor: {kind}")
    helper = pathlib.Path(__file__).with_name("detail-transport-sqlite.ts").resolve()
    if not helper.is_file():
        raise FixtureError(f"SQLite helper is missing: {helper}")
    return {
        "kind": "docker",
        "image": SQLITE_CONTAINER_IMAGE,
        "helper_path": str(helper),
        "container_workdir": SQLITE_CONTAINER_WORKDIR,
        "container_database": f"{SQLITE_CONTAINER_WORKDIR}/store.sqlite",
    }


def register_missing_helper_image(image: str) -> None:
    try:
        inspected = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise FixtureError(f"could not inspect Docker SQLite image: {exc}") from exc
    if inspected.returncode != 0:
        ledger("docker_image", image, f"docker image rm -- {image}")
        try:
            pulled = subprocess.run(
                ["docker", "pull", image],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError as exc:
            raise FixtureError(f"could not pull Docker SQLite image: {exc}") from exc
        if pulled.returncode != 0:
            detail = pulled.stderr.strip() or pulled.stdout.strip()
            raise FixtureError(f"could not pull Docker SQLite image: {detail}")

def encode_sql_value(value: object) -> object:
    if isinstance(value, bytes):
        return {"type": "blob", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [encode_sql_value(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise FixtureError(f"unsupported SQLite parameter type: {type(value).__name__}")

def docker_sqlite(
    work_dir: pathlib.Path,
    executor: dict[str, str],
    request: dict[str, object],
) -> dict[str, object]:
    global _SQLITE_CONTAINER_COUNTER
    register_missing_helper_image(executor["image"])
    _SQLITE_CONTAINER_COUNTER += 1
    name = f"cc-lb-detail-sqlite-{os.getpid()}-{uuid.uuid4().hex[:12]}"
    ledger("container", name, f"docker rm -f -- {name}")
    command = [
        "docker", "run", "--interactive", "--name", name,
        "--user", f"{os.getuid()}:{os.getgid()}",
        "--workdir", executor["container_workdir"],
        "--env", "HOME=/tmp",
        "--mount", f"type=bind,src={work_dir},dst={executor['container_workdir']}",
        "--mount", f"type=bind,src={executor['helper_path']},dst={SQLITE_HELPER_PATH},readonly",
        "--entrypoint", "bun",
        executor["image"],
        SQLITE_HELPER_PATH,
    ]
    try:
        result = subprocess.run(
            command,
            input=json.dumps(request, ensure_ascii=False),
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise FixtureError(f"could not start Docker SQLite helper: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise FixtureError(f"Docker SQLite helper failed ({result.returncode}): {detail}")
    try:
        value = json.loads(result.stdout)
    except ValueError as exc:
        raise FixtureError("Docker SQLite helper returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise FixtureError("Docker SQLite helper response must be an object")
    return value


def credentials(work_dir: pathlib.Path) -> tuple[pathlib.Path, str, str]:
    token = os.urandom(32).hex()
    master = os.urandom(32).hex()
    path = work_dir / "credentials.env"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(f"QA_ADMIN_TOKEN={token}\nQA_MASTER_KEY={master}\n")
    finally:
        if fd != -1:
            os.close(fd)
    return path, token, master


def config(work_dir: pathlib.Path, admin_port: int, proxy_port: int, metrics_port: int) -> pathlib.Path:
    path = work_dir / "config.toml"
    path.write_text("\n".join([
        "request_event_retention_days = 90",
        "[listener]",
        f'proxy_addr = "127.0.0.1:{proxy_port}"',
        f'admin_addr = "127.0.0.1:{admin_port}"',
        f'metrics_addr = "127.0.0.1:{metrics_port}"',
        "[storage]",
        'kind = "sqlite"',
        f'path = {json.dumps(str(work_dir / "store.sqlite"))}',
        "[aead]",
        'key_env = "QA_MASTER_KEY"',
        "[[admin.auth.providers]]",
        'kind = "static_token"',
        'id = "synthetic"',
        'token_env = "QA_ADMIN_TOKEN"',
        "[price_catalog]",
        f'cache_path = {json.dumps(str(work_dir / "catalog.json"))}',
        "",
    ]), encoding="utf-8")
    return path


def seed_statements(now: int) -> tuple[list[dict[str, object]], set[str]]:
    snapshot_json = json.dumps({
        "refresh_lead_time_5m_secs": 30,
        "refresh_lead_time_1h_secs": 300,
        "max_refreshes_per_session": 12,
        "max_total_duration_secs": 14400,
        "snapshot_max_bytes": 524288,
    }, separators=(",", ":"), sort_keys=True)
    statements: list[dict[str, object]] = [{
        "sql": "INSERT INTO upstream_spec_v1 (id,name,kind,base_url,enabled,warmup_enabled,warmup_dialect_plugin,spec_revision,created_at,updated_at,deleted_at) VALUES (?,?,?,?,?,?,?,?,?,?,NULL)",
        "params": [UPSTREAM, "detail-transport-upstream", "anthropic_api_key", "http://127.0.0.1:9", 1, 0, None, 1, now, now],
    }, {
        "sql": "INSERT INTO principals_v1 (id,name,kind,enabled,allowed_models,allowed_upstreams,default_limits,router_terminal_strategy,revision,created_at,updated_at,deleted_at,cache_keepalive_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,?)",
        "params": [PID, "detail-transport-principal", "machine", 1, '["claude-3-5-sonnet-20241022"]', json.dumps([UPSTREAM]), "[]", "first-pick", 1, now, now, json.dumps({"enabled": True, "refresh_lead_time_5m_secs": 30, "refresh_lead_time_1h_secs": 300, "max_refreshes_per_session": 12, "max_total_duration_secs": 14400, "snapshot_max_bytes": 524288}, separators=(",", ":"))],
    }]
    session_rows = []
    for state, identifier in list(IDS.items())[:5] + [("control", CONTROL_SESSION)]:
        status, refresh, reason, error = "active", 1, "agent-in-turn", None
        if state == "scheduled": refresh, reason = 0, "scheduled"
        if state == "capped": status, refresh, reason = "terminal", 12, "max_refreshes"
        if state == "expired": status, refresh, reason = "terminal", 2, "expired"
        if state == "error": error, reason = "renewal dispatch unavailable", "renewal dispatch unavailable"
        session_rows.append([identifier, PID, None, UPSTREAM, 1, refresh, now - 300, now - 120, now + 300, "5m", status, "enqueued" if status == "active" else "pending", None, f"cache_keepalive:{identifier}:1", b"qa", reason if status == "terminal" else None, now + 86400, now - 300, now, reason, error, snapshot_json, (now - 120) * 1000])
    statements.append({
        "sql": "INSERT INTO cache_keepalive_sessions (session_key_hash,principal_id,accounting_key_id,upstream_id,generation,refresh_count,first_scheduled_at,cache_anchor_at,run_at,ttl,status,enqueue_state,running_since_unix_secs,current_job_key,encrypted_payload,terminal_reason,expires_at,created_at,updated_at,display_reason,error,config_snapshot,last_message_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "params": session_rows,
    })
    statements.append({
        "sql": "INSERT INTO cache_keepalive_decisions (source_ref_id,decision,reason,generation,ts,principal_id,session_key_hash,upstream_id,error,ttl,config_snapshot,last_message_at_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        "params": [IDS["not_tracked"], "not_tracked", "below_threshold", 0, now - 100, PID, None, UPSTREAM, None, "5m", snapshot_json, (now - 100) * 1000],
    })
    turns = []
    for _state, identifier in list(IDS.items())[:5] + [("control", CONTROL_SESSION)]:
        turns.append([f"{identifier}:turn:1", identifier, PID, None, UPSTREAM, "claude-3-5-sonnet-20241022", 8000, 200, 0, 0, 0, 8000, 1200, "hit", now - 120])
    statements.append({
        "sql": "INSERT INTO cache_keepalive_turns (source_ref_id,session_key_hash,principal_id,accounting_key_id,upstream_id,model,input_tokens,output_tokens,cache_creation_input_tokens,cache_creation_input_tokens_5m,cache_creation_input_tokens_1h,cache_read_input_tokens,cost_micros,hit_miss,ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "params": turns,
    })
    event_rows = []
    for state, identifier in list(IDS.items())[:5]:
        event_id = "renewal:" + identifier + ":1"
        event_rows.append([f"req-renewal-{state}", now, "renewal", identifier + ":1", UPSTREAM, PID, now, "claude-3-5-sonnet-20241022", "synthetic", 120, event_id, now * 1000, event_id, 200, 4200, "renewal", 8000, 200, 1234])
    event_rows.append(["req-ordinary-1", now, "proxy", ORDINARY_EVENT, UPSTREAM, PID, now, "claude-3-5-sonnet-20241022", "synthetic", 80, ORDINARY_EVENT, now * 1000, ORDINARY_EVENT, 200, 2400, "messages", 4000, 100, 4321])
    statements.append({
        "sql": "INSERT INTO request_events_v1 (request_id,ts,source_kind,source_ref_id,upstream_id,principal_id,created_at,model,upstream_name,list_duration_ms,event_id,list_ts_ms,list_event_key,list_status,list_cost_usd_micros,event_kind,input_tokens,output_tokens,body_bytes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        "params": event_rows,
    })
    return statements, {"principals_v1", "upstream_spec_v1", "cache_keepalive_sessions", "cache_keepalive_decisions", "cache_keepalive_turns", "request_events_v1"}


def snapshot_queries() -> list[dict[str, object]]:
    queries = []
    for table, column, values, selected in (
        ("cache_keepalive_sessions", "session_key_hash", list(IDS.values())[:-1] + [CONTROL_SESSION], ["session_key_hash", "principal_id"]),
        ("cache_keepalive_decisions", "source_ref_id", [IDS["not_tracked"]], ["source_ref_id", "principal_id"]),
        ("cache_keepalive_turns", "session_key_hash", list(IDS.values())[:-1] + [CONTROL_SESSION], ["session_key_hash", "source_ref_id"]),
        ("request_events_v1", "event_id", ["renewal:" + value + ":1" for value in list(IDS.values())[:-1]] + [ORDINARY_EVENT], ["event_id", "request_id", "source_ref_id", "event_kind"]),
    ):
        placeholders = ",".join("?" for _ in values)
        queries.append({"name": table, "sql": f"SELECT {','.join(selected)} FROM {table} WHERE {column} IN ({placeholders}) ORDER BY {column}", "params": [encode_sql_value(value) for value in values]})
    return queries


def snapshot(db: pathlib.Path, executor: dict[str, str] | None = None) -> dict[str, object]:
    if executor and executor["kind"] == "docker":
        result = docker_sqlite(db.parent, executor, {"operation": "snapshot", "database": executor["container_database"], "queries": snapshot_queries()})
        return result
    connection = sqlite3.connect(db)
    try:
        rows: dict[str, list[dict[str, object]]] = {}
        for query in snapshot_queries():
            selected = query["sql"].split("SELECT ", 1)[1].split(" FROM ", 1)[0].split(",")
            result = connection.execute(str(query["sql"]), [decode_sql_value(value) for value in query["params"]])
            rows[str(query["name"])] = [dict(zip(selected, row)) for row in result.fetchall()]
        return rows
    finally:
        connection.close()


def decode_sql_value(value: object) -> object:
    if isinstance(value, dict) and value.get("type") == "blob":
        return base64.b64decode(str(value["base64"]))
    return value


def seed(db: pathlib.Path, executor: dict[str, str] | None = None) -> dict[str, object]:
    now = int(time.time())
    statements, required = seed_statements(now)
    if executor and executor["kind"] == "docker":
        encoded_statements = [
            {"sql": statement["sql"], "params": encode_sql_value(statement["params"])}
            for statement in statements
        ]
        result = docker_sqlite(db.parent, executor, {"operation": "seed", "database": executor["container_database"], "required_tables": sorted(required), "statements": encoded_statements})
        if result != {"ok": True}:
            raise FixtureError("Docker SQLite helper returned an invalid seed response")
        return snapshot(db, executor)
    conn = sqlite3.connect(db, timeout=20)
    try:
        existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing = required - existing
        if missing:
            raise FixtureError("migrated SQLite schema missing: " + ", ".join(sorted(missing)))
        conn.execute("BEGIN IMMEDIATE")
        for statement in statements:
            params = statement["params"]
            if isinstance(params, list) and params and isinstance(params[0], list):
                conn.executemany(str(statement["sql"]), [[decode_sql_value(value) for value in row] for row in params])
            else:
                conn.execute(str(statement["sql"]), [decode_sql_value(value) for value in params])
        conn.commit()
        return snapshot(db)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def normalize_path(path: str) -> str:
    return re.sub(r"%2f|%5c", lambda match: "/" if match.group(0).lower() == "%2f" else "\\", path, flags=re.IGNORECASE)


def make_origin(backend_port: int, spa_dir: pathlib.Path | None, mode: str) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def do_GET(self) -> None:  # noqa: N802
            raw_path, _, query = self.path.partition("?")
            path = normalize_path(raw_path) if mode != "direct" else raw_path
            target = path + (("?" + query) if query else "")
            if mode == "redirect" and path != raw_path:
                self.send_response(307)
                self.send_header("Location", target)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if path.startswith("/admin/") or spa_dir is None:
                self.forward(target)
                return
            relative = unquote(path.lstrip("/"))
            candidate = (spa_dir / relative).resolve()
            if spa_dir not in candidate.parents and candidate != spa_dir:
                self.send_error(404)
                return
            if not candidate.is_file():
                candidate = spa_dir / "index.html"
            try:
                body = candidate.read_bytes()
            except OSError:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", mimetypes.guess_type(str(candidate))[0] or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def forward(self, target: str) -> None:
            conn = http.client.HTTPConnection("127.0.0.1", backend_port, timeout=20)
            try:
                conn.request("GET", target, headers={"Authorization": self.headers.get("Authorization", ""), "Accept": self.headers.get("Accept", "application/json")})
                response = conn.getresponse()
                streaming = "text/event-stream" in (response.getheader("Content-Type") or "")
                body = b"" if streaming else response.read()
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() in {"connection", "keep-alive", "transfer-encoding", "content-length"}:
                        continue
                    self.send_header(key, value)
                if streaming:
                    self.send_header("Connection", "close")
                    self.close_connection = True
                else:
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if streaming:
                    while chunk := response.read1(8192):
                        self.wfile.write(chunk)
                        self.wfile.flush()
                else:
                    self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # Browsers cancel the live feed when navigating between drawers.
                return
            finally:
                conn.close()
        def log_message(self, _format: str, *_args: object) -> None:
            return
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name=f"detail-{mode}", daemon=True).start()
    return server


def wait_health(port: int, token: str, process: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise FixtureError("cc-lb exited before admin health became ready")
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            conn.request("GET", "/admin/health", headers={"Authorization": "Bearer " + token})
            response = conn.getresponse()
            response.read(); conn.close()
            if response.status == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise FixtureError("admin health did not become ready")


def read_manifest(path: pathlib.Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise FixtureError("invalid fixture manifest") from exc
    if not isinstance(value, dict):
        raise FixtureError("fixture manifest must be an object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path)
    parser.add_argument("--work-dir", type=pathlib.Path)
    parser.add_argument("--mode", choices=("old", "new"), default="new")
    parser.add_argument("--spa-dir", type=pathlib.Path)
    parser.add_argument("--manifest", type=pathlib.Path)
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--cleanup-ledger", type=pathlib.Path)
    parser.add_argument("--sqlite-executor", choices=("native", "docker"), default="native", help="SQLite seed/snapshot executor; docker uses the pinned Bun helper container")
    parser.add_argument("--hold", action="store_true")
    args = parser.parse_args()
    global LEDGER_PATH
    if args.cleanup_ledger is not None:
        LEDGER_PATH = args.cleanup_ledger.expanduser().resolve()
    if args.snapshot:
        manifest = read_manifest(args.manifest or pathlib.Path("fixture-manifest.json"))
        if args.cleanup_ledger is None and isinstance(manifest.get("cleanup_ledger"), str):
            LEDGER_PATH = pathlib.Path(str(manifest["cleanup_ledger"]))
        executor = manifest.get("sqlite_executor")
        if executor is not None and not isinstance(executor, dict):
            raise FixtureError("fixture manifest has an invalid sqlite_executor")
        print(json.dumps(snapshot(pathlib.Path(str(manifest["database"])), executor), ensure_ascii=False, sort_keys=True))
        return 0
    if args.binary is None or args.work_dir is None:
        raise FixtureError("--binary and --work-dir are required")
    binary = args.binary.expanduser().resolve()
    work_dir = args.work_dir.expanduser().resolve()
    executor = sqlite_executor(args.sqlite_executor, work_dir)
    spa_dir = args.spa_dir.expanduser().resolve() if args.spa_dir else None
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise FixtureError("--binary must be executable")
    if spa_dir is not None and (not spa_dir.is_dir() or not (spa_dir / "index.html").is_file()):
        raise FixtureError("--spa-dir must contain index.html")
    if not args.hold:
        raise FixtureError("use --hold to retain the isolated fixture until explicitly stopped")
    if work_dir.exists():
        raise FixtureError("--work-dir must be a fresh directory")
    work_dir.mkdir(parents=True)
    ledger("directory", str(work_dir), f"rm -r -- {work_dir}")
    print(f"Cleanup ledger: {LEDGER_PATH}", flush=True)
    db = work_dir / "store.sqlite"
    admin_port = free_port()
    proxy_port = free_port()
    metrics_port = free_port()
    config_path = config(work_dir, admin_port, proxy_port, metrics_port)
    cred_path, token, master = credentials(work_dir)
    ledger("credential_file", str(cred_path), f"rm -- {cred_path}")
    env = os.environ.copy()
    env.update({"QA_ADMIN_TOKEN": token, "QA_MASTER_KEY": master})
    log_path = work_dir / "cc-lb.log"
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stop.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stop.set())
    origins: dict[str, ThreadingHTTPServer] = {}
    process: subprocess.Popen[bytes] | None = None
    with log_path.open("wb") as log_handle:
        try:
            process = subprocess.Popen([str(binary), "serve", "--config", str(config_path), "--data-dir", str(work_dir / "data")], cwd=str(work_dir), env=env, stdout=log_handle, stderr=subprocess.STDOUT)
            ledger("process", f"detail-transport-server-pid:{process.pid}", f"kill {process.pid}")
            ledger("process", f"detail-transport-harness-pid:{os.getpid()}", f"kill {os.getpid()}")
            ledger("listener", f"detail-transport-admin:127.0.0.1:{admin_port};pid:{process.pid}", f"kill {process.pid}")
            if stop.is_set():
                return 0
            registering = False
            try:
                wait_health(admin_port, token, process)
                before = seed(db, executor)
                manifest_path = work_dir / "fixture-manifest.json"
                for name in ("direct", "forward", "redirect"):
                    origins[name] = make_origin(admin_port, spa_dir, name)
                    registering = True
                    ledger("listener", f"detail-transport-{name}:127.0.0.1:{origins[name].server_address[1]}", f"stop listener in harness PID {os.getpid()}")
                    registering = False
                manifest = {
                    "issue": 1052,
                    "mode": args.mode,
                    "database": str(db),
                    "sqlite_executor": executor,
                    "cleanup_ledger": str(LEDGER_PATH),
                    "origins": {name: f"http://127.0.0.1:{server.server_address[1]}" for name, server in origins.items()},
                    "principal_id": PID,
                    "keepalive_ids": IDS,
                    "control_session_id": CONTROL_SESSION,
                    "event_ids": {"renewal": RENEWAL_EVENT, "ordinary": ORDINARY_EVENT},
                    "credentials_file": str(cred_path),
                    "db_snapshot_before": before,
                    "server_log": str(log_path),
                }
                manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
                print("DETAIL_FIXTURE_READY", flush=True)
                print(json.dumps({"manifest": str(manifest_path), "mode": args.mode, "origins": manifest["origins"], "credentials_file": str(cred_path)}, ensure_ascii=False, sort_keys=True), flush=True)
                while not stop.wait(1.0):
                    if process.poll() is not None:
                        raise FixtureError("cc-lb exited while held")
                return 0
            except Exception as exc:
                if registering:
                    raise
                print(f"DETAIL_FIXTURE_FAILED_RETAINED: {exc}", file=sys.stderr, flush=True)
                stop.wait()
                return 1
        finally:
            try:
                for server in origins.values():
                    server.shutdown(); server.server_close()
            finally:
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill(); process.wait(timeout=5)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except FixtureError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
