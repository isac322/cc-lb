#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "$SCRIPT_DIR/../.." && pwd)
export CC_LB_MULTI_REPLICA_SCRIPT_DIR="$SCRIPT_DIR"
export CC_LB_MULTI_REPLICA_ROOT_DIR="$ROOT_DIR"

exec python3 - <<'PY'
import asyncio
import contextlib
import json
import os
import shutil
import signal
import socket
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(os.environ["CC_LB_MULTI_REPLICA_SCRIPT_DIR"])
ROOT_DIR = Path(os.environ["CC_LB_MULTI_REPLICA_ROOT_DIR"])
EVIDENCE_DIR = ROOT_DIR / "target/test-evidence/multi-replica"
COMPOSE_FILE = SCRIPT_DIR / "docker-compose.yml"
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", f"task37-{os.getpid()}")
POSTGRES_URL = os.environ.get("CC_LB_MULTI_REPLICA_POSTGRES_URL", "")
SERVER_BIN = os.environ.get("CC_LB_MULTI_REPLICA_SERVER_BIN", "")
FAKE_ANTHROPIC_BIN = os.environ.get("CC_LB_MULTI_REPLICA_FAKE_ANTHROPIC_BIN", "")
ADMIN_TOKEN = "00000000-0000-4000-8000-000000000037"
MASTER_KEY = "0" * 64
CLUSTER_TOKEN = "00000000-0000-4000-8000-000000000038"
UPSTREAM_NAME = "multi-replica-oauth"
PRINCIPAL_NAME = "multi-replica-principal"
PROXY_A_PORT = 8888
ADMIN_A_PORT = 8001
METRICS_A_PORT = 8003
PROXY_B_PORT = 8889
ADMIN_B_PORT = 8002
METRICS_B_PORT = 8004
FAKE_PORT = 18888
POSTGRES_WAIT_TIMEOUT = 60
PROCESS_EVENT_TIMEOUT = 60
PROCESS_EXIT_TIMEOUT = 15
COMMAND_TIMEOUT = 60
HTTP_TIMEOUT = 10
REBIND_COMPLETE_MARKERS = (
    "dynamic view rebound after runtime change notification",
    "notify-triggered view rejected: newer generation already resident",
)
REBIND_FAILURE_MARKER = "dynamic view rebind failed after runtime change notification"


class HarnessFailure(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class ManagedProcess:
    def __init__(self, name, process, log_path):
        self.name = name
        self.process = process
        self.log_path = log_path
        self.lines = []
        self.closed = False
        self.condition = asyncio.Condition()
        self.log_file = log_path.open("a", encoding="utf-8", buffering=1)
        self.log_file.write(f"\n--- {name} pid={process.pid} started ---\n")
        self.reader_task = asyncio.create_task(self._read_output())

    @classmethod
    async def start(cls, name, argv, env, log_path):
        process = await asyncio.create_subprocess_exec(
            *argv,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
        )
        return cls(name, process, log_path)

    async def _read_output(self):
        assert self.process.stdout is not None
        try:
            while True:
                raw = await self.process.stdout.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                self.log_file.write(line + "\n")
                async with self.condition:
                    self.lines.append(line)
                    self.condition.notify_all()
        finally:
            async with self.condition:
                self.closed = True
                self.condition.notify_all()
            self.log_file.flush()

    def checkpoint(self):
        return len(self.lines)

    async def wait_for_any(self, markers, *, after=0, failure_markers=(), label):
        async def observe():
            cursor = after
            while True:
                async with self.condition:
                    while cursor < len(self.lines):
                        line = self.lines[cursor]
                        cursor += 1
                        for failure_marker in failure_markers:
                            if failure_marker in line:
                                raise HarnessFailure(
                                    f"{label} observed failure event: {line}"
                                )
                        for marker in markers:
                            if marker in line:
                                return line
                    if self.closed:
                        raise HarnessFailure(
                            f"{label} did not occur before {self.name} exited "
                            f"with status {self.process.returncode}"
                        )
                    await self.condition.wait()

        try:
            return await asyncio.wait_for(observe(), timeout=PROCESS_EVENT_TIMEOUT)
        except asyncio.TimeoutError as exc:
            raise HarnessFailure(
                f"timed out after {PROCESS_EVENT_TIMEOUT}s waiting for {label} from {self.name}"
            ) from exc

    async def wait_for_exit(self, timeout=PROCESS_EXIT_TIMEOUT):
        try:
            return await asyncio.wait_for(self.process.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise HarnessFailure(
                f"{self.name} did not exit within {timeout}s"
            ) from exc
        finally:
            if self.process.returncode is not None:
                await self.reader_task
                self.log_file.close()

    async def terminate(self, *, require_graceful):
        if self.process.returncode is not None:
            await self.reader_task
            if not self.log_file.closed:
                self.log_file.close()
            return self.process.returncode
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, signal.SIGTERM)
        try:
            return await self.wait_for_exit()
        except HarnessFailure:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGKILL)
            await self.wait_for_exit(timeout=5)
            if require_graceful:
                raise
            return self.process.returncode


class Harness:
    def __init__(self):
        self.tmp_dir = None
        self.fake = None
        self.replica_a = None
        self.replica_b = None
        self.compose_started = False
        self.cleanup_errors = []
        self.postgres = None

    async def run(self):
        self.validate_prerequisites()
        self.postgres = parse_postgres_url(POSTGRES_URL)
        ensure_ports_free(
            PROXY_A_PORT,
            ADMIN_A_PORT,
            METRICS_A_PORT,
            PROXY_B_PORT,
            ADMIN_B_PORT,
            METRICS_B_PORT,
            FAKE_PORT,
            self.postgres["port"],
        )
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="cc-lb-multi-replica."))
        (self.tmp_dir / "A-data").mkdir()
        (self.tmp_dir / "B-data").mkdir()
        write_config(
            self.tmp_dir / "A.toml",
            PROXY_A_PORT,
            ADMIN_A_PORT,
            METRICS_A_PORT,
            self.tmp_dir / "A-data",
        )
        write_config(
            self.tmp_dir / "B.toml",
            PROXY_B_PORT,
            ADMIN_B_PORT,
            METRICS_B_PORT,
            self.tmp_dir / "B-data",
        )

        compose_env = os.environ.copy()
        compose_env.update(
            {
                "CC_LB_MULTI_REPLICA_POSTGRES_PORT": str(self.postgres["port"]),
                "CC_LB_MULTI_REPLICA_POSTGRES_USER": self.postgres["user"],
                "CC_LB_MULTI_REPLICA_POSTGRES_PASSWORD": self.postgres["password"],
                "CC_LB_MULTI_REPLICA_POSTGRES_DB": self.postgres["database"],
            }
        )
        await run_command(
            *compose_command(
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                str(POSTGRES_WAIT_TIMEOUT),
                "postgres",
            ),
            env=compose_env,
            timeout=POSTGRES_WAIT_TIMEOUT + 15,
        )
        self.compose_started = True
        print("SYNC postgres: docker compose reported service healthy", flush=True)
        await self.psql_exec("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")

        self.fake = await ManagedProcess.start(
            "fake-anthropic",
            [str(Path(FAKE_ANTHROPIC_BIN).resolve()), "--port", str(FAKE_PORT)],
            os.environ.copy(),
            self.tmp_dir / "fake-anthropic.log",
        )
        fake_line = await self.fake.wait_for_any(
            (f"fake-anthropic listening on http://127.0.0.1:{FAKE_PORT}",),
            label="fake listener readiness",
        )
        print(f"SYNC fake: {fake_line}", flush=True)

        self.replica_a = await self.start_replica("A")
        self.replica_b = await self.start_replica("B")
        await asyncio.gather(
            self.wait_for_replica_ready(self.replica_a, PROXY_A_PORT, "replica A"),
            self.wait_for_replica_ready(self.replica_b, PROXY_B_PORT, "replica B"),
        )

        principal_checkpoint = self.replica_b.checkpoint()
        principal_status = await self.create_principal()
        if principal_status == 201:
            await self.wait_for_rebind(
                self.replica_b,
                principal_checkpoint,
                "principal propagation to replica B",
            )

        upstream_checkpoint = self.replica_b.checkpoint()
        upstream_id = await self.create_oauth_upstream()
        await self.wait_for_rebind(
            self.replica_b,
            upstream_checkpoint,
            "upstream propagation to replica B",
        )

        await self.complete_oauth(upstream_id)
        await self.assert_upstream_active(ADMIN_B_PORT, "B")

        for index in range(1, 11):
            if index % 2 == 1:
                await self.proxy_request(PROXY_A_PORT, f"round-robin-{index}-A")
            else:
                await self.proxy_request(PROXY_B_PORT, f"round-robin-{index}-B")

        await self.stop_replica_a_for_failover()
        await self.assert_audit_entries_landed()

        for index in range(1, 6):
            await self.proxy_request(PROXY_B_PORT, f"B-after-A-kill-{index}")

        self.replica_a = await self.start_replica("A")
        await self.wait_for_replica_ready(
            self.replica_a,
            PROXY_A_PORT,
            "replica A restarted",
        )
        await self.assert_upstream_active(ADMIN_B_PORT, "B")
        await self.proxy_request(PROXY_A_PORT, "A-rejoined")

        await self.capture_evidence()
        print(
            f"PASS multi-replica postgres smoke; evidence in {EVIDENCE_DIR}",
            flush=True,
        )

    def validate_prerequisites(self):
        if not POSTGRES_URL:
            raise HarnessFailure("CC_LB_MULTI_REPLICA_POSTGRES_URL is required")
        if not SERVER_BIN or not os.access(SERVER_BIN, os.X_OK):
            raise HarnessFailure(
                "CC_LB_MULTI_REPLICA_SERVER_BIN must name an executable cc-lb binary"
            )
        if not FAKE_ANTHROPIC_BIN or not os.access(FAKE_ANTHROPIC_BIN, os.X_OK):
            raise HarnessFailure(
                "CC_LB_MULTI_REPLICA_FAKE_ANTHROPIC_BIN must name an executable fake-anthropic binary"
            )
        if shutil.which("docker") is None:
            raise HarnessFailure("docker command not found")
        if not COMPOSE_FILE.is_file():
            raise HarnessFailure(f"missing compose file: {COMPOSE_FILE}")

    async def start_replica(self, name):
        if self.tmp_dir is None:
            raise HarnessFailure("temporary directory is not initialized")
        is_a = name == "A"
        env = os.environ.copy()
        env.update(
            {
                "CC_LB_MASTER_KEY": MASTER_KEY,
                "CC_LB_ADMIN_TOKEN": ADMIN_TOKEN,
                "CC_LB_CLUSTER_TOKEN": CLUSTER_TOKEN,
                "CC_LB_DATA_DIR": str(self.tmp_dir / f"{name}-data"),
                "RUST_LOG": "info,hyper=warn,hyper_util=warn,axum=warn",
            }
        )
        if is_a:
            env["CC_LB_BOOTSTRAP_ADMIN_TOKEN"] = ADMIN_TOKEN
        return await ManagedProcess.start(
            f"replica {name}",
            [
                str(Path(SERVER_BIN).resolve()),
                "serve",
                "--config",
                str(self.tmp_dir / f"{name}.toml"),
                "--data-dir",
                str(self.tmp_dir / f"{name}-data"),
            ],
            env,
            self.tmp_dir / f"{name}.log",
        )

    async def wait_for_replica_ready(self, replica, proxy_port, label):
        preflight_line = await replica.wait_for_any(
            ("preflight: ok",),
            label=f"{label} preflight completion",
        )
        print(f"SYNC {label}: {preflight_line}", flush=True)
        await self.wait_for_rebind(replica, 0, f"{label} initial LISTEN/rebuild")
        status, body, _ = await http_request(
            "GET", f"http://127.0.0.1:{proxy_port}/healthz"
        )
        write_bytes(self.tmp_dir / f"health-{label.replace(' ', '-')}.json", body)
        if status != 200:
            raise HarnessFailure(f"{label} healthz expected HTTP 200, got {status}")
        print(f"SYNC {label}: one-shot healthz returned HTTP 200", flush=True)

    async def wait_for_rebind(self, replica, checkpoint, label):
        line = await replica.wait_for_any(
            REBIND_COMPLETE_MARKERS,
            after=checkpoint,
            failure_markers=(REBIND_FAILURE_MARKER,),
            label=label,
        )
        print(f"SYNC {label}: {line}", flush=True)

    async def create_principal(self):
        output = self.tmp_dir / "principal-create.json"
        body = json.dumps(
            {
                "name": PRINCIPAL_NAME,
                "kind": "machine",
                "allowed_models": ["*"],
            }
        ).encode()
        status, response, _ = await http_request(
            "POST",
            f"http://127.0.0.1:{ADMIN_A_PORT}/admin/v1/principals",
            body=body,
            headers=admin_headers(json_body=True),
        )
        write_bytes(output, response)
        if status not in (201, 409):
            raise HarnessFailure(
                f"create principal expected HTTP 201 or 409, got {status}; "
                f"response={response.decode(errors='replace')}"
            )
        return status

    async def create_oauth_upstream(self):
        output = self.tmp_dir / "upstream-create.json"
        body = json.dumps(
            {
                "name": UPSTREAM_NAME,
                "kind": "anthropic_oauth",
                "base_url": f"http://127.0.0.1:{FAKE_PORT}",
            }
        ).encode()
        status, response, _ = await http_request(
            "POST",
            f"http://127.0.0.1:{ADMIN_A_PORT}/admin/v1/upstreams",
            body=body,
            headers=admin_headers(json_body=True),
        )
        write_bytes(output, response)
        if status != 201:
            raise HarnessFailure(
                f"create upstream expected HTTP 201, got {status}; "
                f"response={response.decode(errors='replace')}"
            )
        return json.loads(response)["id"]

    async def complete_oauth(self, upstream_id):
        start_output = self.tmp_dir / "oauth-start.json"
        status, response, _ = await http_request(
            "POST",
            f"http://127.0.0.1:{ADMIN_A_PORT}/admin/v1/upstreams/{upstream_id}/oauth/start",
            body=b"{}",
            headers=admin_headers(json_body=True),
        )
        write_bytes(start_output, response)
        assert_status(status, 200, "oauth start", response)
        start = json.loads(response)
        authorize_url = start["authorize_url"]
        state_token = start["state_token"]

        authorize_status, _, authorize_headers = await http_request(
            "GET", authorize_url, follow_redirects=False
        )
        if authorize_status not in (301, 302, 303, 307, 308):
            raise HarnessFailure(
                f"oauth authorize expected redirect, got HTTP {authorize_status}"
            )
        redirect = authorize_headers.get("Location")
        if not redirect:
            raise HarnessFailure("oauth authorize response omitted Location")
        params = urllib.parse.parse_qs(urllib.parse.urlparse(redirect).query)
        codes = params.get("code", [])
        if not codes:
            raise HarnessFailure("oauth authorize redirect omitted code")

        checkpoint = self.replica_b.checkpoint()
        complete_output = self.tmp_dir / "oauth-complete.json"
        body = json.dumps(
            {"state_token": state_token, "code": codes[0]}
        ).encode()
        status, response, _ = await http_request(
            "POST",
            f"http://127.0.0.1:{ADMIN_A_PORT}/admin/v1/upstreams/{upstream_id}/oauth/complete",
            body=body,
            headers=admin_headers(json_body=True),
        )
        write_bytes(complete_output, response)
        assert_status(status, 200, "oauth complete", response)
        await self.wait_for_rebind(
            self.replica_b,
            checkpoint,
            "OAuth completion propagation to replica B",
        )

    async def assert_upstream_active(self, admin_port, replica_name):
        output = self.tmp_dir / f"status-{replica_name.lower()}.json"
        status, response, _ = await http_request(
            "GET",
            f"http://127.0.0.1:{admin_port}/admin/v1/status",
            headers=admin_headers(),
        )
        write_bytes(output, response)
        assert_status(status, 200, f"{replica_name} status", response)
        payload = json.loads(response)
        if not any(
            upstream.get("name") == UPSTREAM_NAME
            and upstream.get("status") == "active"
            for upstream in payload.get("upstreams", [])
        ):
            raise HarnessFailure(
                f"upstream {UPSTREAM_NAME} is not active on {replica_name}; "
                f"status={json.dumps(payload, indent=2)}"
            )
        print(f"upstream {UPSTREAM_NAME} active on {replica_name}", flush=True)

    async def proxy_request(self, port, label):
        output = self.tmp_dir / f"proxy-{label}.json"
        body = json.dumps(
            {
                "model": "claude-3-5-sonnet-20241022",
                "messages": [
                    {"role": "user", "content": "hello multi replica"}
                ],
                "max_tokens": 32,
            }
        ).encode()
        status, response, _ = await http_request(
            "POST",
            f"http://127.0.0.1:{port}/v1/messages",
            body=body,
            headers={
                "content-type": "application/json",
                "x-api-key": "sk-ant-test",
                "anthropic-version": "2023-06-01",
            },
        )
        write_bytes(output, response)
        assert_status(status, 200, f"proxy {label}", response)

    async def stop_replica_a_for_failover(self):
        if self.replica_a is None:
            raise HarnessFailure("replica A is not running")
        return_code = await self.replica_a.terminate(require_graceful=True)
        print(
            f"SYNC replica A failover stop: process exited with status {return_code}",
            flush=True,
        )
        self.replica_a = None

    async def assert_audit_entries_landed(self):
        count_text = await self.psql_scalar(
            "SELECT count(*) FROM audit_log_v1 "
            "WHERE route IN "
            "('admin_v1_upstreams','admin_v1_upstream_oauth','admin_v1_principals');"
        )
        count = int(count_text or "0")
        if count < 3:
            raise HarnessFailure(
                f"expected at least 3 admin audit entries, got {count}"
            )
        print(f"audit entries visible in postgres: {count}", flush=True)

    async def psql_exec(self, sql):
        await run_command(
            *compose_command(
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                self.postgres["user"],
                "-d",
                self.postgres["database"],
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                sql,
            ),
            timeout=COMMAND_TIMEOUT,
        )

    async def psql_scalar(self, sql):
        stdout = await run_command(
            *compose_command(
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                self.postgres["user"],
                "-d",
                self.postgres["database"],
                "-Atc",
                sql,
            ),
            timeout=COMMAND_TIMEOUT,
        )
        return "".join(stdout.split())

    async def capture_evidence(self):
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        if self.tmp_dir is not None:
            for name in ("A.log", "B.log", "fake-anthropic.log"):
                source = self.tmp_dir / name
                if source.is_file():
                    shutil.copyfile(source, EVIDENCE_DIR / name)
        for port, name in (
            (METRICS_A_PORT, "A-final-metrics.txt"),
            (METRICS_B_PORT, "B-final-metrics.txt"),
        ):
            try:
                status, body, _ = await http_request(
                    "GET", f"http://127.0.0.1:{port}/metrics"
                )
                if status == 200:
                    write_bytes(EVIDENCE_DIR / name, body)
            except HarnessFailure:
                pass

    async def cleanup(self):
        with contextlib.suppress(Exception):
            await self.capture_evidence()
        for process in (self.replica_a, self.replica_b, self.fake):
            if process is None:
                continue
            try:
                await process.terminate(require_graceful=False)
            except Exception as exc:
                self.cleanup_errors.append(f"failed to stop {process.name}: {exc}")
        try:
            await run_command(
                *compose_command("down", "-v", "--remove-orphans"),
                timeout=COMMAND_TIMEOUT,
                check=False,
            )
        except Exception as exc:
            self.cleanup_errors.append(f"docker compose cleanup failed: {exc}")
        if self.tmp_dir is not None:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def dump_diagnostics(self):
        print("--- multi-replica diagnostics ---", file=sys.stderr)
        for process in (self.replica_a, self.replica_b, self.fake):
            if process is None:
                continue
            print(f"--- {process.name} log tail ---", file=sys.stderr)
            for line in process.lines[-200:]:
                print(line, file=sys.stderr)


def parse_postgres_url(raw):
    parsed = urllib.parse.urlparse(raw)
    database = parsed.path.removeprefix("/")
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname not in {"127.0.0.1", "localhost"}
        or parsed.port is None
        or not parsed.username
        or parsed.password is None
        or not database
    ):
        raise HarnessFailure(
            "CC_LB_MULTI_REPLICA_POSTGRES_URL must be a local postgres URL "
            "with explicit port, user, password, and database"
        )
    return {
        "port": parsed.port,
        "user": urllib.parse.unquote(parsed.username),
        "password": urllib.parse.unquote(parsed.password),
        "database": urllib.parse.unquote(database),
    }


def ensure_ports_free(*ports):
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError as exc:
            raise HarnessFailure(f"port {port} is unavailable: {exc}") from exc
        finally:
            sock.close()


def write_config(path, proxy_port, admin_port, metrics_port, data_dir):
    path.write_text(
        f'''[listener]
proxy_addr = "127.0.0.1:{proxy_port}"
admin_addr = "127.0.0.1:{admin_port}"
metrics_addr = "127.0.0.1:{metrics_port}"

[body]
messages_cap_bytes = 33554432
files_cap_bytes = 104857600

[timeouts]
request_header_secs = 10
request_body_chunk_secs = 30
idle_secs = 300
upstream_total_secs = 30
drain_secs = 5

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "{PRINCIPAL_NAME}"
upstream_kind = "anthropic_o_auth"

[storage]
kind = "postgres"
url = "{POSTGRES_URL}"

[storage.pool]
max_connections = 8
acquire_timeout_secs = 10
statement_timeout_secs = 25
sslmode = "disable"

[cluster]
instance_url = "http://127.0.0.1:{admin_port}"

[aead]
key_env = "CC_LB_MASTER_KEY"

[oauth.anthropic]
client_id = "fake-client"
auth_url = "http://127.0.0.1:{FAKE_PORT}/oauth/authorize"
token_url = "http://127.0.0.1:{FAKE_PORT}/oauth/token"
redirect_uri = "http://127.0.0.1:{admin_port}/oauth/callback"
scopes = ["messages", "files"]

[runtime]
data_dir = "{data_dir}"

[observability]
tracing_level = "info"
log_redaction = true
user_prompt_redaction = false

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[circuit_breaker]
failures_to_open = 5
window_secs = 10
half_open_after_secs = 30

[bulkhead]
max_conns_per_upstream = 50
semaphore_per_upstream = 100

[dns]
cache_ttl_floor_secs = 30
cache_ttl_ceiling_secs = 300

[egress]
''',
        encoding="utf-8",
    )


def compose_command(*args):
    return (
        "docker",
        "compose",
        "-f",
        str(COMPOSE_FILE),
        "-p",
        COMPOSE_PROJECT,
        *args,
    )


def admin_headers(*, json_body=False):
    headers = {"authorization": f"Bearer {ADMIN_TOKEN}"}
    if json_body:
        headers["content-type"] = "application/json"
    return headers


def assert_status(actual, expected, label, body):
    if actual != expected:
        raise HarnessFailure(
            f"{label} expected HTTP {expected}, got {actual}; "
            f"response={body.decode(errors='replace')}"
        )


def write_bytes(path, content):
    path.write_bytes(content)


async def run_command(*argv, env=None, timeout, check=True):
    process = await asyncio.create_subprocess_exec(
        *argv,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError as exc:
        process.kill()
        await process.wait()
        raise HarnessFailure(
            f"command timed out after {timeout}s: {' '.join(argv)}"
        ) from exc
    stdout_text = stdout.decode("utf-8", errors="replace")
    stderr_text = stderr.decode("utf-8", errors="replace")
    if check and process.returncode != 0:
        raise HarnessFailure(
            f"command failed with status {process.returncode}: {' '.join(argv)}\n"
            f"stdout:\n{stdout_text}\nstderr:\n{stderr_text}"
        )
    return stdout_text


async def http_request(method, url, *, body=None, headers=None, follow_redirects=True):
    def perform():
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers or {},
            method=method,
        )
        opener = (
            urllib.request.build_opener()
            if follow_redirects
            else urllib.request.build_opener(NoRedirect())
        )
        try:
            with opener.open(request, timeout=HTTP_TIMEOUT) as response:
                return response.status, response.read(), response.headers
        except urllib.error.HTTPError as error:
            return error.code, error.read(), error.headers
        except (OSError, urllib.error.URLError) as error:
            raise HarnessFailure(
                f"one-shot {method} {url} failed: {error}"
            ) from error

    return await asyncio.to_thread(perform)


async def async_main():
    harness = Harness()
    current_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signum, current_task.cancel)

    failure = None
    try:
        await harness.run()
    except BaseException as exc:
        failure = exc
        harness.dump_diagnostics()
    await harness.cleanup()

    if harness.cleanup_errors:
        cleanup_message = "; ".join(harness.cleanup_errors)
        if failure is None:
            failure = HarnessFailure(cleanup_message)
        else:
            print(f"cleanup errors: {cleanup_message}", file=sys.stderr)
    if failure is not None:
        raise failure


try:
    asyncio.run(async_main())
except HarnessFailure as error:
    print(f"FAIL multi-replica postgres: {error}", file=sys.stderr)
    sys.exit(1)
except asyncio.CancelledError:
    print("FAIL multi-replica postgres: interrupted", file=sys.stderr)
    sys.exit(1)
PY
