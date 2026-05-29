#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(cd -- "$SCRIPT_DIR/../.." && pwd)
EVIDENCE_DIR="$ROOT_DIR/target/test-evidence/multi-replica"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-task37-${RANDOM}}"
POSTGRES_URL="${CC_LB_MULTI_REPLICA_POSTGRES_URL:-}"
ADMIN_TOKEN="00000000-0000-4000-8000-000000000037"
MASTER_KEY="0000000000000000000000000000000000000000000000000000000000000000"
UPSTREAM_NAME="multi-replica-oauth"
PRINCIPAL_NAME="multi-replica-principal"
PROXY_A_PORT=8888
ADMIN_A_PORT=8001
METRICS_A_PORT=8003
PROXY_B_PORT=8889
ADMIN_B_PORT=8002
METRICS_B_PORT=8004
FAKE_PORT=18888
A_PID=''
B_PID=''
FAKE_PID=''
TMP_DIR=''

warn_skip() {
  printf 'SKIP multi-replica postgres: %s\n' "$1"
  exit 0
}

fail() {
  printf 'FAIL multi-replica postgres: %s\n' "$1" >&2
  exit 1
}

cleanup_pid_tree() {
  local pid=$1
  [ -n "$pid" ] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    sleep 1
  fi
  if kill -0 "$pid" 2>/dev/null; then
    pkill -KILL -P "$pid" 2>/dev/null || true
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

capture_evidence() {
  mkdir -p "$EVIDENCE_DIR"
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    cp "$TMP_DIR/A.log" "$EVIDENCE_DIR/A.log" 2>/dev/null || true
    cp "$TMP_DIR/B.log" "$EVIDENCE_DIR/B.log" 2>/dev/null || true
    cp "$TMP_DIR/fake-anthropic.log" "$EVIDENCE_DIR/fake-anthropic.log" 2>/dev/null || true
  fi
  curl -fsS "http://127.0.0.1:$METRICS_A_PORT/metrics" > "$EVIDENCE_DIR/A-final-metrics.txt" 2>/dev/null || true
  curl -fsS "http://127.0.0.1:$METRICS_B_PORT/metrics" > "$EVIDENCE_DIR/B-final-metrics.txt" 2>/dev/null || true
}

cleanup() {
  capture_evidence
  cleanup_pid_tree "$A_PID"
  cleanup_pid_tree "$B_PID"
  cleanup_pid_tree "$FAKE_PID"
  DOCKER_HOST=tcp://localhost:2375 docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" down -v >/dev/null 2>&1 || true
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT INT TERM

if [ -z "$POSTGRES_URL" ]; then
  warn_skip "CC_LB_MULTI_REPLICA_POSTGRES_URL not set"
fi
if ! command -v docker >/dev/null 2>&1; then
  warn_skip "docker command not found"
fi
if ! DOCKER_HOST=tcp://localhost:2375 docker ps >/dev/null 2>&1; then
  warn_skip "docker daemon unreachable at DOCKER_HOST=tcp://localhost:2375"
fi
if [ ! -f "$COMPOSE_FILE" ]; then
  fail "missing compose file: $COMPOSE_FILE"
fi

CC_LB_MULTI_REPLICA_POSTGRES_PORT=$(python3 - "$POSTGRES_URL" <<'PY'
import sys
import urllib.parse

url = urllib.parse.urlparse(sys.argv[1])
if url.hostname not in {"127.0.0.1", "localhost"} or url.port is None:
    print("CC_LB_MULTI_REPLICA_POSTGRES_URL must use localhost with an explicit port", file=sys.stderr)
    sys.exit(1)
print(url.port)
PY
) || fail "invalid CC_LB_MULTI_REPLICA_POSTGRES_URL"
export CC_LB_MULTI_REPLICA_POSTGRES_PORT

ensure_port_free() {
  python3 - "$@" <<'PY'
import socket
import sys

for raw in sys.argv[1:]:
    port = int(raw)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError as exc:
        print(f"port {port} is unavailable: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        sock.close()
PY
}

wait_for_healthz() {
  local port=$1
  local name=$2
  python3 - "$port" "$name" <<'PY'
import sys
import time
import urllib.error
import urllib.request

port = int(sys.argv[1])
name = sys.argv[2]
url = f"http://127.0.0.1:{port}/healthz"
deadline = time.time() + 60
last = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=1) as response:
            if response.status == 200:
                print(f"{name} healthz ok on :{port}")
                sys.exit(0)
            last = f"HTTP {response.status}"
    except (OSError, urllib.error.URLError) as exc:
        last = exc
    time.sleep(0.25)
print(f"{name} healthz failed on :{port}: {last}", file=sys.stderr)
sys.exit(1)
PY
}

wait_for_postgres() {
  local deadline=$((SECONDS + 60))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if DOCKER_HOST=tcp://localhost:2375 docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" exec -T postgres pg_isready -U cc_lb -d cc_lb >/dev/null 2>&1; then
      printf 'postgres ready\n'
      return 0
    fi
    sleep 1
  done
  fail "postgres did not become ready"
}

psql_scalar() {
  local sql=$1
  DOCKER_HOST=tcp://localhost:2375 docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" exec -T postgres \
    psql -U cc_lb -d cc_lb -Atc "$sql" | tr -d '[:space:]'
}

psql_exec() {
  local sql=$1
  DOCKER_HOST=tcp://localhost:2375 docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" exec -T postgres \
    psql -U cc_lb -d cc_lb -v ON_ERROR_STOP=1 -c "$sql" >/dev/null
}

request_with_retry() {
  local method=$1
  local url=$2
  local body=${3:-}
  local output=$4
  local code
  local attempt
  for attempt in 1 2 3 4 5; do
    if [ -n "$body" ]; then
      code=$(curl -sS -o "$output" -w '%{http_code}' -X "$method" \
        -H "authorization: Bearer $ADMIN_TOKEN" \
        -H 'content-type: application/json' \
        --data "$body" "$url") || code=000
    else
      code=$(curl -sS -o "$output" -w '%{http_code}' -X "$method" \
        -H "authorization: Bearer $ADMIN_TOKEN" "$url") || code=000
    fi
    if [ "$code" = "200" ] || [ "$code" = "201" ]; then
      printf '%s' "$code"
      return 0
    fi
    sleep "$attempt"
  done
  printf '%s' "$code"
  return 1
}

assert_status_200() {
  local code=$1
  local label=$2
  if [ "$code" != "200" ]; then
    fail "$label expected HTTP 200, got $code"
  fi
}

json_get() {
  local file=$1
  local expr=$2
  python3 - "$file" "$expr" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle)
for part in sys.argv[2].split("."):
    if part:
        value = value[int(part)] if isinstance(value, list) else value[part]
print(value)
PY
}

extract_query_param() {
  local url=$1
  local name=$2
  python3 - "$url" "$name" <<'PY'
import sys
import urllib.parse

url = urllib.parse.urlparse(sys.argv[1])
params = urllib.parse.parse_qs(url.query)
values = params.get(sys.argv[2], [])
if not values:
    sys.exit(1)
print(values[0])
PY
}

write_config() {
  local path=$1
  local proxy_port=$2
  local admin_port=$3
  local metrics_port=$4
  local data_dir=$5
  cat > "$path" <<TOML
[listener]
proxy_addr = "127.0.0.1:$proxy_port"
admin_addr = "127.0.0.1:$admin_port"
metrics_addr = "127.0.0.1:$metrics_port"

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
principal_id = "$PRINCIPAL_NAME"
upstream_kind = "anthropic_o_auth"
upstream_credential_ref = "$UPSTREAM_NAME"

[storage]
kind = "postgres"
url = "$POSTGRES_URL"

[storage.pool]
max_connections = 8
acquire_timeout_secs = 10
statement_timeout_secs = 25
sslmode = "disable"

[aead]
key_env = "CC_LB_MASTER_KEY"

[oauth.anthropic]
client_id = "fake-client"
auth_url = "http://127.0.0.1:$FAKE_PORT/oauth/authorize"
token_url = "http://127.0.0.1:$FAKE_PORT/oauth/token"
redirect_uri = "http://127.0.0.1:$admin_port/oauth/callback"
scopes = ["messages", "files"]

[runtime]
data_dir = "$data_dir"

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
TOML
}

start_fake_anthropic() {
  cargo run -q -p fake-anthropic -- --port "$FAKE_PORT" > "$TMP_DIR/fake-anthropic.log" 2>&1 &
  FAKE_PID=$!
  python3 - "$FAKE_PORT" <<'PY'
import socket
import sys
import time

port = int(sys.argv[1])
deadline = time.time() + 30
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            print(f"fake-anthropic ready on :{port}")
            sys.exit(0)
    except OSError:
        time.sleep(0.1)
print(f"fake-anthropic did not start on :{port}", file=sys.stderr)
sys.exit(1)
PY
}

start_replica_a() {
  CC_LB_MASTER_KEY="$MASTER_KEY" \
  CC_LB_ADMIN_TOKEN="$ADMIN_TOKEN" \
  CC_LB_BOOTSTRAP_ADMIN_TOKEN="$ADMIN_TOKEN" \
  CC_LB_DATA_DIR="$TMP_DIR/A-data" \
  RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn \
  cargo run -q -p cc-lb-server --features postgres,redb -- serve --config "$TMP_DIR/A.toml" --data-dir "$TMP_DIR/A-data" \
    > "$TMP_DIR/A.log" 2>&1 &
  A_PID=$!
}

start_replica_b() {
  CC_LB_MASTER_KEY="$MASTER_KEY" \
  CC_LB_ADMIN_TOKEN="$ADMIN_TOKEN" \
  CC_LB_DATA_DIR="$TMP_DIR/B-data" \
  RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn \
  cargo run -q -p cc-lb-server --features postgres,redb -- serve --config "$TMP_DIR/B.toml" --data-dir "$TMP_DIR/B-data" \
    > "$TMP_DIR/B.log" 2>&1 &
  B_PID=$!
}

create_principal() {
  local output="$TMP_DIR/principal-create.json"
  local body
  body=$(printf '{"name":"%s","kind":"machine","allowed_models":["*"]}' "$PRINCIPAL_NAME")
  local code
  code=$(request_with_retry POST "http://127.0.0.1:$ADMIN_A_PORT/admin/v1/principals" "$body" "$output") || true
  if [ "$code" != "201" ] && [ "$code" != "409" ]; then
    printf '%s\n' "--- create principal response ---" >&2
    cat "$output" >&2 || true
    fail "create principal expected HTTP 201 or 409, got $code"
  fi
}

create_oauth_upstream() {
  local output="$TMP_DIR/upstream-create.json"
  local body
  body=$(printf '{"name":"%s","kind":"anthropic_oauth","base_url":"http://127.0.0.1:%s"}' "$UPSTREAM_NAME" "$FAKE_PORT")
  local code
  code=$(request_with_retry POST "http://127.0.0.1:$ADMIN_A_PORT/admin/v1/upstreams" "$body" "$output") || true
  if [ "$code" != "201" ]; then
    printf '%s\n' "--- create upstream response ---" >&2
    cat "$output" >&2 || true
    fail "create upstream expected HTTP 201, got $code"
  fi
  json_get "$output" id
}

complete_oauth() {
  local upstream_id=$1
  local start_json="$TMP_DIR/oauth-start.json"
  local complete_json="$TMP_DIR/oauth-complete.json"
  local headers="$TMP_DIR/oauth-authorize.headers"
  local code
  code=$(request_with_retry POST "http://127.0.0.1:$ADMIN_A_PORT/admin/v1/upstreams/$upstream_id/oauth/start" '{}' "$start_json") || true
  assert_status_200 "$code" "oauth start"
  local authorize_url state_token redirect code_param
  authorize_url=$(json_get "$start_json" authorize_url)
  state_token=$(json_get "$start_json" state_token)
  curl -fsS -D "$headers" -o /dev/null "$authorize_url"
  redirect=$(python3 - "$headers" <<'PY'
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    for line in handle:
        if line.lower().startswith("location:"):
            print(line.split(":", 1)[1].strip())
            sys.exit(0)
sys.exit(1)
PY
)
  code_param=$(extract_query_param "$redirect" code)
  code=$(request_with_retry POST "http://127.0.0.1:$ADMIN_A_PORT/admin/v1/upstreams/$upstream_id/oauth/complete" \
    "{\"state_token\":\"$state_token\",\"code\":\"$code_param\"}" "$complete_json") || true
  assert_status_200 "$code" "oauth complete"
}

assert_upstream_active_on_b() {
  local output="$TMP_DIR/status-b.json"
  local code
  code=$(request_with_retry GET "http://127.0.0.1:$ADMIN_B_PORT/admin/v1/status" '' "$output") || true
  assert_status_200 "$code" "B status"
  python3 - "$output" "$UPSTREAM_NAME" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    status = json.load(handle)
name = sys.argv[2]
for upstream in status.get("upstreams", []):
    if upstream.get("name") == name and upstream.get("status") == "active":
        print(f"upstream {name} active on B")
        sys.exit(0)
print(json.dumps(status, indent=2), file=sys.stderr)
sys.exit(1)
PY
}

proxy_request() {
  local port=$1
  local label=$2
  local output="$TMP_DIR/proxy-$label.json"
  local code
  code=$(curl -sS -o "$output" -w '%{http_code}' -X POST \
    -H 'content-type: application/json' \
    -H 'x-api-key: sk-ant-test' \
    -H 'anthropic-version: 2023-06-01' \
    --data '{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hello multi replica"}],"max_tokens":32}' \
    "http://127.0.0.1:$port/v1/messages") || code=000
  assert_status_200 "$code" "proxy $label"
}

assert_audit_entries_landed() {
  local count
  sleep 1
  count=$(psql_scalar "SELECT count(*) FROM audit_log_v1 WHERE route IN ('admin_v1_upstreams','admin_v1_upstream_oauth','admin_v1_principals');")
  if [ "${count:-0}" -lt 3 ]; then
    fail "expected at least 3 admin audit entries, got ${count:-0}"
  fi
  printf 'audit entries visible in postgres: %s\n' "$count"
}

assert_refresh_lease_contention() {
  local upstream_id=$1
  local holder_a='11111111-1111-4111-8111-111111111111'
  local holder_b='22222222-2222-4222-8222-222222222222'
  psql_exec "UPDATE upstreams_v1 SET refresh_lease_holder = NULL, refresh_lease_until = NULL WHERE id = '$upstream_id';"
  psql_exec "UPDATE upstreams_v1 SET refresh_lease_holder = '$holder_a', refresh_lease_until = NOW() + INTERVAL '90 seconds' WHERE id = '$upstream_id' AND (refresh_lease_until IS NULL OR refresh_lease_until <= NOW() OR refresh_lease_holder = '$holder_a');"
  local stolen
  stolen=$(psql_scalar "WITH stolen AS (UPDATE upstreams_v1 SET refresh_lease_holder = '$holder_b', refresh_lease_until = NOW() + INTERVAL '90 seconds' WHERE id = '$upstream_id' AND (refresh_lease_until IS NULL OR refresh_lease_until <= NOW() OR refresh_lease_holder = '$holder_b') RETURNING 1) SELECT count(*) FROM stolen;")
  if [ "${stolen:-0}" != "0" ]; then
    fail "refresh lease contention allowed second holder"
  fi
  psql_exec "UPDATE upstreams_v1 SET refresh_lease_holder = NULL, refresh_lease_until = NULL WHERE id = '$upstream_id';"
  printf 'refresh lease contention preserved single holder\n'
}

mkdir -p "$EVIDENCE_DIR"
ensure_port_free "$PROXY_A_PORT" "$ADMIN_A_PORT" "$METRICS_A_PORT" "$PROXY_B_PORT" "$ADMIN_B_PORT" "$METRICS_B_PORT" "$FAKE_PORT"
DOCKER_HOST=tcp://localhost:2375 docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" up -d postgres
wait_for_postgres
psql_exec 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-multi-replica.XXXXXX")
mkdir -p "$TMP_DIR/A-data" "$TMP_DIR/B-data"
write_config "$TMP_DIR/A.toml" "$PROXY_A_PORT" "$ADMIN_A_PORT" "$METRICS_A_PORT" "$TMP_DIR/A-data"
write_config "$TMP_DIR/B.toml" "$PROXY_B_PORT" "$ADMIN_B_PORT" "$METRICS_B_PORT" "$TMP_DIR/B-data"

start_fake_anthropic
start_replica_a
start_replica_b
if ! wait_for_healthz "$PROXY_A_PORT" "replica A"; then
  printf '%s\n' '--- replica A log ---' >&2
  cat "$TMP_DIR/A.log" >&2 || true
  fail "replica A did not become healthy"
fi
if ! wait_for_healthz "$PROXY_B_PORT" "replica B"; then
  printf '%s\n' '--- replica B log ---' >&2
  cat "$TMP_DIR/B.log" >&2 || true
  fail "replica B did not become healthy"
fi

create_principal
upstream_id=$(create_oauth_upstream)
complete_oauth "$upstream_id"
sleep 5
assert_upstream_active_on_b

for index in 1 2 3 4 5 6 7 8 9 10; do
  if [ $((index % 2)) -eq 1 ]; then
    proxy_request "$PROXY_A_PORT" "round-robin-$index-A"
  else
    proxy_request "$PROXY_B_PORT" "round-robin-$index-B"
  fi
done
assert_audit_entries_landed

kill -TERM "$A_PID" 2>/dev/null || true
sleep 2
wait "$A_PID" 2>/dev/null || true
A_PID=''
for index in 1 2 3 4 5; do
  proxy_request "$PROXY_B_PORT" "B-after-A-kill-$index"
done

start_replica_a
if ! wait_for_healthz "$PROXY_A_PORT" "replica A restarted"; then
  printf '%s\n' '--- replica A restarted log ---' >&2
  cat "$TMP_DIR/A.log" >&2 || true
  fail "replica A did not rejoin"
fi
sleep 5
assert_upstream_active_on_b
proxy_request "$PROXY_A_PORT" "A-rejoined"
assert_refresh_lease_contention "$upstream_id"
capture_evidence

printf 'PASS multi-replica postgres smoke; evidence in %s\n' "$EVIDENCE_DIR"
