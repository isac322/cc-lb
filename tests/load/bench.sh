#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
MODE=${1:-all}
FAKE_PID=''
PROXY_PID=''
TMP_DIR=''
EVIDENCE_PATH="$ROOT_DIR/.omo/evidence/task-40-perf-budget.json"
BASELINE_PATH="$ROOT_DIR/tests/load/baseline.json"
API_KEY='sk-ant-test'

usage() {
  printf 'usage: %s <non-streaming|streaming|all>\n' "$0" >&2
  exit 2
}

fail() {
  printf 'FAIL reason: %s\n' "$1" >&2
  exit 1
}

cleanup_pid_tree() {
  pid=$1
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

cleanup() {
  cleanup_pid_tree "$PROXY_PID"
  cleanup_pid_tree "$FAKE_PID"
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
  rmdir "$SCRIPT_DIR/.tmp" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

case "$MODE" in
  non-streaming|streaming|all) ;;
  *) usage ;;
esac

if command -v oha >/dev/null 2>&1; then
  OHA_AVAILABLE=true
else
  OHA_AVAILABLE=false
fi

SELECTED_TOOL='cc-lb-loadgen'
FALLBACK='oha unavailable; using deterministic local raw TCP load generator in tests/load'
if [ "$OHA_AVAILABLE" = true ]; then
  SELECTED_TOOL='oha'
  FALLBACK='oha available; local generator still writes normalized budget evidence for deterministic parsing'
fi

free_port() {
  python3 - <<'PY'
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('127.0.0.1', 0))
print(sock.getsockname()[1])
sock.close()
PY
}

wait_http() {
  port=$1
  path=$2
  name=$3
  python3 - "$port" "$path" "$name" "$API_KEY" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
path = sys.argv[2]
name = sys.argv[3]
api_key = sys.argv[4]
deadline = time.time() + 30
last = None
request = (
    f"GET {path} HTTP/1.1\r\n"
    f"Host: 127.0.0.1:{port}\r\n"
    f"x-api-key: {api_key}\r\n"
    "Connection: close\r\n\r\n"
).encode()
while time.time() < deadline:
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.5) as sock:
            sock.sendall(request)
            data = sock.recv(256)
        if b" 200 " in data.split(b"\r\n", 1)[0]:
            print(f'{name} ready on 127.0.0.1:{port}')
            sys.exit(0)
        last = data[:120].decode(errors='replace')
    except OSError as exc:
        last = str(exc)
    time.sleep(0.1)
print(f'{name} did not become ready on 127.0.0.1:{port}: {last}', file=sys.stderr)
sys.exit(1)
PY
}

render_bootstrap() {
  bootstrap_path=$1
  cat > "$bootstrap_path" <<TOML
[[upstreams]]
name = "fake_anthropic"
kind = "custom"
base_url = "http://127.0.0.1:$fake_port"

[[principals]]
name = "api-key"
kind = "machine"
TOML
}

render_config() {
  config_path=$1
  cat > "$config_path" <<TOML
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

[runtime]
data_dir = "$TMP_DIR"

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "api-key"
upstream_kind = "anthropic_key"
upstream_credential_ref = ""

[storage]
kind = "redb"
path = "$TMP_DIR/cc-lb.redb"

[aead]
key_env = "CC_LB_MASTER_KEY"

[api_keys.price_catalog]
cache_path = "$TMP_DIR/price-catalog.json"

[observability]
tracing_level = "warn"
log_redaction = true
user_prompt_redaction = false

[quotas]
default_window_secs = 60
default_requests_per_window = 100000
default_input_tokens = 100000000
default_output_tokens = 100000000

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[circuit_breaker]
failures_to_open = 5
window_secs = 10
half_open_after_secs = 30

[bulkhead]
max_conns_per_upstream = 100
semaphore_per_upstream = 200

[dns]
cache_ttl_floor_secs = 30
cache_ttl_ceiling_secs = 300

[egress]
TOML
}

run_mode() {
  mode=$1
  case "$mode" in
    non-streaming)
      body="$SCRIPT_DIR/small-req.json"
      requests=${CC_LB_LOAD_NON_STREAMING_REQUESTS:-300}
      concurrency=${CC_LB_LOAD_NON_STREAMING_CONCURRENCY:-4}
      warmup=${CC_LB_LOAD_NON_STREAMING_WARMUP:-32}
      ;;
    streaming)
      body="$SCRIPT_DIR/stream-req.json"
      requests=${CC_LB_LOAD_STREAMING_REQUESTS:-120}
      concurrency=${CC_LB_LOAD_STREAMING_CONCURRENCY:-16}
      warmup=${CC_LB_LOAD_STREAMING_WARMUP:-16}
      ;;
    *) fail "unsupported mode: $mode" ;;
  esac

  "$ROOT_DIR/target/debug/cc-lb-loadgen" \
    --mode "$mode" \
    --direct-url "http://127.0.0.1:$fake_port/v1/messages" \
    --proxy-url "http://127.0.0.1:$proxy_port/v1/messages" \
    --body "$body" \
    --requests "$requests" \
    --concurrency "$concurrency" \
    --warmup "$warmup" \
    --output "$TMP_DIR/$mode-summary.json" \
    --evidence "$EVIDENCE_PATH" \
    --baseline "$BASELINE_PATH" \
    --tool "$SELECTED_TOOL" \
    --oha-available "$OHA_AVAILABLE" \
    --fallback "$FALLBACK"
}

mkdir -p "$SCRIPT_DIR/.tmp" "$ROOT_DIR/.omo/evidence"
TMP_DIR=$(mktemp -d "$SCRIPT_DIR/.tmp/run.XXXXXX")

cargo build -q -p fake-anthropic -p cc-lb-server
cargo build -q -p cc-lb-loadgen --bin cc-lb-loadgen

fake_port=$(free_port)
proxy_port=$(free_port)
admin_port=$(free_port)
metrics_port=$(free_port)
config_path="$TMP_DIR/cc-lb.toml"
bootstrap_path="$TMP_DIR/bootstrap.toml"
render_bootstrap "$bootstrap_path"
render_config "$config_path"

"$ROOT_DIR/target/debug/fake-anthropic" --port "$fake_port" > "$TMP_DIR/fake-anthropic.log" 2>&1 &
FAKE_PID=$!
wait_http "$fake_port" "/v1/models" "fake-anthropic"

CC_LB_ADMIN_TOKEN=admin-token \
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
RUST_LOG=warn,hyper=warn,hyper_util=warn,axum=warn \
"$ROOT_DIR/target/debug/cc-lb" serve --config "$config_path" > "$TMP_DIR/cc-lb.log" 2>&1 &
PROXY_PID=$!
wait_http "$proxy_port" "/healthz" "cc-lb"

if [ "$MODE" = all ]; then
  rm -f "$EVIDENCE_PATH"
  run_mode non-streaming
  run_mode streaming
else
  run_mode "$MODE"
fi

printf 'evidence: %s\n' "$EVIDENCE_PATH"
