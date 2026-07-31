#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PROFILE=smoke
FAKE_PID=''
PROXY_PID=''
TMP_DIR=''
API_KEY='sk-ant-test'
ADMIN_TOKEN='admin-token'

usage() {
  printf 'usage: %s [--profile smoke|soak|burst|leak] [smoke|soak|burst|leak]\n' "$0" >&2
  exit 2
}

fail() {
  printf 'FAIL reason: %s\n' "$1" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --profile)
      shift
      [ "$#" -gt 0 ] || usage
      PROFILE=$1
      ;;
    smoke|soak|burst|leak)
      PROFILE=$1
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
  shift
done

case "$PROFILE" in
  smoke)
    RPS=${CC_LB_LOAD_RPS:-20}
    DURATION_SECS=${CC_LB_LOAD_DURATION_SECS:-90}
    STREAM_RATIO=${CC_LB_LOAD_STREAM_RATIO:-20}
    SSE_SUBSCRIBERS=${CC_LB_LOAD_SSE_SUBSCRIBERS:-1}
    RECONNECT_CHURN_SECS=${CC_LB_LOAD_RECONNECT_CHURN_SECS:-0}
    ;;
  soak)
    RPS=${CC_LB_LOAD_RPS:-100}
    DURATION_SECS=${CC_LB_LOAD_DURATION_SECS:-900}
    STREAM_RATIO=${CC_LB_LOAD_STREAM_RATIO:-20}
    SSE_SUBSCRIBERS=${CC_LB_LOAD_SSE_SUBSCRIBERS:-3}
    RECONNECT_CHURN_SECS=${CC_LB_LOAD_RECONNECT_CHURN_SECS:-30}
    ;;
  burst)
    RPS=${CC_LB_LOAD_RPS:-500}
    DURATION_SECS=${CC_LB_LOAD_DURATION_SECS:-180}
    STREAM_RATIO=${CC_LB_LOAD_STREAM_RATIO:-10}
    SSE_SUBSCRIBERS=${CC_LB_LOAD_SSE_SUBSCRIBERS:-5}
    RECONNECT_CHURN_SECS=${CC_LB_LOAD_RECONNECT_CHURN_SECS:-15}
    ;;
  leak)
    RPS=${CC_LB_LOAD_RPS:-50}
    DURATION_SECS=${CC_LB_LOAD_DURATION_SECS:-3600}
    STREAM_RATIO=${CC_LB_LOAD_STREAM_RATIO:-20}
    SSE_SUBSCRIBERS=${CC_LB_LOAD_SSE_SUBSCRIBERS:-2}
    RECONNECT_CHURN_SECS=${CC_LB_LOAD_RECONNECT_CHURN_SECS:-60}
    ;;
  *) usage ;;
esac

MAX_IN_FLIGHT=${CC_LB_LOAD_MAX_IN_FLIGHT:-$RPS}
METRICS_SCRAPE_SECS=${CC_LB_LOAD_METRICS_SCRAPE_SECS:-5}
EVIDENCE_DIR="$ROOT_DIR/tests/load/evidence"
EVIDENCE_PATH="$EVIDENCE_DIR/live-tail-soak.$PROFILE.json"
METRICS_OUTPUT="$EVIDENCE_DIR/live-tail-soak.$PROFILE.metrics.jsonl"

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

seed_runtime() {
  principal_code=$(curl -sS -o "$TMP_DIR/admin-principal.json" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H 'content-type: application/json' \
    --data '{"name":"api-key","kind":"machine","allowed_models":["*"]}' \
    "http://127.0.0.1:$admin_port/admin/v1/principals") || principal_code=000
  if [ "$principal_code" != "201" ] && [ "$principal_code" != "409" ]; then
    command cat "$TMP_DIR/admin-principal.json" >&2 || true
    fail "create principal expected HTTP 201 or 409, got $principal_code"
  fi

  upstream_body=$(printf '{"name":"fake_anthropic","kind":"anthropic_api_key","base_url":"http://127.0.0.1:%s","api_key_value":"sk-ant-test"}' "$fake_port")
  upstream_code=$(curl -sS -o "$TMP_DIR/admin-upstream.json" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H 'content-type: application/json' \
    --data "$upstream_body" \
    "http://127.0.0.1:$admin_port/admin/v1/upstreams") || upstream_code=000
  if [ "$upstream_code" != "201" ] && [ "$upstream_code" != "409" ]; then
    command cat "$TMP_DIR/admin-upstream.json" >&2 || true
    fail "create upstream expected HTTP 201 or 409, got $upstream_code"
  fi
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

[storage]
kind = "sqlite"
path = "$TMP_DIR/cc-lb.sqlite"

[event_bus]
broadcast_capacity = 4096
storage_tail_poll_interval_ms = 250

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
default_requests_per_window = 1000000
default_input_tokens = 100000000
default_output_tokens = 100000000

[admin]
token_env = "CC_LB_ADMIN_TOKEN"

[circuit_breaker]
failures_to_open = 5
window_secs = 10
half_open_after_secs = 30

[bulkhead]
max_conns_per_upstream = 1000
semaphore_per_upstream = 1000

[dns]
cache_ttl_floor_secs = 30
cache_ttl_ceiling_secs = 300

[egress]
TOML
}

mkdir -p "$SCRIPT_DIR/.tmp" "$EVIDENCE_DIR"
TMP_DIR=$(mktemp -d "$SCRIPT_DIR/.tmp/live-tail.XXXXXX")

cargo build --release -q -p fake-anthropic
cargo build --release -q -p cc-lb-server --features sqlite
cargo build --release -q -p cc-lb-loadgen --bin cc-lb-loadgen

fake_port=$(free_port)
proxy_port=$(free_port)
admin_port=$(free_port)
metrics_port=$(free_port)
config_path="$TMP_DIR/cc-lb.toml"
render_config "$config_path"

"$ROOT_DIR/target/release/fake-anthropic" --port "$fake_port" > "$TMP_DIR/fake-anthropic.log" 2>&1 &
FAKE_PID=$!
wait_http "$fake_port" "/v1/models" "fake-anthropic"

CC_LB_ADMIN_TOKEN=$ADMIN_TOKEN \
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
RUST_LOG=warn,hyper=warn,hyper_util=warn,axum=warn \
"$ROOT_DIR/target/release/cc-lb" serve --config "$config_path" > "$TMP_DIR/cc-lb.log" 2>&1 &
PROXY_PID=$!
wait_http "$proxy_port" "/healthz" "cc-lb"
wait_http "$admin_port" "/admin/health" "cc-lb-admin"
wait_http "$metrics_port" "/metrics" "cc-lb-metrics"
seed_runtime

CC_LB_LOAD_PROFILE=$PROFILE "$ROOT_DIR/target/release/cc-lb-loadgen" \
  --mode live-tail-soak \
  --proxy-url "http://127.0.0.1:$proxy_port/v1/messages" \
  --body "$SCRIPT_DIR/small-req.json" \
  --stream-body "$SCRIPT_DIR/stream-req.json" \
  --output "$EVIDENCE_PATH" \
  --rps "$RPS" \
  --duration-secs "$DURATION_SECS" \
  --stream-ratio "$STREAM_RATIO" \
  --max-in-flight "$MAX_IN_FLIGHT" \
  --sse-subscribers "$SSE_SUBSCRIBERS" \
  --reconnect-churn-secs "$RECONNECT_CHURN_SECS" \
  --sse-stream-url "http://127.0.0.1:$admin_port/admin/events/stream" \
  --admin-token "$ADMIN_TOKEN" \
  --metrics-scrape-url "http://127.0.0.1:$metrics_port/metrics" \
  --metrics-scrape-interval-secs "$METRICS_SCRAPE_SECS" \
  --metrics-output "$METRICS_OUTPUT" \
  --rss-pid "$PROXY_PID"

printf 'evidence: %s\nmetrics: %s\n' "$EVIDENCE_PATH" "$METRICS_OUTPUT"
