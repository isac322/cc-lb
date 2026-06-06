#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
DURATION=${1:-24h}
CONCURRENCY=${CC_LB_SOAK_CONCURRENCY:-10}
API_KEY='sk-ant-test'
ADMIN_TOKEN='admin-token'
FAKE_PID=''
PROXY_PID=''
LOAD_PID=''
TMP_DIR=''

usage() {
  printf 'usage: %s <duration: Ns|Nm|Nh>\n' "$0" >&2
  exit 2
}

fail() {
  printf 'FAIL reason: %s\n' "$1" >&2
  exit 1
}

parse_duration_secs() {
  python3 - "$1" <<'PYDUR'
import re, sys
value = sys.argv[1]
match = re.fullmatch(r'([1-9][0-9]*)([smh])', value)
if not match:
    print('duration must look like 1s, 1m, or 1h', file=sys.stderr)
    sys.exit(2)
amount = int(match.group(1))
unit = match.group(2)
multiplier = {'s': 1, 'm': 60, 'h': 3600}[unit]
print(amount * multiplier)
PYDUR
}

free_port() {
  python3 - <<'PYPORT'
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('127.0.0.1', 0))
print(sock.getsockname()[1])
sock.close()
PYPORT
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
  cleanup_pid_tree "$LOAD_PID"
  cleanup_pid_tree "$PROXY_PID"
  cleanup_pid_tree "$FAKE_PID"
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
  rmdir "$SCRIPT_DIR/.tmp" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

wait_http() {
  port=$1
  path=$2
  name=$3
  python3 - "$port" "$path" "$name" "$API_KEY" <<'PYWAIT'
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
PYWAIT
}

seed_runtime() {
  principal_code=$(curl -sS -o "$TMP_DIR/admin-principal.json" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H 'content-type: application/json' \
    --data '{"name":"api-key","kind":"machine","allowed_models":["*"]}' \
    "http://127.0.0.1:$admin_port/admin/v1/principals") || principal_code=000
  if [ "$principal_code" != "201" ] && [ "$principal_code" != "409" ]; then
    cat "$TMP_DIR/admin-principal.json" >&2 || true
    fail "create principal expected HTTP 201 or 409, got $principal_code"
  fi

  upstream_body=$(printf '{"name":"fake_anthropic","kind":"anthropic_api_key","base_url":"http://127.0.0.1:%s","api_key_value":"sk-ant-test"}' "$fake_port")
  upstream_code=$(curl -sS -o "$TMP_DIR/admin-upstream.json" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -H 'content-type: application/json' \
    --data "$upstream_body" \
    "http://127.0.0.1:$admin_port/admin/v1/upstreams") || upstream_code=000
  if [ "$upstream_code" != "201" ] && [ "$upstream_code" != "409" ]; then
    cat "$TMP_DIR/admin-upstream.json" >&2 || true
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

[downstream_auth]
mode = "none"

[downstream_auth.none_mode]
principal_id = "api-key"
upstream_kind = "anthropic_key"


[storage]
oauth_aead_key_env = "CC_LB_MASTER_KEY"

[observability]
tracing_level = "warn"
log_redaction = true
user_prompt_redaction = false

[quotas]
default_window_secs = 60
default_requests_per_window = 10000000
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

rss_kib() {
  pid=$1
  awk '/^VmRSS:/ { print $2; found=1 } END { if (!found) exit 1 }' "/proc/$pid/status"
}

sample_rss() {
  now=$(date +%s)
  rss=$(rss_kib "$PROXY_PID")
  printf '%s,%s\n' "$now" "$rss" >> "$CSV_PATH"
}

verify_no_lingering() {
  local found=0
  for pattern in '[c]c-lb' '[f]ake-anthropic' '[c]c-lb-loadgen'; do
    if pgrep -af "$pattern" >/dev/null 2>&1; then
      printf 'lingering process match for %s:\n' "$pattern" >&2
      pgrep -af "$pattern" >&2 || true
      found=1
    fi
  done
  [ "$found" -eq 0 ] || return 1
}

DURATION_SECS=$(parse_duration_secs "$DURATION") || usage
SAMPLE_INTERVAL=$((DURATION_SECS / 12))
if [ "$SAMPLE_INTERVAL" -lt 5 ]; then
  SAMPLE_INTERVAL=5
fi
case "$DURATION" in
  *[!A-Za-z0-9_.-]*) usage ;;
esac
CSV_PATH=${CC_LB_SOAK_CSV:-"$ROOT_DIR/.omo/evidence/task-41-soak-$DURATION.csv"}

mkdir -p "$SCRIPT_DIR/.tmp" "$(dirname -- "$CSV_PATH")"
TMP_DIR=$(mktemp -d "$SCRIPT_DIR/.tmp/run.XXXXXX")

cargo build --release -q -p fake-anthropic
cargo build --release -q -p cc-lb-server
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

CC_LB_ADMIN_TOKEN=admin-token \
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
RUST_LOG=warn,hyper=warn,hyper_util=warn,axum=warn \
"$ROOT_DIR/target/release/cc-lb" serve --config "$config_path" > "$TMP_DIR/cc-lb.log" 2>&1 &
PROXY_PID=$!
wait_http "$proxy_port" "/healthz" "cc-lb"
wait_http "$admin_port" "/admin/health" "cc-lb-admin"
seed_runtime

printf 'unix_time,rss_kib\n' > "$CSV_PATH"
sample_rss

"$ROOT_DIR/target/release/cc-lb-loadgen" \
  --mode non-streaming \
  --direct-url "http://127.0.0.1:$fake_port/v1/messages" \
  --proxy-url "http://127.0.0.1:$proxy_port/v1/messages" \
  --body "$ROOT_DIR/tests/load/small-req.json" \
  --soak-duration-secs "$DURATION_SECS" \
  --concurrency "$CONCURRENCY" \
  --output "$TMP_DIR/soak-summary.json" > "$TMP_DIR/loadgen.log" 2>&1 &
LOAD_PID=$!

while kill -0 "$LOAD_PID" 2>/dev/null; do
  sleep "$SAMPLE_INTERVAL" || true
  if kill -0 "$PROXY_PID" 2>/dev/null; then
    sample_rss
  fi
done
wait "$LOAD_PID"
LOAD_PID=''
sample_rss

initial=$(awk -F, 'NR == 2 { print $2 }' "$CSV_PATH")
final=$(awk -F, 'NF == 2 && $1 !~ /^#/ { value=$2 } END { print value }' "$CSV_PATH")
delta=$((final - initial))
if [ "$delta" -lt 16384 ]; then
  printf '# PASS delta_kib=%s\n' "$delta" >> "$CSV_PATH"
else
  printf '# FAIL delta_kib=%s\n' "$delta" >> "$CSV_PATH"
fi

cleanup_pid_tree "$LOAD_PID"
LOAD_PID=''
cleanup_pid_tree "$PROXY_PID"
PROXY_PID=''
cleanup_pid_tree "$FAKE_PID"
FAKE_PID=''
rm -rf "$TMP_DIR"
TMP_DIR=''
rmdir "$SCRIPT_DIR/.tmp" 2>/dev/null || true
verify_no_lingering

printf 'csv: %s\n' "$CSV_PATH"
if [ "$delta" -lt 16384 ]; then
  printf 'PASS delta_kib=%s\n' "$delta"
else
  fail "RSS delta ${delta} KiB >= 16384 KiB"
fi
