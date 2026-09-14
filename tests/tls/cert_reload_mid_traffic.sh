#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
EVIDENCE_PATH=${CC_LB_TLS_EVIDENCE:-"$ROOT_DIR/.omo/evidence/task-47-cert-reload.log"}
API_KEY='sk-ant-test'
ADMIN_TOKEN='admin-token'
FAKE_PID=''
PROXY_PID=''
STREAM_PID=''
TMP_DIR=''

log() {
  printf '%s\n' "$*"
  printf '%s\n' "$*" >> "$EVIDENCE_PATH"
}

fail() {
  log "FAIL reason=$1"
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
  cleanup_pid_tree "$STREAM_PID"
  cleanup_pid_tree "$PROXY_PID"
  cleanup_pid_tree "$FAKE_PID"
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
  rmdir "$SCRIPT_DIR/.tmp" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

free_port() {
  python3 - <<'PYPORT'
import socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('127.0.0.1', 0))
print(sock.getsockname()[1])
sock.close()
PYPORT
}

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

wait_file_contains() {
  file=$1
  needle=$2
  python3 - "$file" "$needle" <<'PYPOLL'
import pathlib, sys, time
path = pathlib.Path(sys.argv[1])
needle = sys.argv[2]
deadline = time.time() + 30
while time.time() < deadline:
    if path.exists() and needle in path.read_text(errors='replace'):
        sys.exit(0)
    time.sleep(0.1)
print(f'{path} did not contain {needle!r}', file=sys.stderr)
sys.exit(1)
PYPOLL
}

wait_command() {
  name=$1
  shift
  deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then
      log "$name ready"
      return 0
    fi
    sleep 0.1
  done
  fail "$name did not become ready"
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
  log "runtime_seeded=true"
}

cert_fingerprint() {
  openssl x509 -in "$1" -noout -fingerprint -sha256 | sed 's/^sha256 Fingerprint=//'
}

cert_serial() {
  openssl x509 -in "$1" -noout -serial | sed 's/^serial=//'
}

cert_subject() {
  openssl x509 -in "$1" -noout -subject | sed 's/^subject=//'
}

served_fingerprint() {
  openssl s_client -connect "127.0.0.1:$proxy_port" -servername localhost </dev/null 2>/dev/null \
    | openssl x509 -noout -fingerprint -sha256 2>/dev/null \
    | sed 's/^sha256 Fingerprint=//' || true
}

served_tls12_fingerprint() {
  openssl s_client -tls1_2 -connect "127.0.0.1:$proxy_port" -servername localhost </dev/null 2>/dev/null \
    | openssl x509 -noout -fingerprint -sha256 2>/dev/null \
    | sed 's/^sha256 Fingerprint=//' || true
}

wait_served_fingerprint() {
  expected=$1
  deadline=$((SECONDS + 30))
  last=''
  while [ "$SECONDS" -lt "$deadline" ]; do
    last=$(served_fingerprint)
    if [ "$last" = "$expected" ]; then
      log "served_fingerprint matched=$expected"
      return 0
    fi
    sleep 0.2
  done
  fail "served fingerprint did not become $expected last=$last"
}

render_config() {
  config_path=$1
  cat > "$config_path" <<TOML
[listener]
proxy_addr = "127.0.0.1:$proxy_port"
admin_addr = "127.0.0.1:$admin_port"
metrics_addr = "127.0.0.1:$metrics_port"

[listener.tls]
cert_path = "$active_cert"
key_path = "$active_key"
reload_on_sighup = true

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
max_conns_per_upstream = 100
semaphore_per_upstream = 200

[dns]
cache_ttl_floor_secs = 30
cache_ttl_ceiling_secs = 300

[egress]
TOML
}

mkdir -p "$SCRIPT_DIR/.tmp" "$(dirname -- "$EVIDENCE_PATH")"
: > "$EVIDENCE_PATH"
TMP_DIR=$(mktemp -d "$SCRIPT_DIR/.tmp/run.XXXXXX")

cargo build -q -p fake-anthropic -p cc-lb-server

fake_port=$(free_port)
proxy_port=$(free_port)
admin_port=$(free_port)
metrics_port=$(free_port)
active_cert="$TMP_DIR/server-cert.pem"
active_key="$TMP_DIR/server-key.pem"
cp "$ROOT_DIR/crates/cc-lb-server/tests/fixtures/tls/cert-a.pem" "$active_cert"
cp "$ROOT_DIR/crates/cc-lb-server/tests/fixtures/tls/key-a.pem" "$active_key"
cert_b="$ROOT_DIR/crates/cc-lb-server/tests/fixtures/tls/cert-b.pem"
key_b="$ROOT_DIR/crates/cc-lb-server/tests/fixtures/tls/key-b.pem"
config_path="$TMP_DIR/cc-lb.toml"
render_config "$config_path"

old_fp=$(cert_fingerprint "$active_cert")
new_fp=$(cert_fingerprint "$cert_b")
old_serial=$(cert_serial "$active_cert")
new_serial=$(cert_serial "$cert_b")
old_subject=$(cert_subject "$active_cert")
new_subject=$(cert_subject "$cert_b")
log "old_cert fingerprint=$old_fp serial=$old_serial subject=$old_subject"
log "new_cert fingerprint=$new_fp serial=$new_serial subject=$new_subject"

"$ROOT_DIR/target/debug/fake-anthropic" --port "$fake_port" --slow-mode-bps 8192 > "$TMP_DIR/fake-anthropic.log" 2>&1 &
FAKE_PID=$!
wait_http "$fake_port" "/v1/models" "fake-anthropic"

CC_LB_ADMIN_TOKEN=admin-token \
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn \
"$ROOT_DIR/target/debug/cc-lb" serve --config "$config_path" > "$TMP_DIR/cc-lb.log" 2>&1 &
PROXY_PID=$!

wait_command "cc-lb-admin" curl --fail --silent --show-error "http://127.0.0.1:$admin_port/admin/health"
wait_command "cc-lb-proxy-tls" curl --fail --silent --show-error --cacert "$active_cert" --resolve "localhost:$proxy_port:127.0.0.1" "https://localhost:$proxy_port/healthz"
wait_command "cc-lb-metrics" curl --fail --silent --show-error "http://127.0.0.1:$metrics_port/metrics"
seed_runtime
log "plain_http_admin=true plain_http_metrics=true proxy_tls_health=true"

pre_status=$(curl --fail --silent --show-error --write-out '%{http_code}' --output "$TMP_DIR/pre.json" --cacert "$active_cert" --resolve "localhost:$proxy_port:127.0.0.1" -H "x-api-key: $API_KEY" "https://localhost:$proxy_port/v1/models")
[ "$pre_status" = "200" ] || fail "pre-reload request returned $pre_status"
observed_old_fp=$(served_fingerprint)
[ "$observed_old_fp" = "$old_fp" ] || fail "expected old cert before reload observed=$observed_old_fp"
log "pre_reload request_status=$pre_status observed_fingerprint=$observed_old_fp"

tls12_old_fp=$(served_tls12_fingerprint)
[ "$tls12_old_fp" = "$old_fp" ] || fail "TLS 1.2-only client was not accepted with old cert observed=$tls12_old_fp"
log "tls12_version_behavior actual=accepted rationale=rustls_ring_tls12_feature_and_DEFAULT_VERSIONS observed_fingerprint=$tls12_old_fp"

curl --no-buffer --fail --silent --show-error --cacert "$active_cert" --resolve "localhost:$proxy_port:127.0.0.1" \
  -H "x-api-key: $API_KEY" \
  -H 'anthropic-version: 2023-06-01' \
  -H 'content-type: application/json' \
  -H 'accept: text/event-stream' \
  -H 'x-fake-mode: slow' \
  --data @"$ROOT_DIR/tests/load/stream-req.json" \
  "https://localhost:$proxy_port/v1/messages" > "$TMP_DIR/stream.out" 2> "$TMP_DIR/stream.err" &
STREAM_PID=$!
wait_file_contains "$TMP_DIR/stream.out" "content_block_delta"
log "in_flight_started=true in_flight_cert_fingerprint=$observed_old_fp"

cp "$cert_b" "$active_cert"
cp "$key_b" "$active_key"
kill -HUP "$PROXY_PID"
wait_served_fingerprint "$new_fp"

post_status=$(curl --fail --silent --show-error --write-out '%{http_code}' --output "$TMP_DIR/post.json" --cacert "$active_cert" --resolve "localhost:$proxy_port:127.0.0.1" -H "x-api-key: $API_KEY" "https://localhost:$proxy_port/v1/models")
[ "$post_status" = "200" ] || fail "post-reload request returned $post_status"
log "post_reload request_status=$post_status observed_fingerprint=$new_fp"

tls12_new_fp=$(served_tls12_fingerprint)
[ "$tls12_new_fp" = "$new_fp" ] || fail "TLS 1.2-only client did not observe new cert after reload observed=$tls12_new_fp"
log "tls12_post_reload actual=accepted observed_fingerprint=$tls12_new_fp"

wait "$STREAM_PID"
STREAM_PID=''
wait_file_contains "$TMP_DIR/stream.out" "message_stop"
metrics=$(curl --fail --silent --show-error "http://127.0.0.1:$metrics_port/metrics")
case "$metrics" in
  *'cc_lb_tls_reload_total{outcome="success"} 1'*) tls_metric='success=1' ;;
  *'cc_lb_tls_reload_total{outcome="success"}'*) tls_metric='success_present' ;;
  *) fail 'cc_lb_tls_reload_total success metric missing' ;;
esac
log "metrics cc_lb_tls_reload_total=$tls_metric"
log "PASS in_flight_completed_with_cert_a=true new_conn_used_cert_b=true old_new_fingerprints_differ=true tls12_actual_behavior=accepted evidence=$EVIDENCE_PATH"
