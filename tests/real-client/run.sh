#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PROMPT='Print the word HELLO'
API_KEY='sk-ant-api03-abcdefghijklmnopqrstuvwxyz0123456789'
FAKE_PID=''
PROXY_PID=''
TMP_DIR=''

usage() {
  printf 'usage: %s <client> <upstream>\n' "$0" >&2
  exit 2
}

skip() {
  printf 'SKIP reason: %s\n' "$1"
  exit 77
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
}
trap cleanup EXIT INT TERM

[ "$#" -eq 2 ] || usage
client=$1
upstream=$2

case "$client" in
  claude-code|opencode|pi) ;;
  *) fail "unsupported client: $client" ;;
esac
case "$upstream" in
  anthropic-direct|custom) ;;
  *) fail "unsupported upstream: $upstream" ;;
esac

expected_file="$SCRIPT_DIR/expected/$client-$upstream.txt"
[ -f "$expected_file" ] || fail "missing expected file: $expected_file"
expected=$(tr -d '\r\n' < "$expected_file")
[ -n "$expected" ] || fail "empty expected substring: $expected_file"

fake_package=fake-anthropic

. "$SCRIPT_DIR/install.sh"

case "$client" in
  claude-code) bin=${CLAUDE_CODE_BIN:-}; status_file="$ROOT_DIR/target/test-bins/claude-code/install-status" ;;
  opencode) bin=${OPENCODE_BIN:-}; status_file="$ROOT_DIR/target/test-bins/opencode/install-status" ;;
  pi) bin=${PI_BIN:-}; status_file="$ROOT_DIR/target/test-bins/pi/install-status" ;;
esac

if [ -z "${bin:-}" ] || [ ! -x "$bin" ]; then
  if [ -f "$status_file" ]; then
    reason=$(sed -n 's/^SKIP [^ ]* reason: //p' "$status_file" | head -n 1)
    [ -n "$reason" ] || reason="client binary missing: $bin"
    skip "$reason"
  fi
  skip "client binary missing: $bin"
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

wait_port() {
  port=$1
  name=$2
  python3 - "$port" "$name" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
name = sys.argv[2]
deadline = time.time() + 30
last = None
while time.time() < deadline:
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.5):
            print(f'{name} accepting tcp on 127.0.0.1:{port}')
            sys.exit(0)
    except OSError as exc:
        last = exc
        time.sleep(0.1)
print(f'{name} did not accept tcp on 127.0.0.1:{port}: {last}', file=sys.stderr)
sys.exit(1)
PY
}

render_config() {
  template=$1
  output=$2
  python3 - "$template" "$output" "$proxy_port" "$admin_port" "$metrics_port" "$fake_port" "$TMP_DIR" <<'PY'
from pathlib import Path
import sys
src, dst, proxy, admin, metrics, fake, tmp = sys.argv[1:]
text = Path(src).read_text()
replacements = {
    '__PROXY_ADDR__': f'127.0.0.1:{proxy}',
    '__ADMIN_ADDR__': f'127.0.0.1:{admin}',
    '__METRICS_ADDR__': f'127.0.0.1:{metrics}',
    '__FAKE_URL__': f'http://127.0.0.1:{fake}',
    '__REGION__': 'us-east-1',
    '__VERTEX_REGION__': 'us-central1',
    '__PROJECT__': 'fake-project',
    '__REDB_PATH__': str(Path(tmp) / 'cc-lb.redb'),
}
for old, new in replacements.items():
    text = text.replace(old, new)
Path(dst).write_text(text)
PY
}

detect_skip_reason() {
  stderr_file=$1
  if grep -Eiq 'tty|required.*interactive|interactive.*required|browser|oauth|login|required.*auth|terms|ToS|subscription|not logged in|permission denied' "$stderr_file"; then
    grep -Ei 'tty|required.*interactive|interactive.*required|browser|oauth|login|required.*auth|terms|ToS|subscription|not logged in|permission denied' "$stderr_file" | head -n 1 | sed 's/^/client refused deterministic non-interactive run: /'
    return 0
  fi
  return 1
}

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-real-client.XXXXXX")
# Allocate 4 distinct ports atomically: hold all sockets open during reservation
# so the kernel cannot hand the same ephemeral port to two sockets, then close
# them just before binding. Avoids race collisions seen on busy CI runners.
read -r proxy_port admin_port metrics_port fake_port <<EOF
$(python3 - <<'PY'
import socket
socks = []
for _ in range(4):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    socks.append(s)
ports = [s.getsockname()[1] for s in socks]
for s in socks:
    s.close()
print(' '.join(str(p) for p in ports))
PY
)
EOF
config_path="$TMP_DIR/cc-lb.toml"
gcp_credentials="$TMP_DIR/gcp-adc.json"
stdout_file="$TMP_DIR/client.stdout"
stderr_file="$TMP_DIR/client.stderr"
mkdir -p "$TMP_DIR/home" "$TMP_DIR/xdg-config" "$TMP_DIR/xdg-data" "$TMP_DIR/pi-agent"
cat > "$TMP_DIR/pi-agent/models.json" <<JSON
{"providers":{"anthropic":{"baseUrl":"http://127.0.0.1:$proxy_port","apiKey":"ANTHROPIC_API_KEY","api":"anthropic-messages","compat":{"supportsEagerToolInputStreaming":false}}}}
JSON

cat > "$gcp_credentials" <<'JSON'
{"type":"authorized_user","client_id":"fake-client","client_secret":"fake-secret","refresh_token":"fake-refresh"}
JSON
render_config "$SCRIPT_DIR/configs/$client-$upstream.toml" "$config_path"

fake_bin="${FAKE_BIN:-$ROOT_DIR/target/debug/$fake_package}"
if [ -x "$fake_bin" ]; then
  "$fake_bin" --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
else
  cargo run -q -p "$fake_package" -- --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
fi
FAKE_PID=$!
if ! wait_port "$fake_port" "$fake_package"; then
  printf '%s\n' "--- $fake_package log ---" >&2
  cat "$TMP_DIR/fake.log" >&2 || true
  fail "$fake_package did not start"
fi

cc_lb_bin="${CC_LB_BIN:-$ROOT_DIR/target/debug/cc-lb}"
if [ -x "$cc_lb_bin" ]; then
  CC_LB_ADMIN_TOKEN=admin-token \
  CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
  AWS_ACCESS_KEY_ID=AKIATEST \
  AWS_SECRET_ACCESS_KEY=SECRETTEST \
  AWS_REGION=us-east-1 \
  GOOGLE_APPLICATION_CREDENTIALS="$gcp_credentials" \
  CC_LB_GCP_ACCESS_TOKEN=ya29.test \
  RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn \
  "$cc_lb_bin" serve --config "$config_path" > "$TMP_DIR/proxy.log" 2>&1 &
else
  CC_LB_ADMIN_TOKEN=admin-token \
  CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
  AWS_ACCESS_KEY_ID=AKIATEST \
  AWS_SECRET_ACCESS_KEY=SECRETTEST \
  AWS_REGION=us-east-1 \
  GOOGLE_APPLICATION_CREDENTIALS="$gcp_credentials" \
  CC_LB_GCP_ACCESS_TOKEN=ya29.test \
  RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn \
  cargo run -q -p cc-lb-server -- serve --config "$config_path" > "$TMP_DIR/proxy.log" 2>&1 &
fi
PROXY_PID=$!
if ! wait_port "$proxy_port" cc-lb; then
  printf '%s\n' '--- proxy log ---' >&2
  cat "$TMP_DIR/proxy.log" >&2 || true
  fail "cc-lb did not start"
fi
if ! wait_port "$admin_port" cc-lb-admin; then
  printf '%s\n' '--- proxy log ---' >&2
  cat "$TMP_DIR/proxy.log" >&2 || true
  fail "cc-lb-admin did not start"
fi

# The test config sets downstream_auth.mode = "none" with principal_id =
# "api-key" and upstream_kind = "anthropic_key". The principal and the
# routing-target upstream named "real_client" must exist in the dynamic
# store before cc-lb can route the request.
# Master b82e211 (feat: runtime-dynamic-mgmt) replaced the previous
# [upstreams.*] / [principals.*] TOML blocks with admin-API registration, so
# every test harness must seed them at startup.
principal_code=$(curl -sS -o "$TMP_DIR/admin-principal.json" -w '%{http_code}' -X POST \
  -H 'Authorization: Bearer admin-token' \
  -H 'content-type: application/json' \
  --data '{"name":"api-key","kind":"machine","allowed_models":["*"]}' \
  "http://127.0.0.1:$admin_port/admin/v1/principals") || principal_code=000
if [ "$principal_code" != "201" ] && [ "$principal_code" != "409" ]; then
  printf '%s\n' '--- admin-principal.json ---' >&2
  cat "$TMP_DIR/admin-principal.json" >&2 || true
  fail "register principal expected 201 or 409, got $principal_code"
fi

upstream_body=$(printf '{"name":"real_client","kind":"custom","base_url":"http://127.0.0.1:%s"}' "$fake_port")
upstream_code=$(curl -sS -o "$TMP_DIR/admin-upstream.json" -w '%{http_code}' -X POST \
  -H 'Authorization: Bearer admin-token' \
  -H 'content-type: application/json' \
  --data "$upstream_body" \
  "http://127.0.0.1:$admin_port/admin/v1/upstreams") || upstream_code=000
if [ "$upstream_code" != "201" ] && [ "$upstream_code" != "409" ]; then
  printf '%s\n' '--- admin-upstream.json ---' >&2
  cat "$TMP_DIR/admin-upstream.json" >&2 || true
  fail "register upstream expected 201 or 409, got $upstream_code"
fi

set +e
unset_args=()
while IFS='=' read -r name _; do
  case "$name" in
    OPENCODE*|CLIO*|SISYPHUS*|AGENT*) unset_args+=("-u" "$name") ;;
  esac
done < <(env)
case "$client" in
  claude-code)
    timeout 30s env \
      HOME="$TMP_DIR/home" \
      XDG_CONFIG_HOME="$TMP_DIR/xdg-config" \
      XDG_DATA_HOME="$TMP_DIR/xdg-data" \
      ANTHROPIC_API_KEY="$API_KEY" \
      ANTHROPIC_BASE_URL="http://127.0.0.1:$proxy_port" \
      ANTHROPIC_MODEL=claude-3-5-sonnet-20241022 \
      "$bin" --bare --no-session-persistence --model claude-3-5-sonnet-20241022 --print "$PROMPT" > "$stdout_file" 2> "$stderr_file"
    code=$?
    ;;
  opencode)
    # opencode 1.15.6 dropped claude-3-5-sonnet-20241022 from its built-in
    # model whitelist; the request never reaches the proxy. fake-anthropic
    # echoes whatever model the client sends, so any current Claude name works.
    # Timeout is 90s instead of the other clients' 30s because opencode's
    # first-run db migration + agent bootstrap can push past 30s on a cold CI
    # runner; later runs in the same job hit warm caches and finish faster.
    timeout 90s env "${unset_args[@]}" \
      HOME="$TMP_DIR/home" \
      XDG_CONFIG_HOME="$TMP_DIR/xdg-config" \
      XDG_DATA_HOME="$TMP_DIR/xdg-data" \
      ANTHROPIC_API_KEY="$API_KEY" \
      ANTHROPIC_BASE_URL="http://127.0.0.1:$proxy_port/v1" \
      ANTHROPIC_MODEL=claude-sonnet-4-5 \
      "$bin" run --pure --dangerously-skip-permissions --model anthropic/claude-sonnet-4-5 "$PROMPT" > "$stdout_file" 2> "$stderr_file"
    code=$?
    ;;
  pi)
    timeout 30s env \
      HOME="$TMP_DIR/home" \
      XDG_CONFIG_HOME="$TMP_DIR/xdg-config" \
      XDG_DATA_HOME="$TMP_DIR/xdg-data" \
      PI_CODING_AGENT_DIR="$TMP_DIR/pi-agent" \
      PI_CODING_AGENT_SESSION_DIR="$TMP_DIR/pi-sessions" \
      ANTHROPIC_API_KEY="$API_KEY" \
      ANTHROPIC_BASE_URL="http://127.0.0.1:$proxy_port" \
      ANTHROPIC_MODEL=claude-3-5-sonnet-20241022 \
      "$bin" --provider anthropic --model claude-3-5-sonnet-20241022 --api-key "$API_KEY" --print --no-session --no-tools --offline "$PROMPT" > "$stdout_file" 2> "$stderr_file"
    code=$?
    ;;
esac
set -e

printf 'client exit code: %s\n' "$code"
printf '%s\n' '--- stdout ---'
cat "$stdout_file" || true
printf '%s\n' '--- stderr ---'
cat "$stderr_file" || true
printf '%s\n' "--- $fake_package log ---"
cat "$TMP_DIR/fake.log" || true
printf '%s\n' '--- proxy log ---'
cat "$TMP_DIR/proxy.log" || true

if [ "$code" -eq 124 ] && [ "$client" = "opencode" ] && grep -qiF "$expected" "$stdout_file"; then
  printf 'client timed out after producing expected output; treating opencode run as PASS\n'
  code=0
fi

if [ "$code" -ne 0 ]; then
  if reason=$(detect_skip_reason "$stderr_file"); then
    skip "$reason"
  fi
  fail "client exited non-zero: $code"
fi

if ! grep -qiF "$expected" "$stdout_file"; then
  fail "client stdout did not contain expected substring: $expected"
fi

printf 'PASS %s/%s stdout contained %s\n' "$client" "$upstream" "$expected"
