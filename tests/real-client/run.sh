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
  pgrep -af '[c]c-lb.*serve' || true
  pgrep -af '[f]ake-' || true
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
  anthropic-direct|bedrock-runtime|bedrock-mantle|vertex|custom) ;;
  *) fail "unsupported upstream: $upstream" ;;
esac

expected_file="$SCRIPT_DIR/expected/$client-$upstream.txt"
[ -f "$expected_file" ] || fail "missing expected file: $expected_file"
expected=$(tr -d '\r\n' < "$expected_file")
[ -n "$expected" ] || fail "empty expected substring: $expected_file"

case "$upstream" in
  anthropic-direct)
    skip "cc-lb anthropic_direct dialect hard-codes https://api.anthropic.com; local fake cannot be targeted without changing production routing or host files"
    ;;
  bedrock-runtime)
    skip "cc-lb bedrock_runtime dialect hard-codes https://bedrock-runtime.<region>.amazonaws.com; local fake cannot be targeted without changing production routing or host files"
    ;;
  bedrock-mantle)
    skip "cc-lb bedrock_mantle dialect hard-codes https://bedrock-mantle.<region>.api.aws; local fake cannot be targeted without changing production routing or host files"
    ;;
  vertex)
    skip "cc-lb vertex dialect hard-codes https://<region>-aiplatform.googleapis.com; local fake cannot be targeted without changing production routing or host files"
    ;;
  custom) ;;
esac

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
proxy_port=$(free_port)
admin_port=$(free_port)
metrics_port=$(free_port)
fake_port=$(free_port)
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

cargo run -q -p fake-anthropic -- --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
FAKE_PID=$!
wait_port "$fake_port" fake-anthropic

CC_LB_ADMIN_TOKEN=admin-token \
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
AWS_ACCESS_KEY_ID=AKIATEST \
AWS_SECRET_ACCESS_KEY=SECRETTEST \
AWS_REGION=us-east-1 \
GOOGLE_APPLICATION_CREDENTIALS="$gcp_credentials" \
RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn \
cargo run -q -p cc-lb-server -- serve --config "$config_path" > "$TMP_DIR/proxy.log" 2>&1 &
PROXY_PID=$!
wait_port "$proxy_port" cc-lb

set +e
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
    timeout 30s env -u OPENCODE -u OPENCODE_RUN_ID -u OPENCODE_PROCESS_ROLE -u OPENCODE_PID -u OPENCODE_EXPERIMENTAL_LSP_TY -u OPENCODE_SERVER_USERNAME -u OPENCODE_SERVER_PASSWORD \
      HOME="$TMP_DIR/home" \
      XDG_CONFIG_HOME="$TMP_DIR/xdg-config" \
      XDG_DATA_HOME="$TMP_DIR/xdg-data" \
      ANTHROPIC_API_KEY="$API_KEY" \
      ANTHROPIC_BASE_URL="http://127.0.0.1:$proxy_port/v1" \
      ANTHROPIC_MODEL=claude-3-5-sonnet-20241022 \
      "$bin" run --pure --model anthropic/claude-3-5-sonnet-20241022 "$PROMPT" > "$stdout_file" 2> "$stderr_file"
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
printf '%s\n' '--- fake log ---'
cat "$TMP_DIR/fake.log" || true
printf '%s\n' '--- proxy log ---'
cat "$TMP_DIR/proxy.log" || true

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
