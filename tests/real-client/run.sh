#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
PROMPT='Print the word HELLO'
API_KEY='sk-ant-api03-abcdefghijklmnopqrstuvwxyz0123456789'
FAKE_PID=''
PROXY_PID=''
CLIENT_PID=''
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
  cleanup_pid_tree "$CLIENT_PID"
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
  claude-code|opencode|pi|senpi) ;;
  *) fail "unsupported client: $client" ;;
esac
case "$upstream" in
  anthropic-direct|custom) ;;
  anthropic-affinity)
    [ "$client" = "senpi" ] || fail "unsupported upstream for $client: $upstream"
    ;;
  *) fail "unsupported upstream: $upstream" ;;
esac

expected_upstream=$upstream
if [ "$client" = "senpi" ] && [ "$upstream" = "anthropic-affinity" ]; then
  expected_upstream=anthropic-direct
fi
expected_file="$SCRIPT_DIR/expected/$client-$expected_upstream.txt"
[ -f "$expected_file" ] || fail "missing expected file: $expected_file"
expected=$(tr -d '\r\n' < "$expected_file")
[ -n "$expected" ] || fail "empty expected substring: $expected_file"

fake_package=fake-anthropic

REAL_CLIENT_ONLY=$client
. "$SCRIPT_DIR/install.sh"

case "$client" in
  claude-code) bin=${CLAUDE_CODE_BIN:-}; status_file="$ROOT_DIR/target/test-bins/claude-code/install-status" ;;
  opencode) bin=${OPENCODE_BIN:-}; status_file="$ROOT_DIR/target/test-bins/opencode/install-status" ;;
  pi) bin=${PI_BIN:-}; status_file="$ROOT_DIR/target/test-bins/pi/install-status" ;;
  senpi) bin=${SENPI_BIN:-}; status_file="$ROOT_DIR/target/test-bins/senpi/install-status" ;;
esac

if [ -z "${bin:-}" ] || [ ! -x "$bin" ]; then
  if [ -f "$status_file" ]; then
    reason=$(sed -n 's/^FAIL reason: //p' "$status_file" | head -n 1)
    [ -z "$reason" ] || fail "$reason"
  fi
  fail "client binary missing: $bin"
fi
senpi_extension=''
if [ "$client" = "senpi" ]; then
  senpi_extension="$ROOT_DIR/target/test-bins/senpi/node_modules/@code-yeongyu/senpi/examples/extensions/subagent/index.ts"
  [ -f "$senpi_extension" ] || fail "installed Senpi package is missing subagent extension: $senpi_extension"
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
wait_http() {
  port=$1
  name=$2
  path=$3
  python3 - "$port" "$name" "$path" <<'PY'
import socket, sys, time
port = int(sys.argv[1])
name = sys.argv[2]
path = sys.argv[3]
deadline = time.time() + 30
last = None
request = (
    f"GET {path} HTTP/1.1\r\n"
    "Host: 127.0.0.1\r\n"
    "Connection: close\r\n\r\n"
).encode()
while time.time() < deadline:
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.5) as sock:
            sock.settimeout(0.5)
            sock.sendall(request)
            response = sock.recv(1024)
            if response.startswith(b"HTTP/1.1 200") or response.startswith(b"HTTP/1.0 200"):
                print(f'{name} ready on http://127.0.0.1:{port}{path}')
                sys.exit(0)
            last = response.splitlines()[0].decode(errors='replace') if response else 'empty response'
    except OSError as exc:
        last = exc
    time.sleep(0.1)
print(f'{name} did not become ready on http://127.0.0.1:{port}{path}: {last}', file=sys.stderr)
sys.exit(1)
PY
}
run_isolated_client() {
  timeout_secs=$1
  shift
  python3 - "$timeout_secs" "$@" <<'PY'
import os
import signal
import subprocess
import sys

timeout = int(sys.argv[1])
process = subprocess.Popen(sys.argv[2:], start_new_session=True)
try:
    code = process.wait(timeout=timeout)
except subprocess.TimeoutExpired:
    code = 124
finally:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()
sys.exit(code)
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
    '__SQLITE_PATH__': str(Path(tmp) / 'cc-lb.sqlite'),
}
for old, new in replacements.items():
    text = text.replace(old, new)
Path(dst).write_text(text)
PY
}

detect_skip_reason() {
  stderr_file=$1
  skip_pattern='not a tty|no tty available|raw mode is not supported|requires? (a )?tty|tty.*(required|not (available|supported))|required.*tty|interactive.*required|required.*interactive|browser.*(login|auth)|oauth.*(login|required|authorize)|please.*(log in|login|authenticate)|login required|authentication required|accept.*(terms|tos)|subscription.*required|not logged in|permission denied'
  if grep -Eiq "$skip_pattern" "$stderr_file"; then
    grep -Ei "$skip_pattern" "$stderr_file" | head -n 1 | sed 's/^/client refused deterministic non-interactive run: /'
    return 0
  fi
  return 1
}

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-real-client.XXXXXX")
# Allocate 5 distinct ports atomically: hold all sockets open during reservation
# so the kernel cannot hand the same ephemeral port to two sockets, then close
# them just before binding. Avoids race collisions seen on busy CI runners.
read -r proxy_port admin_port metrics_port fake_port client_port <<EOF
$(python3 - <<'PY'
import socket
socks = []
for _ in range(5):
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
mkdir -p \
  "$TMP_DIR/home" \
  "$TMP_DIR/xdg-config" \
  "$TMP_DIR/xdg-data" \
  "$TMP_DIR/xdg-cache" \
  "$TMP_DIR/xdg-state" \
  "$TMP_DIR/opencode-config/opencode" \
  "$TMP_DIR/opencode-workspace" \
  "$TMP_DIR/pi-agent" \
  "$TMP_DIR/senpi-agent/agents" \
  "$TMP_DIR/senpi-sessions"
touch "$stdout_file" "$stderr_file"
cat > "$TMP_DIR/pi-agent/models.json" <<JSON
{"providers":{"anthropic":{"baseUrl":"http://127.0.0.1:$proxy_port","apiKey":"ANTHROPIC_API_KEY","api":"anthropic-messages","compat":{"supportsEagerToolInputStreaming":false}}}}
JSON

cat > "$TMP_DIR/opencode-config/opencode/opencode.json" <<JSON
{"provider":{"anthropic":{"options":{"baseURL":"http://127.0.0.1:$proxy_port/v1","headers":{"x-fake-mode":"opencode-tools"}}}}}
JSON

if [ "$client" = "senpi" ]; then
  case "$upstream" in
    custom)
      senpi_provider=senpi-mock
      senpi_model=senpi-main
      # Any accidental fallback to the ambient Anthropic provider must fail
      # locally instead of reaching the real API or silently passing via cc-lb.
      senpi_anthropic_base_url=http://127.0.0.1:1
      cat > "$TMP_DIR/senpi-agent/models.json" <<JSON
{"providers":{"senpi-mock":{"baseUrl":"http://127.0.0.1:$proxy_port","apiKey":"$API_KEY","api":"anthropic-messages","models":[{"id":"senpi-main","name":"Senpi Main","api":"anthropic-messages","reasoning":false,"input":["text"],"contextWindow":128000,"maxTokens":4096,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"headers":{"x-fake-mode":"senpi-tools","x-senpi-image-path":"$TMP_DIR/senpi-look-at.png"},"compat":{"sendSessionAffinityHeaders":true}},{"id":"senpi-vision","name":"Senpi Vision","api":"anthropic-messages","reasoning":false,"input":["text","image"],"contextWindow":128000,"maxTokens":4096,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"headers":{"x-fake-mode":"senpi-tools"},"compat":{"sendSessionAffinityHeaders":true}}]}}}
JSON
      cat > "$TMP_DIR/senpi-agent/settings.json" <<'JSON'
{"defaultProvider":"senpi-mock","defaultModel":"senpi-main","lookAt":{"enabled":true,"models":["senpi-mock/senpi-vision"]}}
JSON
      ;;
    anthropic-direct|anthropic-affinity)
      senpi_provider=anthropic
      senpi_model=claude-sonnet-4-5
      senpi_anthropic_base_url="http://127.0.0.1:$proxy_port"
      # Child subagent/look_at processes do not inherit the parent's
      # `--api-key`; the isolated `ANTHROPIC_API_KEY` environment below
      # supplies their credentials. Direct mode intentionally omits
      # session-affinity compatibility.
      senpi_compat=''
      if [ "$upstream" = "anthropic-affinity" ]; then
        senpi_compat=',"compat":{"sendSessionAffinityHeaders":true}'
      fi
      cat > "$TMP_DIR/senpi-agent/models.json" <<JSON
{"providers":{"anthropic":{"baseUrl":"http://127.0.0.1:$proxy_port"$senpi_compat}}}
JSON
      cat > "$TMP_DIR/senpi-agent/settings.json" <<JSON
{"defaultProvider":"anthropic","defaultModel":"$senpi_model","lookAt":{"enabled":true,"models":["anthropic/$senpi_model"]}}
JSON
      ;;
  esac
  cat > "$TMP_DIR/senpi-agent/agents/reviewer.md" <<EOF
---
name: reviewer
description: Reviews one small request for the real-client proxy test.
model: $senpi_provider/$senpi_model
---
Answer the delegated request directly.
EOF
fi

python3 - "$TMP_DIR/senpi-look-at.png" <<'PY'
import binascii
import struct
import sys
import zlib

def chunk(kind, data):
    payload = kind + data
    return struct.pack(">I", len(data)) + payload + struct.pack(">I", binascii.crc32(payload))

width = 2
height = 2
pixels = b"".join(
    [
        b"\x00\xff\x00\x00\x00\xff\x00",
        b"\x00\x00\x00\xff\xff\xff\xff",
    ]
)
png = (
    b"\x89PNG\r\n\x1a\n"
    + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    + chunk(b"IDAT", zlib.compress(pixels))
    + chunk(b"IEND", b"")
)
with open(sys.argv[1], "wb") as output:
    output.write(png)
PY
cat > "$gcp_credentials" <<'JSON'
{"type":"authorized_user","client_id":"fake-client","client_secret":"fake-secret","refresh_token":"fake-refresh"}
JSON
render_config "$SCRIPT_DIR/configs/$client-$expected_upstream.toml" "$config_path"

fake_default_mode=''
if [ "$client" = "senpi" ] && [ "$upstream" != "custom" ]; then
  fake_default_mode=senpi-tools
fi
fake_bin="${FAKE_BIN:-$ROOT_DIR/target/debug/$fake_package}"
if [ -x "$fake_bin" ]; then
  FAKE_DEFAULT_MODE="$fake_default_mode" "$fake_bin" --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
else
  FAKE_DEFAULT_MODE="$fake_default_mode" cargo run -q -p "$fake_package" -- --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
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

# The test uses a managed API key issued for the DB principal. The principal
# and routing-target upstream named "real_client" must exist in the dynamic
# store before cc-lb can route the request. The database records are the only
# source of principal and upstream kinds.
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

upstream_body=$(printf '{"name":"real_client","kind":"anthropic_api_key","base_url":"http://127.0.0.1:%s","api_key_value":"sk-ant-test"}' "$fake_port")
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

principal_id=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['id'])" "$TMP_DIR/admin-principal.json")
curl -sS -o "$TMP_DIR/admin-key.json" -X POST \
  -H 'Authorization: Bearer admin-token' \
  -H 'content-type: application/json' \
  --data '{"label":"real-client"}' \
  "http://127.0.0.1:$admin_port/admin/v1/principals/$principal_id/keys"
API_KEY=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['plaintext_key'])" "$TMP_DIR/admin-key.json")

if [ -f "$TMP_DIR/senpi-agent/models.json" ]; then
  python3 - "$TMP_DIR/senpi-agent/models.json" "$API_KEY" <<'PY'
import json
import sys

path, api_key = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    config = json.load(handle)
for provider in config.get("providers", {}).values():
    if "apiKey" in provider:
        provider["apiKey"] = api_key
with open(path, "w", encoding="utf-8") as handle:
    json.dump(config, handle, separators=(",", ":"))
PY
fi

set +e
case "$client" in
  claude-code)
    run_isolated_client 30 env \
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
    (
      cd "$TMP_DIR/opencode-workspace"
      exec env -i PATH="$PATH" \
        HOME="$TMP_DIR/home" \
        XDG_CONFIG_HOME="$TMP_DIR/opencode-config" \
        XDG_DATA_HOME="$TMP_DIR/xdg-data" \
        XDG_CACHE_HOME="$TMP_DIR/xdg-cache" \
        XDG_STATE_HOME="$TMP_DIR/xdg-state" \
        OPENCODE_DISABLE_MODELS_FETCH=1 \
        ANTHROPIC_API_KEY="$API_KEY" \
        ANTHROPIC_BASE_URL="http://127.0.0.1:$proxy_port/v1" \
        ANTHROPIC_MODEL=claude-sonnet-4-5 \
        "$bin" serve --pure --print-logs --hostname 127.0.0.1 --port "$client_port"
    ) > "$TMP_DIR/opencode-server.log" 2>&1 &
    CLIENT_PID=$!
    if ! wait_http "$client_port" opencode /global/health; then
      printf '%s\n' '--- opencode server log ---' >&2
      cat "$TMP_DIR/opencode-server.log" >&2 || true
      fail "opencode did not start"
    fi

    session_file="$TMP_DIR/opencode-session.json"
    if ! curl -fsS --max-time 30 \
      -H 'content-type: application/json' \
      --data '{"agent":"build","model":{"providerID":"anthropic","id":"claude-sonnet-4-5"}}' \
      "http://127.0.0.1:$client_port/session" \
      -o "$session_file"; then
      printf '%s\n' '--- opencode server log ---' >&2
      cat "$TMP_DIR/opencode-server.log" >&2 || true
      printf '%s\n' '--- proxy log ---' >&2
      cat "$TMP_DIR/proxy.log" >&2 || true
      printf '%s\n' "--- $fake_package log ---" >&2
      cat "$TMP_DIR/fake.log" >&2 || true
      fail "opencode session creation failed"
    fi
    session_id=$(python3 - "$session_file" <<'PY'
import json
import sys
with open(sys.argv[1]) as source:
    print(json.load(source)["id"])
PY
)
    if ! printf '%s' "$session_id" | grep -q '^ses'; then
      fail "opencode session creation returned an invalid ID"
    fi

    curl -fsS --max-time 90 \
      -H 'content-type: application/json' \
      --data '{"model":{"providerID":"anthropic","modelID":"claude-sonnet-4-5"},"agent":"build","parts":[{"type":"text","text":"Use the task tool to ask a general subagent to reply CHILD, then reply HELLO."}]}' \
      "http://127.0.0.1:$client_port/session/$session_id/message" \
      -o "$stdout_file"
    code=$?
    if [ "$code" -eq 0 ]; then
      curl -fsS --max-time 30 \
        -H 'content-type: application/json' \
        --data '{"providerID":"anthropic","modelID":"claude-sonnet-4-5","auto":false}' \
        "http://127.0.0.1:$client_port/session/$session_id/summarize" \
        >> "$stdout_file"
      code=$?
    fi
    ;;
  pi)
    run_isolated_client 30 env \
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
  senpi)
    senpi_env=(
      HOME="$TMP_DIR/home"
      USERPROFILE="$TMP_DIR/home"
      XDG_CONFIG_HOME="$TMP_DIR/xdg-config"
      XDG_DATA_HOME="$TMP_DIR/xdg-data"
      SENPI_CODING_AGENT_DIR="$TMP_DIR/senpi-agent"
      SENPI_CODING_AGENT_SESSION_DIR="$TMP_DIR/senpi-sessions"
      ANTHROPIC_BASE_URL="$senpi_anthropic_base_url"
      ANTHROPIC_API_KEY="$API_KEY"
      SENPI_OMO_LOCAL_UPDATE=0
      PI_OFFLINE=1
      PI_TELEMETRY=0
    )
    run_isolated_client 60 env -i PATH="$PATH" "${senpi_env[@]}" \
      "$bin" \
      --provider "$senpi_provider" \
      --model "$senpi_model" \
      --api-key "$API_KEY" \
      --session-id senpi-real-client-main \
      --extension "$senpi_extension" \
      --no-skills \
      --no-prompt-templates \
      --no-themes \
      --no-context-files \
      --offline \
      --print "$PROMPT" > "$stdout_file" 2> "$stderr_file"
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

if [ "$client" = "opencode" ]; then
  python3 - "$TMP_DIR/cc-lb.sqlite" "$session_id" <<'PY'
import sqlite3
import sys
import time

database, parent_session_id = sys.argv[1:]
deadline = time.monotonic() + 10
rows = []
expected = {"main", "subagent", "session_title", "compaction"}
while time.monotonic() < deadline:
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT request_kind, observed_session_id, session_id_source,
                            parent_session_id, client_app
            FROM request_events_v1
            WHERE request_kind IS NOT NULL
            ORDER BY request_kind, observed_session_id
            """
        ).fetchall()
    if expected.issubset({row[0] for row in rows}):
        break
    time.sleep(0.05)

kinds = {row[0] for row in rows}
missing = expected - kinds
if missing:
    print(f"FAIL reason: missing OpenCode request kinds {sorted(missing)}; rows={rows}", file=sys.stderr)
    sys.exit(1)
parent_kinds = {"main", "session_title", "compaction"}
if any(row[0] in parent_kinds and row[1] != parent_session_id for row in rows):
    print(f"FAIL reason: OpenCode parent requests changed session ID; rows={rows}", file=sys.stderr)
    sys.exit(1)
subagent_rows = [row for row in rows if row[0] == "subagent"]
if not any(row[1] != parent_session_id and row[3] == parent_session_id for row in subagent_rows):
    print(f"FAIL reason: OpenCode subagent linkage was not preserved; rows={rows}", file=sys.stderr)
    sys.exit(1)
if any(row[0] in expected and row[2] != "x-session-affinity" for row in rows):
    print(f"FAIL reason: OpenCode requests used an unexpected session source; rows={rows}", file=sys.stderr)
    sys.exit(1)
if any(row[0] in expected and row[4] is not None for row in rows):
    print(f"FAIL reason: OpenCode requests invented client_app; rows={rows}", file=sys.stderr)
    sys.exit(1)

print("PASS opencode request kinds: main, subagent, session_title, compaction")
PY
fi

if [ "$client" = "senpi" ]; then
  python3 - "$TMP_DIR/cc-lb.sqlite" "$upstream" <<'PY'
import sqlite3
import sys
import time

database, upstream = sys.argv[1:]
expected_kinds = {"main", "subagent"}
if upstream == "custom":
    expected_kinds.add("look_at")
deadline = time.monotonic() + 10
rows = []
stable_rows = None
stable_since = None
quiescent = False
while time.monotonic() < deadline:
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT request_kind, observed_session_id, session_id_source,
                            client_app, cache_prefix_hash
            FROM request_events_v1
            WHERE request_kind IS NOT NULL
            ORDER BY request_kind, observed_session_id
            """
        ).fetchall()
    kinds = {row[0] for row in rows}
    if expected_kinds.issubset(kinds):
        now = time.monotonic()
        if rows != stable_rows:
            stable_rows = rows
            stable_since = now
        elif stable_since is not None and now - stable_since >= 0.2:
            quiescent = True
            break
    else:
        stable_rows = None
        stable_since = None
    time.sleep(0.05)

kinds = {row[0] for row in rows}
missing = expected_kinds - kinds
if missing:
    print(f"FAIL reason: missing Senpi request kinds {sorted(missing)}; rows={rows}", file=sys.stderr)
    sys.exit(1)
if not quiescent:
    print(f"FAIL reason: Senpi request rows did not quiesce before deadline; rows={rows}", file=sys.stderr)
    sys.exit(1)
if upstream != "custom" and "look_at" in kinds:
    # Senpi disables look_at when the active model accepts images; built-in Claude does.
    print(f"FAIL reason: image-capable Senpi mode unexpectedly sent a look_at request; rows={rows}", file=sys.stderr)
    sys.exit(1)
session_rows = [row for row in rows if row[0] in {"main", "subagent"}]
if upstream == "anthropic-direct":
    if any(row[1] is not None or row[2] is not None for row in session_rows):
        print(f"FAIL reason: baseUrl-only Senpi unexpectedly emitted session identity; rows={rows}", file=sys.stderr)
        sys.exit(1)
    if any(row[4] is None for row in session_rows):
        print(f"FAIL reason: sessionless Senpi request lacked cache-prefix fallback; rows={rows}", file=sys.stderr)
        sys.exit(1)
else:
    main_session_ids = {row[1] for row in rows if row[0] == "main" and row[1] is not None}
    if main_session_ids != {"senpi-real-client-main"}:
        print(f"FAIL reason: Senpi main session ID was not preserved; rows={rows}", file=sys.stderr)
        sys.exit(1)
    if any(row[0] == "subagent" and row[1] == "senpi-real-client-main" for row in rows):
        print(f"FAIL reason: Senpi subagent reused the parent session ID; rows={rows}", file=sys.stderr)
        sys.exit(1)
    if any(row[2] != "x-session-affinity" for row in session_rows):
        print(f"FAIL reason: Senpi session-bearing requests used an unexpected source; rows={rows}", file=sys.stderr)
        sys.exit(1)
look_at_identities = {(row[1], row[2]) for row in rows if row[0] == "look_at"}
if upstream == "custom" and look_at_identities != {(None, None)}:
    print(f"FAIL reason: Senpi look_at unexpectedly carried session identity; rows={rows}", file=sys.stderr)
    sys.exit(1)
if any(row[3] is not None for row in rows):
    print(f"FAIL reason: Senpi requests invented client_app; rows={rows}", file=sys.stderr)
    sys.exit(1)

identity = "cache-prefix fallback" if upstream == "anthropic-direct" else "x-session-affinity"
print(f"PASS senpi request kinds: {', '.join(sorted(expected_kinds))}; identity={identity}")
PY
fi

printf 'PASS %s/%s stdout contained %s\n' "$client" "$upstream" "$expected"
