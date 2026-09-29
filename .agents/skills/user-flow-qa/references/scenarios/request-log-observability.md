# Request Log Observability

Prove that cc-lb request logs correctly capture and display various request lifecycle events, including rate limits, slow streaming, client disconnects, and timeouts, across storage, the admin API, SSE streams, and the admin-web UI.

The latency remediation cases below cover storage, API, metrics, and browser checks in an isolated fixture.

## §0 Environment/Preconditions

Run an isolated instance with dynamic ports and a temporary SQLite database. Never mutate the shared prod DB.

Run the shell blocks from any directory inside the repository. Keep the same shell alive for the full scenario so its variables, functions, process IDs, and `trap` remain available.

```bash
set -euo pipefail

# Resolve every repository path once and fail before creating processes.
command -v git >/dev/null 2>&1 || {
  echo "Missing required QA tool: git" >&2
  exit 1
}
REPO_ROOT=$(git rev-parse --show-toplevel)
for binary in fake-anthropic cc-lb; do
  [ -x "$REPO_ROOT/target/debug/$binary" ] || {
    echo "Missing debug binary: $REPO_ROOT/target/debug/$binary" >&2
    exit 1
  }
done
for tool in agent-browser awk bun cmp curl grep jq lsof mktemp openssl pkill python3 sqlite3 tr uuidgen wc; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Missing required QA tool: $tool" >&2
    exit 1
  }
done


# 1. Setup isolated environment and dynamic ports
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-qa.XXXXXX")
read -r proxy_port admin_port metrics_port fake_port vite_port <<EOF
$(python3 -c "import socket; socks = [socket.socket() for _ in range(5)]; [s.bind(('127.0.0.1', 0)) for s in socks]; print(' '.join(str(s.getsockname()[1]) for s in socks)); [s.close() for s in socks]")
EOF

# 2. Generate per-run random credentials (do not print these)
ADMIN_TOKEN=$(uuidgen)
MASTER_KEY=$(openssl rand -hex 32)
CLIENT_KEY=""
SESSION_ID="qa-req-logs-$(openssl rand -hex 4)"

# 3. Render config
cat <<EOF > "$TMP_DIR/cc-lb.toml"
[listener]
proxy_addr = "127.0.0.1:$proxy_port"
admin_addr = "127.0.0.1:$admin_port"
metrics_addr = "127.0.0.1:$metrics_port"

[body]
messages_cap_bytes = 33554432
files_cap_bytes = 104857600

[timeouts]
upstream_total_secs = 30
drain_secs = 5
[storage]
kind = "sqlite"
path = "$TMP_DIR/cc-lb.sqlite"

[aead]
key_env = "CC_LB_MASTER_KEY"

[observability]
tracing_level = "info"
log_redaction = true
user_prompt_redaction = false

[[admin.auth.providers]]
kind = "static_token"
id = "qa"
token_env = "CC_LB_ADMIN_TOKEN"

[circuit_breaker]
failures_to_open = 5
window_secs = 10
half_open_after_secs = 30

[bulkhead]
max_conns_per_upstream = 50
semaphore_per_upstream = 100

EOF

# 4. Create exact-size valid JSON request fixtures and an evidence directory.
# The payload generator uses ASCII-only content, so file bytes equal JSON bytes.
mkdir -p "$TMP_DIR/evidence"
python3 - "$TMP_DIR/body-854336.json" 854336 false <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
target = int(sys.argv[2])
stream = sys.argv[3].lower() == "true"
event = {
    "model": "claude-3-5-sonnet-20241022",
    "max_tokens": 24,
    "stream": stream,
    "messages": [{"role": "user", "content": ""}],
}
empty = json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode()
event["messages"][0]["content"] = "x" * (target - len(empty))
encoded = json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode()
assert len(encoded) == target, (len(encoded), target)
path.write_bytes(encoded)
PY
python3 - "$TMP_DIR/body-917567.json" 917567 false <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
target = int(sys.argv[2])
stream = sys.argv[3].lower() == "true"
event = {
    "model": "claude-3-5-sonnet-20241022",
    "max_tokens": 24,
    "stream": stream,
    "messages": [{"role": "user", "content": ""}],
}
empty = json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode()
event["messages"][0]["content"] = "x" * (target - len(empty))
encoded = json.dumps(event, separators=(",", ":"), ensure_ascii=True).encode()
assert len(encoded) == target, (len(encoded), target)
path.write_bytes(encoded)
PY
[ "$(wc -c < "$TMP_DIR/body-854336.json" | tr -d ' ')" = 854336 ]
[ "$(wc -c < "$TMP_DIR/body-917567.json" | tr -d ' ')" = 917567 ]
openssl dgst -sha256 "$TMP_DIR/body-854336.json" > "$TMP_DIR/evidence/body-854336.sha256"
openssl dgst -sha256 "$TMP_DIR/body-917567.json" > "$TMP_DIR/evidence/body-917567.sha256"

# All writes in this scenario must stay below TMP_DIR. Storage inspection later
# uses sqlite3 -readonly; the one renewal fixture mutation calls this guard first.
assert_isolated_path() {
  local candidate isolated_root
  candidate=$(python3 - "$1" <<'PY'
import os, sys
print(os.path.realpath(sys.argv[1]))
PY
)
  isolated_root=$(python3 - "$TMP_DIR" <<'PY'
import os, sys
print(os.path.realpath(sys.argv[1]))
PY
)
  case "$candidate" in
    "$isolated_root"/*) ;;
    *) echo "Refusing non-isolated path: $candidate" >&2; return 1 ;;
  esac
}
assert_isolated_path "$TMP_DIR/cc-lb.sqlite"

# Stable per-case correlation values. Filter API events by thread_id instead of
# assuming the newest row belongs to a case.
CASE_F_SESSION="qa-upload-854336-$(openssl rand -hex 4)"
CASE_F_FAST_SESSION="qa-upload-854336-fast-$(openssl rand -hex 4)"
CASE_G_SESSION="qa-upload-917567-$(openssl rand -hex 4)"
CASE_I_SESSION="qa-stream-$(openssl rand -hex 4)"
CASE_J_SESSION="qa-499-$(openssl rand -hex 4)"
CASE_K_REQUEST_ID="req_renewal_$(openssl rand -hex 8)"


# 5. Define bounded readiness and teardown
wait_port() {
  local port=$1 name=$2
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

wait_http_200() {
  local url=$1 name=$2
  local deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    if curl --connect-timeout 1 --max-time 1 -sS -f -o /dev/null "$url"; then
      echo "$name is ready at $url"
      return 0
    fi
    sleep 0.1
  done
  echo "$name failed to return 200 OK at $url" >&2
  return 1
}

cleanup_pid_tree() {
  local pid=$1
  [ -n "$pid" ] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    local deadline=$((SECONDS + 5))
    while kill -0 "$pid" 2>/dev/null && [ $SECONDS -lt $deadline ]; do
      sleep 0.1
    done
  fi
  if kill -0 "$pid" 2>/dev/null; then
    pkill -KILL -P "$pid" 2>/dev/null || true
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}

TEARDOWN_STARTED=0
teardown() {
  [ "$TEARDOWN_STARTED" -eq 0 ] || return 0
  TEARDOWN_STARTED=1
  echo "Tearing down..."
  agent-browser --session "$SESSION_ID" close || true
  cleanup_pid_tree "${CURL_PID_B:-}"
  cleanup_pid_tree "${CURL_PID_C:-}"
  cleanup_pid_tree "${CURL_PID_D:-}"
  cleanup_pid_tree "${SSE_PID:-}"
  cleanup_pid_tree "${CURL_PID_I:-}"
  cleanup_pid_tree "${CURL_PID_J:-}"
  cleanup_pid_tree "${VITE_PID:-}"
  cleanup_pid_tree "${CC_LB_PID:-}"
  cleanup_pid_tree "${FAKE_PID:-}"
  
  # Verify listeners are gone
  local orphaned=0
  for port in $proxy_port $admin_port $metrics_port $fake_port $vite_port; do
    if lsof -i ":$port" >/dev/null 2>&1; then
      echo "WARNING: Port $port still in use!"
      orphaned=1
    fi
  done
  
  if [ $orphaned -eq 0 ]; then
    rm -rf "$TMP_DIR"
    echo "Teardown complete."
  else
    echo "WARNING: Orphaned listeners detected. Temp dir $TMP_DIR retained for debugging."
  fi
}
trap teardown EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# 6. Start services
"$REPO_ROOT/target/debug/fake-anthropic" --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
FAKE_PID=$!
wait_port $fake_port "fake-anthropic"

CC_LB_ADMIN_TOKEN=$ADMIN_TOKEN CC_LB_MASTER_KEY=$MASTER_KEY RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn "$REPO_ROOT/target/debug/cc-lb" serve --config "$TMP_DIR/cc-lb.toml" > "$TMP_DIR/proxy.log" 2>&1 &
CC_LB_PID=$!
wait_port $proxy_port "cc-lb proxy"
wait_port $admin_port "cc-lb admin"
wait_http_200 "http://127.0.0.1:$admin_port/admin/health" "cc-lb admin health"

(
  cd "$REPO_ROOT/crates/cc-lb-admin/web"
  exec env CC_LB_ADMIN_URL="http://127.0.0.1:$admin_port" \
    bun run dev --port "$vite_port"
) > "$TMP_DIR/vite.log" 2>&1 &
wait_port $vite_port "vite"
wait_http_200 "http://127.0.0.1:$vite_port/" "vite dev server"

# 7. Seed the DB-owned principal and upstream, then issue the managed proxy key.
curl -sS -f -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' --data '{"name":"api-key","kind":"machine","allowed_models":["*"]}' "http://127.0.0.1:$admin_port/admin/v1/principals" > "$TMP_DIR/principal.json"
PRINCIPAL_ID=$(jq -er '.id' "$TMP_DIR/principal.json")
curl -sS -f -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' --data '{"name":"real_client","kind":"anthropic_api_key","base_url":"http://127.0.0.1:'$fake_port'","api_key_value":"sk-ant-test"}' "http://127.0.0.1:$admin_port/admin/v1/upstreams" > /dev/null
curl -sS -f -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' --data '{"label":"request-log-qa"}' "http://127.0.0.1:$admin_port/admin/v1/principals/$PRINCIPAL_ID/keys" > "$TMP_DIR/proxy-key.json"
CLIENT_KEY=$(jq -er '.plaintext_key' "$TMP_DIR/proxy-key.json")

```
The managed key authenticates the request as the selected database principal.
Principal kind comes from that principal record, and upstream kind comes from
the database upstream selected for the request.

## Context Data and Surface Capture

Use the case-specific `x-claude-code-session-id` values above as correlation keys. Do not select the newest row without a correlation predicate.

- **Storage**: query only with `sqlite3 -readonly "$TMP_DIR/cc-lb.sqlite"`. The timing fields must match both the JSON payload and the SQLite `list_*` columns.
- **Recent API**: `GET /admin/v1/events/recent?limit=100`; select the event whose `thread_id` equals the case session.
- **Delta API**: let the Logs page issue its real delta request, replay that exact URL in the browser, and select the same `request_id`. This avoids hard-coding cursor syntax.
- **Detail API**: open the matching drawer, capture the real detail resource URL from the browser performance entries, replay it, and select the same `request_id`.
- **SSE**: `curl -sS -N -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/v1/events/stream"`.
- **UI**: `http://127.0.0.1:$vite_port/logs`.

The commands below save a before snapshot from every available surface before each mutation and an after snapshot after the final event. A detail response does not exist before a request ID exists; the in-progress SSE event and open drawer are its before state, and the final detail response is its after state.

```bash
recent_event_for_session() {
  local session=$1
  curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" \
    "http://127.0.0.1:$admin_port/admin/v1/events/recent?limit=100" |
    jq -c --arg session "$session" '[.events[] | select(.thread_id == $session)] | first // empty'
}

wait_for_final_event() {
  local session=$1 deadline event
  deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    event=$(recent_event_for_session "$session")
    if [ -n "$event" ] && printf '%s' "$event" | jq -e '.duration_ms != null' >/dev/null; then
      printf '%s\n' "$event"
      return 0
    fi
    sleep 0.2
  done
  echo "No final event for thread_id=$session within 30s" >&2
  return 1
}

assert_normal_residual() {
  local event_file=$1 expected_bytes=$2 expected_stream=$3
  jq -e --argjson expected_bytes "$expected_bytes" --arg expected_stream "$expected_stream" '
    def n: . // 0;
    . as $e |
    (if $e.stream_total_ms != null then $e.stream_total_ms else ($e.upstream_body_ms // 0) end) as $body |
    (($e.request_body_read_ms | n)
      + ($e.proxy_setup_ms | n)
      + ($e.shape_ms | n)
      + ($e.sign_ms | n)
      + ($e.upstream_ttfb_ms | n)
      + $body
      + ($e.finalize_ms | n)) as $accounted |
    ($e.duration_ms - $accounted) as $raw_residual |
    select(
      $e.request_body_bytes == $expected_bytes
      and $e.request_body_read_ms != null
      and $e.finalize_ms != null
      and (if $expected_stream == "stream"
           then $e.stream_total_ms != null
           else $e.stream_total_ms == null and $e.upstream_body_ms != null
           end)
      and $raw_residual >= -10
      and $raw_residual <= 10
    ) |
    {
      request_id,
      request_body_read_ms,
      request_body_bytes,
      finalize_ms,
      stream_total_ms,
      upstream_body_ms,
      accounted_ms: $accounted,
      raw_residual_ms: $raw_residual,
      unaccounted_ms: (if $raw_residual < 0 then 0 else $raw_residual end)
    }
  ' "$event_file"
}
```

## Point-in-Time Cases

### Existing observability cases

Run Cases A–E in the isolated fixture and record outcomes in the report template.

| Case | Given State | Expected Storage | Expected API | Expected UI | Verdict |
|---|---|---|---|---|---|
| **A. 429 Rate Limit** | `curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: 429" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages"` | `status=429`, `error_code=upstream_4xx`, `upstream_error_type=rate_limit_error`, `upstream_error_message=forced fake rate limit response` | `status: 429`, `error_code: "upstream_4xx"`, `upstream_error_type: "rate_limit_error"`, `upstream_error_message: "forced fake rate limit response"` | Row shows `429`. Drawer shows `upstream_4xx`, `rate_limit_error`, and capped message. | |
| **D. Timeout** | Start the tracked background request in the verbatim Case D steps below; the fixture sleeps 60s, while this scenario's `upstream_total_secs=30` produces the terminal proxy timeout. | `status=504`, `error_code=tower_timeout` | `status: 504`, `error_code: "tower_timeout"` | Row shows `504`. Drawer shows `tower_timeout` and no disconnect label. | |
| **E. Long Canonical Error** | `curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: 429-long" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages"` | `status=429`, `error_code=upstream_4xx`, `upstream_error_type=rate_limit_error`; `upstream_error_message` is `<=1024` UTF-8 bytes and ends in `...` (marker included in the cap). | Same status/type/message boundary as storage, proven with the `jq` byte-length projection below. | Row shows `429`; drawer preserves line breaks, wraps the continuous token, permits text selection, and has no horizontal clipping at 375/768/1280px. | |

### Latency remediation cases

Run these cases in the isolated fixture and record outcomes in the report
template; no historical result is retained here.

| Case | Exercised state | Required checks | Expected UI | Verdict |
|---|---|---|---|---|
| **F. Slow chunked ingress and exact-body parity** | Throttled chunked request plus an exact-body request | Compare event bytes, materialized fields, capture length, fixture/capture SHA-256, and timing fields | Desktop/mobile drawer shows body-read and finalize stages without overflow | |
| **G. Exact large ingress** | Normal non-stream request with the exact fixture body | Compare request-body bytes, SQLite payload/materialized value, capture length, and SHA-256 | Body size and timing stages render without overflow | |
| **H. Normal non-stream accounting** | Completed non-stream response with parent timing fields | Confirm selected response-body fields are counted once and optional stream fields remain absent | Body-read, response-body, finalize, and residual stages render once | |
| **I. Normal stream accounting** | Completed slow stream with complete-stream fields | Confirm stream and upstream body timing are counted once across recent, delta, detail, and SQLite | Drawer shows one complete stream-body stage | |
| **K. Renewal source-specific timeline** | Isolated renewal row without proxy/new timing fields | Confirm source-specific fields remain absent/NULL and are not synthesized | Drawer shows one renewal cycle without proxy timeline stages | |

## State-Transition Cases

### Existing observability transitions

| Case | Given State | Mutation | Expected Storage | Expected API | Expected UI | Verdict |
|---|---|---|---|---|---|---|
| **B. Slow Streaming** | SSE connected, UI open | `curl -sS -N -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: slow" -H "x-claude-code-session-id: session-123" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"stream":true,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages"` | N/A (in progress) | For one `event_id`, SSE partial payloads first contain `thread_id` + `model`, then first contain `principal_id`, then first contain `upstream_name`; exactly one `final` follows. `RequestEventUpdate` wires only `phase: partial|final`, not lifecycle trigger names. | Row appears as `In progress`. Drawer shows early fields. Becomes final without reload. | |
| **C. Client Disconnected** | Slow streaming request in progress | Terminate client (`kill -TERM`) | `status=499`, `error_code=client_closed_request` | `status: 499`, `error_code: "client_closed_request"` | Row shows `Client disconnected`. Drawer shows `client_closed_request`. | |


## UI Verification Steps (agent-browser)

```bash
# 1. Auth and navigate
agent-browser --session "$SESSION_ID" open "http://127.0.0.1:$vite_port/"
agent-browser --session "$SESSION_ID" wait --load networkidle
cat <<EOF | agent-browser --session "$SESSION_ID" eval --stdin
localStorage.setItem('cc-lb-admin-token', '$ADMIN_TOKEN');
window.location.href = 'http://127.0.0.1:$vite_port/logs';
EOF
agent-browser --session "$SESSION_ID" wait --load networkidle

# 2. Verify Case A (429). The just-created event is the newest request row.
curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: 429" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages" > "$TMP_DIR/case_a_response.txt"
agent-browser --session "$SESSION_ID" wait --text "429"
agent-browser --session "$SESSION_ID" snapshot -i -c
agent-browser --session "$SESSION_ID" find text "429" click --exact
agent-browser --session "$SESSION_ID" wait --text "upstream_4xx"
agent-browser --session "$SESSION_ID" wait --text "rate_limit_error"
agent-browser --session "$SESSION_ID" wait --text "forced fake rate limit response"
# Check console for errors.
agent-browser --session "$SESSION_ID" console
agent-browser --session "$SESSION_ID" errors
# Take screenshots at different viewports.
agent-browser --session "$SESSION_ID" set viewport 375 812
agent-browser --session "$SESSION_ID" screenshot case_a_375_new.png
agent-browser --session "$SESSION_ID" set viewport 768 1024
agent-browser --session "$SESSION_ID" screenshot case_a_768_new.png
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot case_a_1280_new.png
# Close drawer.
agent-browser --session "$SESSION_ID" press Escape

# 3. Verify Case B (Slow Streaming)
# Start the slow streaming request in the background
curl -sS -N -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: slow" -H "x-claude-code-session-id: session-123" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"stream":true,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages" > "$TMP_DIR/case_b_curl.txt" &
CURL_PID_B=$!
# Wait for the row to appear as "In progress"
agent-browser --session "$SESSION_ID" wait --text "In progress"
agent-browser --session "$SESSION_ID" snapshot -i -c
agent-browser --session "$SESSION_ID" find text "In progress" click --exact
# Wait for the same drawer to become final without a reload.
agent-browser --session "$SESSION_ID" wait --text "Stream relay"
# Take screenshots.
agent-browser --session "$SESSION_ID" set viewport 375 812
agent-browser --session "$SESSION_ID" screenshot case_b_375_new.png
agent-browser --session "$SESSION_ID" set viewport 768 1024
agent-browser --session "$SESSION_ID" screenshot case_b_768_new.png
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot case_b_1280_new.png
agent-browser --session "$SESSION_ID" press Escape

# 4. Verify Case C (Client Disconnected)
# Start slow streaming request.
curl -sS -N -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: slow" -H "x-claude-code-session-id: session-123" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"stream":true,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages" > "$TMP_DIR/case_c_curl.txt" &
CURL_PID_C=$!
# Wait for body to start.
deadline=$((SECONDS + 10))
while [ $SECONDS -lt $deadline ]; do
  if grep -q "event: message_start" "$TMP_DIR/case_c_curl.txt" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
# Terminate client.
cleanup_pid_tree "$CURL_PID_C"
# Wait for the row to show "Client disconnected".
agent-browser --session "$SESSION_ID" wait --text "Client disconnected"
agent-browser --session "$SESSION_ID" snapshot -i -c
agent-browser --session "$SESSION_ID" find text "Client disconnected" click --exact
agent-browser --session "$SESSION_ID" wait --text "client_closed_request"
# Take screenshots.
agent-browser --session "$SESSION_ID" set viewport 375 812
agent-browser --session "$SESSION_ID" screenshot case_c_375_new.png
agent-browser --session "$SESSION_ID" set viewport 768 1024
agent-browser --session "$SESSION_ID" screenshot case_c_768_new.png
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot case_c_1280_new.png
agent-browser --session "$SESSION_ID" press Escape

# 5. Verify Case D (Timeout)
# The fixture sleeps for 60s; cc-lb reaches its configured 30s upstream total
# timeout first. Keep the client curl tracked until the persisted terminal row
# proves the proxy, rather than the fixture, produced the 504.
curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: timeout" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages" > "$TMP_DIR/case_d_curl.txt" &
CURL_PID_D=$!
deadline=$((SECONDS + 45))
case_d_event=""
while [ $SECONDS -lt $deadline ]; do
  case_d_event=$(curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/v1/events/recent?limit=100" | jq -c '[.events[] | select(.status == 504 and .error_code == "tower_timeout")] | first // empty')
  if [ -n "$case_d_event" ]; then
    break
  fi
  sleep 0.2
done
[ -n "$case_d_event" ] || { echo "Case D did not persist 504/tower_timeout within 45s" >&2; exit 1; }
printf '%s\n' "$case_d_event" | jq -e 'select(.status == 504 and .error_code == "tower_timeout")' > "$TMP_DIR/case_d_api.json"
cleanup_pid_tree "$CURL_PID_D"
CURL_PID_D=""
agent-browser --session "$SESSION_ID" wait --text "504"
agent-browser --session "$SESSION_ID" snapshot -i -c
agent-browser --session "$SESSION_ID" find text "504" click --exact
agent-browser --session "$SESSION_ID" wait --text "tower_timeout"
# Verify the open drawer, not an older table row, does not contain the
# `Client disconnected` label.
agent-browser --session "$SESSION_ID" eval "document.querySelector('[role=dialog]')?.innerText.includes('Client disconnected') ? (() => { throw new Error('Case D incorrectly rendered a disconnect label'); })() : 'no drawer disconnect label'"
# Take screenshots.
agent-browser --session "$SESSION_ID" set viewport 375 812
agent-browser --session "$SESSION_ID" screenshot case_d_375_new.png
agent-browser --session "$SESSION_ID" set viewport 768 1024
agent-browser --session "$SESSION_ID" screenshot case_d_768_new.png
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot case_d_1280_new.png
agent-browser --session "$SESSION_ID" press Escape

# 6. Verify Case E (429-long), including storage/API truncation and drawer wrapping.
curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: 429-long" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages" > "$TMP_DIR/case_e_response.txt"
deadline=$((SECONDS + 10))
case_e_api=""
while [ $SECONDS -lt $deadline ]; do
  case_e_api=$(curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/v1/events/recent?limit=100" | jq -c '[.events[] | select(.status == 429 and (.upstream_error_message | startswith("forced fake long rate limit response")))] | first // empty')
  if [ -n "$case_e_api" ]; then
    break
  fi
  sleep 0.2
done
[ -n "$case_e_api" ] || { echo "Case E did not persist in the recent-events API within 10s" >&2; exit 1; }
printf '%s\n' "$case_e_api" | jq -e '{status, error_code, upstream_error_type, message: .upstream_error_message} | .message_bytes = (.message | utf8bytelength) | .marker_inclusive = (.message | endswith("...")) | select(.status == 429 and .error_code == "upstream_4xx" and .upstream_error_type == "rate_limit_error" and .message_bytes <= 1024 and .marker_inclusive)' > "$TMP_DIR/case_e_api.json"
deadline=$((SECONDS + 10))
case_e_sqlite=""
while [ $SECONDS -lt $deadline ]; do
  case_e_sqlite=$(sqlite3 -readonly -json "$TMP_DIR/cc-lb.sqlite" "SELECT json_extract(payload,'$.status') AS status, error_code, upstream_error_type, upstream_error_message, length(CAST(upstream_error_message AS BLOB)) AS message_bytes, substr(upstream_error_message, -3) = '...' AS marker_inclusive FROM request_events_v1 WHERE json_extract(payload,'$.status') = 429 AND upstream_error_message LIKE 'forced fake long rate limit response%' ORDER BY ts DESC LIMIT 1;")
  if printf '%s' "$case_e_sqlite" | jq -e 'length == 1' > /dev/null; then
    break
  fi
  sleep 0.2
done
[ "$(printf '%s' "$case_e_sqlite" | jq 'length')" = "1" ] || { echo "Case E did not persist in SQLite within 10s" >&2; exit 1; }
printf '%s' "$case_e_sqlite" | jq -e '.[0] | {status, error_code, upstream_error_type, message: .upstream_error_message, message_bytes, marker_inclusive: (.marker_inclusive == 1)} | select(.status == 429 and .error_code == "upstream_4xx" and .upstream_error_type == "rate_limit_error" and .message_bytes <= 1024 and .marker_inclusive)' > "$TMP_DIR/case_e_sqlite.json"
agent-browser --session "$SESSION_ID" wait --fn "document.querySelector('tbody tr[aria-label^=\"View request\"]')?.innerText.includes('429')"
agent-browser --session "$SESSION_ID" snapshot -i -c
agent-browser --session "$SESSION_ID" find text "429" click --exact
agent-browser --session "$SESSION_ID" wait --text "forced fake long rate limit response"

assert_long_error_drawer() {
  cat <<'EOF' | agent-browser --session "$SESSION_ID" eval --stdin
const message = Array.from(document.querySelectorAll('div')).find((element) =>
  element.textContent?.startsWith('forced fake long rate limit response'),
);
if (!message) throw new Error('Case E upstream error message is not rendered');
const style = getComputedStyle(message);
const selection = getSelection();
const range = document.createRange();
range.selectNodeContents(message);
selection?.removeAllRanges();
selection?.addRange(range);
const result = {
  whiteSpace: style.whiteSpace,
  overflowWrap: style.overflowWrap,
  selectable: selection?.toString().includes('forced fake long rate limit response'),
  longTokenPreserved: message.textContent?.includes('z'.repeat(200)),
  horizontalOverflow: message.scrollWidth > message.clientWidth + 1,
  clippedOutsideViewport: message.getBoundingClientRect().right > window.innerWidth + 1,
};
if (
  result.whiteSpace !== 'pre-wrap' ||
  !['break-word', 'anywhere', 'break-all'].includes(result.overflowWrap) ||
  !result.selectable ||
  !result.longTokenPreserved ||
  result.horizontalOverflow ||
  result.clippedOutsideViewport
) {
  throw new Error(`Case E drawer assertion failed: ${JSON.stringify(result)}`);
}
JSON.stringify(result);
EOF
}

agent-browser --session "$SESSION_ID" set viewport 375 812
assert_long_error_drawer > "$TMP_DIR/case_e_375_drawer.json"
agent-browser --session "$SESSION_ID" screenshot case_e_375_new.png
agent-browser --session "$SESSION_ID" set viewport 768 1024
assert_long_error_drawer > "$TMP_DIR/case_e_768_drawer.json"
agent-browser --session "$SESSION_ID" screenshot case_e_768_new.png
agent-browser --session "$SESSION_ID" set viewport 1280 800
assert_long_error_drawer > "$TMP_DIR/case_e_1280_drawer.json"
agent-browser --session "$SESSION_ID" screenshot case_e_1280_new.png
agent-browser --session "$SESSION_ID" press Escape
```

## Latency remediation verification procedure

Run these checks against the isolated fixture and record outcomes outside the
public scenario; no execution result is retained here.

```bash
wait_for_event_by_request_id() {
  local request_id=$1 deadline event
  deadline=$((SECONDS + 30))
  while [ $SECONDS -lt $deadline ]; do
    event=$(curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" \
      "http://127.0.0.1:$admin_port/admin/events/recent?limit=100" |
      jq -c --arg request_id "$request_id" \
        '[.events[] | select(.request_id == $request_id)] | first // empty')
    if [ -n "$event" ]; then
      printf '%s\n' "$event"
      return 0
    fi
    sleep 0.2
  done
  echo "No event for request_id=$request_id within 30s" >&2
  return 1
}

assert_browser_absent() {
  local correlation=$1 evidence_name=$2
  cat <<EOF | agent-browser --session "$SESSION_ID" eval --stdin > "$TMP_DIR/evidence/${evidence_name}.json"
const correlation = '$correlation';
const token = localStorage.getItem('cc-lb-admin-token');
if (!token) throw new Error('Missing cc-lb admin token in localStorage');
const findEvent = (value) => {
  if (!value || typeof value !== 'object') return null;
  if (value.request_id === correlation || value.thread_id === correlation) return value;
  for (const child of Array.isArray(value) ? value : Object.values(value)) {
    const found = findEvent(child);
    if (found) return found;
  }
  return null;
};
const deadline = Date.now() + 15000;
let deltaUrls = [];
while (Date.now() < deadline && deltaUrls.length === 0) {
  deltaUrls = [...new Set(performance.getEntriesByType('resource')
    .map((entry) => entry.name)
    .filter((url) => url.includes('/admin/v1/events/delta')))];
  if (deltaUrls.length === 0) await new Promise((resolve) => setTimeout(resolve, 200));
}
if (deltaUrls.length === 0) throw new Error('No Logs delta request observed before mutation');
for (const url of deltaUrls) {
  const response = await fetch(url, {headers: {Authorization: 'Bearer ' + token}});
  if (!response.ok) throw new Error('Delta replay failed: ' + response.status + ' ' + url);
  if (findEvent(await response.json())) {
    throw new Error('Correlation ' + correlation + ' already exists in delta before mutation');
  }
}
const row = [...document.querySelectorAll('tbody tr')].find((element) =>
  element.innerText.includes(correlation) || element.getAttribute('aria-label')?.includes(correlation),
);
if (row) throw new Error('Correlation ' + correlation + ' already exists in UI before mutation');
JSON.stringify({correlation, deltaRequestsChecked: deltaUrls.length, absent: true});
EOF
}

open_event_drawer() {
  local request_id=$1
  agent-browser --session "$SESSION_ID" wait --fn \
    "[...document.querySelectorAll('tbody tr')].some((row) => row.innerText.includes('$request_id') || row.getAttribute('aria-label')?.includes('$request_id'))"
  agent-browser --session "$SESSION_ID" eval \
    "const id='$request_id'; const row=[...document.querySelectorAll('tbody tr')].find((element)=>element.innerText.includes(id)||element.getAttribute('aria-label')?.includes(id)); if(!row) throw new Error('row not found: '+id); row.click(); 'opened '+id"
}

assert_recent_delta_detail_match() {
  local event_file=$1 evidence_name=$2
  local expected
  expected=$(cat "$event_file")
  cat <<EOF | agent-browser --session "$SESSION_ID" eval --stdin > "$TMP_DIR/evidence/${evidence_name}.json"
const expected = $expected;
const requestId = expected.request_id;
const token = localStorage.getItem('cc-lb-admin-token');
if (!token) throw new Error('Missing cc-lb admin token in localStorage');
const findEvent = (value) => {
  if (!value || typeof value !== 'object') return null;
  if (value.request_id === requestId) return value;
  for (const child of Array.isArray(value) ? value : Object.values(value)) {
    const found = findEvent(child);
    if (found) return found;
  }
  return null;
};
const has = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const fields = [
  'request_body_read_ms',
  'request_body_bytes',
  'finalize_ms',
  'upstream_body_ms',
  'stream_total_ms',
  'source_kind',
  'duration_ms',
];
const assertSame = (actual, surface) => {
  if (!actual) throw new Error(surface + ' did not contain ' + requestId);
  for (const key of fields) {
    if (has(actual, key) !== has(expected, key) ||
        (has(expected, key) && !Object.is(actual[key], expected[key]))) {
      throw new Error(
        surface + ' mismatch for ' + key +
        ': expected=' + JSON.stringify(expected[key]) +
        ', actual=' + JSON.stringify(actual[key])
      );
    }
  }
};

const recentResponse = await fetch(
  'http://127.0.0.1:$admin_port/admin/events/recent?limit=100',
  {headers: {Authorization: 'Bearer ' + token}},
);
if (!recentResponse.ok) throw new Error('Recent fetch failed: ' + recentResponse.status);
const recentEvent = findEvent(await recentResponse.json());
assertSame(recentEvent, 'recent');

const deadline = Date.now() + 15000;
let deltaUrls = [];
let deltaEvent = null;
while (Date.now() < deadline && !deltaEvent) {
  const resourcesNow = performance.getEntriesByType('resource').map((entry) => entry.name);
  deltaUrls = [...new Set(resourcesNow.filter((url) => url.includes('/admin/v1/events/delta')))];
  for (const url of deltaUrls) {
    const response = await fetch(url, {headers: {Authorization: 'Bearer ' + token}});
    if (!response.ok) throw new Error('Delta replay failed: ' + response.status + ' ' + url);
    deltaEvent ||= findEvent(await response.json());
  }
  if (!deltaEvent) await new Promise((resolve) => setTimeout(resolve, 200));
}
assertSame(deltaEvent, 'delta final');

const resources = performance.getEntriesByType('resource').map((entry) => entry.name);
const detailUrls = [...new Set(resources.filter((url) =>
  url.includes('/admin/events/') &&
  !url.includes('/recent') &&
  !url.includes('/delta') &&
  !url.includes('/stream')
))].reverse();
let detailEvent = null;
let matchingDetailUrl = null;
for (const url of detailUrls) {
  const response = await fetch(url, {headers: {Authorization: 'Bearer ' + token}});
  if (!response.ok) continue;
  const candidate = findEvent(await response.json());
  if (candidate) {
    detailEvent = candidate;
    matchingDetailUrl = url;
    break;
  }
}
assertSame(detailEvent, 'detail');

JSON.stringify({
  requestId,
  fields,
  recent: true,
  deltaFinal: true,
  detail: true,
  deltaRequestsChecked: deltaUrls.length,
  detailUrl: matchingDetailUrl,
});
EOF
}

assert_timeline_drawer() {
  local request_id=$1 expected_kind=$2 expected_bytes=$3 evidence_name=$4
  cat <<EOF | agent-browser --session "$SESSION_ID" eval --stdin > "$TMP_DIR/evidence/${evidence_name}.json"
const requestId = '$request_id';
const expectedKind = '$expected_kind';
const expectedBytes = Number('$expected_bytes');
const dialog = document.querySelector('[role=dialog]');
if (!dialog) throw new Error('No open drawer for ' + requestId);
const text = dialog.innerText;
const rect = dialog.getBoundingClientRect();
const common = {
  requestId,
  viewport: {width: innerWidth, height: innerHeight},
  withinViewport: rect.left >= -1 && rect.right <= innerWidth + 1,
  hasHorizontalOverflow: dialog.scrollWidth > dialog.clientWidth + 1,
  text,
};
if (!common.withinViewport) throw new Error('Drawer clips at ' + innerWidth + 'px');

if (expectedKind === 'proxy') {
  if (!text.includes('Request body read')) throw new Error('Missing Request body read stage');
  if (!text.includes('Finalize')) throw new Error('Missing Finalize stage');
  if (expectedBytes > 0) {
    const acceptedByteLabels = expectedBytes === 854336
      ? [/854[,. ]?336\s*(B|bytes)/i, /834(\.3)?\s*KiB/i, /854(\.3)?\s*kB/i]
      : [/917[,. ]?567\s*(B|bytes)/i, /896(\.1)?\s*KiB/i, /917(\.6)?\s*kB/i];
    if (!acceptedByteLabels.some((pattern) => pattern.test(text))) {
      throw new Error('Drawer does not show a recognizable ' + expectedBytes + '-byte label');
    }
  }
}
if (expectedKind === 'stream') {
  const relayLabels = text.match(/Stream relay/gi) ?? [];
  if (relayLabels.length !== 1) {
    throw new Error('Expected one Stream relay stage, got ' + relayLabels.length);
  }
}
if (expectedKind === 'partial') {
  if (!text.includes('Partial stream (client cancelled)')) {
    throw new Error('Missing partial client-cancelled stream stage');
  }
}
if (expectedKind === 'renewal') {
  if (!text.includes('Renewal cycle')) throw new Error('Missing Renewal cycle stage');
  if (text.includes('Request body read') || text.includes('Proxy setup') ||
      /Unaccounted\s+100%/i.test(text)) {
    throw new Error('Renewal drawer rendered proxy-only stages or Unaccounted 100%');
  }
}
JSON.stringify({...common, expectedKind, expectedBytes});
EOF
}

# F before: the correlated slow-upload row is absent from storage, recent, delta,
# and UI. A detail URL cannot exist before the request ID exists.
sqlite3 -readonly "$TMP_DIR/cc-lb.sqlite" \
  "SELECT count(*) FROM request_events_v1 WHERE json_extract(payload,'$.thread_id')='$CASE_F_SESSION';" |
  grep -qx '0'
[ -z "$(recent_event_for_session "$CASE_F_SESSION")" ]
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_f_before_1280.png"
agent-browser --session "$SESSION_ID" set viewport 375 812
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_f_before_375.png"
assert_browser_absent "$CASE_F_SESSION" "case_f_before_surfaces"
agent-browser --session "$SESSION_ID" eval "performance.clearResourceTimings(); 'resource timings cleared'"

# F fast baseline and slow HTTP/1.1 chunked mutation use identical bytes.
curl -sS --http1.1 -D "$TMP_DIR/evidence/case_f_fast.headers" \
  -o "$TMP_DIR/evidence/case_f_fast.response" \
  -X POST \
  -H "x-api-key: $CLIENT_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -H "x-claude-code-session-id: $CASE_F_FAST_SESSION" \
  --data-binary @"$TMP_DIR/body-854336.json" \
  "http://127.0.0.1:$proxy_port/v1/messages"
wait_for_final_event "$CASE_F_FAST_SESSION" > "$TMP_DIR/evidence/case_f_fast.recent.json"

UPLOAD_START_NS=$(python3 -c 'import time; print(time.monotonic_ns())')
curl -sS --http1.1 --limit-rate 128K \
  -D "$TMP_DIR/evidence/case_f_slow.headers" \
  -o "$TMP_DIR/evidence/case_f_slow.response" \
  -X POST \
  -H "x-api-key: $CLIENT_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -H "Transfer-Encoding: chunked" \
  -H "x-claude-code-session-id: $CASE_F_SESSION" \
  --data-binary @"$TMP_DIR/body-854336.json" \
  "http://127.0.0.1:$proxy_port/v1/messages"
UPLOAD_END_NS=$(python3 -c 'import time; print(time.monotonic_ns())')
UPLOAD_WALL_MS=$(python3 - "$UPLOAD_START_NS" "$UPLOAD_END_NS" <<'PY'
import sys
print((int(sys.argv[2]) - int(sys.argv[1])) // 1_000_000)
PY
)
wait_for_final_event "$CASE_F_SESSION" > "$TMP_DIR/evidence/case_f_slow.recent.json"

openssl dgst -sha256 -r "$TMP_DIR/evidence/case_f_fast.response" |
  awk '{print $1}' > "$TMP_DIR/evidence/case_f_fast.response.sha256"
openssl dgst -sha256 -r "$TMP_DIR/evidence/case_f_slow.response" |
  awk '{print $1}' > "$TMP_DIR/evidence/case_f_slow.response.sha256"
cmp "$TMP_DIR/evidence/case_f_fast.response.sha256" \
  "$TMP_DIR/evidence/case_f_slow.response.sha256"

assert_normal_residual "$TMP_DIR/evidence/case_f_slow.recent.json" 854336 nonstream \
  > "$TMP_DIR/evidence/case_f_slow.residual.json"
jq -e --argjson wall "$UPLOAD_WALL_MS" '
  select(
    .request_body_read_ms >= ($wall - 750)
    and .request_body_read_ms <= ($wall + 2000)
  ) |
  {request_body_read_ms, upload_wall_ms: $wall}
' "$TMP_DIR/evidence/case_f_slow.recent.json" \
  > "$TMP_DIR/evidence/case_f_slow.wall-correlation.json"

CASE_F_REQUEST_ID=$(jq -r '.request_id' "$TMP_DIR/evidence/case_f_slow.recent.json")
sqlite3 -readonly -json "$TMP_DIR/cc-lb.sqlite" "
  SELECT
    request_id,
    json_extract(payload,'$.request_body_read_ms') AS payload_request_body_read_ms,
    json_extract(payload,'$.request_body_bytes') AS payload_request_body_bytes,
    json_extract(payload,'$.finalize_ms') AS payload_finalize_ms,
    list_request_body_read_ms,
    list_request_body_bytes,
    list_finalize_ms
  FROM request_events_v1
  WHERE request_id='$CASE_F_REQUEST_ID';
" | jq -e '
  length == 1 and
  .[0].payload_request_body_bytes == 854336 and
  .[0].list_request_body_bytes == 854336 and
  .[0].payload_request_body_read_ms == .[0].list_request_body_read_ms and
  .[0].payload_finalize_ms == .[0].list_finalize_ms
' > "$TMP_DIR/evidence/case_f_slow.storage.json"

open_event_drawer "$CASE_F_REQUEST_ID"
agent-browser --session "$SESSION_ID" wait --text "Request body read"
agent-browser --session "$SESSION_ID" set viewport 375 812
assert_timeline_drawer "$CASE_F_REQUEST_ID" proxy 854336 "case_f_after_375.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_f_after_375.png"
agent-browser --session "$SESSION_ID" set viewport 1280 800
assert_timeline_drawer "$CASE_F_REQUEST_ID" proxy 854336 "case_f_after_1280.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_f_after_1280.png"
assert_recent_delta_detail_match "$TMP_DIR/evidence/case_f_slow.recent.json" \
  "case_f_recent_delta_detail"
agent-browser --session "$SESSION_ID" press Escape

# G/H: exact 917,567-byte normal non-stream row and residual.
[ -z "$(recent_event_for_session "$CASE_G_SESSION")" ]
curl -sS --http1.1 -D "$TMP_DIR/evidence/case_g.headers" \
  -o "$TMP_DIR/evidence/case_g.response" \
  -X POST \
  -H "x-api-key: $CLIENT_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -H "x-claude-code-session-id: $CASE_G_SESSION" \
  --data-binary @"$TMP_DIR/body-917567.json" \
  "http://127.0.0.1:$proxy_port/v1/messages"
wait_for_final_event "$CASE_G_SESSION" > "$TMP_DIR/evidence/case_g.recent.json"
assert_normal_residual "$TMP_DIR/evidence/case_g.recent.json" 917567 nonstream \
  > "$TMP_DIR/evidence/case_g.residual.json"
CASE_G_REQUEST_ID=$(jq -r '.request_id' "$TMP_DIR/evidence/case_g.recent.json")
sqlite3 -readonly -json "$TMP_DIR/cc-lb.sqlite" "
  SELECT
    request_id,
    json_extract(payload,'$.request_body_bytes') AS payload_request_body_bytes,
    json_extract(payload,'$.body_bytes') AS response_body_bytes,
    list_request_body_bytes
  FROM request_events_v1
  WHERE request_id='$CASE_G_REQUEST_ID';
" | jq -e '
  length == 1 and
  .[0].payload_request_body_bytes == 917567 and
  .[0].list_request_body_bytes == 917567 and
  .[0].payload_request_body_bytes != .[0].response_body_bytes
' > "$TMP_DIR/evidence/case_g.storage.json"
open_event_drawer "$CASE_G_REQUEST_ID"
agent-browser --session "$SESSION_ID" set viewport 375 812
assert_timeline_drawer "$CASE_G_REQUEST_ID" proxy 917567 "case_g_after_375.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_g_after_375.png"
agent-browser --session "$SESSION_ID" set viewport 1280 800
assert_timeline_drawer "$CASE_G_REQUEST_ID" proxy 917567 "case_g_after_1280.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_g_after_1280.png"
assert_recent_delta_detail_match "$TMP_DIR/evidence/case_g.recent.json" \
  "case_g_recent_delta_detail"
agent-browser --session "$SESSION_ID" press Escape

# I: complete stream. stream_total_ms is the selected response-body parent once.
cat > "$TMP_DIR/body-stream.json" <<'JSON'
{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"stream":true,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}
JSON
STREAM_REQUEST_BYTES=$(wc -c < "$TMP_DIR/body-stream.json" | tr -d ' ')
curl -sS -N -X POST \
  -H "x-api-key: $CLIENT_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -H "x-fake-mode: slow" \
  -H "x-claude-code-session-id: $CASE_I_SESSION" \
  --data-binary @"$TMP_DIR/body-stream.json" \
  "http://127.0.0.1:$proxy_port/v1/messages" \
  > "$TMP_DIR/evidence/case_i.response" &
CURL_PID_I=$!
wait "$CURL_PID_I"
CURL_PID_I=""
wait_for_final_event "$CASE_I_SESSION" > "$TMP_DIR/evidence/case_i.recent.json"
assert_normal_residual "$TMP_DIR/evidence/case_i.recent.json" \
  "$STREAM_REQUEST_BYTES" stream > "$TMP_DIR/evidence/case_i.residual.json"
jq -e '
  select(
    .stream_total_ms != null and
    (.upstream_body_ms == null or .upstream_body_ms == .stream_total_ms)
  )
' "$TMP_DIR/evidence/case_i.recent.json" > "$TMP_DIR/evidence/case_i.body-parent.json"
CASE_I_REQUEST_ID=$(jq -r '.request_id' "$TMP_DIR/evidence/case_i.recent.json")
open_event_drawer "$CASE_I_REQUEST_ID"
agent-browser --session "$SESSION_ID" set viewport 375 812
assert_timeline_drawer "$CASE_I_REQUEST_ID" stream 0 "case_i_after_375.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_i_after_375.png"
agent-browser --session "$SESSION_ID" set viewport 1280 800
assert_timeline_drawer "$CASE_I_REQUEST_ID" stream 0 "case_i_after_1280.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_i_after_1280.png"
assert_recent_delta_detail_match "$TMP_DIR/evidence/case_i.recent.json" \
  "case_i_recent_delta_detail"
agent-browser --session "$SESSION_ID" press Escape

# J before/during/after: observe an in-progress row, wait after the first SSE
# chunk, terminate the client, and compare wall cancellation delay to the stored
# relay partial. The tolerance covers polling and process-termination overhead.
curl -sS -N -X POST \
  -H "x-api-key: $CLIENT_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -H "x-fake-mode: slow" \
  -H "x-claude-code-session-id: $CASE_J_SESSION" \
  --data-binary @"$TMP_DIR/body-stream.json" \
  "http://127.0.0.1:$proxy_port/v1/messages" \
  > "$TMP_DIR/evidence/case_j.response" &
CURL_PID_J=$!
deadline=$((SECONDS + 10))
while [ $SECONDS -lt $deadline ]; do
  grep -q "event: message_start" "$TMP_DIR/evidence/case_j.response" 2>/dev/null && break
  sleep 0.1
done
grep -q "event: message_start" "$TMP_DIR/evidence/case_j.response"
agent-browser --session "$SESSION_ID" wait --text "In progress"
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_j_before_cancel_1280.png"
CANCEL_START_NS=$(python3 -c 'import time; print(time.monotonic_ns())')
sleep 1.2
cleanup_pid_tree "$CURL_PID_J"
CURL_PID_J=""
CANCEL_END_NS=$(python3 -c 'import time; print(time.monotonic_ns())')
CANCEL_WALL_MS=$(python3 - "$CANCEL_START_NS" "$CANCEL_END_NS" <<'PY'
import sys
print((int(sys.argv[2]) - int(sys.argv[1])) // 1_000_000)
PY
)
wait_for_final_event "$CASE_J_SESSION" > "$TMP_DIR/evidence/case_j.recent.json"
jq -e --argjson wall "$CANCEL_WALL_MS" '
  select(
    .status == 499 and
    .error_code == "client_closed_request" and
    .stream_total_ms == null and
    .upstream_body_ms != null and
    .upstream_body_ms >= ($wall - 750) and
    .upstream_body_ms <= ($wall + 1500)
  ) |
  {
    request_id,
    status,
    error_code,
    upstream_body_ms,
    stream_total_ms,
    cancellation_wall_ms: $wall
  }
' "$TMP_DIR/evidence/case_j.recent.json" > "$TMP_DIR/evidence/case_j.partial.json"
CASE_J_REQUEST_ID=$(jq -r '.request_id' "$TMP_DIR/evidence/case_j.recent.json")
sqlite3 -readonly -json "$TMP_DIR/cc-lb.sqlite" "
  SELECT
    request_id,
    json_extract(payload,'$.status') AS status,
    error_code,
    json_extract(payload,'$.upstream_body_ms') AS payload_upstream_body_ms,
    json_extract(payload,'$.stream_total_ms') AS payload_stream_total_ms
  FROM request_events_v1
  WHERE request_id='$CASE_J_REQUEST_ID';
" | jq -e --argjson partial "$(jq '.upstream_body_ms' "$TMP_DIR/evidence/case_j.recent.json")" '
  length == 1 and
  .[0].status == 499 and
  .[0].error_code == "client_closed_request" and
  .[0].payload_upstream_body_ms == $partial and
  .[0].payload_stream_total_ms == null
' > "$TMP_DIR/evidence/case_j.storage.json"
open_event_drawer "$CASE_J_REQUEST_ID"
agent-browser --session "$SESSION_ID" wait --text "Client disconnected"
agent-browser --session "$SESSION_ID" set viewport 375 812
assert_timeline_drawer "$CASE_J_REQUEST_ID" partial 0 "case_j_after_375.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_j_after_375.png"
agent-browser --session "$SESSION_ID" set viewport 1280 800
assert_timeline_drawer "$CASE_J_REQUEST_ID" partial 0 "case_j_after_1280.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_j_after_1280.png"
assert_recent_delta_detail_match "$TMP_DIR/evidence/case_j.recent.json" \
  "case_j_recent_delta_detail"
agent-browser --session "$SESSION_ID" press Escape

# K before: prove the renewal ID is absent. The only direct DB write in this
# scenario follows and is guarded to the throwaway SQLite path.
sqlite3 -readonly "$TMP_DIR/cc-lb.sqlite" \
  "SELECT count(*) FROM request_events_v1 WHERE request_id='$CASE_K_REQUEST_ID';" |
  grep -qx '0'
curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" \
  "http://127.0.0.1:$admin_port/admin/events/recent?limit=100" |
  jq -e --arg request_id "$CASE_K_REQUEST_ID" \
    '[.events[] | select(.request_id == $request_id)] | length == 0' \
  > "$TMP_DIR/evidence/case_k_before_recent.json"
assert_browser_absent "$CASE_K_REQUEST_ID" "case_k_before_surfaces"
agent-browser --session "$SESSION_ID" set viewport 1280 800
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_k_before_1280.png"
agent-browser --session "$SESSION_ID" set viewport 375 812
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_k_before_375.png"

assert_isolated_path "$TMP_DIR/cc-lb.sqlite"
python3 - "$TMP_DIR/cc-lb.sqlite" "$CASE_K_REQUEST_ID" "$CASE_G_REQUEST_ID" <<'PY'
import datetime
import json
import sqlite3
import sys
import uuid

db_path, request_id, source_request_id = sys.argv[1:4]
connection = sqlite3.connect(db_path)
connection.row_factory = sqlite3.Row
columns = [row[1] for row in connection.execute("PRAGMA table_info(request_events_v1)")]
source = connection.execute(
    "SELECT * FROM request_events_v1 WHERE request_id = ?",
    (source_request_id,),
).fetchone()
if source is None:
    raise SystemExit(
        f"Completed proxy source event {source_request_id!r} is required "
        "before the renewal fixture"
    )
values = {column: source[column] for column in columns}

raw_payload = values["payload"]
was_bytes = isinstance(raw_payload, bytes)
payload = json.loads(raw_payload.decode() if was_bytes else raw_payload)
timing_keys = {
    "request_body_read_ms", "request_body_bytes", "finalize_ms",
    "bulkhead_wait_ms", "dns_ms", "connect_ms", "shape_ms", "sign_ms",
    "upstream_ttfb_ms", "stream_total_ms", "upstream_body_ms",
    "first_body_chunk_ms", "proxy_setup_ms",
    "json_parse_ms", "cache_tokenizer_queue_ms", "cache_structure_ms",
    "cache_serialize_ms", "cache_token_key_ms", "cache_count_lookup_ms",
    "cache_tokenize_ms", "prepare_signer_ms", "auth_ms", "route_ms",
    "limit_reserve_ms", "body_bytes",
}
for key in timing_keys:
    payload.pop(key, None)
payload["request_id"] = request_id
payload["source_kind"] = "renewal"
payload["duration_ms"] = 3210
payload.pop("thread_id", None)
if "event_id" in payload:
    payload["event_id"] = f"evt_renewal_{uuid.uuid4().hex}"
encoded = json.dumps(payload, separators=(",", ":"))
values["payload"] = encoded.encode() if was_bytes else encoded

primary_keys = {
    row[1] for row in connection.execute("PRAGMA table_info(request_events_v1)")
    if row[5]
}
for column in columns:
    if column in primary_keys:
        values[column] = None
    elif column == "request_id":
        values[column] = request_id
    elif column == "event_id":
        values[column] = payload.get("event_id", f"evt_renewal_{uuid.uuid4().hex}")
    elif column == "source_kind" or column == "list_source_kind":
        values[column] = "renewal"
    elif column == "duration_ms" or column == "list_duration_ms":
        values[column] = 3210
    elif column.startswith("list_") and (
        column.endswith("_ms") or column in {
            "list_request_body_bytes", "list_body_bytes",
        }
    ):
        values[column] = None
    elif column == "ts":
        if isinstance(values[column], int):
            values[column] = connection.execute(
                "SELECT coalesce(max(ts), 0) + 1 FROM request_events_v1"
            ).fetchone()[0]
        else:
            values[column] = datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat().replace("+00:00", "Z")

insert_columns = [column for column in columns if values[column] is not None]
placeholders = ",".join("?" for _ in insert_columns)
connection.execute(
    f"INSERT INTO request_events_v1 ({','.join(insert_columns)}) VALUES ({placeholders})",
    [values[column] for column in insert_columns],
)
connection.commit()
connection.close()
PY

wait_for_event_by_request_id "$CASE_K_REQUEST_ID" > "$TMP_DIR/evidence/case_k.recent.json"
jq -e '
  select(
    .source_kind == "renewal" and
    .duration_ms == 3210 and
    (has("request_body_read_ms") | not) and
    (has("request_body_bytes") | not) and
    (has("finalize_ms") | not) and
    (has("proxy_setup_ms") | not) and
    (has("upstream_ttfb_ms") | not) and
    (has("stream_total_ms") | not) and
    (has("upstream_body_ms") | not)
  )
' "$TMP_DIR/evidence/case_k.recent.json" > "$TMP_DIR/evidence/case_k.api-contract.json"
sqlite3 -readonly -json "$TMP_DIR/cc-lb.sqlite" "
  SELECT
    request_id,
    json_extract(payload,'$.source_kind') AS source_kind,
    json_extract(payload,'$.duration_ms') AS duration_ms,
    json_type(payload,'$.proxy_setup_ms') AS proxy_setup_type,
    json_type(payload,'$.upstream_ttfb_ms') AS upstream_ttfb_type,
    json_type(payload,'$.request_body_read_ms') AS request_body_read_type,
    list_request_body_read_ms,
    list_request_body_bytes,
    list_finalize_ms
  FROM request_events_v1
  WHERE request_id='$CASE_K_REQUEST_ID';
" | jq -e '
  length == 1 and
  .[0].source_kind == "renewal" and
  .[0].duration_ms == 3210 and
  .[0].proxy_setup_type == null and
  .[0].upstream_ttfb_type == null and
  .[0].request_body_read_type == null and
  .[0].list_request_body_read_ms == null and
  .[0].list_request_body_bytes == null and
  .[0].list_finalize_ms == null
' > "$TMP_DIR/evidence/case_k.storage.json"
open_event_drawer "$CASE_K_REQUEST_ID"
agent-browser --session "$SESSION_ID" wait --text "Renewal cycle"
agent-browser --session "$SESSION_ID" set viewport 375 812
assert_timeline_drawer "$CASE_K_REQUEST_ID" renewal 0 "case_k_after_375.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_k_after_375.png"
agent-browser --session "$SESSION_ID" set viewport 1280 800
assert_timeline_drawer "$CASE_K_REQUEST_ID" renewal 0 "case_k_after_1280.dom"
agent-browser --session "$SESSION_ID" screenshot "$TMP_DIR/evidence/case_k_after_1280.png"
assert_recent_delta_detail_match "$TMP_DIR/evidence/case_k.recent.json" \
  "case_k_recent_delta_detail"
agent-browser --session "$SESSION_ID" press Escape

# Final UI non-regression checks and teardown proof.
agent-browser --session "$SESSION_ID" console > "$TMP_DIR/evidence/browser-console.txt"
agent-browser --session "$SESSION_ID" errors > "$TMP_DIR/evidence/browser-errors.txt"
```

The scenario must end through the `trap` defined in §0. A PASS requires the teardown output to confirm that no proxy, admin, metrics, fake-upstream, or Vite listener remains. The temporary directory may be copied to a separate evidence location before shell exit; otherwise teardown removes it. Never replace `$TMP_DIR/cc-lb.sqlite` with an operational database path.

## Automated coverage map

Maintain coverage for admin event ingestion/querying, lifecycle
cancellation/terminal behavior, storage mappings, request-event schemas,
timeline calculations, drawer rendering, and partial→final live updates.
Record results only from the current isolated run.


## Verification boundary

- When browser verification is run, cover the desktop and mobile viewports
  required by the scenario and record them for that run.
- Record metrics and span/export limitations explicitly; do not infer exporter
  coverage when the collector is unavailable.
- Keep temporary paths run-scoped; no screenshot or evidence path is promised
  as permanent public evidence.


## Latency remediation report template

Record each case from the current isolated run:

| Case | Required surfaces | Verdict | Evidence |
|---|---|---|---|
| Case F (slow chunked ingress and exact-body parity) | Storage/recent/delta/detail/UI + upstream body SHA-256 |  |  |
| Cases G/H (large ingress and non-stream accounting) | Storage/recent/delta/detail/UI + upstream body SHA-256 |  |  |
| Case I (normal stream accounting) | Storage/recent/delta/detail/UI |  |  |
| Case J (partial response body) | Storage/recent/delta/detail/UI transition |  |  |
| Case K (renewal source-specific timeline) | Storage/recent/delta/detail/UI transition |  |  |
| Run boundary | Browser console/layout + metrics + OTLP limitation |  |  |


## Cases A–E report template

Record only outcomes from the current isolated run. Do not retain private
evidence paths or historical execution claims in this public scenario.

| Case | Required surfaces | Verdict | Evidence |
|---|---|---|---|
| Case A (429 Rate Limit) | Storage/API/UI |  |  |
| Case B (Slow Streaming) | Live UI transition |  |  |
| Case C (Client Disconnected) | Storage/API/UI |  |  |
| Case D (Timeout) | Storage/API/UI |  |  |
| Case E (Long Canonical Error) | Storage/API/UI |  |  |
