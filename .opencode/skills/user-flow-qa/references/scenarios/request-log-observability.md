# Request Log Observability

Prove that cc-lb request logs correctly capture and display various request lifecycle events, including rate limits, slow streaming, client disconnects, and timeouts, across storage, the admin API, SSE streams, and the admin-web UI.

## §0 Environment/Preconditions

Run an isolated instance with dynamic ports and a temporary SQLite database. Never mutate the shared prod DB.

```bash
# 1. Setup isolated environment and dynamic ports
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-qa.XXXXXX")
read -r proxy_port admin_port metrics_port fake_port vite_port <<EOF
$(python3 -c "import socket; socks = [socket.socket() for _ in range(5)]; [s.bind(('127.0.0.1', 0)) for s in socks]; print(' '.join(str(s.getsockname()[1]) for s in socks)); [s.close() for s in socks]")
EOF

# 2. Generate per-run random credentials (do not print these)
ADMIN_TOKEN=$(uuidgen)
MASTER_KEY=$(openssl rand -hex 32)
CLIENT_KEY="sk-ant-$(openssl rand -hex 16)"
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
kind = "sqlite"
path = "$TMP_DIR/cc-lb.sqlite"

[aead]
key_env = "CC_LB_MASTER_KEY"

[observability]
tracing_level = "info"
log_redaction = true
user_prompt_redaction = false

[quotas]
default_window_secs = 60
default_requests_per_window = 1000
default_input_tokens = 1000000
default_output_tokens = 1000000

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
EOF

# 4. Define bounded readiness and teardown
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

teardown() {
  echo "Tearing down..."
  agent-browser --session "$SESSION_ID" close || true
  cleanup_pid_tree "${CURL_PID_B:-}"
  cleanup_pid_tree "${CURL_PID_C:-}"
  cleanup_pid_tree "${CURL_PID_D:-}"
  cleanup_pid_tree "${SSE_PID:-}"
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
trap teardown EXIT INT TERM

# 5. Start services
./target/debug/fake-anthropic --port $fake_port > "$TMP_DIR/fake.log" 2>&1 &
FAKE_PID=$!
wait_port $fake_port "fake-anthropic"

CC_LB_ADMIN_TOKEN=$ADMIN_TOKEN CC_LB_MASTER_KEY=$MASTER_KEY RUST_LOG=info,hyper=warn,hyper_util=warn,axum=warn ./target/debug/cc-lb serve --config "$TMP_DIR/cc-lb.toml" > "$TMP_DIR/proxy.log" 2>&1 &
CC_LB_PID=$!
wait_port $proxy_port "cc-lb proxy"
wait_port $admin_port "cc-lb admin"
wait_http_200 "http://127.0.0.1:$admin_port/admin/health" "cc-lb admin health"

cd crates/cc-lb-admin/web
CC_LB_ADMIN_URL=http://127.0.0.1:$admin_port bun run dev --port $vite_port > "$TMP_DIR/vite.log" 2>&1 &
VITE_PID=$!
cd ../../../
wait_port $vite_port "vite"
wait_http_200 "http://127.0.0.1:$vite_port/" "vite dev server"

# 6. Seed principal and upstream
curl -sS -f -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' --data '{"name":"api-key","kind":"machine","allowed_models":["*"]}' "http://127.0.0.1:$admin_port/admin/v1/principals" > /dev/null
curl -sS -f -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' --data '{"name":"real_client","kind":"anthropic_api_key","base_url":"http://127.0.0.1:'$fake_port'","api_key_value":"sk-ant-test"}' "http://127.0.0.1:$admin_port/admin/v1/upstreams" > /dev/null
```

## Context Data

- **Storage**: `sqlite3 -readonly $TMP_DIR/cc-lb.sqlite "SELECT request_id, json_extract(payload,'$.status') AS status, error_code, upstream_error_type, upstream_error_message FROM request_events_v1 ORDER BY ts DESC LIMIT 1;"`
- **API**: `curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/events/recent?limit=1" | jq -c '{status: .events[0].status, error_code: .events[0].error_code, upstream_error_type: .events[0].upstream_error_type, upstream_error_message: .events[0].upstream_error_message}'`
- **SSE**: `curl -sS -N -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/events/stream"`
- **UI**: `http://127.0.0.1:$vite_port/logs`

## Point-in-Time Cases

| Case | Given State | Expected Storage | Expected API | Expected UI | Verdict |
|---|---|---|---|---|---|
| **A. 429 Rate Limit** | `curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: 429" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages"` | `status=429`, `error_code=upstream_4xx`, `upstream_error_type=rate_limit_error`, `upstream_error_message=forced fake rate limit response` | `status: 429`, `error_code: "upstream_4xx"`, `upstream_error_type: "rate_limit_error"`, `upstream_error_message: "forced fake rate limit response"` | Row shows `429`. Drawer shows `upstream_4xx`, `rate_limit_error`, and capped message. | |
| **D. Timeout** | Start the tracked background request in the verbatim Case D steps below; the fixture sleeps 60s, while this scenario's `upstream_total_secs=30` produces the terminal proxy timeout. | `status=504`, `error_code=tower_timeout` | `status: 504`, `error_code: "tower_timeout"` | Row shows `504`. Drawer shows `tower_timeout` and no disconnect label. | |
| **E. Long Canonical Error** | `curl -sS -i -X POST -H "x-api-key: $CLIENT_KEY" -H "anthropic-version: 2023-06-01" -H "Content-Type: application/json" -H "x-fake-mode: 429-long" -d '{"model":"claude-3-5-sonnet-20241022","max_tokens":24,"messages":[{"role":"user","content":"Reply with exactly: pong"}]}' "http://127.0.0.1:$proxy_port/v1/messages"` | `status=429`, `error_code=upstream_4xx`, `upstream_error_type=rate_limit_error`; `upstream_error_message` is `<=1024` UTF-8 bytes and ends in `...` (marker included in the cap). | Same status/type/message boundary as storage, proven with the `jq` byte-length projection below. | Row shows `429`; drawer preserves line breaks, wraps the continuous token, permits text selection, and has no horizontal clipping at 375/768/1280px. | |

## State-Transition Cases

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
  case_d_event=$(curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/events/recent?limit=100" | jq -c '[.events[] | select(.status == 504 and .error_code == "tower_timeout")] | first // empty')
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
  case_e_api=$(curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" "http://127.0.0.1:$admin_port/admin/events/recent?limit=100" | jq -c '[.events[] | select(.status == 429 and (.upstream_error_message | startswith("forced fake long rate limit response")))] | first // empty')
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

## Automated Coverage Map

- **Rust tests**: 
  - `crates/cc-lb-admin/tests/events_delta_rest.rs` covers basic event ingestion and querying.
  - `crates/cc-lb-engine/src/usage_parser.rs` (`canonical_error_body_rejects_malformed_or_wrong_shapes`) covers malformed/empty/wrong-shape canonical error controls.
  - `crates/cc-lb-engine/tests/lifecycle_client_disconnect.rs` (`generic_observer_drop_remains_terminal_dropped`) covers the generic observer `terminal_dropped` state.
  - `tests/fixtures/fake-anthropic/tests/long_error_mode.rs` covers the deterministic `429-long` canonical error envelope, a message above the 1024-byte persistence cap, explicit line breaks, and a >=200-byte unbroken run.
- **Web tests**: 
  - `crates/cc-lb-admin/web/src/components/ui/RequestEventsTable.live.test.tsx` covers live tailing behavior and partial→final row updates.
  - `crates/cc-lb-admin/web/src/components/ui/RequestEventDrawer.test.tsx` covers drawer content formatting and specific error states.
  - `crates/cc-lb-admin/web/src/lib/useLiveEventStream.test.tsx` covers live-tail trigger docs and SSE stream management.

## Manual-only/Boundary gaps

- UI rendering of specific error states (429, 499, 504), live tailing behavior, drawer content formatting.
- **Malformed/noncanonical structured-error controls**: Covered by parser tests. These result in broad diagnostics with no fabricated structured fields in the UI.
- **Generic observer `terminal_dropped`**: Covered by deterministic lifecycle tests. This state is not reliably reachable or distinguishable as a real downstream HTTP/UI flow. Only `499`/`client_closed_request` receives the explicit `Client disconnected` label in the UI.

## Verdict Table

| Dimension | Pass | Verdict | Evidence |
|---|---|---|---|
| Case A (429 Rate Limit) | Storage/API/UI | PASS | Fresh isolated run proved `json_extract(payload,'$.status')=429`, `upstream_4xx`, `rate_limit_error`, and the expected message. Drawer fully visible at 375/768/1280px; all value nodes (Model/Error/Status/Upstream) within viewport bounds. Evidence: `.omo/evidence/task-8-request-log-observability/viewport-{375,768,1280}/measurements.json`. |
| Case B (Slow Streaming) | Live UI transition | PASS (UI only) | Before/after 1280px screenshots show the same `req_server_1` drawer transition Live/In progress → final without reload or visible clipping. The sanitized SSE projection contains 45 partial and 5 final updates, but the prescribed `Stream relay` text wait timed out, so exact per-event field ordering is not claimed. |
| Case C (Client Disconnected) | Storage/API/UI | PASS | Fresh isolated run persisted `499`/`client_closed_request`. Drawer fully visible at 375/768/1280px; `Client disconnected` badge in-bounds at all viewports. Evidence: `.omo/evidence/task-8-request-log-observability/viewport-{375,768,1280}/measurements.json`. |
| Case D (Timeout) | Storage/API/UI | PASS | Fresh isolated run persisted `504`/`tower_timeout`; the open drawer had no disconnect label. Drawer fully visible at 375/768/1280px; all value nodes within viewport bounds. Evidence: `.omo/evidence/task-8-request-log-observability/viewport-{375,768,1280}/measurements.json`. |
| Case E (Long Canonical Error) | Storage/API/UI | PASS | Fresh isolated run proved the stored and API message cap is 1024 UTF-8 bytes, marker-inclusive. Long message rect (798..1251) selectable with `pre-wrap`/`break-word`, no horizontal overflow at 375/768/1280px. Evidence: `.omo/evidence/task-8-request-log-observability/viewport-{375,768,1280}/measurements.json`. |

*Fresh evidence is under `.omo/evidence/task-8-request-log-observability/`; the cleanup receipt confirms the isolated listeners closed and the temporary directory was removed.*
