#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
EVIDENCE_DIR="$ROOT_DIR/.omo/evidence"
mkdir -p "$EVIDENCE_DIR"

: "${CI_POSTGRES_URL:?CI_POSTGRES_URL must be set (e.g. postgres://user:pw@host:5432/db)}"

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-postgres-e2e.XXXXXX")
cleanup() {
  if [ -n "${PROXY_PID:-}" ]; then kill "$PROXY_PID" 2>/dev/null || true; wait "$PROXY_PID" 2>/dev/null || true; fi
  if [ -n "${FAKE_PID:-}" ]; then kill "$FAKE_PID" 2>/dev/null || true; wait "$FAKE_PID" 2>/dev/null || true; fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

read -r proxy_port admin_port metrics_port fake_port <<EOF
$(python3 - <<'PY'
import socket
socks = []
for _ in range(4):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    socks.append(s)
ports = [s.getsockname()[1] for s in socks]
for s in socks: s.close()
print(' '.join(str(p) for p in ports))
PY
)
EOF

config_path="$TMP_DIR/cc-lb.toml"
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
upstream_credential_ref = "real_client"

[storage]
kind = "postgres"
url = "$CI_POSTGRES_URL"

[storage.pool]
acquire_timeout_secs = 10
statement_timeout_secs = 25

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
TOML

wait_port() {
  python3 - "$1" "$2" <<'PY'
import socket, sys, time
port, name = int(sys.argv[1]), sys.argv[2]
deadline = time.time() + 30
while time.time() < deadline:
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.5):
            print(f'{name} on :{port}')
            sys.exit(0)
    except OSError:
        time.sleep(0.1)
print(f'{name} did not start on :{port}', file=sys.stderr)
sys.exit(1)
PY
}

seed_runtime() {
  principal_code=$(curl -sS -o "$TMP_DIR/admin-principal.json" -w '%{http_code}' -X POST \
    -H 'Authorization: Bearer test' \
    -H 'content-type: application/json' \
    --data '{"name":"api-key","kind":"machine","allowed_models":["*"]}' \
    "http://127.0.0.1:$admin_port/admin/v1/principals") || principal_code=000
  if [ "$principal_code" != "201" ] && [ "$principal_code" != "409" ]; then
    echo "FAIL: create principal expected 201 or 409, got $principal_code" >&2
    cat "$TMP_DIR/admin-principal.json" >&2 || true
    exit 1
  fi

  upstream_body=$(printf '{"name":"real_client","kind":"custom","base_url":"http://127.0.0.1:%s"}' "$fake_port")
  upstream_code=$(curl -sS -o "$TMP_DIR/admin-upstream.json" -w '%{http_code}' -X POST \
    -H 'Authorization: Bearer test' \
    -H 'content-type: application/json' \
    --data "$upstream_body" \
    "http://127.0.0.1:$admin_port/admin/v1/upstreams") || upstream_code=000
  if [ "$upstream_code" != "201" ] && [ "$upstream_code" != "409" ]; then
    echo "FAIL: create upstream expected 201 or 409, got $upstream_code" >&2
    cat "$TMP_DIR/admin-upstream.json" >&2 || true
    exit 1
  fi
}

# Verify psql is available and reset prior-test runtime state before spawning
# cc-lb. The postgres-conformance CI job runs Postgres-gated cc-lb-server tests
# (e.g. managed_key_multi_instance) before this smoke step; they seed
# principals_v1 + upstreams_v1 via seed_app_testing_storage and leave the rows
# behind. cc-lb's dynamic routing then picks the stale `test-upstream` (with
# a dead base_url) over the smoke's freshly seeded `real_client`, yielding 502.
psql --version >/dev/null 2>&1 || {
  echo "FAIL: psql not installed in runner image; this script depends on it" >&2
  exit 1
}
echo "===> step 0: reset dynamic runtime tables in public schema"
psql "$CI_POSTGRES_URL" -c "TRUNCATE principals_v1, upstreams_v1, managed_api_keys_v1, managed_api_key_index_v1, oauth_credentials_v1 RESTART IDENTITY CASCADE" > /dev/null
psql "$CI_POSTGRES_URL" -c "DELETE FROM meta WHERE key = 'backend_kind'" > /dev/null

echo "===> step 1: spawn fake-anthropic on :$fake_port"
cargo run -q -p fake-anthropic -- --port "$fake_port" > "$TMP_DIR/fake.log" 2>&1 &
FAKE_PID=$!
wait_port "$fake_port" fake-anthropic

echo "===> step 2: spawn cc-lb-server with [storage] kind=postgres"
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
CC_LB_ADMIN_TOKEN=test \
cargo run -q -p cc-lb-server --features postgres,redb -- serve --config "$config_path" \
  > "$TMP_DIR/proxy.log" 2>&1 &
PROXY_PID=$!
if ! wait_port "$proxy_port" cc-lb; then
  echo "--- proxy.log ---" >&2; cat "$TMP_DIR/proxy.log" >&2 || true
  exit 1
fi
wait_port "$admin_port" cc-lb-admin
seed_runtime

echo "===> step 3: send /v1/messages through postgres-backed proxy"
response=$(curl -sS -w '\n%{http_code}' -X POST \
  -H 'content-type: application/json' \
  -H 'x-api-key: sk-ant-test' \
  -H 'anthropic-version: 2023-06-01' \
  --data '{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hello postgres backend"}],"max_tokens":32}' \
  "http://127.0.0.1:$proxy_port/v1/messages")
body=$(printf '%s' "$response" | head -n -1)
status=$(printf '%s' "$response" | tail -n 1)
echo "HTTP $status"
echo "body: $body"

if [ "$status" != "200" ]; then
  echo "FAIL: expected 200, got $status" >&2
  echo "--- proxy.log ---" >&2; cat "$TMP_DIR/proxy.log" >&2 || true
  exit 1
fi
if ! printf '%s' "$body" | grep -q 'fake anthropic fixture response HELLO'; then
  echo "FAIL: body did not contain expected fixture text" >&2
  exit 1
fi

echo "===> step 4: verify meta.backend_kind row stamped 'postgres'"
backend_kind=$(psql "$CI_POSTGRES_URL" -tAc "SELECT value FROM meta WHERE key = 'backend_kind'" 2>/dev/null || echo "")
echo "meta.backend_kind = '$backend_kind'"
if [ "$backend_kind" != "postgres" ]; then
  echo "FAIL: meta.backend_kind expected 'postgres', got '$backend_kind'" >&2
  exit 1
fi

cp "$TMP_DIR/proxy.log" "$EVIDENCE_DIR/postgres-e2e-proxy.log" || true

echo "===> step 5: tear down healthy cc-lb instance"
kill "$PROXY_PID" 2>/dev/null || true
wait "$PROXY_PID" 2>/dev/null || true
PROXY_PID=""

echo "===> step 6: tamper meta.backend_kind to 'redb' to simulate a wrong-backend startup"
psql "$CI_POSTGRES_URL" -c "UPDATE meta SET value = 'redb' WHERE key = 'backend_kind'" > /dev/null
new_kind=$(psql "$CI_POSTGRES_URL" -tAc "SELECT value FROM meta WHERE key = 'backend_kind'")
[ "$new_kind" = "redb" ] || { echo "FAIL: tamper did not stick"; exit 1; }

echo "===> step 7: restart cc-lb against tampered DB - expect fatal exit (BackendKindMismatch)"
mismatch_log="$TMP_DIR/proxy-mismatch.log"
set +e
CC_LB_MASTER_KEY=0000000000000000000000000000000000000000000000000000000000000000 \
CC_LB_ADMIN_TOKEN=test \
timeout 30 cargo run -q -p cc-lb-server --features postgres,redb -- serve --config "$config_path" \
  > "$mismatch_log" 2>&1
exit_code=$?
set -e
echo "exit_code=$exit_code"
cat "$mismatch_log" || true
if [ "$exit_code" -eq 0 ]; then
  echo "FAIL: expected non-zero exit when backend kind mismatches, got 0" >&2
  exit 1
fi
if ! grep -qi 'backend.kind\|mismatch' "$mismatch_log"; then
  echo "FAIL: expected backend kind mismatch message in log" >&2
  exit 1
fi
cp "$mismatch_log" "$EVIDENCE_DIR/postgres-e2e-mismatch.log" || true

echo "===> step 8: restore meta.backend_kind to 'postgres' for clean teardown"
psql "$CI_POSTGRES_URL" -c "UPDATE meta SET value = 'postgres' WHERE key = 'backend_kind'" > /dev/null

echo "PASS postgres-backed e2e smoke (200 round-trip + backend_kind mismatch fatal exit)"
