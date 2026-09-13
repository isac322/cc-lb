# Principal API-key issuance and revoke pending-state QA

## Purpose

Prove the complete principal API-key lifecycle across an isolated SQLite store,
the real admin API, and the real admin-web UI, with special coverage for the
original duplicate-issuance bug.

The bug was a synchronous double-submit race: repeated clicks could enter the
issue mutation more than once before React Query published `isPending`. Every
accepted `POST` minted a different live key, but the UI could show only one
one-time plaintext response. The other key remained active with plaintext the
operator could never recover.

This scenario proves both halves of the user flow:

- **Point in time:** SQLite, API, and UI agree about the empty, active, and
  revoked key states. Plaintext exists only in the single issue response and the
  protected success dialog.
- **State transition:** a deterministic HTTP response gate holds real issue and
  revoke responses after the isolated server commits them. While the response
  is held, the browser must expose and lock the pending state without sleeps,
  timing guesses, or retry loops. Releasing the gate drives the success state.

The load-bearing invariant is:

> One intentional issue action creates exactly one HTTP `POST`, one
> `managed_keys_v1` row, one active API record, and one one-time plaintext key.

Run the same steps against the known-buggy revision to reproduce the failure,
then against the candidate fix. Never run this scenario against shared or
production state.

## 0. Isolated environment and preconditions

### 0.1 Required local tools

- A current `./target/debug/cc-lb` binary.
- `bun`, `python3`, `curl`, `jq`, `sqlite3`, and `agent-browser`.
- A shell that supports the commands below.

Use `agent-browser` for UI assertions. Unit-test DOM output is not browser
evidence.

### 0.2 Create disposable ports, credentials, and SQLite configuration

From the repository root:

```bash
QA_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/cc-lb-principal-key-qa.XXXXXX")
read -r proxy_port admin_port metrics_port gate_port vite_port <<EOF
$(python3 -c "import socket; s=[socket.socket() for _ in range(5)]; [x.bind(('127.0.0.1',0)) for x in s]; print(' '.join(str(x.getsockname()[1]) for x in s)); [x.close() for x in s]")
EOF

ADMIN_TOKEN=$(uuidgen)
MASTER_KEY=$(openssl rand -hex 32)
SESSION_ID="qa-principal-keys-$(openssl rand -hex 4)"
DB="$QA_ROOT/cc-lb.sqlite"
ADMIN_URL="http://127.0.0.1:$admin_port"
GATE_URL="http://127.0.0.1:$gate_port"
WEB_URL="http://127.0.0.1:$vite_port"

cat > "$QA_ROOT/cc-lb.toml" <<EOF
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
principal_id = "qa-principal-keys"
upstream_kind = "anthropic_key"

[storage]
kind = "sqlite"
path = "$DB"

[aead]
key_env = "CC_LB_MASTER_KEY"

[observability]
tracing_level = "info"
log_redaction = true
user_prompt_redaction = true

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
```

This is a fresh database, not a copy of a shared database. The scenario must
leave every non-QA listener and database untouched.

### 0.3 Create the deterministic response gate

The gate forwards every request to the real isolated admin server. It delays
only these two responses, and only after the upstream response is ready:

- `POST /admin/v1/principals/{id}/keys` (`issue`)
- `POST /admin/v1/principals/{id}/keys/{key_id}/revoke` (`revoke`)

Therefore a held response means the real SQLite mutation has completed while
the browser request is still pending. `/__qa/wait/{operation}` blocks on an
event from the gate; `/__qa/release/{operation}` releases it. This is the
required synchronization mechanism. Do not replace it with `sleep`, polling,
or retry loops.

```bash
cat > "$QA_ROOT/response_gate.py" <<'PY'
import json
import os
import re
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = os.environ["QA_UPSTREAM"].rstrip("/")
LISTEN_PORT = int(os.environ["QA_GATE_PORT"])
OPS = ("issue", "revoke")
COUNTS = {op: 0 for op in OPS}
WAITING = {op: threading.Event() for op in OPS}
RELEASE = {op: threading.Event() for op in OPS}
LOCK = threading.Lock()
HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding", "upgrade", "content-length", "host",
}


def operation(method, path):
    if method != "POST":
        return None
    if re.fullmatch(r"/admin/v1/principals/[^/]+/keys", path):
        return "issue"
    if re.fullmatch(r"/admin/v1/principals/[^/]+/keys/[^/]+/revoke", path):
        return "revoke"
    return None


class GateServer(ThreadingHTTPServer):
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        print("gate:", fmt % args, flush=True)

    def send_json(self, status, value):
        body = json.dumps(value, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def control(self):
        path = self.path.split("?", 1)[0]
        if path == "/__qa/status":
            with LOCK:
                counts = dict(COUNTS)
            self.send_json(200, {
                "counts": counts,
                "waiting": {op: WAITING[op].is_set() for op in OPS},
                "released": {op: RELEASE[op].is_set() for op in OPS},
            })
            return True
        match = re.fullmatch(r"/__qa/wait/(issue|revoke)", path)
        if match:
            op = match.group(1)
            if not WAITING[op].wait(timeout=30):
                self.send_json(504, {"error": "gate_wait_timeout", "operation": op})
                return True
            with LOCK:
                count = COUNTS[op]
            self.send_json(200, {"operation": op, "count": count, "waiting": True})
            return True
        match = re.fullmatch(r"/__qa/release/(issue|revoke)", path)
        if match:
            op = match.group(1)
            RELEASE[op].set()
            self.send_json(200, {"operation": op, "released": True})
            return True
        return False

    def proxy(self):
        path = self.path.split("?", 1)[0]
        op = operation(self.command, path)
        if op:
            with LOCK:
                COUNTS[op] += 1

        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length) if length else None
        headers = {
            key: value for key, value in self.headers.items()
            if key.lower() not in HOP_HEADERS
        }
        request = urllib.request.Request(
            UPSTREAM + self.path,
            data=body,
            headers=headers,
            method=self.command,
        )
        try:
            response = urllib.request.urlopen(request, timeout=30)
            status = response.status
            response_headers = response.headers
            response_body = response.read()
        except urllib.error.HTTPError as error:
            status = error.code
            response_headers = error.headers
            response_body = error.read()

        if op:
            WAITING[op].set()
            RELEASE[op].wait()

        self.send_response(status)
        for key, value in response_headers.items():
            if key.lower() not in HOP_HEADERS:
                self.send_header(key, value)
        self.send_header("content-length", str(len(response_body)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(response_body)
        self.close_connection = True

    def do_GET(self):
        if not self.control():
            self.proxy()

    def do_POST(self):
        if not self.control():
            self.proxy()

    def do_PUT(self):
        self.proxy()

    def do_PATCH(self):
        self.proxy()

    def do_DELETE(self):
        self.proxy()

    def do_OPTIONS(self):
        self.proxy()


GateServer(("127.0.0.1", LISTEN_PORT), Handler).serve_forever()
PY
```

### 0.4 Start the isolated stack and seed one principal

Start the server, gate, and Vite in separate tracked processes. Confirm their
health before continuing; readiness is setup, not a pending-state assertion.

```bash
CC_LB_ADMIN_TOKEN="$ADMIN_TOKEN" \
CC_LB_MASTER_KEY="$MASTER_KEY" \
./target/debug/cc-lb serve --config "$QA_ROOT/cc-lb.toml" \
  > "$QA_ROOT/cc-lb.log" 2>&1 &
CC_LB_PID=$!

QA_UPSTREAM="$ADMIN_URL" QA_GATE_PORT="$gate_port" \
python3 "$QA_ROOT/response_gate.py" > "$QA_ROOT/gate.log" 2>&1 &
GATE_PID=$!

(
  cd crates/cc-lb-admin/web
  CC_LB_ADMIN_URL="$GATE_URL" bun run dev --port "$vite_port"
) > "$QA_ROOT/vite.log" 2>&1 &
VITE_PID=$!

curl -fsS "$ADMIN_URL/admin/health" > "$QA_ROOT/health.json"
curl -fsS "$GATE_URL/__qa/status" > "$QA_ROOT/gate-initial.json"
curl -fsS "$WEB_URL/" > /dev/null

curl -fsS -X POST \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' \
  --data '{"name":"qa-principal-keys","kind":"machine","allowed_models":["*"],"default_limits":[]}' \
  "$ADMIN_URL/admin/v1/principals" > "$QA_ROOT/principal.json"
PRINCIPAL_ID=$(jq -er '.id' "$QA_ROOT/principal.json")
```

The seed request goes directly to `$ADMIN_URL`; the browser uses `$GATE_URL`
through Vite. API evidence should also use `$ADMIN_URL` so checks never queue
behind a held browser response.

### 0.5 Authenticate and open the principal in a real browser

```bash
agent-browser --session "$SESSION_ID" open "$WEB_URL/"
agent-browser --session "$SESSION_ID" wait --load networkidle
cat <<EOF | agent-browser --session "$SESSION_ID" eval --stdin
localStorage.setItem('cc-lb-admin-token', '$ADMIN_TOKEN');
window.location.href = '$WEB_URL/principals?selectedId=$PRINCIPAL_ID';
EOF
agent-browser --session "$SESSION_ID" wait --load networkidle
agent-browser --session "$SESSION_ID" wait --text "API Keys"
agent-browser --session "$SESSION_ID" snapshot -i -c
```

Never print the admin token or include it in screenshots.

### 0.6 Teardown

Always close the browser, terminate the three tracked process trees, confirm all
five disposable ports have no listeners, and only then remove `$QA_ROOT`. If a
listener survives, retain the directory for diagnosis instead of claiming
cleanup. Confirm the shared/prod database timestamp and checksum are unchanged
if one exists on the workstation.

```bash
agent-browser --session "$SESSION_ID" close || true

cleanup_pid_tree() {
  pid=$1
  [ -n "$pid" ] || return 0
  if kill -0 "$pid" 2>/dev/null; then
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

cleanup_pid_tree "$VITE_PID"
cleanup_pid_tree "$GATE_PID"
cleanup_pid_tree "$CC_LB_PID"

orphaned=0
for port in "$proxy_port" "$admin_port" "$metrics_port" "$gate_port" "$vite_port"; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "QA listener still active on port $port" >&2
    orphaned=1
  fi
done

if [ "$orphaned" -eq 0 ]; then
  rm -rf "$QA_ROOT"
else
  echo "Retaining $QA_ROOT because teardown is incomplete" >&2
  false
fi
```

## 1. Context discovery and evidence commands

Use these exact checks throughout the scenario.

### 1.1 Storage

```bash
sqlite3 -readonly "file:$DB?mode=ro" <<SQL
.headers on
.mode box
SELECT principal_id, key_id, label, status, created_at, revoked_at, last_4,
       length(verify_hash) AS verify_hash_bytes,
       length(secret_salt) AS secret_salt_bytes,
       length(index_hash) AS index_hash_bytes
FROM managed_keys_v1
WHERE principal_id = '$PRINCIPAL_ID'
ORDER BY created_at, key_id;
SQL
```

Exact-one invariant:

```bash
sqlite3 -readonly "file:$DB?mode=ro" \
  "SELECT COUNT(*) FROM managed_keys_v1 WHERE principal_id='$PRINCIPAL_ID' AND label='duplicate-race-qa';"
```

Revocation zeroization:

```bash
sqlite3 -readonly "file:$DB?mode=ro" <<SQL
SELECT status,
       revoked_at IS NOT NULL AS has_revoked_at,
       hex(verify_hash) = printf('%064d', 0) AS verify_hash_zeroed,
       hex(secret_salt) = printf('%032d', 0) AS secret_salt_zeroed,
       hex(index_hash) = printf('%064d', 0) AS index_hash_zeroed
FROM managed_keys_v1
WHERE principal_id = '$PRINCIPAL_ID' AND key_id = '$KEY_ID';
SQL
```

Expected after revoke: `revoked|1|1|1|1`. Do not assert that `secret_hash` is
blank; the revoke contract zeroes the active verification/index material.

### 1.2 Admin API

```bash
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$ADMIN_URL/admin/v1/principals/$PRINCIPAL_ID/keys" \
  | tee "$QA_ROOT/keys-active.json" | jq .

curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$ADMIN_URL/admin/v1/principals/$PRINCIPAL_ID/keys?status=revoked" \
  | tee "$QA_ROOT/keys-revoked.json" | jq .
```

The list API may expose `key_id`, label, issue/revoke timestamps, last four, and
last-used time. It must never expose `plaintext_key`, `verify_hash`,
`secret_salt`, `index_hash`, or the complete secret.

### 1.3 Gate state

```bash
curl -fsS "$GATE_URL/__qa/status" | tee "$QA_ROOT/gate-status.json" | jq .
```

`counts.issue` and `counts.revoke` count browser HTTP requests received by the
gate. This is independent of SQLite row counts and must be recorded separately.

## 2. Point-in-time checks

### P1 — Empty principal

Given the fresh principal before issuance:

- **Storage:** `managed_keys_v1` has zero rows for `$PRINCIPAL_ID`.
- **API:** `GET .../keys` returns `{ "keys": [] }`.
- **UI:** the API Keys card retains its table headers and shows
  `No API keys issued.` with an enabled `Issue Key` action.
- **Security:** no plaintext-like key appears in the DOM, browser console,
  screenshot, API list response, or server logs.

### P2 — Active key record

Given a completed successful issuance:

- **Storage:** exactly one `managed_keys_v1` row exists for the issued `key_id`;
  label is `duplicate-race-qa`, status is `active`, `revoked_at` is null, and
  `last_4` matches the issued plaintext suffix.
- **API:** the default list returns exactly one record with the same `key_id`,
  label, `last_4`, and null `revoked_at_unix_secs`; it contains no plaintext or
  verification material.
- **UI:** the API Keys table refreshes without a page reload and shows one Active
  row with the same label, key ID, and masked `···<last4>` value.

### P3 — One-time plaintext boundary

Given the issue response has been released but the operator has not selected
`Done`:

- The dialog title changes from `Issue API key` to `API key issued`.
- Exactly one plaintext key is visible in the dialog with the warning
  `Copy the key now. It will not be shown again` and a Copy action.
- The close X is disabled and marked `aria-disabled`; Escape and backdrop clicks
  do not close the dialog. There is no Cancel action. `Done` is the only exit.
- The active-key list endpoint and the SQLite inspection never reveal the
  plaintext.

After `Done`, the dialog closes and the plaintext disappears from the DOM.
Reopening `Issue Key`, refreshing the page, and querying the list API must not
recover it.

### P4 — Revoked key record

Given a completed successful revoke:

- **Storage:** the same row remains, status is `revoked`, `revoked_at` is set,
  and `verify_hash`, `secret_salt`, and `index_hash` are zeroed.
- **API:** the default active list no longer contains the key;
  `?status=revoked` contains it with stable `key_id` and `last_4` but no secret.
- **UI:** the row disappears after query invalidation without a page reload.

## 3. Deterministic state-transition checks

### T1 — Repeated issue clicks produce exactly one key

1. Confirm P1 and save the initial gate status.
2. In the browser, select `Issue Key`, enter label `duplicate-race-qa`, then
   invoke the Issue button three times in one JavaScript task. This recreates
   the original pre-render click burst rather than relying on human timing:

   ```bash
   cat <<'EOF' | agent-browser --session "$SESSION_ID" eval --stdin
   (() => {
     const dialog = [...document.querySelectorAll('[role="dialog"]')]
       .find((node) => node.textContent.includes('Issue API key'));
     if (!dialog) throw new Error('Issue API key dialog not found');
     const issue = [...dialog.querySelectorAll('button')]
       .find((button) => button.textContent.trim() === 'Issue');
     if (!issue) throw new Error('Issue button not found');
     issue.click();
     issue.click();
     issue.click();
     return 'submitted click burst';
   })();
   EOF
   ```

3. Block until the real server has committed the issue response and the gate is
   holding it. This command is event-driven and does not poll:

   ```bash
   curl -fsS "$GATE_URL/__qa/wait/issue" \
     | tee "$QA_ROOT/issue-held.json" | jq .
   ```

4. While the response remains held, prove all pending behavior in the browser:

   - the action reads `Issuing...`, is disabled, has `aria-busy="true"`, and
     shows the progress spinner;
   - the label input, Cancel, and close X are disabled;
   - clicking Issuing, Cancel, or X, pressing Escape, and clicking the backdrop
     leave the same dialog mounted;
   - the gate reports `counts.issue == 1`;
   - SQLite already contains exactly one row labeled `duplicate-race-qa`;
   - the direct admin API already lists exactly one active key.

5. Save a pending screenshot and DOM snapshot, then release the exact response:

   ```bash
   curl -fsS "$GATE_URL/__qa/release/issue" | jq .
   agent-browser --session "$SESSION_ID" wait --load networkidle
   ```

6. Prove P2 and P3. Project only the non-secret key ID and last four from the
   browser value; the full plaintext must not enter terminal history or the
   report:

   ```bash
   cat <<'EOF' | agent-browser --session "$SESSION_ID" eval --stdin
   (() => {
     const dialog = [...document.querySelectorAll('[role="dialog"]')]
       .find((node) => node.textContent.includes('API key issued'));
     const plaintext = dialog?.querySelector('code')?.textContent?.trim() ?? '';
     const match = /^sk-cclb-([0-9A-HJKMNP-TV-Z]{26})_([A-Za-z0-9_-]{43})$/.exec(plaintext);
     if (!match) throw new Error('issued plaintext has an unexpected shape');
     return { key_id: match[1], last_4: match[2].slice(-4) };
   })();
   EOF
   ```

   Compare only those projected fields with the SQLite row and active-list API
   record.
7. Before selecting Done, try close X, Escape, and backdrop again. The plaintext
   dialog must remain mounted and the secret must remain visible.
8. Verify the refreshed API Keys table already contains exactly one matching
   active row. Select `Done`; verify the plaintext is absent from the DOM.
9. Recheck after network idle:

   - gate `counts.issue == 1`;
   - SQLite label count is exactly `1`;
   - active API-list count for the label is exactly `1`;
   - UI row count for the label is exactly `1`.

**Original-bug failure signature:** the gate count and SQLite/API/UI row counts
become `2` or `3`. Only one response plaintext can occupy the success dialog,
so every extra active row is an issued credential whose plaintext was stranded.
Any count other than exactly one is FAIL even if the UI appears usable.

### T2 — Revoke stays locked until success, then closes and refreshes

Use the single T1 key. Refresh the active-list evidence, then set `KEY_ID`
without printing plaintext:

```bash
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
  "$ADMIN_URL/admin/v1/principals/$PRINCIPAL_ID/keys" \
  > "$QA_ROOT/keys-active.json"
KEY_ID=$(jq -er '.keys[] | select(.label == "duplicate-race-qa") | .key_id' \
  "$QA_ROOT/keys-active.json")
```

1. In the key row, select the `Revoke key` action. Confirm the alert dialog title
   is `Revoke API key?` and its description names the disposable key label.
2. Select `Revoke` once, then wait on the response gate:

   ```bash
   curl -fsS "$GATE_URL/__qa/wait/revoke" \
     | tee "$QA_ROOT/revoke-held.json" | jq .
   ```

3. While the real revoke response is held:

   - the alert dialog remains open; confirming did not close it optimistically;
   - Cancel and Revoke are disabled;
   - Revoke has `aria-busy="true"` and a visible progress spinner;
   - the row-level `Revoke key` action is disabled;
   - Escape and backdrop clicks do not dismiss the alert dialog;
   - gate `counts.revoke == 1`;
   - direct SQLite inspection already shows the same row revoked and active
     verification/index material zeroed;
   - the browser may still show the prior row because success has intentionally
     not reached the query invalidation path yet.

4. Save the pending screenshot/DOM snapshot, then release the response:

   ```bash
   curl -fsS "$GATE_URL/__qa/release/revoke" | jq .
   agent-browser --session "$SESSION_ID" wait --load networkidle
   ```

5. The success response must close the alert dialog, show the `Key revoked`
   success feedback, invalidate the key query, and remove the active row without
   a page reload. Prove P4 and confirm `counts.revoke == 1`.

This transition is PASS only when the dialog is protected during the request
and closes on success. Closing immediately on confirm, allowing dismissal while
pending, or leaving the successful dialog/row stale is FAIL.


## 4. Automated coverage map

| Contract | Automated coverage | What it proves | Manual/browser gap retained here |
|---|---|---|---|
| Same-render issue submit latch and pending lock | `crates/cc-lb-admin/web/src/routes/-principals.test.tsx` — `API-key issue pending locks actions, ignores dismissal, and submits once` | A synchronous repeated-click burst calls the mutation once; pending Issue/Cancel/X/input states and Escape protection are wired. | Real HTTP request count, real SQLite row count, backdrop pixels, and response-held browser state. |
| One-time plaintext protection | `crates/cc-lb-admin/web/src/routes/-principals.test.tsx` — `issued API-key plaintext cannot be dismissed until Done` | Issued plaintext renders and X/Escape/backdrop cannot dismiss it before Done. | Real server response, secret/list separation, copy affordance pixels, table invalidation, and non-recoverability after reload. |
| Revoke pending protection | `crates/cc-lb-admin/web/src/routes/-principals.test.tsx` — `API-key revoke pending preserves confirmation and locks row action` | Revoke modal remains mounted, Cancel/confirm/row action lock, and Escape is ignored during the mutation. | Delayed real response, SQLite zeroization, backdrop pixels, success close/toast, and refreshed active list. |
| Issue/revoke API and persisted record shape | `crates/cc-lb-admin/tests/principal_keys.rs` — `revoked_key_list_preserves_key_id_last4_and_audit_rows` | Real admin routes issue and revoke a key, revoked listing preserves key ID/last four, and audit actions persist. | Duplicate browser submission and protected one-time plaintext UX. |
| Plaintext/key-ID generation boundary | `crates/cc-lb-control/src/api_keys/secret.rs` tests, including `generate_new_parses_same_key_id` and redacted Display/Debug tests | Generated plaintext parses to the issued key ID, has a four-character suffix, and secret wrappers do not print plaintext. | Browser-only one-time presentation and user acknowledgement. |
| Delayed issue browser regression | `crates/cc-lb-admin/web/e2e/pending-actions.spec.ts` — `API key issuance submits once, locks dismissal, preserves plaintext, and refreshes the key list` | Real Chromium sends two DOM clicks in one task, records exactly one POST, holds an intercepted response on an explicit promise, proves `Issuing...`/input/Cancel/X/Escape/backdrop protection, then releases the response and proves one-time plaintext, Done-only dismissal, and refreshed `key-new` UI. | This scenario adds real isolated SQLite and direct API/storage correlation. |

The automated browser test must delay by retaining the intercepted route response
and releasing it from an explicit promise/event. A timeout, `sleep`, retry, or
arbitrary wait is not an acceptable substitute because it makes the pending
assertion timing-dependent.

## 5. Browser evidence requirements

Record all of the following from the same isolated run:

1. Initial API Keys empty state, with `$PRINCIPAL_ID` and viewport recorded.
2. `issue-held.json`, gate status with `counts.issue == 1`, and the exact SQLite
   label count while the response is held.
3. Screenshot and DOM snapshot of `Issuing...`, disabled Cancel/X/input, and the
   still-open dialog after Escape and backdrop attempts.
4. Screenshot and DOM snapshot of `API key issued`, the warning, Copy, and Done.
   The unredacted disposable plaintext may remain only inside `$QA_ROOT`; redact
   the code value before attaching evidence to an issue or review. Report only
   its parsed key ID and last four.
5. Active list API JSON and a screenshot of the single refreshed row before
   Done, followed by a DOM assertion that plaintext is absent after Done.
6. `revoke-held.json`, gate status with `counts.revoke == 1`, zeroization query,
   and a screenshot/DOM snapshot of the locked revoke alert dialog.
7. Screenshot after success showing the dialog closed and the active row gone,
   plus revoked-list API JSON.
8. Browser console output showing no uncaught errors, and final teardown proof
   showing all disposable listeners gone.

For each screenshot record path, viewport, browser session, operation, and
whether the response gate was held or released. Do not infer visual PASS from
component tests.

## 6. PASS/FAIL table

| Case | Storage | API | UI | Transition/invariant | Verdict/evidence |
|---|---|---|---|---|---|
| P1 empty principal |  |  |  | No secret anywhere |  |
| P2 active key record |  |  |  | One row, matching key ID/last four |  |
| P3 one-time plaintext | N/A |  |  | Protected until Done; unrecoverable afterward |  |
| P4 revoked key record |  |  |  | Same row revoked and verification material zeroed |  |
| T1 repeated issue click burst |  |  |  | Exactly one POST, one row, one plaintext, one refreshed UI row |  |
| T2 delayed revoke |  |  |  | Locked/progress while held; close and refresh only on success |  |
| Teardown/isolation | PASS only if disposable DB removed | N/A | Browser closed | No QA listener or shared/prod mutation remains |  |

Use **PASS**, **FAIL**, **VERIFIED**, or **BLOCKED**. A T1 PASS requires all four
exact-one observations (gate, SQLite, API, UI); none may be substituted for
another. A UI PASS requires real-browser evidence. A blocked browser or storage
layer makes the overall scenario BLOCKED, not PASS.
