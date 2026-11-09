# Multi-Provider Real-CLI E2E QA Suite (`provider-e2e`)

End-to-end coding-agent QA suite that drives real CLI binaries (`omp`, `codex`, `agy`) through `cc-lb` against real OpenAI (`/v1/responses` HTTP SSE + WebSocket) and Google Gemini (`/v1beta/models/*:{generateContent,streamGenerateContent}`) upstreams, while verifying:

- **Client protocol & state fidelity**:
  - OpenAI stateless and stateful `store: true` + `previous_response_id` 5-turn continuation chains
  - OpenAI Responses WebSocket (`GET /v1/responses`, `101 Switching Protocols`), `generate: false` prewarm frames (`service_tier: "auto"`), and injected WebSocket drop + reconnect replay
  - Gemini 3 `thoughtSignature` emission and multi-turn signed `functionCall` replay without mutation
  - Exact session continuation (`omp -r <session_id>`, `codex exec resume <thread_id>`, `agy --conversation <id>`)
- **Accounting & observability invariants**:
  - Pre-dispatch conservative token/USD reservation and post-response settlement (`approved-core3-subscription-budget-v30.mjs`)
  - HTTP/2 gRPC OTLP span capture (`trace-proof/otlp-receiver.mjs`) with zero decode/drop errors
  - 1:1 correlation join (`approved-core3-oracle.mjs`) from raw provider `response.id` -> OTel `gen_ai.response.id` + `cc_lb.request.id` -> durable `/admin/v1/events/delta` `request_id`
  - Token accounting accuracy (`input_tokens - cached_tokens`, OpenAI `reasoning_tokens` no double-add, Gemini `candidatesTokenCount + thoughtsTokenCount == output_tokens`)

## Profiles & Scenarios (4 Profiles, 13 Scenarios)

| Profile | CLI Binary | Provider / Models | Scenarios |
| --- | --- | --- | --- |
| `omp-gpt` (`api-key-omp-gpt`) | `omp` | OpenAI `gpt-5.2` | 1. `omp-gpt.stateless_tools` (`read` -> `edit` -> `node --test`)<br>2. `omp-gpt.stateful_previous_response_id` (5-turn chain)<br>3. `omp-gpt.session_continue` (exact session ID resume) |
| `omp-gemini` (`api-key-omp-gemini`) | `omp` | Gemini `gemini-3.1-pro-preview`, `gemini-3.5-flash-lite` | 4. `omp-gemini.gemini3_tools_thought_signature` (signed tool loop)<br>5. `omp-gemini.session_continue` (exact session ID + signature replay)<br>6. `omp-gemini.flash_lite_transport` (lower-cost model control) |
| `codex` (`api-key-codex`) | `codex` | OpenAI `gpt-6.1-sol` | 7. `http` (SSE coding + `node --test`)<br>8. `ws` (WebSocket `generate:false` prewarm + coding loop)<br>9. `ws_reconnect` (injected WS drop + reconnect replay)<br>10. `exact_session_resume` (`codex exec resume` + prompt cache hit) |
| `agy` (`api-key-agy`) | `agy` | Gemini `gemini-3.1-pro-preview`, `gemini-3.1-flash-lite-preview` | 11. `Actual Antigravity coding tools` (`unittest` 5/5)<br>12. `thoughtSignature continuity` (6 signed pairs, 7 replays)<br>13. `Session continue` (`--conversation` turn 2, `unittest` 6/6) |

## Usage

### 1. Offline Self-Tests (Zero Provider Calls / Zero Cost)

Runs the budget guard, `gpt-6.1-sol` catalog parser, WebSocket `service_tier: "auto"` settlement, oracle WebSocket turn join, Gemini `thoughtsTokenCount` summation, `_adminToken` prototype projection, and HTTP/2 OTLP collector lifecycle tests:

```bash
./tests/real-client/run-provider-e2e.sh --self-test
```

### Offline installed-SDK cached-input pricing regression

`cached-input-pricing-sdk.py` runs the installed Python `openai` Responses SDK and
`google-genai` SDK through an existing real `cc-lb` binary. It covers JSON and SSE
for both providers, with a positive-cache case and a zero-cache control for each
transport (eight actual SDK requests). It does not call live providers or use
live credentials.

Prerequisites: Python 3.10+, its `venv` module, current stable `openai` and
`google-genai` packages, and a `cc-lb` binary built with SQLite and both provider
routes. Install the SDKs into a disposable environment, not the repository:

```bash
SDK_ENV=$(mktemp -d)
SDK_LEDGER="$SDK_ENV/resources.jsonl"
python3 -m venv "$SDK_ENV/venv"
"$SDK_ENV/venv/bin/python" -m pip install --upgrade openai google-genai

# Fixed behavior: cached-input total must be 4010 micros.
CC_LB_BIN="$PWD/target/debug/cc-lb" \
  "$SDK_ENV/venv/bin/python" \
  tests/real-client/provider-e2e/cached-input-pricing-sdk.py \
  --ledger "$SDK_LEDGER"

# Capture the known old behavior using an existing baseline binary.
CC_LB_BIN=/absolute/path/to/baseline/cc-lb \
  "$SDK_ENV/venv/bin/python" \
  tests/real-client/provider-e2e/cached-input-pricing-sdk.py \
  --ledger "$SDK_LEDGER" --expected-cached-total 2010
```

The harness freezes per-token USD rates at `2e-6` input, `1e-5` output, and
`1e-7` cached input. The provider fixture reports 21,000 gross input tokens,
including 20,000 cached tokens, and one output token. Fixed events must contain
2,000 input, 2,000 cache-read, and 10 output micros, totaling 4,010. Zero-cache
controls must total 42,010. Baseline mode expects the old incorrect cached total
of 2,010 and zero cache-read cost; it is reproduction evidence, not a fixed-build
pass.

Both modes use the same native fixtures: `gpt-5.6` Responses with a supported
implicit message-array boundary, and `gemini-2.5-flash` generation. OpenAI usage
reports explicit zero cache-write tokens and zero 30-minute/12-hour write splits,
so this regression tests cached-read pricing without requiring cache-write rates.
No opaque cache key or resource inflates the admission estimate.

Each request gets a fresh admin-issued managed key. Independent read-only SQLite
queries check its terminal event, successful status, normalized tokens, cost
components, and durable per-key settlement in both configured cost windows.
The harness rejects duplicate requests or duplicated settlement. It uses ephemeral
loopback ports, synthetic upstream credentials, explicit SDK base URLs, an
allowlisted child environment, and a Python socket audit guard that rejects
non-loopback SDK connections. The frozen catalog's refresh URL is also loopback.
Readiness and persistence have bounded condition-based polling; `--timeout`
controls each deadline.

Clients, the real server, provider sockets, and fixture threads close on success
or failure. The printed scratch directory retains `server.log`, `prices.json`,
`storage.sqlite`, and `summary.json`; SDK versions and the binary hash appear in
the summary. Resource creation and closure append to the selected `--ledger`
without overwriting existing entries. By default, the ledger is
`cc-lb-cached-input-sdk-resources.jsonl` in Python's temporary directory.
Keep the ledger outside the repository.
Pass `--cleanup` when authorizing immediate evidence cleanup: after closing the
processes, it verifies the run's ownership marker and directory inode before
deleting only that exact scratch directory. The shared ledger remains intact.
Otherwise, retain the evidence until explicit cleanup.

Responses WebSocket is not covered by this SDK harness. The server's native
cached-input pricing tests cover that transport separately.

### 2. Preflight Check (Zero Provider Calls / Zero Cost)

Verifies `target/debug/cc-lb`, installed CLI binaries (`omp`, `codex`, `agy`), free local port offsets, and credential variable presence (without printing secret values):

```bash
./tests/real-client/run-provider-e2e.sh --preflight --case all --env-file /path/to/.env
```

### 3. Live Provider E2E Execution

Stages an isolated working directory, starts the OTLP collector, runs the selected CLI profile(s) through `cc-lb`, and verifies all scenario and accounting invariants:

```bash
# Run a single profile
./tests/real-client/run-provider-e2e.sh --run --case omp-gpt --env-file /path/to/.env
./tests/real-client/run-provider-e2e.sh --run --case omp-gemini --env-file /path/to/.env
./tests/real-client/run-provider-e2e.sh --run --case codex --env-file /path/to/.env
./tests/real-client/run-provider-e2e.sh --run --case agy --env-file /path/to/.env

# Run all 4 profiles sequentially
./tests/real-client/run-provider-e2e.sh --run --case all --env-file /path/to/.env

# Run shared-pool OAuth E2E (local OMP Gemini OAuth + external OpenAI OAuth broker, never_refresh: true + auto re-read on 401)
./tests/real-client/run-provider-e2e.sh --run-oauth
```

### Environment Variables

- `OPENAI_BACKEND_API_KEY` or `OPENAI_API_KEY`: OpenAI API key for `omp-gpt` and `codex`
- `GEMINI_MANAGED_API_KEY` or `GEMINI_API_KEY`: Google Gemini API key for `omp-gemini` and `agy`
- `CC_LB_BIN`: Path to `cc-lb` binary (defaults to `target/debug/cc-lb`)
- `AGY_BIN`: Path to `agy` binary if not on `PATH`

### 4. Manual Claude Code attribution-cache regression (release build)

This is a manual live-provider procedure, not one of the four automated profiles above. Run it only with a published/release `cc-lb` build and an isolated official Anthropic upstream configured with a valid API key or an access-only OAuth credential marked `never_refresh: true`. Install Claude Code through its documented public installation path and use only the command-line options shown by the installed binary's `--help`.

1. Start the release build using the documented setup and serving commands. Confirm the isolated upstream is the official Anthropic endpoint and that refresh is disabled for this run.
2. In one Claude Code session, perform a normal request that reads a file and completes a tool-result round trip.
3. In the same session, run a cron-attribution request, then another normal request, then a healthy normal control. Keep the model, tools, system blocks after `system[0]`, speed, and the first non-text block identical across all four requests. Change only the exact 18-byte attribution suffix pair.
4. Record request IDs from the request diagnostics and compare the final diagnostics with the provider SSE usage. Confirm the old hash differs while the new effective hash is equal across the attribution pair, and confirm the raw diagnostic `prefix_token_count` retains the 18-byte raw-prefix difference.
5. Require an actual streaming response, tool round trip, and positive provider-reported cache read before calling the run a pass. Aggregate SSE cache-read usage is evidence of provider cache activity, but it does not prove which individual breakpoint or full-prefix key was hit.

Do not put credentials, private copies, local filesystem paths, cluster identifiers, or personal data in reports. A requested procedure is not a pass claim; record pass/fail and evidence only after executing it. If provider credentials or Claude Code installation access are unavailable, record the item as blocked rather than inferring success.
