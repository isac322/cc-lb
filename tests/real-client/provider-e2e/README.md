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

### Offline image admission regression

`image-admission-sdk.mjs` exercises installed OpenAI Node Responses/Chat and
Google GenAI generation/streaming builders through a real SQLite-backed binary.
It uses a compressed 1024x768 RGB image with more than three million Base64
characters, fresh managed keys with a 100,000-input-token hourly cap, and a
loopback provider. It checks unchanged request bytes and durable provider usage
of 1,500 input tokens and one output token. Those synthetic usage values are not
image-tokenizer measurements.

Install the current stable `openai`, `@google/genai`, and, for CLI checks,
`@google/gemini-cli` packages in a disposable directory outside the repository.
Use Node 22.13+ with `node:sqlite`, or Bun:

```sh
CC_LB_BIN="$PWD/target/debug/cc-lb" \
SDK_NODE_MODULES="$SDK_ENV/node_modules" \
GEMINI_CLI="$SDK_ENV/node_modules/.bin/gemini" \
node tests/real-client/provider-e2e/image-admission-sdk.mjs \
  --ledger "$SDK_ENV/image-resources.jsonl" --cli
```

The optional CLI checks run actual Codex and Gemini image-attachment builders
with private home directories and synthetic credentials. macOS uses
`sandbox-exec` to permit only loopback networking; Linux requires a network
namespace with only loopback interfaces. Unavailable isolation or binaries are
reported as `NOT_RUN`. CLI capture ends with a controlled error, then replays
the captured native request bytes through cc-lb; capture alone does not claim a
completed CLI generation. The harness closes its processes and listeners and
retains evidence directories recorded in the external ledger for later cleanup.

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

### 4. Offline provider-native local errors and clean EOF (#1165, #1180)

`local-error-contract.py` starts an existing `CC_LB_BIN` with fresh SQLite state,
synthetic credentials, and a loopback-only upstream. It uses the real
`openai-python` Responses streaming/buffered clients and `google-genai`
buffered `generateContent` client. SDK retries and environment proxy discovery
are disabled; a transport guard rejects non-loopback SDK requests. No provider
credentials or provider calls are needed.

Dependencies: Python 3.12+, current stable `openai`, `google-genai`, and
`httpx2`, installed in a separate environment. On 2026-11-09, the upstream
[OpenAI package metadata](https://pypi.org/pypi/openai/json) reports `3.46.0`,
[Google package metadata](https://pypi.org/pypi/google-genai/json) reports
`3.0.0`, and [HTTPX2 metadata](https://pypi.org/pypi/httpx2/json) reports
`2.16.0`. Both current SDKs accept the harness's native HTTPX2 client;
Google's injection field remains named `HttpOptions.httpx_client`.
The harness installs nothing and records actual SDK versions in `report.json`.
It waits for health and provider-specific routing previews, rather than
assuming a fixed startup or cache-settling delay. Optional JavaScript coverage
uses `openai@7.53.0` and Node.js with native `fetch`; provision both SDK environments
outside the repository and record that scratch directory in your cleanup ledger:

```bash
SDK_ENV=/absolute/path/to/task-owned-scratch/sdk
python3 -m venv "$SDK_ENV/venv"
"$SDK_ENV/venv/bin/python" -m pip install \
  openai==3.46.0 google-genai==3.0.0 httpx2==2.16.0
npm install --prefix "$SDK_ENV/js" --ignore-scripts --no-audit --no-fund \
  --save-exact openai@7.53.0
```

Use `"$SDK_ENV/venv/bin/python"` instead of `python3` in the commands below.
Pass `--openai-js-module "$SDK_ENV/js/node_modules/openai/index.mjs"` to add an
actual JavaScript SDK clean-EOF request. The harness checks the package's exact
name/version before creating resources. Use `--js-runtime /path/to/node` if
needed. Omit that option for Python-only coverage; the report and request count
distinguish the two runs. JavaScript retries are disabled, custom fetch rejects
non-loopback URLs and redirects, and the child receives no provider credentials.

```bash
# #1180 regression gate: must fail against its baseline and pass against the fix.
CC_LB_BIN=/absolute/path/to/old/cc-lb \
  python3 tests/real-client/provider-e2e/local-error-contract.py --expect new
CC_LB_BIN=/absolute/path/to/new/cc-lb \
  python3 tests/real-client/provider-e2e/local-error-contract.py --expect new

# #1180 baseline proof on a binary with the earlier #1165 fixes:
CC_LB_BIN=/absolute/path/to/pre-1180/cc-lb \
  python3 tests/real-client/provider-e2e/local-error-contract.py \
  --expect new --expect-clean-eof old

# Historical baseline proof requiring all earlier defects as well:
CC_LB_BIN=/absolute/path/to/pre-1165/cc-lb \
  python3 tests/real-client/provider-e2e/local-error-contract.py --expect old
```

Each Python-only run makes eleven requests (twelve with JavaScript enabled).
The original nine cover healthy, injected-fault,
and provider-failed responses for Responses SSE, buffered Responses, and
buffered Gemini. Streaming faults close chunked transfer after
`response.created` and a normal output delta, without the terminating chunk.
Buffered faults send a truncated `Content-Length` body. The gate checks safe
SDK-visible messages, native error metadata, HTTP status/content type, response
identity and event sequence, byte-identical healthy/provider-failed passthrough,
and exactly one committed failure per faulty request.

The tenth request sends a Responses `response.failed` rate-limit event followed
by a complete SSE event containing malformed JSON. The loopback upstream writes
the entire body once and declares its exact original `Content-Length`, including
the malformed suffix. The proxy must suppress that suffix without changing the
provider prefix or retaining a stale downstream length. The real SDK consumes
the response through its HTTP transport: it must observe one original
`rate_limit_exceeded` failure and message, reach a clean body end, and receive
neither an incomplete-framing error nor a synthetic local override. The report
records response headers, captured body length, transport completion or actual
exception, SDK events, and each wire-contract check. Both upstream and downstream
SSE bodies are retained. `--expect new` requires every check to pass.
`--expect old` requires the tenth wire contract to fail, without assuming an
exception class. In the preserved pre-fix binary at `5656c6ae`, the observed
response advertised `Content-Length: 1871` but delivered zero body bytes;
HTTPX2 raised `RemoteProtocolError` and the SDK reported `APIConnectionError`
with no provider message. The durable row still recorded HTTP 200,
`upstream_stream_error`, and the original provider rate-limit details. This
baseline justifies the framing-defect oracle; its exact exceptions are evidence,
not hard-coded expectations.

The clean-EOF request is separate from the broken-chunk fault. The upstream
writes the complete `response.created`/item/content/output-delta prefix once,
declares its exact `Content-Length`, and finishes that body normally. Every
response usage is null; no `response.completed`, `response.incomplete`, or
`response.failed` event exists upstream. This distinguishes a missing semantic
terminal from a transport failure. `responses-clean-eof.sse` retains the actual
Python SDK wire bytes; the optional JavaScript request separately retains
`responses-clean-eof-js.sse`.

The fixed contract preserves HTTP 200 and every prefix byte, then appends exactly
one native `response.failed` with sequence number 4, the observed response ID,
`status: "failed"`, `api_error`, and the safe message
`upstream SSE response ended before a terminal Responses event`. Both real SDK
iterators must observe that failure once and finish without an HTTP framing
exception. One durable row and its admin projection must record
`upstream_stream_error` / `upstream_response_incomplete`; provider-unknown tokens
and costs must remain null. The per-request difference in the isolated principal's
`cc_lb_requests_completed_total` must be one `terminal_outcome="error"`, not a
success. Metric visibility is polled to a deadline, not inferred from persistence.
`--expect-clean-eof old` instead requires the unmodified prefix, silent SDK EOF,
no durable error, and exactly one success completion. It proves the old false
success; it is not a fixed-build pass. This flag defaults to `--expect` so the
historical combined baseline command remains usable.

For every request, the proxy's canonical response `request-id`
(or `x-request-id` if supplied instead) joins the selected upstream to both
`/admin/v1/events/delta` and a read-only SQLite query. If neither header exists,
the isolated, sequential harness requires exactly one new SQLite row since
its pre-request cursor and joins that actual ID to admin evidence. The report
records both observed headers and the correlation source; it never invents an
ID. A final table-wide count checks that every request has one durable row.
SQLite evidence reads the current relational `request_id`, `upstream_id`,
`error_code`, and `list_status` columns. Full event details come from the admin
delta API; the harness does not depend on the legacy SQLite `payload` column.
As in `qa-runtime.mjs::waitForRequestEvents`, admin projection visibility is
polled to a 30-second deadline after the durable row appears. Cursor-ordered
lifecycle updates are deduplicated by `request_id`, retaining the latest update.
The selected event must agree with durable upstream, error code, and status;
unexpected distinct request IDs fail the isolated-run correlation. Exactly-once
accounting is asserted separately against SQLite rows, not raw delta updates.
The harness neither assumes immediate visibility nor uses a cache-settling delay.
Local SSE disconnects and truncated successful buffered bodies retain the exact
durable code `upstream_stream_error`; this is separate from the client-visible
native `api_error`. Correlation and the final row count are scoped by upstream:
provider routers can generate the same `request-id` for different upstreams.


Evidence stays in a fresh `cc-lb-1165-*` directory outside the repository:
`report.json`, raw response bodies, `responses-fault.sse`,
`responses-clean-eof.sse`, server logs, SQLite, and `cleanup-ledger.json`.
Paths are printed, keys are not. The harness stops
its server and upstream in `finally`; it retains evidence for inspection.
Check ownership before following ledger cleanup commands. To choose a scratch
parent, pass `--output-parent /existing/scratch/directory`.

#### Replay through the actual pinned Codex parser

The replay uses official Codex revision
`4e4767ffa763ec0d0a19e04cf8ff0163a987297b`, specifically
`codex-rs/codex-api/src/sse/responses.rs` and `responses_error.rs`.
It does not copy the algorithm into a substitute parser or stub Codex types.
Provide a local official Codex git repository containing that commit.
`prepare.py` exports its committed tree with `git archive`, records source
hashes, and appends only a test-module declaration to the official parser.
The adapter calls the existing `process_sse` test entry point.
The checkout need not contain full history: a depth-one fetch of the exact
commit is sufficient. Source/dependency provisioning is separate from the
offline run. Record any provisioned checkout in the task's cleanup ledger.
Preparation computes the recursive normal/dev/build/target-specific path and
workspace dependency closure of `codex-api`. It narrows root workspace members
and inherited dependencies to that closure, then follows the official lockfile
graph to remove unused root patches and lock entries. This excludes unrelated
V8/Chromium and terminal-UI resolution without replacing any actual package.
Package manifests and production source remain official and unchanged, apart
from the recorded test-only module declaration. Cargo tests only `codex-api --lib`.


```bash
python3 tests/real-client/provider-e2e/codex-error-replay/prepare.py \
  --codex-source /path/to/official/codex \
  --sse /path/to/cc-lb-1165-run/responses-fault.sse \
  --expect new

# Prepare the same official parser for the separate semantic-EOF capture:
python3 tests/real-client/provider-e2e/codex-error-replay/prepare.py \
  --codex-source /path/to/official/codex \
  --sse /path/to/cc-lb-1165-run/responses-clean-eof.sse \
  --expect new
```

Preparation prints the scratch workspace, its cleanup ledger, and the exact
`cargo test --offline ... local_error_contract_replay` command, plus a separate
`cargo fetch` command for dependency provisioning if needed. Run them from the
printed `codex-rs` directory with the upstream Rust toolchain and native build
dependencies available. Fetching Cargo dependencies may use the network; neither
command calls a provider. Preparation itself neither fetches nor builds.
`--locked` is intentionally omitted: the narrowed lock retains official package
blocks, versions, Git revisions, and checksums, but Cargo may canonicalize its
reduced dependency graph. Original workspace/lock files, selected members,
removed patches, and before/after hashes are recorded beside the replay evidence.
The test command fails closed if its remaining offline dependencies are missing.
For an old-mode capture, prepare with `--expect old`; the same parser must
produce `stream closed before response.completed`, not a safe local message.
The replay selects its contract from the captured filename:
`responses-fault.sse` expects the safe processing-error message;
`responses-clean-eof.sse` (or `responses-clean-eof-js.sse`) expects the safe
incomplete-Responses message. Keep those fixture names when retaining evidence.
The preparer's `--expect` is the selected fixture's expectation, independent of
the Python harness's earlier-error controls.

The replay feeds the captured bytes in seven-byte fragments and requires the
selected new failure to surface its exact local message once. Two always-on
negative controls retain normal output first, then send either the old HTTP
envelope or a generic `type: "error"`/`api_error` event with that same message.
Both must produce generic EOF rather than the local message.
`source-manifest.json` records the revision and input hashes;
`replay-report.json` records the scenario and observed messages. Neither
preparing the workspace nor passing the Python harness proves parser replay:
the printed Cargo command must also pass for each capture.

Limits: this harness covers transport-body failure, clean nonterminal Responses
SSE, and malformed post-terminal Responses SSE under explicit `Content-Length`.
It does not cover private principal disappearance, affinity-store failures,
decode/transform fault injection, or Anthropic/Gemini SSE. Those remain engine
integration-test gates.
It proves SDK HTTP handling and the official Codex SSE parser, not the Codex
CLI's rendering, retry loop, or live-provider behavior. All documentation here
describes runnable checks, not an assertion that they have already passed.

### 5. Manual Claude Code attribution-cache regression (release build)

This is a manual live-provider procedure, not one of the four automated profiles above. Run it only with a published/release `cc-lb` build and an isolated official Anthropic upstream configured with a valid API key or an access-only OAuth credential marked `never_refresh: true`. Install Claude Code through its documented public installation path and use only the command-line options shown by the installed binary's `--help`.

1. Start the release build using the documented setup and serving commands. Confirm the isolated upstream is the official Anthropic endpoint and that refresh is disabled for this run.
2. In one Claude Code session, perform a normal request that reads a file and completes a tool-result round trip.
3. In the same session, run a cron-attribution request, then another normal request, then a healthy normal control. Keep the model, tools, system blocks after `system[0]`, speed, and the first non-text block identical across all four requests. Change only the exact 18-byte attribution suffix pair.
4. Record request IDs from the request diagnostics and compare the final diagnostics with the provider SSE usage. Confirm the old hash differs while the new effective hash is equal across the attribution pair, and confirm the raw diagnostic `prefix_token_count` retains the 18-byte raw-prefix difference.
5. Require an actual streaming response, tool round trip, and positive provider-reported cache read before calling the run a pass. Aggregate SSE cache-read usage is evidence of provider cache activity, but it does not prove which individual breakpoint or full-prefix key was hit.

Do not put credentials, private copies, local filesystem paths, cluster identifiers, or personal data in reports. A requested procedure is not a pass claim; record pass/fail and evidence only after executing it. If provider credentials or Claude Code installation access are unavailable, record the item as blocked rather than inferring success.
