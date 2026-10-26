# Multi-Provider Real-CLI E2E QA Suite (`provider-e2e`)

End-to-end coding-agent QA suite that drives real CLI binaries (`omp`, `codex`, `agy`) through `cc-lb` against real OpenAI (`/v1/responses` HTTP SSE + WebSocket) and Google Gemini (`/v1beta/models/*:{generateContent,streamGenerateContent}`) upstreams, while verifying:

- **Client protocol & state fidelity**:
  - OpenAI stateless and stateful `store: true` + `previous_response_id` 5-turn continuation chains
  - OpenAI Responses WebSocket (`GET /v1/responses`, `101 Switching Protocols`), `generate: false` prewarm frames (`service_tier: "auto"`), and injected WebSocket drop + reconnect replay
  - Gemini 3 `thoughtSignature` emission and multi-turn signed `functionCall` replay without mutation
  - Exact session continuation (`omp -r <session_id>`, `codex exec resume <thread_id>`, `agy --conversation <id>`)
- **Accounting & observability invariants**:
  - Pre-dispatch conservative token/USD reservation and post-response settlement (`approved-core3-subscription-budget-v30.mjs`)
  - Local TLS 1.3 / HTTP / WebSocket boundary capture (`approved-core3-provider-boundary.mjs`)
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
