# Real Client Versions

Pinned versions are installed by `tests/real-client/install.sh` under `target/test-bins/<client>`. The script prefers `bun add` and falls back to `npm install`, except for `pi`, which uses npm with exact companion-package pins so the tested binary and companion packages stay in lockstep.

| Client | npm package | Binary | Pinned version | Confirmation command | Output |
| --- | --- | --- | --- | --- | --- |
| `claude-code` | `@anthropic-ai/claude-code` | `claude` | `2.1.303` | `npm view @anthropic-ai/claude-code version` | `2.1.303` |
| `opencode` | `opencode-ai` | `opencode` | `1.19.3` | `npm view opencode-ai version` | `1.19.3` |
| `pi` | `@earendil-works/pi-coding-agent` | `pi` | `1.3.0` | `npm view @earendil-works/pi-coding-agent version` | `1.3.0` |
| `senpi` | `@code-yeongyu/senpi` | `senpi` | `2026.10.18-1` | `npm view @code-yeongyu/senpi@2026.10.18-1 version` | `2026.10.18-1` |

The `pi` install pins `@earendil-works/pi-ai`, `@earendil-works/pi-agent-core`, and `@earendil-works/pi-tui` to `1.3.0`; `@earendil-works/pi-coding-agent` is the package that exposes the tested `pi` binary.

## Multi-Provider Live E2E CLI Suite (`tests/real-client/provider-e2e/`)

`tests/real-client/run-provider-e2e.sh` exercises real multi-turn coding-agent workflows (tool loops, session continuation, OpenAI `previous_response_id` and Responses WebSocket + reconnect, Gemini 3 `thoughtSignature` replay, OTLP trace join, and catalog cost reconciliation) across four live provider profiles:

| Profile | Binary | Tested Version | Provider | Primary / Control Models | Entrypoint |
| --- | --- | --- | --- | --- | --- |
| `omp-gpt` | `omp` | `18.12.6` | OpenAI (`/v1/responses` HTTP SSE) | `gpt-5.2` | `./tests/real-client/run-provider-e2e.sh --run --case omp-gpt` |
| `omp-gemini` | `omp` | `18.12.6` | Google Gemini (`streamGenerateContent` SSE) | `gemini-3.1-pro-preview`, `gemini-3.5-flash-lite` | `./tests/real-client/run-provider-e2e.sh --run --case omp-gemini` |
| `codex` | `codex` | `0.163.0` | OpenAI (`/v1/responses` HTTP SSE + WebSocket) | `gpt-6.1-sol` | `./tests/real-client/run-provider-e2e.sh --run --case codex` |
| `agy` | `agy` | `1.4.4` | Google Gemini (`streamGenerateContent` SSE) | `gemini-3.1-pro-preview`, `gemini-3.1-flash-lite-preview` | `./tests/real-client/run-provider-e2e.sh --run --case agy` |

Run `./tests/real-client/run-provider-e2e.sh --self-test` for offline unit and OTLP receiver lifecycle verification, or `./tests/real-client/run-provider-e2e.sh --preflight --case all` to check binary and credential readiness without making provider calls.

## Offline local-error and clean-EOF SDK contract

`provider-e2e/local-error-contract.py` uses separately installed Python packages;
it does not install dependencies or change the live campaign's `contract.json`.
The documented setup pins `openai==3.46.0`, `google-genai==3.0.0`, and
`httpx2==2.16.0`. Actual loaded versions appear in each run's `report.json`.
Optional `--openai-js-module /scratch/node_modules/openai/index.mjs` coverage
requires exactly `openai@7.53.0` in caller-owned scratch plus Node.js with native
`fetch`; no repository dependency is added.

The captured `responses-fault.sse` and `responses-clean-eof.sse` fixtures replay
through official Codex revision `4e4767ffa763ec0d0a19e04cf8ff0163a987297b`
(`codex-api::sse::responses::process_sse`). Its test adapter also accepts the
optional `responses-clean-eof-js.sse` capture. `prepare.py` exports that committed
source and prints a separate Cargo replay command; preparation alone does not
execute it. These are runnable version requirements, not a claim that an SDK
request or parser replay has passed. See `provider-e2e/README.md` for baseline
and fixed-binary commands.
