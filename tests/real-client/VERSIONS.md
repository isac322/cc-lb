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
