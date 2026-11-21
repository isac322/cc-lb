# ADR 0014: OpenAI OAuth public v1 compatibility

- Status: Accepted
- Date: 2026-11-20
- Issue: [#1355](https://github.com/isac322/cc-lb/issues/1355)

## Context

ChatGPT-subscription (`openai_oauth`) upstreams expose the Codex backend contract, while ordinary OpenAI clients send the public `/v1` contract. The backend rejects several public fields and does not implement many public operation families. Codex-shaped traffic must remain byte-identical and account-owned state must never cross accounts.

## Decision

Serve public Responses, Chat Completions, model catalogs, compaction, input-token counting, images, realtime calls, and optional transcription through explicit translation or emulation. Preserve Codex-shaped requests unchanged. Route operations that need provider-owned state or have no subscription equivalent to eligible `openai_api_key` upstreams; choose a generic account-owned route only when exactly one eligible API-key upstream exists, otherwise reject. Never translate across providers or fail over across an ownership boundary.

Use `[openai_oauth_compat]` for server-wide restart-time defaults and `openai_oauth_compat` for per-principal fields that inherit or override them. Use `openai_public_api.realtime_origin` for the restart-time public realtime host. Transcription remains disabled by default because the upstream retains audio for 30 days. WebSocket lanes are emulated per subscription account and continuation is limited to the latest response in the same lane.

cc-lb stores no request or response data. Only bounded request-scoped buffers are used, except for the WebSocket owner body retained until connection close for steering.

## Consequences

Public SDK clients can use the supported operations against subscription pools, while unsupported or ownership-sensitive operations fail explicitly instead of being guessed. Configuration changes require restart, except principal overrides, which apply to subsequent requests and WebSocket frames. The compatibility surface remains intentionally narrower than the full public OpenAI API.
