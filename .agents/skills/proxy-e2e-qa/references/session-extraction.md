# Extracted source session notes

Source session: `ses_1817dabe1ffeXI7R0UTrzmckQ4`.
Coverage verified by session metadata and subagent DB extraction: 1008 messages from 2026-05-31T14:48:51.100Z to 2026-06-01T10:46:16.464Z.

Key correction preserved:
- The user asked whether real proxy requests were being processed and later corrected the agent (translated user correction): "No — I told you earlier to test the proxy, you dummy."
- The agent had only checked compilation, release build, systemd deployment, and `/admin/health` 200.
- Corrected behavior: query `/admin/v1/status`, find active upstream `qdd`, issue a temporary principal API key, send a real Anthropic-compatible `/v1/messages` request through proxy port `52251`, verify response text `pong`, and revoke the temporary key.

Relevant paths from the source work:
- `crates/cc-lb-server/src/app.rs`
- `crates/cc-lb-core/src/lifecycle.rs`
- `crates/cc-lb-admin/web/vite.config.ts`
- `crates/cc-lb-admin/web/src/lib/api.ts`
- `crates/cc-lb-admin/web/src/routes/upstreams.tsx`
- `crates/cc-lb-admin/web/src/routes/principals.tsx`
