# Memory Allocation Root Fixes Proxy E2E QA Scenario

Goal: prove that the proxy path correctly handles prompt-cache hit/miss routing, Wasmtime filter/shape plugin execution (including header stripping, signer credential override, origin mismatch fail-open, and filter fail-closed), and proxy body limits/compression (including non-streaming/SSE byte identity, gzip/br/zstd decoding, body-cap 413 pre-collection, and per-path caps).

This is an isolated in-process proxy-path scenario. It uses temporary SQLite main storage, a fresh disposable SQLite database, an ephemeral local fake Anthropic listener, and a temporary managed API key. It does not use production storage, credentials, services, Postgres, a paid provider, or the real Anthropic API.

## 0. Environment and preconditions

- Run from the cc-lb workspace root with the repository Rust toolchain and Cargo.
- Build with `--no-default-features --features sqlite`.
- Save raw command output under `/tmp`; retain summaries only in task evidence.
- Do not restart or mutate a service and do not touch the Postgres container.
- No fixed port is required. The fake upstream binds `127.0.0.1:0`.

The `proxy-e2e-qa` `SKILL.md` has no scenario index table, so this scenario does not require or add an index row.

## 1. Deterministic scenario steps

### C2, Prompt-Cache Hit/Miss Routing and Token Accounting
- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **Steps:**
  1. Send a non-streaming request with a cacheable body containing the Anthropic `cache_control` breakpoint.
  2. Verify the first request is routed to the `cache-miss` upstream and reports `usage.cache_creation_input_tokens: 128` and zero cache-read tokens.
  3. Send the identical request again.
  4. Verify the second request is routed to the `cache-hit` upstream and reports `usage.cache_read_input_tokens: 128`.
  5. Send a streaming request with `stream: true` and the same cacheable body.
  6. Verify the streaming request returns HTTP 200, is routed to `cache-hit`, and the client-visible SSE bytes are byte-identical to the fixture stream.
  7. Verify `/admin/usage` records at least 128 cache-read input tokens.

### C3, Wasmtime Filter/Shape Plugin Behavior
- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **Steps:**
  1. Upload and bind `shape-strip.wasm` in the `shape` slot for the principal. The plugin attempts to inject spoofed values for `connection`, `authorization`, `x-api-key`, and `x-anthropic-qa`.
  2. Send a request through the proxy.
  3. Verify the captured request reaches the selected fixture origin with `x-qa-keep: shaped` and none of `connection`, `x-api-key`, or `x-anthropic-qa`.
  4. Verify the captured `authorization` header equals the genuine signer-issued OAuth credential, never the plugin's spoofed value. This proves the trusted signer overrides the plugin's attempted credential injection.
  5. Replace the shape entry with `shape-origin-mismatch.wasm` which returns a foreign origin.
  6. Send a request through the proxy.
  7. Verify the client still receives the raw pass-through response from the selected upstream, proving the origin-policy rejection fails open to raw pass-through.
  8. Bind `filter-reject-all.wasm` in the `filter` slot.
  9. Send a request through the proxy.
  10. Verify the client receives HTTP 503 with `route_no_upstream_after_filter`, proving the filter fails closed.

### C5, Proxy Body Limits, Non-Streaming, SSE, and Compressed Streams
- **Source and Context:** Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes`.
- **Steps:**
  1. Send a non-streaming request and verify the response is byte-identical to the fixture JSON.
  2. Send an identity SSE streaming request and verify the output bytes exactly equal `identity.sse` and headers identify `text/event-stream`.
  3. Send streaming requests with `gzip`, `br`, and `zstd` encodings.
  4. Verify each raw output is byte-identical to its compressed fixture and retains its exact `Content-Encoding`.
  5. Verify `/admin/usage` observes at least 24 output tokens, proving compressed streams are decoded for usage accounting without corrupting client-visible bytes.
  6. Send requests with oversize bodies (exceeding the 1024-byte cap) using both `Content-Length` and `transfer-encoding: chunked`.
  7. Verify both requests are rejected before collection with HTTP 413 and a `body_too_large` error.
  8. Send a 2048-byte request to `/v1/messages` and `/v1/files`.
  9. Verify the `/v1/messages` request is rejected with HTTP 413, while the `/v1/files` request is accepted (using the 4096-byte files cap), proving per-path body caps.

## 2. Assertion verdicts

| Assertion | Result | Evidence boundary |
|---|---|---|
| C2.1 Non-streaming miss then hit | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C2.2 Streaming cache hit | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C3.1 Shape output strips and overrides | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C3.2 Origin-policy fail-open | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C3.3 Filter fail-closed | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.1 Non-streaming byte-identity | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.2 Identity SSE stream byte-identity | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.3 Compressed SSE decoding | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.4 Oversize body pre-collection rejection | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
| C5.5 Per-path body caps | PASS | Verified live 2026-07-13 via F5, see plan `memory-allocation-root-fixes` |
