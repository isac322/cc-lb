<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/brand/readme/cc-lb-hero-dark.svg">
    <img alt="cc-lb" src="assets/brand/readme/cc-lb-hero-light.svg" width="100%">
  </picture>
</p>

# cc-lb

cc-lb is a Rust workspace for an Anthropic-compatible multi-principal reverse proxy with a static `cc-lb` musl binary, a wasmtime + rkyv plugin runtime, and a layered server/runtime split. Build it with `cargo build --workspace` or `cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server`.

## Quick start

1. Set the secrets referenced by the config: `export CC_LB_MASTER_KEY=$(openssl rand -hex 32)`, `export CC_LB_ADMIN_TOKEN=$(openssl rand -hex 24)`, and your upstream credential such as `ANTHROPIC_UPSTREAM_API_KEY`.
2. Configure an explicit admin authentication provider in `cc-lb.toml`:

   ```toml
   [[admin.auth.providers]]
   kind = "static_token"
   id = "local"
   token_env = "CC_LB_ADMIN_TOKEN"
   ```

3. Run `cc-lb-server serve --config cc-lb.toml`.
4. Create the database-backed upstream with `POST /admin/v1/upstreams`.
5. Create the database-backed principal with `POST /admin/v1/principals`, then issue its proxy key with `POST /admin/v1/principals/{id}/keys`.
6. Open the dashboard at `http://localhost:<admin_port>/` to manage upstreams, principals, and plugin chains.

See [docs/runtime-management.md](docs/runtime-management.md) for request bodies, the full API, and the architecture.

## Operator Guides

- [Upstream warm-up](./docs/upstream-warmup.md): keep Anthropic 5h windows ticking
- [Distributed Scheduler](./docs/scheduler.md): topology, retry classes, metrics, and runbook

### Stream diagnostics

HTTP 200 means response headers were sent, not that the response body completed. Use `cc_lb_stream_terminations_total{outcome,cause}` to distinguish `completed`, `upstream_error`, `proxy_error`, and `client_cancelled`. Causes are bounded categories such as `unexpected_eof`, `h2_cancel`, and `affinity_error`; request IDs and error messages are never metric labels.

With OTLP enabled, `proxy.response_stream` spans remain open until the response body ends or is dropped. Transport failures record a bounded, redacted `error.chain` and, when available, `error.io.kind`, `error.io.os_error`, and `error.h2.reason`. These describe the failed response path, not necessarily which party caused the disconnect.

`cc_lb_requests_started_total` counts proxied requests when proxy response headers are sent, including locally generated responses. `cc_lb_request_headers_duration_seconds` measures request-start-to-header latency for those responses. Neither metric measures terminal completion.

The `proxy.handle` span stays open through terminal lifecycle completion and records:

- `cc_lb.request.retry_count` and final-attempt `cc_lb.upstream.bulkhead_wait_ms`, `cc_lb.upstream.dns_ms`, `cc_lb.upstream.connect_ms`, `cc_lb.upstream.connection_reused`, `cc_lb.upstream.shape_ms`, `cc_lb.upstream.sign_ms`, and `cc_lb.upstream.ttfb_ms`. Upstream timings are not totals across retries.
- `cc_lb.response.first_body_chunk_ms` for transport arrival, and `cc_lb.response.first_content_delta_ms` for consumer TTFT. A body chunk or metadata-only SSE event need not contain a content delta.
- Observed scalar token fields `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.usage.cache_creation_input_tokens`, and `gen_ai.usage.cache_read_input_tokens`. `cc_lb.usage.completeness` is `complete` only for complete usage without a terminal error; otherwise it is `partial`. Usage attributes contain no raw bodies and make no cost claims.
- Terminal `cc_lb.request.outcome` (`success`, `client_cancelled`, `error`, or `timeout`), `cc_lb.request.duration_ms`, `cc_lb.request.finalize_ms`, and `cc_lb.request.unaccounted_ms`.

Admin HTTP requests use a separate `admin.request` span with bounded matched-route templates, response status/error categories, and optional request-ID correlation, not raw paths, query strings, authentication headers, credentials, or bodies. Request IDs are span attributes, never metric labels. Instrumentation creates no per-chunk child spans and does not wait for telemetry export on the request path; this is not a zero-overhead guarantee. See the [Prometheus and OTLP contract](./docs/request-latency-unaccounted-remediation.md#13-prometheus-and-otlp-contract) for timing boundaries and residual computation.

The `stream latency breakdown` log runs on a dedicated worker with a fixed 4,096-entry queue, so emitting it cannot delay the final downstream body bytes. Queue overflow and closure increment `cc_lb_dropped_events_total` with bounded `stream_completion_observer_*` reasons; panic accounting and recovery apply to unwind builds, while the release profile aborts native Rust panics. Mandatory lifecycle events, accounting, affinity decisions, and stream termination metrics remain inline. Graceful server shutdown prioritizes durable writer flushes, then waits up to the existing cleanup budget for remaining stream completion logs before telemetry teardown; a timeout increments `stream_completion_observer_shutdown_timeout` and leaves the worker detached.

## Plugin authors

Plugins are wasm modules authored with the published `cc-lb-pdk-wasmtime`; bundled guest plugins depend only on that PDK, which re-exports the guest-facing wire API from `cc-lb-plugin-wire`. The wire-only `cc-lb-runtime-wasmtime` runtime compiles each upload via wasmtime 46, validates imports, required plugin and hook metadata, per-hook wire versions, per-hook BLAKE3 layout fingerprints, and an upload-time runtime probe before dispatching calls. Local execution defaults to on-demand allocation with fresh per-call `Store`s, per-store `StoreLimits`, and a process-wide store budget; operators can opt into Wasmtime pooling through `[runtime.wasmtime] allocation_strategy = "pooling"`.

The slots a plugin may target:

- **filter**: return a `FilterResponse` deciding which upstream candidates to keep. The V1 filter request exposes the requested `service_tier`.
- **shape**: unified slot that transforms the incoming request into an upstream-bound `ShapedRequest`, and transforms downstream responses (both buffered and SSE). A shape plugin must implement request shaping, buffered response transform, and SSE event transform, with explicit no-op handlers for unneeded response hooks.

The published crates.io set is exactly 5 crates: `cc-lb-plugin-wire`, `cc-lb-pdk-wasmtime-macros`, `cc-lb-pdk-wasmtime`, `cc-lb-runtime-wasmtime`, and `cc-lb-plugin-conformance`. The six unpublished domain crates are `cc-lb-domain`, `cc-lb-upstream`, `cc-lb-routing`, `cc-lb-quota`, `cc-lb-request-log`, and `cc-lb-lifecycle`. Start with [docs/plugin-author-guide.md](./docs/plugin-author-guide.md), then use the crate READMEs for focused API notes: [`cc-lb-plugin-wire`](./crates/cc-lb-plugin-wire/README.md), [`cc-lb-pdk-wasmtime`](./crates/cc-lb-pdk-wasmtime/README.md), [`cc-lb-pdk-wasmtime-macros`](./crates/cc-lb-pdk-wasmtime-macros/README.md), and [`cc-lb-plugin-conformance`](./crates/cc-lb-plugin-conformance/README.md). Runtime design background is in the historical [RFC-0001](./docs/rfc/0001-plugin-runtime-vnext.md).

Upload flow: build the plugin to `wasm32-unknown-unknown`, then POST the artifact to `POST /admin/v1/plugins/wasm`. The host runs `admit_wasm`, derives the supported slots from the exported hooks, persists the SHA-256, plugin metadata name/version, plugin description, plugin usage, and per-hook metadata, then triggers a dynamic-view rebind. Re-uploading the same plugin name and SHA is a successful noop; uploading the same metadata name with a higher semantic version replaces the existing registry row in place; same/lower version replacements require an explicit confirmation retry. Registry reference counts include both plugin-chain bindings and upstream warmup dialect plugin bindings.
