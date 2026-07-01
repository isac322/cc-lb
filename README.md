# cc-lb

cc-lb is a Rust workspace for an Anthropic-compatible multi-principal reverse proxy with a static `cc-lb` musl binary, a wasmtime + rkyv plugin runtime, and a layered server/runtime split. Build it with `cargo build --workspace` or `cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server`. The implementation plan lives in [.omo/plans/anthropic-proxy.md](./.omo/plans/anthropic-proxy.md).

## Quick start

cc-lb no longer reads a TOML config file. Boot-critical values come from
environment variables; everything else is edited from the dashboard.

1. Export the boot env contract:
   ```
   export CC_LB_LISTENER__PROXY_ADDR=127.0.0.1:8080
   export CC_LB_LISTENER__ADMIN_ADDR=127.0.0.1:8081
   export CC_LB_LISTENER__METRICS_ADDR=127.0.0.1:9090
   export CC_LB_STORAGE__KIND=sqlite
   export CC_LB_STORAGE__PATH=$HOME/.local/share/cc-lb/storage.sqlite
   export CC_LB_DATA_DIR=$HOME/.local/share/cc-lb
   export CC_LB_MASTER_KEY=$(openssl rand -hex 32)
   export CC_LB_BOOTSTRAP_ADMIN_TOKEN=$(uuidgen)
   ```
2. Run `cc-lb-server serve` — the bootstrap admin principal is auto-seeded
   from `CC_LB_BOOTSTRAP_ADMIN_TOKEN` on first boot. No file-based seeding.
3. Open the dashboard at `http://localhost:8081/` and use the **Config** page
   to edit every runtime setting (timeouts, body caps, circuit breaker, etc.).
4. Add upstreams, principals, and plugin chains via the dashboard or the
   `/admin/v1/*` REST API using the bootstrap admin bearer token.

See [docs/runtime-management.md](docs/runtime-management.md) for the full API and architecture.

## Operator Guides

- [Upstream warm-up](./docs/upstream-warmup.md): keep Anthropic 5h windows ticking
- [Distributed Scheduler](./docs/scheduler.md): topology, retry classes, metrics, and runbook

## Plugin authors

Plugins are wasm modules authored with the published `cc-lb-pdk-wasmtime`; bundled guest plugins depend only on that PDK, which re-exports the guest-facing wire API from `cc-lb-plugin-wire`. The wire-only `cc-lb-runtime-wasmtime` runtime compiles each upload via wasmtime 46, validates imports, required plugin and hook metadata, per-hook wire versions, per-hook BLAKE3 layout fingerprints, and an upload-time runtime probe before dispatching calls. Local execution defaults to on-demand allocation with fresh per-call `Store`s, per-store `StoreLimits`, and a process-wide store budget; operators can opt into Wasmtime pooling through `[runtime.wasmtime] allocation_strategy = "pooling"`.

The slots a plugin may target:

- **filter**: return a `FilterResponse` deciding which upstream candidates to keep. The V1 filter request exposes the requested `service_tier`.
- **shape**: unified slot that transforms the incoming request into an upstream-bound `ShapedRequest`, and transforms downstream responses (both buffered and SSE). A shape plugin must implement request shaping, buffered response transform, and SSE event transform, with explicit no-op handlers for unneeded response hooks.
- **observe**: receive lifecycle events; side-effect only.

The published crates.io set is exactly 5 crates: `cc-lb-plugin-wire`, `cc-lb-pdk-wasmtime-macros`, `cc-lb-pdk-wasmtime`, `cc-lb-runtime-wasmtime`, and `cc-lb-plugin-conformance`. The six unpublished domain crates are `cc-lb-domain`, `cc-lb-upstream`, `cc-lb-routing`, `cc-lb-quota`, `cc-lb-request-log`, and `cc-lb-lifecycle`. Start with [docs/plugin-author-guide.md](./docs/plugin-author-guide.md), then use the crate READMEs for focused API notes: [`cc-lb-plugin-wire`](./crates/cc-lb-plugin-wire/README.md), [`cc-lb-pdk-wasmtime`](./crates/cc-lb-pdk-wasmtime/README.md), [`cc-lb-pdk-wasmtime-macros`](./crates/cc-lb-pdk-wasmtime-macros/README.md), and [`cc-lb-plugin-conformance`](./crates/cc-lb-plugin-conformance/README.md). Runtime design background is in the historical [RFC-0001](./docs/rfc/0001-plugin-runtime-vnext.md).

Upload flow: build the plugin to `wasm32-unknown-unknown`, then POST the artifact + `slot_kind=filter|shape|observe` to `POST /admin/v1/plugins/wasm`. The host runs `admit_wasm`, persists the SHA-256, plugin metadata name/version, plugin description, plugin usage, and per-hook metadata, then triggers a dynamic-view rebind. Re-uploading the same plugin name and SHA is a successful noop; uploading the same metadata name with a higher semantic version replaces the existing registry row in place; same/lower version replacements require an explicit confirmation retry. Registry reference counts include both plugin-chain bindings and upstream warmup dialect plugin bindings.
