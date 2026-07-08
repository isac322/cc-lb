# cc-lb

cc-lb is a Rust workspace for an Anthropic-compatible multi-principal reverse proxy with a static `cc-lb` musl binary, a wasmtime + rkyv plugin runtime, and a layered server/runtime split. Build it with `cargo build --workspace` or `cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server`. The implementation plan lives in [.omo/plans/anthropic-proxy.md](./.omo/plans/anthropic-proxy.md).

## Quick start

1. Set the admin bootstrap token: `export CC_LB_BOOTSTRAP_ADMIN_TOKEN=$(uuidgen)`
2. Optionally seed initial state via `bootstrap.toml` in your data_dir
3. Run `cc-lb-server serve --config cc-lb.toml`
4. Open the dashboard at `http://localhost:<admin_port>/`
5. Add upstreams, principals, and plugin chains via the dashboard

See [docs/runtime-management.md](docs/runtime-management.md) for the full API and architecture.

## Operator Guides

- [Upstream warm-up](./docs/upstream-warmup.md): keep Anthropic 5h windows ticking
- [Distributed Scheduler](./docs/scheduler.md): topology, retry classes, metrics, and runbook
- [Subscription quota checkpoint cleanup](./docs/runbook/subscription-quota-checkpoint-cleanup.md): offline backfill/drop/VACUUM runbook for reclaiming raw quota-history storage

## Plugin authors

Plugins are wasm modules authored against the published `cc-lb-plugin-api`, `cc-lb-plugin-wire`, `cc-lb-pdk-wasmtime`, and `cc-lb-pdk-wasmtime-macros` crates, with `cc-lb-plugin-conformance` available as a dev-dependency. The runtime side is `cc-lb-runtime-wasmtime`, which compiles each upload via wasmtime 46, validates imports, required plugin and hook metadata, per-hook wire versions, per-hook BLAKE3 layout fingerprints, and an upload-time runtime probe before dispatching calls. Local execution defaults to on-demand allocation with fresh per-call `Store`s, per-store `StoreLimits`, and a process-wide store budget; operators can opt into Wasmtime pooling through `[runtime.wasmtime] allocation_strategy = "pooling"`.

The hooks a plugin may implement:

- **filter** — return a `FilterResponse` deciding which upstream candidates to keep.
- **shape** — transform the incoming request into an upstream-bound `ShapedRequest`.
- **observe** — receive lifecycle events; side-effect only.
- **transform_response** — transform a buffered upstream response before delivery.
- **transform_sse_event** — transform each upstream SSE event before delivery.

The crates.io authoring surface is `cc-lb-plugin-api`, `cc-lb-plugin-wire`, `cc-lb-pdk-wasmtime`, `cc-lb-pdk-wasmtime-macros`, and `cc-lb-plugin-conformance`; host/runtime crates remain in-tree. Start with [docs/plugin-author-guide.md](./docs/plugin-author-guide.md), then use the crate READMEs for focused API notes: [`cc-lb-plugin-wire`](./crates/cc-lb-plugin-wire/README.md), [`cc-lb-pdk-wasmtime`](./crates/cc-lb-pdk-wasmtime/README.md), [`cc-lb-pdk-wasmtime-macros`](./crates/cc-lb-pdk-wasmtime-macros/README.md), and [`cc-lb-plugin-conformance`](./crates/cc-lb-plugin-conformance/README.md). Runtime design background is in the historical [RFC-0001](./docs/rfc/0001-plugin-runtime-vnext.md).

Upload flow: build the plugin to `wasm32-unknown-unknown`, then POST the artifact + `slot_kind=filter|shape|observe|transform_response|transform_sse_event` to `POST /admin/v1/plugins/wasm`. The host runs `admit_wasm`, persists the SHA-256, plugin description, plugin usage, and per-hook metadata, then triggers a dynamic-view rebind.
