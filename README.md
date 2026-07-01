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

## Plugin authors

Plugins are wasm modules authored against `cc-lb-pdk-wasmtime` (re-exports `#[plugin]` + `#[handler]` proc-macros) with rkyv wire types from `cc-lb-plugin-types`. The runtime side is `cc-lb-runtime-wasmtime`, which compiles each upload via wasmtime 46 + a `PoolingAllocationConfig`, validates imports + per-hook BLAKE3 schema fingerprints at load time, and dispatches every call against a fresh `Store` by default (pure mode).

The three hooks a plugin may implement:

- **filter** — return a `FilterResponse` deciding which upstream candidates to keep.
- **shape** — transform the incoming request into an upstream-bound `ShapedRequest`. Shape plugins may also expose `cc_lb_normalize_error` for upstream-error rewriting.
- **observe** — receive lifecycle events; side-effect only.

Only `cc-lb-plugin-api` is published to crates.io. The wasmtime PDK + runtime + plugin-types crates ship in-tree only; see [release-plz.toml](./release-plz.toml) for the publish policy. Author guide: [docs/plugin-author-guide.md](./docs/plugin-author-guide.md). Runtime design: [docs/rfc/0001-plugin-runtime-vnext.md](./docs/rfc/0001-plugin-runtime-vnext.md).

Upload flow: build the plugin to `wasm32-unknown-unknown`, then POST the artifact + `slot_kind=filter|shape|observe` to `POST /admin/v1/plugins/wasm`. The host runs `inspect_wasm`, persists the SHA-256 + the 32-byte schema hash, and triggers a dynamic-view rebind.
