# cc-lb

cc-lb is a Rust workspace for an Anthropic-compatible multi-principal reverse proxy with a static `cc-lb` musl binary, Extism-based plugin boundaries, and a layered server/runtime split. Build it with `cargo build --workspace` or `cargo build --release --target x86_64-unknown-linux-musl -p cc-lb-server`. The implementation plan lives in [.omo/plans/anthropic-proxy.md](./.omo/plans/anthropic-proxy.md).

## Quick start

1. Set the admin bootstrap token: `export CC_LB_BOOTSTRAP_ADMIN_TOKEN=$(uuidgen)`
2. Optionally seed initial state via `bootstrap.toml` in your data_dir
3. Run `cc-lb-server serve --config cc-lb.toml`
4. Open the dashboard at `http://localhost:<admin_port>/`
5. Add upstreams, principals, and plugin chains via the dashboard

See [docs/runtime-management.md](docs/runtime-management.md) for the full API and architecture.

## Operator Guides

- [Upstream warm-up](./docs/upstream-warmup.md): keep Anthropic 5h windows ticking

## Plugin authors

Plugins are Extism WASM modules built against the `cc-lb-pdk` proc-macro crate
and the shared types in `cc-lb-plugin-wire` / `cc-lb-plugin-api`. The plugin
trio is the only public surface of this workspace published to crates.io;
all other crates are internal. See [release-plz.toml](./release-plz.toml) for
the versioning policy.

### Compatibility matrix

Four independent integer version axes govern host ↔ plugin compatibility,
each tracked separately from the Rust SemVer of the plugin trio. Bumping the
trio's Rust version does **not** automatically imply an ABI break; bumping
any of the four integers below does.

| Axis | Constant | Current | Source of truth |
|---|---|---|---|
| WASM custom-section envelope | `ABI_ENVELOPE_VERSION` | `1` | [crates/cc-lb-pdk/src/codegen/section.rs](./crates/cc-lb-pdk/src/codegen/section.rs) |
| Handshake schema | `HANDSHAKE_SCHEMA_VERSION_V1` | `1` | [crates/cc-lb-plugin-wire/src/handshake/mod.rs](./crates/cc-lb-plugin-wire/src/handshake/mod.rs) |
| Plugin call wire | `WIRE_VERSION_V{1,2,3}` | `1`, `2`, `3` | [crates/cc-lb-runtime-extism/src/lib.rs](./crates/cc-lb-runtime-extism/src/lib.rs) |
| Built-in cache-affinity wire | `BUILTIN_CACHE_AFFINITY_WIRE_VERSION` | `3` | [crates/cc-lb-plugin-api/src/lib.rs](./crates/cc-lb-plugin-api/src/lib.rs) |

| cc-lb-server | Plugin trio | ABI envelope | Handshake schema | Wire versions accepted | cache-affinity wire |
|---|---|---|---|---|---|
| 0.1.x | 0.1.x | 1 | 1 | 1, 2, 3 | 3 |

Plugins compiled against `cc-lb-pdk` 0.1 emit ABI envelope `1` and handshake
schema `1`. The host (via `cc-lb-runtime-extism`) accepts plugin call wire
versions 1 – 3, with V1 as fallback when the plugin manifest omits
`wire_version`. Drop legacy wire support only by bumping the host's major
version and updating this matrix.

