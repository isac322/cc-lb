# Plugins

This directory contains WebAssembly plugins for cc-lb, built against the cc-lb plugin handshake protocol.

For the full plugin author reference (PDK macros, the seven wire functions, identity / handshake / self-check contracts, registration API, lifecycle, and limits), see **[docs/plugin-author-guide.md](../docs/plugin-author-guide.md)**.

## Directory layout

Plugins are grouped by category. Each plugin is its own workspace crate, targets `wasm32-unknown-unknown` or `wasm32-wasip1`, and depends on `cc-lb-pdk-wasmtime` for its guest runtime API.

```
plugins/
├── router/
│   └── cache-aware/        # cache-aware router implementation
├── dialect/
├── signer/
└── README.md (this file)
```

Plugin crate names follow `cc-lb-<category>-<name>` (for example `cc-lb-router-cache-aware`).

## Quick build + register

```bash
# Build
rustup target add wasm32-unknown-unknown
cargo build -p cc-lb-router-cache-aware --target wasm32-unknown-unknown --release

# Register against a running cc-lb
curl -X POST http://127.0.0.1:9091/admin/plugins \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F bytes=@target/wasm32-unknown-unknown/release/cc_lb_router_cache_aware.wasm \
  -F name=cache-aware \
  -F original_filename=cc_lb_router_cache_aware.wasm
```

The host validates identity, handshake, and self-check before the record is persisted. To then route traffic through the plugin, attach it to a principal's chain via the admin API documented in [docs/runtime-management.md](../docs/runtime-management.md).
