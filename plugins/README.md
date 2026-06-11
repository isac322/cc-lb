# Plugins

This directory contains WebAssembly plugins for cc-lb, built against the cc-lb plugin handshake protocol.

For the full plugin author reference (PDK macros, the seven wire functions, identity / handshake / self-check contracts, registration API, lifecycle, and limits), see **[docs/plugin-author-guide.md](../docs/plugin-author-guide.md)**.

## Directory layout

Plugins are grouped by category. Each plugin is its own crate excluded from the workspace's default build; it targets `wasm32-unknown-unknown` or `wasm32-wasip1`.

```
plugins/
├── router/
│   └── cache-aware/        # cache-aware router implementation
├── dialect/
├── signer/
├── observability/
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

## Reference Implementation: Cache-Aware Filter

The `cache-aware` plugin serves as the canonical reference implementation for routing and replaces the removed legacy router plugin.

This plugin implements the wire v3 filter contract. It does not select a final upstream candidate itself. Instead, it filters the list of available candidates based on predicted prompt-cache warmth and returns kept upstream IDs with optional candidate-level reasons.

### Wire v3 Filter Contract

A filter plugin must export the `filter` function. The host calls this function with a serialized `FilterRequest` containing:

- `request_id`: A unique identifier for the request.
- `headers`: The request headers.
- `method`, `path`, `query`, `body_base64`: The HTTP request details.
- `principal`: The authenticated principal.
- `candidates`: The list of available upstream candidates.

The plugin must return a `FilterResponse` containing:

- `kept_upstream_ids`: The IDs of candidates that survived the filter.
- `reason`: A human-readable summary for the filtering decision.
- `per_candidate_reasons`: Optional candidate-level reasons. Each item includes `upstream_id`, `kept`, and `reason`.

The host keeps only `kept_upstream_ids` before running terminal selection. If a plugin fails, the host defaults to accepting all candidates.
