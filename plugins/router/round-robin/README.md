# Round-Robin Router Plugin

A reference router plugin for cc-lb that distributes requests evenly across upstream candidates in cyclic order. Also serves as the canonical example of a cc-lb-pdk plugin: it exercises `route`, `shape`, and `normalize_error` wire functions.

For the protocol-level reference (PDK macros, lifecycle, wire types, registration, limits, anti-patterns), see [docs/plugin-author-guide.md](../../../docs/plugin-author-guide.md).

## Algorithm

The router maintains a process-local `AtomicUsize` counter. For each `route` call, the selected upstream index is `counter.fetch_add(1, Relaxed) % candidates.len()`, returning the corresponding `upstream_id`. If `candidates` is empty, the response carries `upstream_id = None` and the host falls back to `RouteFn`'s `FallbackPolicy::UseDefault`. The counter is per cc-lb process; on restart it resets to zero. The `shape` handler builds the upstream URL from `UpstreamWire::AnthropicDirect` (canonical Anthropic base) or `UpstreamWire::AnthropicDirect.base_url`, and `normalize_error` returns `body_base64 = None` so the host passes upstream errors through unchanged.

## Build

```bash
rustup target add wasm32-unknown-unknown
cargo build -p cc-lb-router-round-robin --target wasm32-unknown-unknown --release
```

The wasm artifact lands at `target/wasm32-unknown-unknown/release/cc_lb_router_round_robin.wasm`. `wasm32-wasip1` also works if you prefer WASI.

## Register

```bash
curl -X POST http://127.0.0.1:9091/admin/plugins \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F bytes=@target/wasm32-unknown-unknown/release/cc_lb_router_round_robin.wasm \
  -F name=round-robin \
  -F original_filename=cc_lb_router_round_robin.wasm
```

The host validates identity, runs the handshake (negotiates `route@1`, `shape@1`, `normalize_error@1`), runs self-check (wire types round-trip), then persists the record. The response body is the `PluginRegistryRecord`. To make the plugin actually serve traffic, attach it to a principal's router slot via the existing admin API; see [docs/runtime-management.md](../../../docs/runtime-management.md).

## Wire surface implemented

- `route` — picks the next upstream by atomic counter; returns `RouteResponse { upstream_id, dialect: SelfReferenced, upstream: AnthropicDirect }`.
- `shape` — concatenates the upstream base URL with the downstream request path and query, forwarding the original method, headers, and body. Returns `ShapeResponse`.
- `normalize_error` — returns `NormalizeErrorResponse { body_base64: None }`, opting into host pass-through on upstream errors.

See [src/lib.rs](src/lib.rs) for the full implementation; the unit tests in the same file double as concrete examples of the wire types in use.
