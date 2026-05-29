# Round-Robin Router Plugin

A simple round-robin load balancing router plugin for cc-lb. Distributes requests evenly across available upstreams in a cyclic manner.

## Purpose

The round-robin router plugin selects upstreams by cycling through the list of available candidates in order. This provides basic load distribution across multiple upstreams without considering latency, error rates, or other performance metrics.

## Algorithm

The router maintains an atomic counter that increments with each routing decision. The selected upstream is determined by `counter % candidates.len()`, ensuring deterministic cyclic distribution across all available candidates.

## Build

Build for `wasm32-wasip1` target:

```bash
cargo build --manifest-path plugins/router/round-robin/Cargo.toml --target wasm32-wasip1 --release
```

The compiled WASM binary is located at `target/wasm32-wasip1/release/cc_lb_router_round_robin.wasm`.

## Registration

To register the round-robin router in cc-lb:

1. Place the compiled WASM binary in the plugins directory
2. Configure it in your cc-lb config under the router chain:
   ```toml
   [[router_chain]]
   name = "round-robin-chain"
   plugins = [
       { path = "plugins/cc_lb_router_round_robin.wasm" }
   ]
   ```

The plugin will be loaded via Extism and invoked for each request, receiving the list of available upstream candidates sorted by upstream ID.

## Input

- `RequestContext`: Information about the incoming request (principal, request kind, etc.)
- `candidates`: Array of `UpstreamCandidate` structs, sorted by `upstream_id` in ascending UUID byte order

## Output

Returns a `RouteDecision` containing the selected `upstream_id` (or `None` if no candidates are available).
