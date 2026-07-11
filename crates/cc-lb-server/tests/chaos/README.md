# Chaos Tests

The `chaos` cargo feature enables proxy-only failure injection for resilience tests. It is disabled by default and must never be enabled in production builds.

Environment variables:

- `CC_LB_CHAOS_LATENCY_MS`: fixed latency in milliseconds before returning the response head. Default: `0`.
- `CC_LB_CHAOS_DROP_PCT`: request drop probability from `0` to `100`; dropped requests return a synthetic `502`. Default: `0`.
- `CC_LB_CHAOS_RST_AFTER_BYTES`: abort non-SSE response bodies after this many bytes. `0` disables it.
- `CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS`: cleanly end SSE responses after this many `data:` events. `0` disables it.

Run individual tests with:

```bash
cargo test --features chaos -p cc-lb-server --test integration latency_injection::
cargo test --features chaos -p cc-lb-server --test integration drop_pct_50::
cargo test --features chaos -p cc-lb-server --test integration truncate_mid_stream::
cargo test --features chaos -p cc-lb-server --test integration rst_after_bytes::
```

The default feature set does not compile or install `ChaosLayer`.
