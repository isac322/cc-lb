# cc-lb-capture

Local-only per-request capture of routing-formula inputs and outputs into a separate SQLite database.

## What it does

When enabled (via the `[capture]` config section and the `capture` cargo feature), this crate records the complete set of inputs your routing formula saw for each request — including each backend's quota, rate-limit, cache and warning state, the request's cache breakpoints and their BLAKE3 prefix fingerprints, the pricing, and the computed scores — together with what actually happened (which backend was used, the real cached/created/read token counts, and the outcome). The data is stored in an isolated SQLite database, separate from the main `storage.sqlite`, with indexed columns and a full JSON snapshot per request for later analysis and replay.

## Accepted Limitation

**Local-only means captured data represents only traffic through the local instance, not the full production fleet.** If you run multiple cc-lb instances, each captures only its own traffic. This is by design: the capture database is isolated to the local operator's data and is not intended for fleet-wide aggregation.

## Accepted Risk

**BLAKE3 prefix-hash fingerprints and per-org/seat/tier quota data are derived-but-sensitive.** These fields are not raw prompt text, but they do encode information about your request patterns and subscription state. This risk is acceptable given the local-only scope and the fact that the operator owns the data, but the captured database should not be treated as fully anonymous or suitable for sharing without careful review.

## Configuration

Enable capture in your `cc-lb.toml`:

```toml
[capture]
enabled = true
path = "capture.sqlite"  # relative to data_dir
channel_capacity = 1024
retention_max_rows = 100000
```

Capture is **off by default** and requires both the `capture` cargo feature and an explicit `enabled = true` in the config.

## Zero-cost when disabled

When the `capture` feature is not enabled, this crate is not compiled into the binary at all. The default build has zero overhead.
