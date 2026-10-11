# reset-priority-wasmtime

A cc-lb filter plugin that prioritizes expiring subscription quota and keeps
useful cache affinity among accounts with comparable reset urgency.

Built against `cc-lb-pdk-wasmtime`; **filter hook wire version 2** — the
contract that carries subscription quota snapshots and the host routing clock.
V1 filter plugins keep loading unchanged; a plugin that needs quotas or the
clock declares `wire = 2` and needs a host that supports it.

## Routing priorities

The plugin keeps at most one usable OAuth subscription and passes API-key
candidates through to the built-in `subscription-preference` filter.

1. **Eligibility:** block fresh rejected/exhausted five-hour, shared weekly,
   and relevant model weekly windows. A stale rejection blocks until its known
   reset passes; without a reset, it stays blocked. Prefer accounts with at
   least 0.5% usable weekly quota when available, using the smaller remaining
   fraction of shared and model weekly windows. This cutoff is a heuristic,
   not an estimate of the next request's size.
2. **Weekly urgency:** compute expiry urgency for the shared and relevant model
   weekly windows and use their maximum. Only candidates within the 10%
   multiplicative band of the best weekly urgency stay eligible for affinity.
   Five-hour expiry urgency does not participate in this ranking.
3. **Cache hits:** inside that weekly band, prefer the largest predicted cache
   read. Keep a warm account even at 99% five-hour utilization. It can consume
   the rest of its window; at exhaustion or provider rejection it is blocked.
4. **Cold sessions:** with no predicted hit in the weekly band, prefer accounts
   below 90% five-hour utilization. If none have that headroom, keep the usable
   tight accounts eligible. The headroom preference never overrides weekly
   urgency or a warm cache.
5. **Stable ties:** use an FNV-1a hash of `thread_id || upstream_id`, then upstream
   ID, so input ordering does not change ownership. With no conversation ID,
   use weekly urgency then upstream ID.

Weekly expiry urgency is a libm-free heuristic:

```text
remaining_fraction / clamp(time_to_reset_fraction, 0.01, 1.0)^2
```

`remaining_fraction = 1 - utilization`, and
`time_to_reset_fraction = secs_to_reset / 604800`. The minimum denominator is
`0.0001`, so maximum urgency is `10,000`. This is distinct from the host's
burn-pace pressure. Urgency above 1 means remaining quota exceeds the squared
time fraction, not necessarily the linear time share.

For example, with comparable weekly urgency, an existing conversation with a
warm cache stays on account A at 99% five-hour usage even when account B is at
10%. A new session with no matching cache starts on B. Once A is exhausted,
requests use B. If B's weekly quota is substantially more urgent, weekly
priority can move the conversation earlier. Cache affinity is a preference,
not a persistent session binding.

When every subscription is blocked, only API-key candidates pass through. With
no fallback, the host answers `route_no_upstream_after_filter`.

## The clock — read this before writing another quota plugin

Wire V2 supplies `routing_now_unix_secs`: the host's injected clock, sampled for
this routing pass. That is the clock this plugin counts down from, so a reset
600 seconds away reads as 600 seconds even when the newest quota observation is
an hour old. On an idle account that difference is the whole deadline term.

The obvious-looking alternatives are traps. `UpstreamCandidate.observed_at_unix_secs`
is the host's **rate-limit** cache time, and the host sets it to `0` unless a
rate-limit snapshot exists — which is the normal state of a healthy pool
(`build_candidates`, `lifecycle.rs`). A plugin that treats that field as "now"
therefore gets `now = 0`: every `resets_at - now` turns into an absolute epoch
value, every time fraction clamps to `1.0`, and the deadline term vanishes
silently, degrading the rule to "most quota left wins" with no error anywhere.
V1 hosts have no routing clock at all, so a V1 plugin can only approximate one.

The quota windows carry their own `observed_at_unix_millis`. This plugin keeps
the maximum of those (and of the candidate stamp) as a **floor** under the host
clock, mirroring the built-in filter's `candidate_estimated_now`, so a synthetic
request or a host that reports no clock still cannot make the deadline term
disappear. `estimated_now` does that;
`the_clock_comes_from_the_quota_snapshots_not_the_candidate_stamp` pins the
floor, and `routing_clock_supersedes_stale_observation_stamps` pins that the
host clock wins over an hour-old snapshot.

## Requirements

This plugin declares filter `wire = 2` and reads the `subscription_quotas`
snapshots on each candidate (`window`, `state`, `utilization`,
`resets_at_unix_secs`, `observed_at_unix_millis`, `status`, `disabled_reason`)
plus `routing_now_unix_secs`. V2 was added alongside V1, which stays
byte-identical: existing V1 filter artifacts still load and keep their
fingerprints, but they cannot see quotas or a routing clock.

The filter admission fingerprint covers both the candidate and the nested
quota-window descriptors. A host and plugin built from incompatible layouts
are rejected at admission, rather than silently passing candidates through
when guest decoding fails. Older hosts lack this protection.

Build the host and all attached filter plugins from the same source revision,
then upload matching artifacts. A V2 guest must be rebuilt against the V2 wire;
a V1-only host on the other side of the wire rejects it. Shape/transform
fingerprints are unchanged. The plugin is a workspace member. These source changes do not update a running service.

## Build

The plugin follows the workspace conventions and is covered by workspace tests.
Build its Wasm artifact separately from the server.

```bash
rustup target add wasm32-unknown-unknown
cargo build -p reset-priority-wasmtime \
  --target wasm32-unknown-unknown --release
# target/wasm32-unknown-unknown/release/reset_priority_wasmtime.wasm
```

## Tests

```bash
cargo test -p reset-priority-wasmtime   # decision-table tests + invariant sweep
cargo test -p cc-lb-runtime-wasmtime                                          # host admission + rkyv boundary
```

`src/algorithm.rs` holds the decision core as pure `no_std` Rust over plain
structs; `src/lib.rs` is only the wasm ABI adapter. That split is what makes the
routing rules testable without a host or a wasm toolchain.

## Attach

Upload the artifact, then insert it on the principal's **router** chain *before*
the built-in `subscription-preference` entry. Order matters: a filter can only
drop candidates, so a plugin that runs after the built-in sees a single
candidate and can never compare reset times.

```bash
ADMIN=http://127.0.0.1:9090
TOKEN="$CC_LB_ADMIN_TOKEN"
PRINCIPAL=<principal uuid>

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" \
  -F "bytes=@target/wasm32-unknown-unknown/release/reset_priority_wasmtime.wasm" \
  -F "original_filename=reset-priority.wasm" \
  "$ADMIN/admin/v1/plugins/wasm"

curl -fsS -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"slot":"router","wasm_registry_id":"<registry id>","position":{"before":"<built-in entry id>"}}' \
  "$ADMIN/admin/v1/principals/$PRINCIPAL/plugin-chain"
```

`~/ai/cc-lb/PLUGIN-DEPLOY.md` has the full playbook, including rollback.
