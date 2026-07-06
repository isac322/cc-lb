# ADR 0005 — Thread-keyed subscription-preference with warning demotion (salt v8)

- Status: Proposed
- Date: 2026-07-06
- Ships with: pending
- Supersedes: ADR 0004's v7 WRH key source and base-warning classification; ADR 0004's cache-weighted WRH formula remains the within-tier weight.

## Context

ADR 0004 moved cache weighting into `subscription_preference` and keyed WRH on `request_id`. That was safe only if cache locality was already narrowed elsewhere. The active `isac-opencode` router chain now contains only `subscription-preference`, with no `cache_affinity` stage, so `subscription_preference` is again the routing stage responsible for session stickiness.

Read-only SQLite evidence was inspected only at aggregate/quota level, without raw payloads, request bodies, headers, API keys, OAuth tokens, admin tokens, or secret-bearing rows:

- thread `ses_0d9087c8fffesHxGh8CLLdklyP` had 411 request events over 310 requests.
- The selected upstream set contained four OAuth upstreams: `bear-max`, `isac-personal`, `Runbear`, and `bh322yoo-max`.
- `isac-personal` was selected while its 7d base status was `allowed_warning`, utilization was approximately 0.91-0.92, and `surpassed_threshold` was 0.75.
- The same aggregate shape showed overage rejected for `isac-personal`, but the relevant failure was not a clean overage fallback decision; it was a base-window warning being treated as full `KnownBase` health.
- The active principal was `isac-opencode`, `router_terminal_strategy` was `first-pick`, and the active router chain was only `subscription-preference`.

Two root causes follow:

- WRH used `RequestContext.request_id` even when `RequestContext.thread_id` was non-empty, so turns from the same conversation drew independent winners and could scatter across warm-cache candidates.
- `classify_base_snapshot` treated fresh `allowed_warning` the same as fresh `allowed`, and ignored finite `utilization >= surpassed_threshold`, so near-surpassed base windows stayed in `KnownBase` and beat clean peers.

## Decision

Bump the subscription-preference salt from v7 to v8:

```rust
pub const SALT_VERSION: &str = "v8";
pub const RENDEZVOUS_SALT: &str =
    "cc-lb:subscription-preference:v8:thread-keyed-warning-demote:2026-07-06";
```

Within a tier, WRH now hashes the routing key chosen as:

```rust
routing_key = if RequestContext.thread_id is Some(non_empty) {
    thread_id
} else {
    request_id
}
```

`SubscriptionPreferenceTrace.wrh_key_source` records `ThreadId` for the first branch and `RequestId` for the fallback branch. This preserves request-id spread for stateless clients while pinning multi-turn sessions to a stable upstream when the session id exists.

Base-window classification gains a warning-positive signal:

- Fresh `disabled_reason` remains `HardNegative`.
- Fresh `status == "rejected"` remains `HardNegative`.
- Fresh finite `utilization >= 1.0` remains `HardNegative`, even if a status string says `allowed` or `allowed_warning`.
- Fresh `status == "allowed_warning"` becomes `WarningPositive`.
- Fresh `status == "allowed"` with finite `utilization >= finite surpassed_threshold` becomes `WarningPositive`.
- Other fresh positive utilization below 1.0 remains ordinary positive.

`WarningPositive` counts as a positive base signal, so it does not hard-block the candidate. Any warning-positive base window demotes an otherwise complete base candidate from `KnownBase` to `PartialBase`. If no clean `KnownBase` peer exists, a warning-only candidate remains selectable from `PartialBase`.

## Consequences

### Positive

- Multi-turn sessions with non-empty `thread_id` regain deterministic prompt-cache locality inside `subscription_preference`, which matches the active single-filter chain.
- Provider warning signals are no longer treated as full base health. A clean `KnownBase` peer beats an `allowed_warning` or threshold-surpassed candidate.
- Warning-only candidates are still usable when no clean peer exists; this avoids unnecessary API-key fallback or fail-open behavior.
- Trace consumers can distinguish session-pinned draws from request-id fallback draws using `wrh_key_source`, and can attribute distribution shifts to salt `v8`.

### Negative

- v8 changes the WRH key space, so selected upstream distributions will shift immediately after deployment for both threaded and stateless requests.
- Thread-keyed routing can concentrate a long session on one candidate until tier assessment demotes or blocks it. The warning demotion rule is the safety valve for provider-declared near-surpassed base windows.
- `PartialBase` now includes both incomplete evidence and warning-positive complete evidence. Operators must inspect per-candidate quota snapshots to distinguish those causes.

### Neutral

- No new public plugin API is required. Existing `WrhKeySource::ThreadId` and `WrhKeySource::RequestId` already model the trace distinction.
- ADR 0004's cache-weighted effective weight remains unchanged; v8 only changes the key used to draw WRH and the tier assigned to warning-positive base snapshots.

## Alternatives considered

- **Keep v7 request-id keying and re-add `cache_affinity`.** Rejected for this fix because the active production-like chain evidence is subscription-preference only, and restoring a second filter would not address warning snapshots being misclassified as `KnownBase`.
- **Hard-block `allowed_warning`.** Rejected because provider warnings still indicate usable base capacity. The safer interpretation is positive-but-demoted, allowing clean peers to win without dropping the warning candidate when it is the only subscription route.
- **Demote only explicit `allowed_warning`, not threshold crossings.** Rejected because the recorded shape included `surpassed_threshold = 0.75`, and a fresh `allowed` window that crosses that finite threshold is semantically equivalent to a provider warning for routing tier purposes.

## Verification

- `same_thread_id_pins_when_request_ids_vary` covers non-empty `thread_id` pinning and `wrh_key_source = ThreadId`.
- `wrh_key_source_is_thread_id_when_thread_id_present_v8`, `wrh_key_source_falls_back_to_request_id_when_thread_id_absent`, and `wrh_key_source_falls_back_to_request_id_when_thread_id_is_empty_string` cover trace source reporting and fallback.
- `different_request_ids_spread_across_upstreams` keeps request-id distribution behavior for stateless requests.
- `subscription_preference_v8_regression.rs` covers the recorded `isac-personal` warning shape, threshold-crossed `allowed` demotion, and warning-only candidate selection.
