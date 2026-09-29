# cc-lb Observability Runbook

This document describes the metrics and troubleshooting procedures for observability in `cc-lb`, including the prompt cache shadow system and the subscription-preference routing pipeline.

## Metric: cc_lb_cache_hit_total

- **Type**: Counter
- **Labels**: `upstream`, `model`
- **Label Cardinality Bounds**: Low, typically under 50. It's bounded by the number of configured upstreams multiplied by the number of active models.

### Interpretation

This metric counts the total number of cache hit responses.
High values are expected and indicate successful prompt cache hits, which reduces latency and cost.
Low values or a sudden drop in the rate of hits might indicate cache invalidation, upstream changes, or configuration issues.

### Typical PromQL Query

```promql
sum(rate(cc_lb_cache_hit_total[5m])) by (upstream, model)
```

### Suggested Alerting Threshold

Consider alerting if the cache hit ratio drops below 10% for over 15 minutes when total request volume is high.
The hit ratio is calculated by dividing hits by the sum of hits and misses.

```promql
sum(rate(cc_lb_cache_hit_total[5m])) / (sum(rate(cc_lb_cache_hit_total[5m])) + sum(rate(cc_lb_cache_miss_total[5m]))) < 0.10
```

## Metric: cc_lb_cache_miss_total

- **Type**: Counter
- **Labels**: `upstream`, `model`
- **Label Cardinality Bounds**: Low, typically under 50. It's bounded by the number of configured upstreams multiplied by the number of active models.

### Interpretation

This metric counts the total number of cache miss responses.
High values indicate that requests are not hitting the cache, which could be normal for dynamic or unique prompts.
A sudden spike in misses relative to hits indicates that the cache is not being populated or is being bypassed.

### Typical PromQL Query

```promql
sum(rate(cc_lb_cache_miss_total[5m])) by (upstream, model)
```

### Suggested Alerting Threshold

Consider alerting if the miss rate is significantly higher than the hit rate for a sustained period.
This suggests that the cache is not being used effectively.

```promql
sum(rate(cc_lb_cache_miss_total[5m])) > 10 * sum(rate(cc_lb_cache_hit_total[5m]))
```

## Metric: cc_lb_cache_observation_dropped_total

- **Type**: Counter
- **Labels**: `reason`
- **Label Cardinality Bounds**: Very low, bounded by `queue_full`, `channel_closed`, and `below_threshold`.

### Interpretation

This metric tracks prompt-cache observations that were dropped before being written to the store.
High values indicate that observations are being discarded.
Spikes in `queue_full` indicate that the background writer queue is overwhelmed.
`channel_closed` means the background writer is no longer accepting observations.
When `below_threshold` rises, many requests do not meet the minimum token threshold for caching.

### Typical PromQL Query

```promql
sum(rate(cc_lb_cache_observation_dropped_total[5m])) by (reason)
```

### Suggested Alerting Threshold

Consider alerting if the rate of dropped observations due to `queue_full` is greater than 5 per second for over 5 minutes.
This indicates that the background worker is bottlenecked.

```promql
sum(rate(cc_lb_cache_observation_dropped_total{reason="queue_full"}[5m])) > 5
```

## Metric: cc_lb_cache_observation_read_failed_total

- **Type**: Counter
- **Labels**: None

### Interpretation

Counts failed shared-store lookups during routing. These failures leave cache
affinity unknown; they do not mark the provider response as a cache miss or fail
the client request. Inspect the `proxy.prompt_cache_observation_lookup` child
span and database availability when this counter rises.

### Typical PromQL Query

```promql
rate(cc_lb_cache_observation_read_failed_total[5m])
```

## Metric: cc_lb_cache_observation_write_failed_total

- **Type**: Counter
- **Labels**: `store`
- **Label Cardinality Bounds**: Very low, bounded by the 2 store kinds: `sqlite` and `postgres`.

### Interpretation

This metric tracks failures when writing prompt-cache observations to the persistent store.
Any value above 0 indicates a write failure, which means cache observations are being lost.
High values indicate persistent database issues, such as disk full for `sqlite` or connection/permission issues for `postgres`.

### Typical PromQL Query

```promql
sum(rate(cc_lb_cache_observation_write_failed_total[5m])) by (store)
```

### Suggested Alerting Threshold

Consider alerting immediately if any write failures are detected.
This indicates a critical storage issue.

```promql
sum(rate(cc_lb_cache_observation_write_failed_total[1m])) > 0
```

## Metric: cc_lb_routing_tier_selections_total

- **Type**: Counter
- **Labels**: `tier`, `upstream`, `principal_id`
- **Label Cardinality Bounds**: Bounded by `4 tiers × configured upstreams × active principals`. Typical live estimate ~16k series at 20 upstreams × 200 principals; comfortably within Prometheus safe bounds.

### Interpretation

This metric counts routing decisions won by each upstream in each subscription-preference tier, per principal. It surfaces which tier the cost-first within-tier selection actually placed candidates in, and which upstream captured the pick.

- `tier` ∈ `{known_base, partial_base, overage, unknown_probe}` from the `SubscriptionTier` enum.
- `upstream` matches the upstream name (same convention as `cc_lb_requests_total` / `cc_lb_cache_hit_total`), NOT the UUID.
- `principal_id` is the UUID string (same convention as `cclb_api_key_requests_total`).

Bookkeeping counter `cc_lb_lifecycle_routing_tier_events_total{outcome=emitted|missing_principal_id|orphan_ttl_evicted|cap_evicted}` tracks subscriber-side health without contaminating the main tier signal.

### Typical PromQL Query

Per-tier share by upstream:

```promql
sum by (tier, upstream) (rate(cc_lb_routing_tier_selections_total[15m]))
/ ignoring(upstream) group_left
sum by (tier) (rate(cc_lb_routing_tier_selections_total[15m]))
```

### Suggested Alerting Threshold

An alert `RoutingUpstreamFunneling` in `deploy/alerts/routing-anomaly.yml` fires when any single upstream captures more than 70% of decisions within a tier over 15 minutes, sustained for 10 minutes, guarded by a low-volume floor (`> 2` req/s per tier). This detects unexpected concentration of the cost-first within-tier selection.

```promql
(
  sum by (tier, upstream) (rate(cc_lb_routing_tier_selections_total[15m]))
  / ignoring(upstream) group_left
  sum by (tier) (rate(cc_lb_routing_tier_selections_total[15m]))
) > 0.7
and on(tier) (
  sum by (tier) (rate(cc_lb_routing_tier_selections_total[15m])) > 2
)
```

### Response Playbook

When `RoutingUpstreamFunneling` fires:

1. Query `sum by (tier, upstream) (rate(cc_lb_routing_tier_selections_total[15m]))` in Prometheus to confirm the funneling upstream and the tier.
2. Compare against the approved baseline distribution for the deployment. Deviation from that shape is the signal; do not use private operator or account names in the runbook.
3. Query `/admin/v1/subscription-quotas/latest` for the tier's upstreams; look for stale, zero-remaining, or `disabled_reason`-set windows that could distort the urgency computation.
4. Use `POST /admin/v1/router/preview` with a known `request_id` to inspect the `SubscriptionPreferenceTrace` on `RoutingTrace.stages[..]`. The `candidate_assessments[].quota_urgency` and `estimated_input_cost_micros` numbers show WHY cost-first selected that upstream (cheapest near-cost tier, urgency tiebreak).
5. Common causes: (a) a single upstream is the only one with fresh quota snapshots and everyone else is stale, (b) a plan-capacity change made one upstream saturate the cap while others fell below, (c) the collector stopped ingesting subscription-quota headers from N-1 of the N upstreams.

False positives: sustained low traffic that clears the `> 2 req/s` floor after the alert has already latched. If confirmed low-volume, no action; alert will self-clear.

## Metric: cc_lb_request_stage_duration_seconds

- **Type**: Histogram
- **Labels**: `source_kind`, `stage`, `outcome`
- **Label Cardinality Bounds**: Fixed. The lifecycle event logger maps inputs to a closed vocabulary and does not add request IDs, principals, models, or other unbounded dimensions.

### Label vocabulary

- `source_kind` ∈ `{proxy, renewal, unknown}`
- `outcome` ∈ `{success, client_cancelled, error, timeout}`
- Parent `stage` ∈ `{request_body_read, proxy_setup, shape, sign, upstream_ttfb, response_body, finalize, renewal_cycle}`
- Diagnostic I/O `stage` ∈ `{request_body_first_chunk_marker, request_body_receive_marker, request_body_wait_mixed, request_body_process, response_body_wait_mixed, response_body_process, downstream_poll_gap_mixed, retry_overhead_mixed}`

The logger emits only stages available in its bounded per-request aggregate when the terminal lifecycle event arrives. Renewal events emit `renewal_cycle` and do not emit zero-valued proxy stages. A measured fractional or zero-valued I/O stage remains observable. `request_body_chunk_count` remains request-event data; it is not emitted as a duration histogram.

### Boundaries and interpretation

The parent stages preserve request chronology:

- `request_body_read` covers handler-side request-body collection.
- `proxy_setup`, `shape`, and `sign` cover the local pre-dispatch parent intervals.
- `upstream_ttfb` is a parent that includes bulkhead wait, DNS, TCP/TLS connect, and the remaining combined header wait.
- `response_body` selects one parent: completed `stream_total_ms`, buffered `upstream_body_ms`, or partial `upstream_body_ms` for `client_cancelled`.
- `finalize` covers the final local parent interval.
- `retry_overhead_mixed` covers the earlier attempt path only when a retry starts. The final attempt's stage values are reset, so parent accounting includes retry overhead once.

The diagnostic I/O stages have narrower meanings:

- `request_body_first_chunk_marker` and `request_body_receive_marker` are overlapping markers. Do not add them to the request-body parent or to its wait/process split.
- `request_body_wait_mixed` combines client pacing, downstream transit, and runtime scheduling after handler entry. It is not RTT.
- `request_body_process` is elapsed local frame handling, copying, and validation, not CPU time.
- `response_body_wait_mixed` combines provider generation, upstream transit, and runtime scheduling. It is not a pure provider-processing metric.
- `response_body_process` is local relay or buffered-body work contained in the response-body parent.
- `downstream_poll_gap_mixed` is the streamed consumer/backpressure/scheduler gap between a yielded item and the next downstream poll. It is not a TCP acknowledgement, wire delivery, or RTT. Buffered non-stream delivery after finalization is unobserved.

DNS and TCP/TLS connect values are independently available in request-event data and the four-category Logs UI. Header wait and response-frame wait remain combined upstream observations; do not derive a pure provider-processing duration from this histogram.

### Queries

Rate by fixed stage and outcome:

```promql
sum by (stage, outcome) (
  rate(cc_lb_request_stage_duration_seconds_count{source_kind="proxy"}[5m])
)
```

Average duration for one non-overlapping parent stage:

```promql
sum(rate(cc_lb_request_stage_duration_seconds_sum{
  source_kind="proxy",
  stage="upstream_ttfb"
}[5m]))
/
sum(rate(cc_lb_request_stage_duration_seconds_count{
  source_kind="proxy",
  stage="upstream_ttfb"
}[5m]))
```

Inspect downstream consumer/scheduler gaps without calling them network RTT:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(cc_lb_request_stage_duration_seconds_bucket{
      source_kind="proxy",
      stage="downstream_poll_gap_mixed"
    }[5m])
  )
)
```

### Aggregation warning

Do not sum all `stage` series or add their quantiles. Markers and child stages overlap parent intervals, and Prometheus histogram buckets do not preserve per-request correlation. For parent accounting, use the non-overlapping parent stages plus `retry_overhead_mixed` once. For diagnosis, query one child or marker at a time and compare distributions rather than constructing a synthetic critical path.

The per-request Logs UI applies a separate five-group responsibility attribution:

- **Downstream**: observed request-frame waits and streamed downstream poll gaps, with client/transit/backpressure/scheduler caveats.
- **cc-lb**: measured setup, shaping, signing, bulkhead waiting, local body work, and finalization.
- **Upstream net**: observed DNS plus combined TCP/TLS connect only.
- **Upstream wait**: the response-header residual plus response-frame waits. These combine provider generation, upstream transit, and runtime scheduling.
- **Unattributed**: retry aggregates and residual time without a finer ownership witness.

The compact Logs popover and Request Detail Sheet share these totals. The Sheet keeps its chronological timeline and SSE markers separate because responsibility attribution is not a wall-clock sequence. A missing value means not measured; a present zero means measured zero.

## Troubleshooting

### When hit-rate suddenly drops

Follow these steps to diagnose and resolve a sudden drop in the prompt cache hit-rate.

1. **Check observation_dropped counter for spikes**
   Run a query on `cc_lb_cache_observation_dropped_total`.
   If `queue_full` is spiking, the background worker is bottlenecked.
   A high `below_threshold` count means the incoming prompts might be too short to qualify for caching.

2. **Check write_failed counter**
   Check `cc_lb_cache_observation_write_failed_total`.
   If writes are failing, new observations are not being saved, preventing future hits.

3. **Verify Anthropic upstream availability**
   Ensure the upstream Anthropic endpoints are healthy and returning 200 OK.

4. **Review recent config changes**
   Check if the cache threshold or queue capacity was recently modified.
   Verify if any new upstreams or models were added that might not support prompt caching.
