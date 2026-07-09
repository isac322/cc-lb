# cc-lb Observability Runbook

This document describes the metrics and troubleshooting procedures for observability in `cc-lb`, including the prompt cache shadow system and the subscription-preference routing pipeline.

## Metric: cc_lb_cache_token_drift

- **Type**: Histogram
- **Labels**: `upstream`, `model`
- **Label Cardinality Bounds**: Low, typically under 50. It's bounded by the number of configured upstreams multiplied by the number of active models.
- **Buckets**: `-1000.0`, `-500.0`, `-100.0`, `-50.0`, `-10.0`, `0.0`, `10.0`, `50.0`, `100.0`, `500.0`, `1000.0`

### Interpretation

This metric tracks the difference between actual cache read tokens returned by the upstream and predicted cache read tokens.
A value of 0 indicates perfect prediction.
Positive values mean the upstream returned more cache read tokens than predicted.
Negative values mean the upstream returned fewer cache read tokens than predicted.
Large drift values suggest that the prompt cache shadow prediction logic is out of sync with the upstream's actual behavior.

### Typical PromQL Query

```promql
histogram_quantile(0.95, sum(rate(cc_lb_cache_token_drift_bucket[5m])) by (le, upstream, model))
```

### Suggested Alerting Threshold

Consider alerting if the absolute p95 drift exceeds 100 tokens for more than 10 minutes.
This indicates a significant prediction mismatch.

```promql
abs(histogram_quantile(0.95, sum(rate(cc_lb_cache_token_drift_bucket[5m])) by (le))) > 100
```

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
- **Label Cardinality Bounds**: Very low, bounded by the 4 fixed reasons: `queue_full`, `below_threshold`, `status_4xx`, and `abort`.

### Interpretation

This metric tracks prompt-cache observations that were dropped before being written to the store.
High values indicate that observations are being discarded.
Spikes in `queue_full` indicate that the background writer queue is overwhelmed.
When `below_threshold` rises, many requests do not meet the minimum token threshold for caching.
Increases in `status_4xx` or `abort` indicate client-side errors or aborted requests.

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

This metric counts routing decisions won by each upstream in each subscription-preference tier, per principal. It surfaces which tier the WRH within-tier selection actually placed candidates in, and which upstream captured the pick.

- `tier` ∈ `{known_base, partial_base, overage, unknown_probe}` from the `SubscriptionTier` enum.
- `upstream` matches the upstream name (same convention as `cc_lb_requests_total` / `cc_lb_cache_hit_total`), NOT the UUID.
- `principal_id` is the UUID string (same convention as `cclb_api_key_requests_total`).

Bookkeeping counter `cc_lb_contract_routing_tier_events_total{outcome=emitted|missing_principal_id|orphan_ttl_evicted|cap_evicted}` tracks subscriber-side health without contaminating the main tier signal.

### Typical PromQL Query

Per-tier share by upstream:

```promql
sum by (tier, upstream) (rate(cc_lb_routing_tier_selections_total[15m]))
/ ignoring(upstream) group_left
sum by (tier) (rate(cc_lb_routing_tier_selections_total[15m]))
```

### Suggested Alerting Threshold

An alert `RoutingUpstreamFunneling` in `deploy/alerts/routing-anomaly.yml` fires when any single upstream captures more than 70% of decisions within a tier over 15 minutes, sustained for 10 minutes, guarded by a low-volume floor (`> 2` req/s per tier). This detects regression of the WRH within-tier distribution shipped in PR #312.

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
2. Compare against the shadow-eval baseline distribution captured during PR #312 development (Runbear ~55%, isac-personal ~16%, bh322yoo-max ~15%, bear-max ~13%). Deviation from this shape is the signal.
3. Query `/admin/v1/subscription-quotas/latest` for the tier's upstreams; look for stale, zero-remaining, or `disabled_reason`-set windows that could distort the urgency computation.
4. Use `POST /admin/v1/router/preview` with a known `request_id` to inspect the `SubscriptionPreferenceTrace` on `RoutingTrace.stages[..]`. The `candidate_assessments[].urgency` numbers show WHY the WRH placed weight there.
5. Common causes: (a) a single upstream is the only one with fresh quota snapshots and everyone else is stale, (b) a plan-capacity change made one upstream saturate the cap while others fell below, (c) the collector stopped ingesting subscription-quota headers from N-1 of the N upstreams.

False positives: sustained low traffic that clears the `> 2 req/s` floor after the alert has already latched. If confirmed low-volume, no action; alert will self-clear.

## Troubleshooting

### When hit-rate suddenly drops

Follow these steps to diagnose and resolve a sudden drop in the prompt cache hit-rate.

1. **Inspect drift histogram p95**
   Check if `cc_lb_cache_token_drift` p95 is high.
   Large drift values mean the shadow cache prediction is inaccurate, causing the proxy to make incorrect caching decisions.

2. **Check observation_dropped counter for spikes**
   Run a query on `cc_lb_cache_observation_dropped_total`.
   If `queue_full` is spiking, the background worker is bottlenecked.
   A high `below_threshold` count means the incoming prompts might be too short to qualify for caching.

3. **Check write_failed counter**
   Check `cc_lb_cache_observation_write_failed_total`.
   If writes are failing, new observations are not being saved, preventing future hits.

4. **Verify Anthropic upstream availability**
   Ensure the upstream Anthropic endpoints are healthy and returning 200 OK.
   If the upstream is returning errors, observations will be dropped with `status_4xx` or other error reasons.

5. **Review recent config changes**
   Check if the cache threshold or queue capacity was recently modified.
   Verify if any new upstreams or models were added that might not support prompt caching.
