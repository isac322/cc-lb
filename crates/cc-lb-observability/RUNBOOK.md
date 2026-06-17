# Prompt Cache Shadow Runbook

This document describes the metrics and troubleshooting procedures for the prompt cache shadow system in `cc-lb`.

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
