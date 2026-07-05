# Dashboard Live-Tail Runbook

## Event Identity

`event_id` is now the single lifecycle-generated identity for each request. It is generated at request start and carried through partial snapshots, final storage rows, SSE messages, storage tail delivery, and PG NOTIFY fanout.

Before this redesign, request log rows could omit `event_id` and the assembler could synthesize one at finalization. That behavior is no longer valid for live-tail because partial and final updates must reconcile under the same key.

## Prometheus Scrape Config

Scrape every `cc-lb` instance that exposes the admin or metrics listener. A minimal Prometheus job looks like this:

```yaml
scrape_configs:
  - job_name: cc-lb
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets:
          - cc-lb-0.internal:9090
          - cc-lb-1.internal:9090
```

Use service discovery in production so every replica contributes to cluster-level live-tail alerts.

## PG NOTIFY Monitoring

Run this query against the production Postgres database:

```sql
SELECT pg_notification_queue_usage();
```

Thresholds:

- Warning: above `0.30` for 5 minutes.
- Critical: above `0.50` for 2 minutes.

Remediation:

- Kick a stuck listener by restarting only the affected `cc-lb` pod.
- If usage keeps rising, restart all listener pods one at a time so they resubscribe cleanly.
- If the queue remains high, scale down write throughput or temporarily reduce admin live-tail consumers until the queue drains.

## Storage Tail Poll Interval

The default storage tail poll interval is 250 ms. Lower values reduce cross-instance final visibility latency but increase storage read load. Higher values reduce database load but widen the reconnect and cross-instance visibility window.

Keep p95 `sse_storage_tail_lag_ms` below 1000 ms. If lag is high while storage CPU is healthy, lower the interval. If database load is high and lag is still acceptable, raise it gradually.

## Metric Label Taxonomy

The finite label vocabulary is documented in `docs/metrics-live-tail.md` and enforced in `crates/cc-lb-core/src/metrics_labels.rs`.

Operationally important labels:

- Reset reasons: `backfill_cap`, `bus_lagged`, `storage_error`.
- Partial triggers: `request_started`, `route_completed`, `upstream_response_started`, `usage_observed`, `stream_completed`, `request_terminated`.
- Notify drop reasons: `queue_full`, `queue_closed`, `serialize_error`, `pg_error`.
- Notify HTTP outcomes: `success`, `not_found`, `unauthorized`, `timeout`, `network_error`.

## Rollback

There is no feature flag for live-tail redesign rollback. Rollback means revert the branch and redeploy the old server and frontend together.

Database migration order is safe for rollback: deploy migration, deploy new server, deploy new frontend. If catastrophic behavior appears after deployment, revert the PR, redeploy the old binary and old frontend, and if needed drop the `NOT NULL` constraint from the migration step that made `event_id` mandatory. The earlier schema additions and backfill are backward compatible.
