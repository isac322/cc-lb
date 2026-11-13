---
title: "Scheduler source notes"
description: "Curated scheduler topology, retry classes, metrics, and runbook sections."
slug: docs/reference/source/scheduler
---

## 1. Topology Overview

The cc-lb scheduler uses a hybrid job model to coordinate background tasks across multiple replicas. It separates local, low-latency operations from durable, cross-replica background tasks. This design satisfies the "local vs durable" rule defined in the local-vs-durable architecture.

The system consists of the following components:

- **Entity Jobs**: Dynamic background tasks enqueued per-upstream or per-entity. Examples include warmup cycles and OAuth token refreshes.
- **Singleton Jobs**: Periodic maintenance tasks that must produce at most one queued job per tick across the cluster. Examples include usage rollups, database pruning, and quota garbage collection.
- **Watchdogs**: Periodic singleton jobs that scan active upstreams and enqueue missing entity jobs into Apalis storage.
- **Cron Producers**: Every replica polls its own `apalis_cron::Schedule` and sleeps until each tick; a tick that has already elapsed fires immediately instead of erroring. Duplicate pushes for the same tick collide on the storage-level unique `(job_type, idempotency_key)` index, so exactly one job per tick is queued. Any Apalis worker on any replica can then claim and execute it. This follows the maintainer-recommended clustered-cron pattern (see [apalis#281](https://github.com/apalis-dev/apalis/issues/281) option 3).

### Worker Layout Diagram

```
+---------------------------------------------------------------------------------+
|                                  Postgres DB                                    |
|  +-------------------------+  +-------------------------+                       |
|  |      Apalis Queues      |  |     Cursor Tables       |                       |
|  |  (apalis.jobs table)    |  | (usage poll cursors)    |                       |
|  |  UNIQUE(job_type,       |  +------------^------------+                       |
|  |    idempotency_key)     |               |                                    |
|  +------------^------------+               |                                    |
+---------------|----------------------------|------------------------------------+
                |                            |
        +-------+-------+            +-------+-------+
        |               |            |               |
+-------v---------------+------------v---------------+----------------------------+
| Replica 1                                                                       |
|                                                                                 |
|  +------------------+    +------------------+    +---------------------------+  |
|  |   Cron Engine    |--->|  Apalis Storage  |    |      Apalis Workers       |  |
|  | (Pushes ticks    |    |  (Dedups by      |    | (Executes claimed jobs)   |  |
|  |  w/ idempotency) |    |   idempotency)   |    |                           |  |
|  +------------------+    +--------^---------+    +---------------------------+  |
+-----------------------------------|---------------------------------------------+
                                    |
+-----------------------------------|---------------------------------------------+
| Replica 2                         |                                             |
|                                                                                 |
|  +------------------+    +--------v---------+    +---------------------------+  |
|  |   Cron Engine    |--->|  Apalis Storage  |    |      Apalis Workers       |  |
|  | (Pushes ticks    |    |  (Rejects dupes) |    | (Executes claimed jobs)   |  |
|  |  w/ idempotency) |    |                  |    |                           |  |
|  +------------------+    +------------------+    +---------------------------+  |
+---------------------------------------------------------------------------------+
```

## 6. Retry Classes

`RetryClass::policy` in `crates/cc-lb-scheduler/src/retry.rs` defines the retry policies. `RetryPolicy`'s `max_attempts` counts retries after the initial execution. The worker uses the 1-based retry ordinal `attempt + 1`, so a policy with one retry permits two total runs.

- **Probe**: Used for lightweight connectivity checks.
  - Max attempts: 3 retries (4 total runs)
  - Base delay: 1s
  - Max delay: 5s
  - Backoff curve: 1s, 2s, 4s, each with ±10% jitter
- **Entity** (`RetryClass::Adaptive`): Used for external operations like warmup or OAuth refresh.
  - Max attempts: 5 retries (6 total runs)
  - Base delay: 30s
  - Max delay: 600s (10 minutes)
  - Backoff curve: 30s, 60s, 120s, 240s, 480s, each with ±10% jitter
- **Maintenance**: Used for internal database cleanup and rollups.
  - Max attempts: 1 retry (2 total runs)
  - Base delay: 60s
  - Max delay: 60s
  - Backoff curve: one 60s base delay with ±10% jitter (54–66s); explicit positive delays are capped at 60s without jitter

Within the retry budget, a positive `JobOutcome::Retry { delay: Some(d) }` reschedules without jitter at `min(d, max_delay)`. This preserves `PriceCatalogRefreshJob`'s 30s retry under the Maintenance class's 60s cap. `delay: None` uses the class's jittered backoff curve. `Duration::ZERO` returns an error and marks the row `Killed`, even when its retry budget is exhausted.

SQL rows pushed with `max_attempts = 1` disable retries through `RetryPayload::allows_retry()`. This override applies to `CacheKeepaliveJob` and immediate API-key usage polls. Cache misses, stale generations, decrypt failures, unsupported providers, and dispatch errors remain cache keepalive business outcomes recorded as terminal/noop state and metrics.

## 7. Scheduler Failures and Admin Endpoint

A rescheduled `Retry` returns the same row to `Pending` with a future `run_at`; the failure endpoint excludes it. When a job exhausts its retry class, `DeadLetter` marks the row `Killed`. Panics and invalid zero retry delays also produce terminal `Killed` rows. `GET /admin/scheduler/failures` lists `Failed` and `Killed` rows directly from Apalis without filtering by attempt count. Apalis may reclaim a `Failed` row from an ordinary handler error while `attempts < max_attempts`.

- **Ticket Model**: Apalis `Failed` and `Killed` rows are the failure tickets, including `Failed` rows that Apalis may retry. This satisfies the "ticket/metric only, no auto-page" operator promise without a `scheduler_failures` table.
- **Triage Endpoint**: Operators can query `GET /admin/scheduler/failures` to read and triage these failures. The endpoint supports filtering by `job_type` and returns details about the failure, including the last error message and the number of attempts.
- **Redaction**: Failure summaries are bounded and redact prompt, `Authorization`, `x-api-key`, downstream API key, ciphertext, and payload-shaped material. Cache keepalive rows surface the job type and hashed/idempotency summary, never the prompt snapshot or downstream auth headers.
- **Resolution**: Once the underlying issue is resolved (for example, fixing invalid upstream credentials), operators can trigger the relevant watchdog or enqueue path to create new work with a new idempotency key.

## 8. Metrics List

The scheduler emits a comprehensive set of Prometheus metrics to monitor health and performance. These metrics are defined in `crates/cc-lb-scheduler/src/scheduler_metrics.rs`.

- `cclb_scheduler_jobs_total` (Counter): Tracks job lifecycle events.
  - Labels: `job_type`, `status` (started, done, retry, skip, panicked, noop)
  - Cardinality: bounded registered job types * 6 statuses; includes `adaptive:cache_keepalive`.
  - `retry` counts both rescheduled `Retry` outcomes and terminal `DeadLetter` outcomes. A rescheduled row stays `Pending`, not `Done`.
- `cclb_scheduler_job_duration_seconds` (Histogram): Tracks job handler execution duration.
  - Labels: `job_type`
  - Cardinality: 15 (one per registered job type)
- `cclb_scheduler_failures_total` (Counter): Tracks terminal job failures.
  - Labels: `job_type`
  - Cardinality: bounded registered job types; includes `adaptive:cache_keepalive`.
  - Rescheduled retries do not increment this counter; terminal dead letters do.

Cache keepalive-specific operator notes:

- `CacheKeepaliveJob` payloads are lightweight metadata only. Prompt snapshots are AEAD-encrypted in `cache_keepalive_sessions.encrypted_payload` with AAD bound to principal, session hash, upstream id, generation, and job type/version.
- New keepalive snapshots store the captured body in framed raw/zstd payload codecs. Before upgrading from the JSON payload format, drain and stop all old workers and request-serving replicas before starting a new writer. The new version reads old snapshots and migrates them on their next successful renewal; old versions cannot read the new format. Do not downgrade while active sessions contain new-format payloads.
- The first schedule uses the response-begin cache anchor (`message_start` for streaming, first upstream response event for non-stream, request dispatch time fallback), not response completion. Run time is `cache_anchor_at + ttl - lead`.
- Cleanup recovers stale active/running sessions through application storage, then purges stale pending and expired sessions. In scheduler storage, `ApalisHousekeepingJob` checks `lock_at` against `APALIS_STALE_LOCK_THRESHOLD_SECS` (120s). It moves stale non-keepalive `Running` or `Queued` rows back to `Pending` and increments `attempts` by one; it marks stale cache keepalive `Running` or `Queued` rows `Killed`. Fresh queued and running work is retained. Retention later removes older pending and terminal Apalis rows. Generation checks make stale rows no-op when they eventually run.
- `cclb_scheduler_init_failure` (Gauge): Tracks scheduler initialization failure state (0 or 1).
  - Cardinality: 1
- `cclb_scheduler_lazy_refresh_timeout_total` (Counter): Tracks timed-out lazy OAuth refresh waits.
  - Cardinality: 1
- `cclb_scheduler_prune_rows_removed_total` (Counter): Tracks pruned rows.
  - Labels: `table` (request_events, audit_log, upstream_affinity, upstream_api_usage)
  - Cardinality: 4
- `cclb_scheduler_price_catalog_status_total` (Counter): Tracks price catalog refresh outcomes.
  - Labels: `status` (applied, noop)
  - Cardinality: 2
- `cclb_scheduler_prompt_cache_purge_rows_removed_total` (Counter): Tracks removed prompt cache observations.
  - Cardinality: 1
- `cclb_scheduler_metadata_refresh_status_total` (Counter): Tracks metadata refresh outcomes.
  - Labels: `status`
  - Cardinality: Variable (low)

- `cclb_scheduler_api_key_usage_poll_total` (Counter): Tracks API-key usage poll outcomes; one per polled upstream per tick.
  - Labels: `outcome` (success, probed, denied, rate_limited, failed, decrypt_failed, not_applicable, no_subject, conflict, claim_lost, lease_spent, storage_error)
  - Cardinality: 12

## 9. "Local vs Durable" Rule

The "local vs durable" rule (defined by the local-vs-durable architecture) governs where background work is executed:

- **Local Rule**: Operations that are low-latency, per-replica, or critical to the request path must run in-process on each replica. They do not use the distributed scheduler. Examples include local price catalog installation (a bounded latency budget) and dynamic view reconciliation.
- **Durable Rule**: Operations that are heavy, cross-replica, or require strict coordination must be managed by the Apalis distributed scheduler. They use database-backed queues to ensure durability and cross-replica safety. Examples include warmup cycles, OAuth token refreshes, and price catalog fetching.

The [Admin consistency contract](https://github.com/isac322/cc-lb/blob/master/docs/consistency-contract.md#scheduler-jobs) states what a completed enqueue or job run guarantees and which state converges later.

## 10. Operator Runbook

### Scheduler Stuck
- **Symptom**: Jobs are not executing; queues are growing; `cclb_scheduler_jobs_total` is flat.
- **Triage**:
  1. Query `GET /admin/scheduler/status` and check `recurring_jobs[].next_run_at`, which includes rescheduled `Pending` retries.
  2. Inspect `apalis.jobs` for the affected `job_type` and idempotency key pattern. A `Pending` row with a future `run_at` is waiting for its retry delay; check the `retry` status in `cclb_scheduler_jobs_total`. For a `Running` or `Queued` row, inspect `lock_at` for a stale lease. Housekeeping recovers non-keepalive rows after the 120s stale-lock threshold by returning them to `Pending` with `attempts + 1`; it marks stale cache keepalive rows `Killed`.
  3. Check the logs for database connection errors or lock contention.
  4. Verify that the worker threads are not blocked by long-running external HTTP calls.
- **Resolution**: Restart the scheduler workers; the watchdog and enqueue paths create new work with a new idempotency key.

### Duplicate Effect
- **Symptom**: Multiple warmup requests or token refreshes are observed for the same cycle.
- **Triage**:
  1. Check Apalis Jobs for duplicate or stale idempotency keys.
  2. Check the logs for worker crashes between the external HTTP call and the Apalis job transition.
- **Resolution**: This is usually a transient at-least-once cost accepted by the at-least-once execution model. If persistent, check for database transaction isolation level issues or unique constraint violations.

### Connection Budget Exhausted
- **Symptom**: Database returns "too many connections" errors; app replicas fail to start.
- **Triage**:
  1. Run `SELECT count(*) FROM pg_stat_activity` to measure active connections.
  2. Verify that the connection budget formula is satisfied: `(main_pool + apalis_pool) * replicas <= 0.7 * max_connections`.
- **Resolution**: Reduce `max_connections` in `SchedulerPoolConfig` or scale down the number of app replicas.

### Migration Failure
- **Symptom**: Scheduler fails to start with database migration errors.
- **Triage**:
  1. Check the logs for the specific migration step that failed.
  2. Verify that the database schema matches the expected version.
- **Resolution**: Roll back the failed migration or manually resolve the schema conflict. Ensure that the first migration phase is fully deployed before later column drops.

### Duplicate Cron Push Noise
- **Symptom**: Elevated `SQLSTATE 23505` (Postgres) or `2067` (SQLite) log lines from the scheduler storage during cron ticks.
- **Triage**: These are expected. Every replica pushes each tick and all but one are rejected by the unique idempotency-key index; the producer logs but does not surface them as errors. If the volume becomes disruptive, downgrade the storage layer's log level for those specific SQL states.
- **Resolution**: No action needed for correctness. Excessive rate typically indicates too many replicas relative to cron cadence; scale down or reduce cron frequency.
