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
- **Cron Producers**: Every replica runs its own `apalis-cron` `CronStream`. Duplicate pushes for the same tick collide on the storage-level unique `(job_type, idempotency_key)` index, so exactly one job per tick is queued. Any Apalis worker on any replica can then claim and execute it. This follows the maintainer-recommended clustered-cron pattern (see [apalis#281](https://github.com/apalis-dev/apalis/issues/281) option 3).

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

The scheduler defines three distinct retry classes to handle different failure modes. Each class uses an exponential backoff curve with 10% jitter to prevent thundering herds. These curves are defined in `SchedulerConfig::default()` and the scheduler retry policy.

- **Probe**: Used for lightweight connectivity checks.
  - Max attempts: 3
  - Base delay: 1s
  - Max delay: 5s
  - Backoff curve: 1s, 2s, 4s (capped at 5s)
- **Entity**: Used for heavy, external-facing operations like warmup or OAuth refresh.
  - Max attempts: 5
  - Base delay: 30s
  - Max delay: 600s (10 minutes)
  - Backoff curve: 30s, 60s, 120s, 240s, 480s
- **Maintenance**: Used for internal database cleanup and rollups.
  - Max attempts: 1
  - Base delay: 60s
  - Max delay: 60s
  - Backoff curve: No retries (fails fast)
- **Cache keepalive override**: `CacheKeepaliveJob` is enqueued with `max_attempts=1` even though it is an entity-shaped Apalis row. Cache misses, stale generations, decrypt failures, unsupported providers, and dispatch errors are business outcomes recorded as terminal/noop state and metrics; they must not rely on Apalis retries.

## 7. Scheduler Failures and Admin Endpoint

When a job exhausts its retry class, it remains in a terminal `Failed` or `Killed` state in Apalis. The admin endpoint reads those Apalis rows directly and surfaces them to operators.

- **Ticket Model**: Terminal Apalis rows are the failure tickets. This satisfies the "ticket/metric only, no auto-page" operator promise without a `scheduler_failures` table.
- **Triage Endpoint**: Operators can query `GET /admin/scheduler/failures` to read and triage these failures. The endpoint supports filtering by `job_type` and returns details about the failure, including the last error message and the number of attempts.
- **Redaction**: Failure summaries are bounded and redact prompt, `Authorization`, `x-api-key`, downstream API key, ciphertext, and payload-shaped material. Cache keepalive rows surface the job type and hashed/idempotency summary, never the prompt snapshot or downstream auth headers.
- **Resolution**: Once the underlying issue is resolved (for example, fixing invalid upstream credentials), operators can trigger the relevant watchdog or enqueue path to create new work with a new idempotency key.

## 8. Metrics List

The scheduler emits a comprehensive set of Prometheus metrics to monitor health and performance. These metrics are defined in `crates/cc-lb-scheduler/src/scheduler_metrics.rs`.

- `cclb_scheduler_jobs_total` (Counter): Tracks job lifecycle events.
  - Labels: `job_type`, `status` (started, done, retry, skip, panicked, noop)
  - Cardinality: bounded registered job types * 6 statuses; includes `adaptive:cache_keepalive`.
- `cclb_scheduler_job_duration_seconds` (Histogram): Tracks job handler execution duration.
  - Labels: `job_type`
  - Cardinality: 12
- `cclb_scheduler_failures_total` (Counter): Tracks terminal job failures.
  - Labels: `job_type`
  - Cardinality: bounded registered job types; includes `adaptive:cache_keepalive`.

Cache keepalive-specific operator notes:

- `CacheKeepaliveJob` payloads are lightweight metadata only. Prompt snapshots are AEAD-encrypted in `cache_keepalive_sessions.encrypted_payload` with AAD bound to principal, session hash, upstream id, generation, and job type/version.
- The first schedule uses the response-begin cache anchor (`message_start` for streaming, first upstream response event for non-stream, request dispatch time fallback), not response completion. Run time is `cache_anchor_at + ttl - lead`.
- Cleanup may delete expired keepalive sessions, terminal keepalive Apalis rows, and old `Pending` keepalive Apalis rows. It must not delete `Queued` or `Running` rows; generation checks make those stale rows no-op when they eventually run.
- `cclb_scheduler_init_failure` (Gauge): Tracks scheduler initialization failure state (0 or 1).
  - Cardinality: 1
- `cclb_scheduler_lazy_refresh_timeout_total` (Counter): Tracks timed-out lazy OAuth refresh waits.
  - Cardinality: 1
- `cclb_scheduler_prune_rows_removed_total` (Counter): Tracks pruned rows.
  - Labels: `table` (request_events, audit_log, upstream_affinity)
  - Cardinality: 3
- `cclb_scheduler_price_catalog_status_total` (Counter): Tracks price catalog refresh outcomes.
  - Labels: `status` (applied, noop)
  - Cardinality: 2
- `cclb_scheduler_prompt_cache_purge_rows_removed_total` (Counter): Tracks removed prompt cache observations.
  - Cardinality: 1
- `cclb_scheduler_metadata_refresh_status_total` (Counter): Tracks metadata refresh outcomes.
  - Labels: `status`
  - Cardinality: Variable (low)

## 9. "Local vs Durable" Rule

The "local vs durable" rule (defined by the local-vs-durable architecture) governs where background work is executed:

- **Local Rule**: Operations that are low-latency, per-replica, or critical to the request path must run in-process on each replica. They do not use the distributed scheduler. Examples include local price catalog installation (a bounded latency budget) and dynamic view reconciliation.
- **Durable Rule**: Operations that are heavy, cross-replica, or require strict coordination must be managed by the Apalis distributed scheduler. They use database-backed queues to ensure durability and cross-replica safety. Examples include warmup cycles, OAuth token refreshes, and price catalog fetching.

## 10. Operator Runbook

### Scheduler Stuck
- **Symptom**: Jobs are not executing; queues are growing; `cclb_scheduler_jobs_total` is flat.
- **Triage**:
  1. Query `GET /admin/scheduler/status` and confirm `recurring_jobs[].next_run_at` is advancing across ticks.
  2. Inspect `apalis.jobs` for the affected `job_type` and idempotency key pattern; a stuck row will show `status = 'Running'` with a stale `run_at`.
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
