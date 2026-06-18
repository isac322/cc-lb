# Distributed Scheduler Architecture

This document describes the architecture, topology, and operational runbook for the distributed scheduler in cc-lb.

## 1. Topology Overview

The cc-lb scheduler uses a hybrid job model to coordinate background tasks across multiple replicas. It separates local, low-latency operations from durable, cross-replica background tasks. This design satisfies the "local vs durable" rule defined in decision IDs D-arch-3 and D-arch-1.

The system consists of the following components:

- **Entity Jobs**: Dynamic background tasks enqueued per-upstream or per-entity. Examples include warmup cycles, OAuth token refreshes, and usage polling.
- **Singleton Jobs**: Periodic maintenance tasks that must run on exactly one replica at any given time. Examples include usage rollups, database pruning, and quota garbage collection.
- **Reconciler**: A periodic singleton job (`SchedulerReconcileJob`) that scans active upstreams and ensures the correct set of entity jobs are enqueued in Apalis storage.
- **Cron Leader**: A single replica elected via database advisory locks to run the cron scheduler and enqueue singleton jobs.

### Worker Layout Diagram

```
+---------------------------------------------------------------------------------+
|                                  Postgres DB                                    |
|  +-------------------------+  +-------------------------+  +-----------------+  |
|  |      Apalis Queues      |  |   Idempotency Tables    |  |  Advisory Lock  |  |
|  |  (apalis.jobs table)    |  | (warmup_effects, etc.)  |  | (0xCC1B...0001) |  |
|  +------------^------------+  +------------^------------+  +--------^--------+  |
+---------------|----------------------------|------------------------|-----------+
                |                            |                        |
        +-------+-------+            +-------+-------+        +-------+-------+
        |               |            |               |        |               |
+-------v---------------+------------v---------------+--------v---------------+---+
| Replica 1 (Cron Leader)                                                         |
|                                                                                 |
|  +------------------+    +------------------+    +---------------------------+  |
|  |   Cron Engine    |--->|  Apalis Storage  |    |      Apalis Workers       |  |
|  | (Enqueues Crons) |    |  (Enqueue/Claim) |    | (Executes claimed jobs)   |  |
|  +------------------+    +--------^---------+    +---------------------------+  |
+-----------------------------------|---------------------------------------------+
                                    |
+-----------------------------------|---------------------------------------------+
| Replica 2 (Follower)              |                                             |
|                                   |                                             |
|  +------------------+             |              +---------------------------+  |
|  |   Cron Engine    |             +------------->|      Apalis Workers       |  |
|  |     (Idle)       |                            | (Executes claimed jobs)   |  |
|  +------------------+                            +---------------------------+  |
+---------------------------------------------------------------------------------+
```

## 2. Per-Job Table

The table below lists every job type registered in the scheduler. This list is defined in decision IDs D-arch-7, D-ido-1, D-ido-2, D-ido-3, D-ido-5, and D-meta-1.

| Name | Kind | Idempotency Key Shape | Retry Class | Cadence / Trigger | Idempotency / Effect Table | Max Latency Budget |
|---|---|---|---|---|---|---|
| **UpstreamWarmupJob** | Entity | `entity:warmup:<upstream_id>` | Entity | Enqueued via Reconcile when warmup is due (every 5 hours) | `warmup_effects` | 10s |
| **OAuthRefreshJob** | Entity | `entity:oauth_refresh:<upstream_id>` | Entity | Enqueued via Reconcile or proactively when token is near expiry | `oauth_refresh_claims` | 10s |
| **OAuthUsagePollJob** | Entity | `entity:oauth_usage_poll:<upstream_id>` | Entity | Enqueued via Reconcile or after usage events | `oauth_usage_poll_cursors` | 10s |
| **AnthropicCompatRefreshJob** | Entity | `entity:anthropic_compat_refresh:<key>` | Entity | Enqueued via Reconcile for each compatibility key | `anthropic_compat_refresh_claims` | 10s |
| **MetadataRefreshJob** | Entity | `entity:metadata_refresh:<upstream_id>:<generation>` | Entity | Enqueued after OAuth refresh completes | `metadata_refresh_claims` | 10s |
| **UsageRollupJob** | Singleton | `singleton:usage_rollup` | Maintenance | Every 30s (with jitter) | `usage_rollups` | 5s |
| **UsagePruneJob** | Singleton | `singleton:usage_prune` | Maintenance | Every 24h (86,400s) | `request_events`, `audit_log` | 60s |
| **SubscriptionQuotaGcJob** | Singleton | `singleton:quota_gc` | Maintenance | Every 1h (3600s) | `subscription_quotas` | 10s |
| **PromptCacheObservationPurgeJob** | Singleton | `singleton:prompt_cache_purge` | Maintenance | Every 10m (600s) | `prompt_cache_observations` | 10s |
| **PriceCatalogRefreshJob** | Singleton | `singleton:price_catalog_refresh` | Maintenance | Every 1h (3600s) | `price_catalog` | 10s |
| **ApalisHousekeepingJob** | Singleton | `singleton:apalis_housekeeping` | Maintenance | Every 1h (3600s) | `apalis.jobs` | 10s |
| **SchedulerReconcileJob** | Singleton | `scheduler_reconcile` | Maintenance | Every 5m (300s) | `apalis.jobs` | 10s |

## 3. DB Pool Isolation

To prevent background tasks from starving the main request-handling path, the scheduler uses a separate connection pool. This isolation is configured via the `separate_pool` setting in `SchedulerConfig`. Both pools connect to the same database file (SQLite) or the same DSN (Postgres), but they maintain separate connection limits.

The connection budget is governed by the formula verified in the Wave 0.3 evidence file `.omo/evidence/task-0-3-connection-budget.md`:

```
(main_pool + apalis_pool) * replicas <= 0.7 * max_connections
```

For example, in our production Postgres environment:
- `max_connections` is 240.
- The 70% safety ceiling is 168 connections.
- The main pool default is 10 connections.
- The Apalis scheduler pool default is 5 connections.

With these defaults, the system easily scales up to 10 replicas:

```
(10 + 5) * 10 = 150 connections <= 168 (Verified)
```

Additionally, the leader election mechanism uses 1 dedicated session-mode connection per leader replica. This connection is not managed by the pool and must be factored into capacity planning.

## 4. Idempotency vs Scheduling State Separation

The scheduler strictly separates scheduling state from idempotency state. This separation ensures correctness in a distributed environment where workers can crash or restart.

- **Scheduling State (Apalis)**: Apalis owns the queueing state, visibility timeouts, and retry attempts. It determines *what* job should run and *when* it should run. It does not guarantee that the side-effect has not already occurred.
- **Idempotency State (cc-lb)**: The domain-level effect tables (such as `warmup_effects`, `oauth_refresh_claims`, and `oauth_usage_poll_cursors`) own the record of completed side-effects. Before executing any external action (like sending an HTTP request to Anthropic), the handler must check the corresponding effect table.

This design ensures that even if a job is enqueued multiple times or retried after a worker crash, the actual domain side-effect is executed exactly once. This satisfies the at-least-once external execution model defined in D-arch-1.

## 5. Leader Election Semantics

Leader election is required to coordinate cron scheduling and prevent multiple replicas from enqueuing duplicate singleton jobs.

- **Postgres Implementation**: Uses a dedicated `PgConnection` to acquire a session-level advisory lock. The default lock key is `0xCC1B_5CDE_0001` (defined as `DEFAULT_SCHEDULER_LEADER_LOCK_KEY` in `crates/cc-lb-config/src/types.rs`).
- **Follower Behavior**: Replicas that fail to acquire the advisory lock become followers. They periodically attempt to acquire the lock every 5 seconds (the heartbeat interval).
- **Handover Window**: If the leader replica crashes or disconnects, Postgres automatically releases the session-level advisory lock. The next follower will acquire the lock and assume the leader role within its next 5-second heartbeat window.
- **SQLite Implementation**: SQLite deployments run in a single-process environment. The SQLite leader election implementation always returns `LeaderState::Single` and assumes leadership immediately.

This leader election design is specified in decision ID D-arch-6.

## 6. Retry Classes

The scheduler defines three distinct retry classes to handle different failure modes. Each class uses an exponential backoff curve with 10% jitter to prevent thundering herds. These curves are defined in `SchedulerConfig::default()` and decision ID D-arch-7.

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

## 7. Scheduler Failures Table and Admin Endpoint

When a job exhausts its retry class, it is moved to the `Failed` state in Apalis. To prevent silent failures, the reconciler detects these failed jobs and surfaces them to operators.

- **Ticket Model**: Instead of automatically retrying indefinitely, failed jobs are recorded as "tickets" in the `scheduler_failures` table. This satisfies the "ticket/metric only, no auto-page" promise defined in D-cut-3.
- **Triage Endpoint**: Operators can query `GET /admin/scheduler/failures` to read and triage these failures. The endpoint supports filtering by `job_type` and returns details about the failure, including the last error message and the number of attempts.
- **Resolution**: Once the underlying issue is resolved (for example, fixing invalid upstream credentials), operators can trigger a manual reconciliation via `POST /admin/scheduler/reconcile` to clear the failures and re-enqueue the jobs.

## 8. Metrics List

The scheduler emits a comprehensive set of Prometheus metrics to monitor health and performance. These metrics are defined in `crates/cc-lb-scheduler/src/scheduler_metrics.rs`.

- `cclb_scheduler_jobs_total` (Counter): Tracks job lifecycle events.
  - Labels: `job_type`, `status` (started, done, retry, skip, panicked, duplicate_effect, noop)
  - Cardinality: 12 job types * 7 statuses = 84
- `cclb_scheduler_job_duration_seconds` (Histogram): Tracks job handler execution duration.
  - Labels: `job_type`
  - Cardinality: 12
- `cclb_scheduler_failures_total` (Counter): Tracks terminal job failures.
  - Labels: `job_type`
  - Cardinality: 12
- `cclb_scheduler_leader_acquired_total` (Counter): Tracks leader lock acquisitions.
  - Cardinality: 1
- `cclb_scheduler_leader_lost_total` (Counter): Tracks leader lock losses.
  - Cardinality: 1
- `cclb_scheduler_reconcile_orphan_pruned_total` (Counter): Tracks pruned orphan queue rows.
  - Labels: `job_type`
  - Cardinality: 12
- `cclb_scheduler_init_failure` (Gauge): Tracks scheduler initialization failure state (0 or 1).
  - Cardinality: 1
- `cclb_scheduler_lazy_refresh_timeout_total` (Counter): Tracks timed-out lazy OAuth refresh waits.
  - Cardinality: 1
- `cclb_scheduler_prune_rows_removed_total` (Counter): Tracks pruned rows.
  - Labels: `table` (request_events, audit_log)
  - Cardinality: 2
- `cclb_scheduler_quota_gc_rows_removed_total` (Counter): Tracks removed subscription quota rows.
  - Cardinality: 1
- `cclb_scheduler_price_catalog_status_total` (Counter): Tracks price catalog refresh outcomes.
  - Labels: `status` (applied, noop)
  - Cardinality: 2
- `cclb_scheduler_prompt_cache_purge_rows_removed_total` (Counter): Tracks removed prompt cache observations.
  - Cardinality: 1
- `cclb_scheduler_metadata_refresh_status_total` (Counter): Tracks metadata refresh outcomes.
  - Labels: `status`
  - Cardinality: Variable (low)

## 9. "Local vs Durable" Rule

The "local vs durable" rule (defined in D-arch-3 and D-arch-1) governs where background work is executed:

- **Local Rule**: Operations that are low-latency, per-replica, or critical to the request path must run in-process on each replica. They do not use the distributed scheduler. Examples include local price catalog installation (SLA <= 60s per D-cut-2) and dynamic view reconciliation.
- **Durable Rule**: Operations that are heavy, cross-replica, or require strict coordination must be managed by the Apalis distributed scheduler. They use database-backed queues to ensure durability and cross-replica safety. Examples include warmup cycles, OAuth token refreshes, and price catalog fetching.

## 10. Operator Runbook

### Scheduler Stuck
- **Symptom**: Jobs are not executing; queues are growing; `cclb_scheduler_jobs_total` is flat.
- **Triage**:
  1. Check the leader status via `GET /admin/scheduler/status`. Ensure a leader is active.
  2. Check the logs for database connection errors or lock contention.
  3. Verify that the worker threads are not blocked by long-running external HTTP calls.
- **Resolution**: Restart the scheduler workers or trigger a manual reconciliation via `POST /admin/scheduler/reconcile`.

### Duplicate Effect
- **Symptom**: Multiple warmup requests or token refreshes are observed for the same cycle.
- **Triage**:
  1. Check the idempotency tables (`warmup_effects` or `oauth_refresh_claims`) to see if duplicate rows exist.
  2. Check the logs for worker crashes between the external HTTP call and the database write.
- **Resolution**: This is usually a transient at-least-once cost accepted under D-arch-1. If persistent, check for database transaction isolation level issues or unique constraint violations.

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
- **Resolution**: Roll back the failed migration or manually resolve the schema conflict. Ensure that Phase 1 (PR A) is fully deployed before running Phase 2 (PR B) column drops per D-cut-6.

### Leader Election Storm
- **Symptom**: High CPU usage on the database; rapid lock acquisition and loss events in metrics.
- **Triage**:
  1. Monitor `cclb_scheduler_leader_acquired_total` and `cclb_scheduler_leader_lost_total`.
  2. Check for network instability between the app replicas and the database.
- **Resolution**: Increase the heartbeat interval or resolve the underlying network latency issues.
