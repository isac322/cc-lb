# Distributed Scheduler Architecture

This document describes the architecture, topology, and operational runbook for the distributed scheduler in cc-lb.

## 1. Topology Overview

The cc-lb scheduler uses a hybrid job model to coordinate background tasks across multiple replicas. It separates local, low-latency operations from durable, cross-replica background tasks. This design satisfies the "local vs durable" rule defined in decision IDs D-arch-3 and D-arch-1.

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

## 2. Per-Job Table

The table below lists every job type registered in the scheduler. This list is defined in decision IDs D-arch-7, D-ido-1, D-ido-2, D-ido-3, D-ido-5, and D-meta-1.

| Name | Kind | Idempotency Key Shape | Retry Class | Cadence / Trigger | Idempotency / Effect Table | Max Latency Budget |
|---|---|---|---|---|---|---|
| **UpstreamWarmupJob** | Entity | `adaptive:warmup:<upstream_id>:<cycle_key>` (primary lane); `adaptive:warmup_third_party:<upstream_id>:<cycle_key>` (third-party lane); immediate jobs use `...:bootstrap:<seed_secs>` | Entity | One job per upstream and lane. Re-enqueued for the lane's next quota reset; the watchdog seeds each lane separately when its job is missing | `apalis.jobs` full unique idempotency key | 10s |
| **OAuthRefreshJob** | Entity | `entity:oauth_refresh:<upstream_id>:<expires_at_unix_secs>` | Entity | Enqueued by watchdog, proactively, or lazily when token is near expiry | `apalis.jobs` full unique idempotency key | 10s |
| **AnthropicCompatRefreshJob** | Singleton | `cron:anthropic_compat_refresh:<tick_unix_secs>` | Maintenance | Every 24h by default; refreshes all registered compatibility keys concurrently | `anthropic_compat_etags` and `anthropic_compatibility_kv_v1` | 60s |
| **MetadataRefreshJob** | Entity | `entity:metadata_refresh:<upstream_id>:<generation>` | Entity | Enqueued after OAuth refresh completes | `metadata_refresh_claims` | 10s |
| **CacheKeepaliveJob** | Entity | `cache_keepalive:<session_key_hash>:<generation>` | Maintenance (SQL `max_attempts = 1`, no retries) | Enqueued after an eligible Anthropic response begins at `message_start` / first response event | `cache_keepalive_sessions` generation fence + `apalis.jobs` idempotency key | TTL lead time |
| **UsageRollupJob** | Singleton | `singleton:usage_rollup` | Maintenance | Every 30s (with jitter) | `usage_rollups` | 5s |
| **UsagePruneJob** | Singleton | `singleton:usage_prune` | Maintenance | Every 24h (86,400s) | `request_events`, `audit_log`, `upstream_api_usage_v1` | 60s |
| **PromptCacheObservationPurgeJob** | Singleton | `singleton:prompt_cache_purge` | Maintenance | Every 10m (600s) | `prompt_cache_observations` | 10s |
| **UpstreamAffinityPurgeJob** | Singleton | `cron:upstream_affinity_purge:<tick_unix_secs>` | Maintenance (1 retry, up to 60s delay) | Every 10m (600s) plus up to 30s jitter | `upstream_affinity_v1` | Bounded to 1,000 rows/batch and 1,024 batches/run |
| **PriceCatalogRefreshJob** | Singleton | `singleton:price_catalog_refresh` | Maintenance | Every 1h (3600s) | `price_catalog` | 10s |
| **ApalisHousekeepingJob** | Singleton | `singleton:apalis_housekeeping` | Maintenance | Every 1h (3600s) | `apalis.jobs` | 10s |
| **OAuthUsagePollCronJob** | Singleton | `cron:oauth_usage_poll:<tick_unix_secs>` | Maintenance | Every 60s (no jitter) | `oauth_usage_poll_cursors` | 60s |
| **ApiKeyUsagePollCronJob** | Singleton | `cron:api_key_usage_poll:<tick_unix_secs>`; immediate polls use `api_key_usage_poll:<upstream_id>:<spec_revision>:<checked_at_unix_secs>` | Maintenance (immediate polls use SQL `max_attempts = 1`, no retries) | Every 300s plus up to 15s jitter; also enqueued once when a key is saved or re-checked as `ok` | `upstream_api_key_details_v1`, `upstream_api_usage_v1` | 10s per provider request |
| **WarmupWatchdogJob** | Singleton | `maintenance:warmup_watchdog:<tick_unix_secs>` | Maintenance | Configured recurring cadence | `apalis.jobs` | 10s |
| **OAuthRefreshWatchdogJob** | Singleton | `maintenance:oauth_refresh_watchdog:<tick_unix_secs>` | Maintenance | Configured recurring cadence | `apalis.jobs` | 10s |

### Claude Code compatibility channels

The daily compatibility tick independently refreshes `claude_code_stable_version`
and `claude_code_latest_version` in `anthropic_compatibility_kv_v1`. Each channel
uses `https://downloads.claude.ai/claude-code-releases/<channel>` and falls back
to the matching npm dist-tag if that request fails. A failed channel does not
prevent the other channel from refreshing or replace its own last successful value.

Anthropic usage polling and its associated account-identity lookup use `latest`;
before the first successful latest refresh, they use `2.1.282`, not the stored
stable version. Other Anthropic metadata requests and Anthropic coupon claims
continue using `stable`. These Claude Code identities do not apply to OpenAI
banked reset inventory or consumption, or to Gemini requests.
This adds a KV entry, not a database migration or a configuration key.

Existing keyed job payloads remain readable. New daily jobs encode `key: null`
to refresh all registered channels; older binaries cannot decode that payload,
which must be considered before rolling back a worker.

### Reset inventory and immediate usage refresh

OAuth usage polling maintains credential-bound reset snapshots: Anthropic grants share the usage response, while OpenAI banked reset credits use a separate inventory request. Admin fleet and per-upstream limit-reset GETs read those snapshots without provider calls. Unknown, stale, and pending-claim state cannot establish availability.

A claim submits one provider POST for the selected grant or credit and is never replayed automatically. After successful terminal `reset` or `already_used` results, the admin handler awaits a direct usage read for that upstream and publication to the serving replica's routing quota cache, rather than enqueueing a post-claim cron job. Its `usage_refresh` result is separate from the reset outcome: `not_applicable` means the upstream is no longer eligible for usage polling, while an unavailable refresh integration reports `failed`. A failed refresh leaves the reset outcome intact and lets normal usage polling recover freshness without another redemption or optimistic zero usage. Peer replicas remain asynchronous; see the [consistency contract](./consistency-contract.md#scheduler-jobs).


### Upstream-affinity retention

`[upstream_affinity] ttl_days = 7` sets the retention policy (default `7` days), independently of request-log retention. Values must be positive; `CC_LB_UPSTREAM_AFFINITY__TTL_DAYS` is the environment override. Restart the server after changing the TTL so the proxy and cleanup worker use the same startup policy.

Rows expire when their last successful binding observation is at least the configured number of days old, or their explicit expiry has passed. Existing NULL-expiry rows also age out. Ordinary replay reads do not refresh the timestamp, so lowering the TTL causes existing bindings older than the new TTL to fail closed with a local 503 immediately after upgrade even before the purge runs.

The recurring job removes expired rows through indexed, bounded batches. PostgreSQL skips locked candidates and rechecks expiry before deletion, so an in-flight renewal is not removed as stale. A shorter TTL applies to existing rows after restart; increasing it cannot restore rows already deleted. Disabling `scheduler.recurring_jobs.upstream_affinity_purge` stops physical cleanup, not the proxy's logical expiry check. SQLite can reuse freed pages without immediately shrinking its database file.

### Long-lived OAuth credentials

Long-lived OAuth credentials skip only token-refresh work. `OAuthRefreshJob` does not refresh them, and no refresh bootstrap task is seeded or watched for them. The OAuth usage/quota poll, warm-up bootstrap and watchdog, and subscription-metadata refresh all include long-lived upstreams. If a stale refresh job is dispatched for one, the handler returns `Skip` without rescheduling it.

The exclusion applies only to credentials that are actually stored as long-lived. When Anthropic accepts a `long_lived_365d` exchange but grants a materially shorter lifetime than requested, cc-lb stores the credential as `refreshing` instead; such a clamped credential gets the normal refresh bootstrap task and watchdog handling.

### API-key usage polling

`scheduler.recurring_jobs.api_key_usage_poll` (default `interval_secs = 300`, `jitter_secs = 15`) pulls Anthropic's usage report for `anthropic_api_key` upstreams. `interval_secs` must be between 60 and 604800 (7 days, the span of the hourly usage window) and `jitter_secs` must be smaller than `interval_secs`. Each tick takes up to 500 enabled API-key upstreams whose next poll is due and that are either org-scoped with a working usage report or not yet checked.

- **Host rule.** Only upstreams whose `base_url` is exactly `https://api.anthropic.com` are polled. Any other host records `usage_report = not_applicable` once and is never contacted by the poller.
- **One cycle.** A due upstream gets at most two usage-report requests, one page each: hourly buckets for the last 7 days and daily buckets since the start of the UTC month. List-price cost is computed from the price catalog at poll time; buckets without a model are stored under an empty model and shown as "Other". Upstreams saved before key checks existed get one capability probe first.
- **One poller per upstream.** Before its first provider request, a cycle takes a 60-second lease on the upstream in `upstream_api_key_details_v1`, counted from the moment of the claim. That claim is a short database write; no transaction stays open across the HTTP calls. Time spent waiting for the claim counts against the lease: before each provider request the cycle rereads its clocks and sends the request only if it can finish, at its 10-second bound, within 30 seconds of the claim returning and at least 16 seconds before the lease expires (15 seconds of clock skew between replicas plus rounding). Otherwise the request is not sent, the result is stored as `lease_spent` without counting a failure, and the lease is released; the cycle never relies on cancelling a request already sent. A regular tick and an overlapping immediate poll can't both call Anthropic for the same upstream: the loser skips it, counted as `claim_lost`. Only the lease owner can store the result, and storing it releases the lease. A key replace or re-check also releases it, and then the in-flight result is dropped. A result is also dropped as `conflict` when the key, workspace, or Base URL changed or the upstream was deleted mid-poll, or when the upstream is disabled at the moment the result would be stored. Any other spec edit keeps the result, such as a rename, or a disable and re-enable that leaves the upstream enabled when the poll completes. The tick log's `not_ok` count leaves out `claim_lost` and `conflict`. An earlier next-poll time never overwrites a later stored one, so a `429` `retry-after` is honored even if another result lands. If a poller crashes, its lease expires and a later tick polls the upstream again. A tick that runs half an interval or more behind schedule, such as a backlog of elapsed ticks after the scheduler stalled, counts the next poll from when it ran rather than from its scheduled time, so the backlog polls each upstream once instead of once per elapsed tick.
- **Outcomes.** Success resets the failure count. `401`/`403` demotes the usage report to `denied`, unless a re-check stored a newer snapshot in the meantime; `/usage` then shows the last report with `stale: true` and `usage_poll_failed`. `5xx` and timeouts increment the failure count; after two consecutive failures `/usage` is `stale: true` with no fix code. `429` does not count as a failure: the next poll waits for `retry-after`, or one interval when it is absent, counted from when the `429` arrived. A `429` on the one capability probe for a key saved before checks existed leaves its report state unset and defers the same way. A credential that cannot be decrypted records `usage_report = error` and backs off one interval with zero outbound calls; the log carries only the upstream id. An org-scoped key with no resolved key id and no workspace makes no calls and is retried on the next interval.
- **Retention.** `UsagePruneJob` deletes reported usage buckets older than both the request-log retention cutoff and the start of the current UTC month, in bounded batches. With request-log retention `0`, reported usage is never pruned.



### Keep-alive dispatch timeout

`scheduler.keepalive_dispatch_timeout_secs` bounds each provider HTTP
attempt for a cache keep-alive renewal. It defaults to `30` seconds and must
be at least `1` and strictly below the scheduler's `60`-second per-job
timeout. This applies to Anthropic and OpenAI Responses keep-alive requests,
which share the engine dispatcher.

Timeout outcomes are reported as `renewal timed out after {n}s`, distinct from
other dispatch errors.

## 3. DB Pool Isolation

To prevent background tasks from starving the main request-handling path, the scheduler uses a separate connection pool. This isolation is configured via the `separate_pool` setting in `SchedulerConfig`. On PostgreSQL, both pools use the same DSN with separate connection limits. On SQLite, the application pool uses the configured database file, while the scheduler pool uses the derived `path.with_extension("scheduler.sqlite")` file. Application-owned tables such as `cache_keepalive_sessions` stay in the application database; Apalis `Jobs` and `Workers` stay in the derived scheduler database.

Budget the configured pool limits across all replicas:

```
(main_pool + apalis_pool) * replicas <= 0.7 * max_connections
```

Set the main and scheduler pool limits against PostgreSQL's configured
`max_connections`. Leave the remaining capacity for other database clients
and operational work; recheck the budget when changing pool limits or replica
count.

### PostgreSQL TLS

Storage and scheduler connections use `storage.url`, but resolve TLS mode independently. Storage connections, including startup preflight, use `storage.pool.sslmode`. The scheduler uses `scheduler.separate_pool.sslmode`; it does not inherit `storage.pool.sslmode`.

For each pool, TLS mode follows this precedence:

1. An explicit pool `sslmode` overrides the URL mode.
2. If the pool setting is omitted, the URL's `sslmode` or `ssl-mode` query parameter applies.
3. If neither specifies a mode, cc-lb uses `prefer`, even when `PGSSLMODE` is set.

Omit each pool's `sslmode` to inherit the URL policy. An explicit `prefer` or `disable` intentionally overrides even a strict URL mode such as `require`, `verify-ca`, or `verify-full`; remove a redundant `prefer` when the URL should control TLS.

After upgrading, a strict URL mode takes effect wherever the pool setting is omitted. Connections that previously relied on the implicit `prefer` fallback may fail until the database and client TLS configuration satisfy that policy.

## 4. Idempotency and Scheduling State

The scheduler now relies on Apalis Jobs as the durable coordination point for scheduler-side effects that do not require separate domain cursors. This keeps queueing, retry attempts, and idempotency keys in one table.

- **Scheduling State (Apalis)**: Apalis owns the queueing state, visibility timeouts, retry attempts, and full `(job_type, idempotency_key)` uniqueness. Warmup and OAuth refresh coordination use this path.
- **Domain Cursor State (cc-lb)**: Jobs that need domain progress markers, such as OAuth usage polling, keep their own cursor tables. Those tables track business progress, not duplicate scheduler queues.
- **Cache Keepalive Fence (`cache_keepalive_sessions`)**: Prompt-cache keepalive uses Apalis for timer transport only. The current session generation, encrypted payload, TTL anchor, and enqueue state live in `cache_keepalive_sessions`. Every new real request and every successful cache-hit self-reschedule bumps the generation, and each Apalis row is keyed as `cache_keepalive:<session_key_hash>:<generation>` so stale queued/running work becomes a no-op instead of refreshing the wrong cache prefix.

This design ensures duplicate enqueues collapse at the Jobs table while retryable `Failed` rows can still be treated as active work. It preserves the at-least-once external execution model defined in D-arch-1 without redundant scheduler-side state tables.

## 5. Cluster Coordination via Idempotency

The scheduler does not use leader election. Cluster-safe cron is achieved entirely through Apalis storage-level idempotency, following the pattern documented by the apalis maintainer in [apalis#281](https://github.com/apalis-dev/apalis/issues/281) (option 3).

- **Every Replica Polls Its Own `Schedule`**: There is no primary or follower. Each replica drives an `apalis_cron::Schedule` in-process and computes the wait to each tick against `Utc::now()`, saturating elapsed ticks to zero delay.
- **Deterministic Idempotency Keys**: Each cron tick is turned into a `TaskBuilder::new(job).run_at_timestamp(tick_secs).with_idempotency_key("cron:{kind}:{tick_secs}").build()` push. `tick_secs` is the schedule-derived instant (not wall-clock at push time), so replicas with modest clock drift still produce identical keys for the same tick.
- **Storage Dedups**: The unique `(job_type, idempotency_key)` index on `apalis.jobs` (Postgres) and `Jobs` (SQLite) collapses simultaneous pushes to exactly one row per tick. The producer swallows the resulting `SQLSTATE 23505` / `SQLite constraint 2067` violations as expected.
- **Any Worker Executes**: Apalis workers on every replica poll the queue via `FOR UPDATE SKIP LOCKED`; whichever worker wins the row-level claim runs the job.
- **SQLite Deployments**: SQLite is single-process by design (see `AGENTS.md` storage rules); the same code path runs but there is no clustering concern.
- **Cache Keepalive Deployments**: SQLite is supported for local development, CI, and bounded single-node operation. Production multi-replica / HA keepalive scheduling requires Postgres so Apalis claims and the `cache_keepalive_sessions` fence are visible to every replica. Redis is intentionally not a scheduler backend for keepalive because it does not share the SQL transaction/fence boundary used by storage-backed signing and payload retention.

This design revokes decision ID D-arch-6 (which specified advisory-lock leader election). It is stateless, has no handover window, and reserves zero dedicated database connections for coordination.

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

- **Ticket Model**: Apalis `Failed` and `Killed` rows are the failure tickets, including `Failed` rows that Apalis may retry. This satisfies the "ticket/metric only, no auto-page" promise defined in D-cut-3 without a `scheduler_failures` table.
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
- Cleanup recovers stale active/running sessions through application storage, then purges stale pending and expired sessions. In scheduler storage, `ApalisHousekeepingJob` reaps `Running` locks older than `APALIS_STALE_LOCK_THRESHOLD_SECS` (120 s). It reaps `Queued` locks older than 120 s only when the owning worker has no fresh heartbeat within that threshold, preserving prefetched batches buffered on a live worker. It moves eligible non-keepalive rows back to `Pending` and increments `attempts` by one; it marks eligible cache keepalive rows `Killed`. Retention later removes older pending and terminal Apalis rows. Generation checks make stale rows no-op when they eventually run.
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

The "local vs durable" rule (defined in D-arch-3 and D-arch-1) governs where background work is executed:

- **Local Rule**: Operations that are low-latency, per-replica, or critical to the request path must run in-process on each replica. They do not use the distributed scheduler. Examples include local price catalog installation (SLA <= 60s per D-cut-2) and dynamic view reconciliation.
- **Durable Rule**: Operations that are heavy, cross-replica, or require strict coordination must be managed by the Apalis distributed scheduler. They use database-backed queues to ensure durability and cross-replica safety. Examples include warmup cycles, OAuth token refreshes, and price catalog fetching.

The [Admin consistency contract](consistency-contract.md#scheduler-jobs) states what a completed enqueue or job run guarantees and which state converges later.

## 10. Operator Runbook

### Scheduler Stuck
- **Symptom**: Jobs are not executing; queues are growing; `cclb_scheduler_jobs_total` is flat.
- **Triage**:
  1. Query `GET /admin/scheduler/status` and check `recurring_jobs[].next_run_at`, which includes rescheduled `Pending` retries.
  2. Inspect `apalis.jobs` for the affected `job_type` and idempotency key pattern. A `Pending` row with a future `run_at` is waiting for its retry delay; check the `retry` status in `cclb_scheduler_jobs_total`. For a `Running` or `Queued` row, inspect `lock_at` for a lease older than 120 s. Housekeeping reaps an old `Queued` lock only when its owning worker has no fresh heartbeat within 120 s, so prefetched batches on live workers remain queued. It returns eligible non-keepalive rows to `Pending` with `attempts + 1` and marks eligible cache keepalive rows `Killed`.
  3. Check the logs for database connection errors or lock contention.
  4. Verify that the worker threads are not blocked by long-running external HTTP calls.
- **Resolution**: Restart the scheduler workers; the watchdog and enqueue paths create new work with a new idempotency key.

### Duplicate Effect
- **Symptom**: Multiple warmup requests or token refreshes are observed for the same cycle.
- **Triage**:
  1. Check Apalis Jobs for duplicate or stale idempotency keys.
  2. Check the logs for worker crashes between the external HTTP call and the Apalis job transition.
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

### Duplicate Cron Push Noise
- **Symptom**: Elevated `SQLSTATE 23505` (Postgres) or `2067` (SQLite) log lines from the scheduler storage during cron ticks.
- **Triage**: These are expected. Every replica pushes each tick and all but one are rejected by the unique idempotency-key index; the producer logs but does not surface them as errors. If the volume becomes disruptive, downgrade the storage layer's log level for those specific SQL states.
- **Resolution**: No action needed for correctness. Excessive rate typically indicates too many replicas relative to cron cadence; scale down or reduce cron frequency.
