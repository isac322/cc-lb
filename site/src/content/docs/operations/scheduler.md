---
title: Scheduler and warm-up
description: Operate durable background work, OAuth warm-up, retries, and scheduler failure views.
slug: docs/operations/scheduler
---

The scheduler separates request-path work from durable background tasks. Apalis storage owns delayed jobs, claims, retry state, and idempotency keys. Workers on each replica can claim work from the shared store.

## Important jobs

- `UpstreamWarmupJob` keeps eligible OAuth upstream windows active.
- `OAuthRefreshJob` refreshes credentials that need renewal.
- `CacheKeepaliveJob` runs generation-fenced prompt-cache probes.
- Usage rollups, pruning, and metadata refresh jobs maintain operational state.

Warm-up is enabled by default for OAuth upstreams after credentials exist. It does not apply to API-key upstreams.

The [consistency contract source notes](/docs/reference/source/consistency-contract/) describe what an enqueued or completed scheduler job guarantees and which state converges later.

## SQLite storage ownership

On SQLite, the application pool uses the configured database file, while the scheduler pool uses the derived `path.with_extension("scheduler.sqlite")` file. Application-owned tables such as `cache_keepalive_sessions` stay in the application database; Apalis `Jobs` and `Workers` stay in the derived scheduler database. Housekeeping recovers stale running sessions and purges stale pending or expired sessions in application storage, kills stale running cache keepalive jobs in scheduler storage, and retains fresh queued/running work. Retention later removes older pending and terminal Apalis rows. No new configuration, schema, or migration is required.

## Retry classes

| Class | Use | Attempts |
| --- | --- | ---: |
| Probe | Lightweight connectivity checks | 3 |
| Entity | External work such as warm-up or refresh | 5 |
| Maintenance | Internal cleanup and rollups | 1 |

Cache keepalive uses one attempt. Misses, stale generations, decrypt failures, unsupported providers, and dispatch errors are business outcomes recorded in metrics.

## Failure triage

Terminal jobs remain visible in the scheduler failure view. Inspect the job type, status, bounded reason, and attempt count. Resolve the underlying credential, storage, or configuration problem, then let the relevant watchdog enqueue a new idempotency key.

Failure summaries redact prompts, authorization headers, API keys, ciphertext, and payload-shaped material.
