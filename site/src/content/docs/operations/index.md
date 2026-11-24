---
title: Operate cc-lb
description: Manage runtime state, scheduled work, deployment changes, and stream observability.
slug: docs/operations
---

Operators manage cc-lb through a database-backed runtime view. The server can apply changes to upstreams, principals, keys, and plugin chains without rebuilding the binary.

## Operating paths

- [Runtime management](/docs/operations/runtime-management/) covers admin authentication, resource updates, revisions, and dynamic rebinding.
- [Scheduler and warm-up](/docs/operations/scheduler/) covers durable jobs, OAuth warm-up, token refresh, and retry classes.
- [Observability](/docs/operations/observability/) covers stream outcomes, metrics, logs, and failure triage.

## Deployment boundaries

SQLite supports local development, CI, and bounded single-node operation via the standalone `cc-lb` binary. PostgreSQL is the supported storage boundary for multi-replica scheduling and for [split control-plane / data-plane deployments](/docs/getting-started/#split-control-plane-and-data-plane-deployment) (`cc-lb-proxy --role proxy-worker|proxy|worker` paired with `cc-lb-admin`) because workers, stateless proxies, and the admin server must observe the same durable queue, `pg_notify` broadcasts, and generation fences.

Plan upgrades around the database migration contract and the published server artifact. Keep a rollback plan for both the binary and schema version.
