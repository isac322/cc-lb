---
title: Metrics
description: Use bounded metrics to diagnose stream completion, scheduler work, and runtime state.
slug: docs/reference/metrics
---

Metrics are designed for diagnosis without putting secrets or unbounded request text into labels.

## Stream termination

`cc_lb_stream_terminations_total{outcome,cause}` counts response body outcomes. Use it with request correlation logs to distinguish an upstream error from a client cancellation.

## Scheduler metrics

- `cclb_scheduler_jobs_total` tracks registered job lifecycle events.
- `cclb_scheduler_job_duration_seconds` records handler duration by job type.
- `cclb_scheduler_failures_total` counts terminal job failures.
- `cclb_scheduler_init_failure` reports scheduler initialization state.
- `cclb_scheduler_prune_rows_removed_total` counts bounded cleanup results.

Job labels are bounded to registered types and lifecycle statuses. Scheduler failure views expose sanitized reasons and hashed summaries rather than prompts or credentials.

## Routing and runtime diagnosis

Routing traces record the selected tier, formula version, warning multiplier, and bounded selection reason. Runtime status includes the last apply error and timestamps for operational actions. Combine these views when a resource update is accepted but traffic still follows an earlier view.
