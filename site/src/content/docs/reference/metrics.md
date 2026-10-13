---
title: Metrics
description: Use bounded metrics to diagnose stream completion, scheduler work, and runtime state.
slug: docs/reference/metrics
---

Metrics are designed for diagnosis without putting secrets or unbounded request text into labels.

## Request counts

`cc_lb_requests_completed_total{source_kind,principal,upstream,model,status,terminal_outcome,client_status_class}` counts each request that reached the credential check exactly once, when its lifecycle ends. `principal` is the authenticated principal ID, `upstream` the routed upstream name, and `model` the model requested in the body. An attribution stage the request never reached is labelled `none`; for example, an authentication failure has `principal="none"`. `cc_lb_request_completion_duration_seconds{source_kind,upstream,model,status,terminal_outcome}` records the matching duration without a `principal` label to bound histogram cardinality.

For example, the per-upstream 429 rate is `sum by (upstream) (rate(cc_lb_requests_completed_total{status="429"}[5m]))`.

`cc_lb_requests_started_total{status}` and `cc_lb_request_headers_duration_seconds` are recorded when response headers are sent, including local responses such as the 404 fallback, 405, and authentication failures. They carry no principal, upstream, or model labels. `cc_lb_response_body_chunks_total` counts transport body chunks and is also unattributed.

## Stream termination

`cc_lb_stream_terminations_total{outcome,cause,upstream,model}` counts response body outcomes per routed upstream and requested model. Use it with request correlation logs to distinguish an upstream error from a client cancellation.

## Request diagnostics log

`cc_lb_request_diagnostics_lines_total{outcome}` counts stdout diagnostics lines when `observability.request_diagnostics_log` is on. `outcome` is `written`, `encode_error`, or `write_error`.

Lines that never reach the writer increment `cc_lb_dropped_events_total{reason}` with one of these reasons:

| Reason | Cause |
| --- | --- |
| `request_diagnostics_queue_full` | The queue was full with the strip off. A backfill restores the line. |
| `request_diagnostics_strip_send_timeout` | With the strip on, the queue stayed full for 500 ms. |
| `request_diagnostics_strip_queue_full` | With the strip on, the queue was full while the sink was still recovering from a timeout. |
| `request_diagnostics_queue_closed` | The sink had already shut down. |
| `request_diagnostics_job_panic` | Writing one line panicked. |
| `request_diagnostics_worker_panic` | The writer thread panicked. |
| `request_diagnostics_shutdown_timeout` | Shutdown stopped waiting for a stalled stdout after 2 seconds; counts every line the writer had not finished, queued or in flight. |

Any `request_diagnostics_strip_*` drop is permanent loss of that request's diagnostic fields, so alert on any increase. With strip on, `request_diagnostics_shutdown_timeout` is permanent loss too. See [Request diagnostics log](/docs/operations/observability/#request-diagnostics-log) for delivery semantics.

## Scheduler metrics

- `cclb_scheduler_jobs_total` tracks registered job lifecycle events.
- `cclb_scheduler_job_duration_seconds` records handler duration by job type.
- `cclb_scheduler_failures_total` counts terminal job failures.
- `cclb_scheduler_init_failure` reports scheduler initialization state.
- `cclb_scheduler_prune_rows_removed_total` counts bounded cleanup results.

Job labels are bounded to registered types and lifecycle statuses. Scheduler failure views expose sanitized reasons and hashed summaries rather than prompts or credentials.

## Routing and runtime diagnosis

Routing traces record the selected tier, formula version, warning multiplier, and bounded selection reason. Runtime status includes the last apply error and timestamps for operational actions. Combine these views when a resource update is accepted but traffic still follows an earlier view.
