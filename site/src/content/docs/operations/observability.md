---
title: Observability
description: Distinguish completed streams, upstream errors, proxy errors, and client cancellation.
slug: docs/operations/observability
---

An HTTP 200 status means response headers were sent. It does not prove that the response body completed. Use the stream termination metrics and tracing events to identify the end state.

## Stream outcomes

The `cc_lb_stream_terminations_total{outcome,cause}` metric distinguishes:

- `completed`
- `upstream_error`
- `proxy_error`
- `client_cancelled`

Causes use bounded categories such as `unexpected_eof`, `h2_cancel`, and `affinity_error`. Request IDs, prompt text, and error messages are not metric labels.

## Traces and logs

With OTLP enabled, `proxy.response_stream` spans remain open until the response body ends or is dropped. Transport failures record bounded, redacted error fields when available.

The stream latency breakdown worker uses a fixed queue. Queue overflow and shutdown timeout are counted with bounded reasons so telemetry work does not delay final response bytes.

## Safe diagnosis

Start with the termination outcome, then compare upstream and proxy logs using the request correlation fields available in your deployment. Treat a client disconnect as different from an upstream failure, even when both end an HTTP stream early.
