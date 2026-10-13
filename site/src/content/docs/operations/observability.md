---
title: Observability
description: Distinguish completed streams, upstream errors, proxy errors, and client cancellation.
slug: docs/operations/observability
---

An HTTP 200 status means response headers were sent. It does not prove that the response body completed. Use the stream termination metrics and tracing events to identify the end state.

## Stream outcomes

The `cc_lb_stream_terminations_total{outcome,cause,upstream,model}` metric distinguishes the following outcomes per routed upstream and requested model (`model="none"` when the request body named no model):

- `completed`
- `upstream_error`
- `proxy_error`
- `client_cancelled`

Causes use bounded categories such as `unexpected_eof`, `h2_cancel`, and `affinity_error`. Request IDs, prompt text, and error messages are not metric labels.

## Traces and logs

With OTLP enabled, `proxy.response_stream` spans remain open until the response body ends or is dropped. Transport failures record bounded, redacted error fields when available.

The stream latency breakdown worker uses a fixed queue. Queue overflow and shutdown timeout are counted with bounded reasons so telemetry work does not delay final response bytes.

## Request diagnostics log

With `observability.request_diagnostics_log = true`, cc-lb writes one JSON line to stdout for each request event it stores. The exception is cache-keepalive renewal rows whose lifecycle events were dropped or that were finalized after the assembler stopped: they are stored but get no line, and their diagnostic fields are always empty (see the [consistency contract](/docs/reference/source/consistency-contract/#request-events-and-dashboards)). The line carries the event's diagnostic fields: `cache_breakpoints`, `cache_control_message_indices`, `routing_trace`, `iterations`, and `internal_errors`. With `request_event_strip_diagnostics = true`, the database stores events without those fields and the line is their only copy. See [Request diagnostics](/docs/reference/configuration/#request-diagnostics) for both settings.

Each line is compact JSON followed by a newline, and every key is always present, in this order:

| Key | Type | Meaning |
| --- | --- | --- |
| `schema` | string | Always `cc_lb.request_diagnostics.v1`. |
| `source` | string | `live` from the server, `backfill` from `cc-lb request-events export-diagnostics`. |
| `event_id` | string | Request event ID. |
| `seq` | integer | Storage cursor of the row. |
| `ts` | string | Event time, RFC 3339 UTC with milliseconds, such as `2026-10-13T05:19:00.123Z`. |
| `ts_ms` | integer | The same instant in Unix milliseconds. |
| `request_id` | string | cc-lb request ID. |
| `principal_id` | string or null | Principal ID. |
| `key_id` | string or null | API key ID. |
| `upstream_id` | string or null | Upstream UUID. |
| `upstream_name` | string or null | Upstream name. |
| `model` | string or null | Requested model. |
| `status` | integer | HTTP status. |
| `error_code` | string or null | cc-lb error code. |
| `event_kind` | string or null | Request event kind. |
| `source_kind` | string or null | Request source, such as `proxy` or `renewal`. |
| `cache_breakpoints` | array | Prompt-cache breakpoints, including `lookback_prefixes`; may be empty. |
| `cache_control_message_indices` | array | Message indices that carry `cache_control`; may be empty. |
| `routing_trace` | object or null | All-candidate routing trace. |
| `iterations` | any JSON or null | Recorded iterations. |
| `internal_errors` | array | Redacted internal errors; may be empty. |

Live and backfill lines for the same row are identical except for `source`.

### Collecting the lines

- **Routing.** Every diagnostics line starts with `{"schema":"cc_lb.request_diagnostics.v1",`. Tracing JSON lines start with `{"timestamp":`. Route on the prefix.
- **Long lines.** Lines are not truncated and are often 2 to 25 KiB. Container runtimes split lines over 16 KiB into partial records, so the log agent must reassemble them; with Fluent Bit, use `multiline.parser cri`.
- **Redaction.** `observability.log_redaction` does not apply to these lines. Producers redact the values before they reach the row: internal error text is redacted, and routing traces are capped and redacted.
- **Duplicates.** A request can produce a second, different line for the same `event_id`: a row persisted after its time-to-live expired is followed by a late terminal event that the database ignores. The first line matches the stored row. Consumers must keep the earliest-ingested line per `event_id` and discard later ones; analytics must use first-write semantics, never the last write. A backfill line always matches the stored row.
- **Reconciliation.** Compare distinct `event_id` per UTC day of `ts_ms` with `cc-lb request-events export-diagnostics --count-only`, excluding `ts_ms = 0` on both sides.

### Delivery

The line is queued after the row insert commits and before the admin event bus publishes the final update. A dedicated thread drains the queue, which holds 1,024 lines, and writes each line with one write call, so lines never interleave with tracing output in the process.

- **Shared stdout.** Diagnostics lines share the stdout pipe and its in-process lock with tracing logs. While the writer thread is blocked on a full pipe, request-path tracing writes wait for the same lock, so a stalled log collector or pipe can delay requests. Keep the collector healthy, and watch the drop metrics below: a rising `request_diagnostics_strip_send_timeout`, `request_diagnostics_queue_full`, or `request_diagnostics_shutdown_timeout` count signals a stalled stdout.
- **Strip off: best effort.** When the queue is full, the line is dropped and counted as `request_diagnostics_queue_full`. The database still holds the fields, so a backfill restores the line.
- **Strip on: bounded wait.** The assembler waits up to 500 ms for queue space. On timeout, it drops the line as `request_diagnostics_strip_send_timeout` and stops waiting until the queue is at least half empty; lines dropped in that state count as `request_diagnostics_strip_queue_full`. These drops are permanent loss.
- **Process death.** On graceful shutdown the queue is drained for up to 2 seconds. If stdout is still stalled at that deadline, shutdown continues without waiting, and every line the writer has not finished is counted as `request_diagnostics_shutdown_timeout` — the queued lines and the in-flight write. A timed-out in-flight line may still land once if its write returns before the process exits, but it stays counted as dropped, so counts are conservative. A SIGKILL or out-of-memory kill loses up to 1,024 queued lines. With the strip on, these losses are permanent.
- **Shutdown ordering.** The process exits only after the proxy listener has drained and every shutdown hook, including the assembler's final row flush and this queue drain, has finished, up to 15 seconds after the listener stops. Past that bound it logs a warning and exits with the hooks still running.

### Metrics and alerts

| Series | Labels |
| --- | --- |
| `cc_lb_request_diagnostics_lines_total` | `outcome`: `written`, `encode_error`, `write_error` |
| `cc_lb_dropped_events_total` | `reason`: `request_diagnostics_queue_full`, `request_diagnostics_strip_queue_full`, `request_diagnostics_strip_send_timeout`, `request_diagnostics_queue_closed`, `request_diagnostics_job_panic`, `request_diagnostics_worker_panic`, `request_diagnostics_shutdown_timeout` |

Alert on any increase of `cc_lb_dropped_events_total{reason=~"request_diagnostics_strip_.*|request_diagnostics_shutdown_timeout"}`, and on `encode_error` or `write_error` lines. `cc_lb_lifecycle_assembler_rows_total{outcome=~"written|written_orphan"}` should equal all `cc_lb_request_diagnostics_lines_total` outcomes plus the `request_diagnostics_*` drop reasons. Every line is counted exactly once — a timed-out in-flight write counts as dropped even when its bytes still land — so the identity is conservative: a pod that exits while a write is stalled counts the line as dropped whether or not it was delivered. Rows written by the keepalive renewal path after the assembler stopped have no line; their diagnostic fields are always empty.

The `upstream_*` fields described under [Diagnostic payload fields](#diagnostic-payload-fields) stay in the request event payload and are not part of this line.

## Upstream stream failures

cc-lb does not retry a streaming request automatically after it has started sending output. How an upstream body failure is recorded depends on whether cc-lb already parsed the upstream `message_stop` event. Only an event whose JSON payload has `"type":"message_stop"` counts; an `event: message_stop` line with a different or malformed payload does not.

- **Before `message_stop`**: the failure stays fatal. Where cc-lb emits the SSE stream itself, the client receives an `event: error` frame. The request row records `error_code=upstream_stream_error` and `upstream_error_type` of `upstream_response_decode_error` (compressed body decoding failed) or `upstream_response_body_error` (the upstream transport failed).
- **After `message_stop`**: when cc-lb emits the SSE stream (response transformation, the affinity inspection gate, or identity-encoded SSE passthrough for transport errors), the stream is accepted with a warning. Any unconsumed SSE bytes must be whitespace, the upstream status must be 2xx, and no provider `event: error` or earlier stream error may have been seen. The client receives no error frame. The stream completes normally, and `upstream_stream_warning_type` and `upstream_stream_warning_message` are set on the row instead of `upstream_error_type`; the abnormal-stop classification is unchanged, so a `stop_reason` of `refusal` or `model_context_window_exceeded` still sets `upstream_refusal` or `upstream_context_window_exceeded`. A WARN log records the error chain.
- **Compressed raw passthrough**: unchanged. When cc-lb forwards the upstream `Content-Encoding` without transforming or inspecting the stream, no error frame is emitted. A decode failure while the body is streaming still ends in a success row, as before; a decode failure detected when the body finishes, such as a missing gzip trailer, still ends in an `upstream_response_decode_error` row.

A decode error in the same upstream chunk that carries `message_stop` stays fatal, because that event was never parsed. An incomplete non-whitespace event after `message_stop` also stays fatal. For identity passthrough with an upstream `Content-Length`, an accepted transport error still leaves the client with a short body, which cc-lb cannot correct.

### Integrity tradeoff

The gzip decoder reports a missing trailer, a CRC mismatch, an ISIZE mismatch, and truncation inside a member with the same error, so cc-lb cannot tell them apart. An accepted stream was fully parsed as SSE through `message_stop`, but its gzip checksum was not verified. Accepted streams follow the full success path: affinity binding, usage and cost accounting, and a `completed` stream outcome. This acceptance applies to gzip trailer failures. Truncation detection for zlib, zstd, and brotli at end of stream is out of scope.

### Diagnostic payload fields

When the upstream body ends abnormally on a streaming response — a transport error, a decode failure in mid-body, or a decode failure detected as the body finishes, including accepted warnings — the request event payload includes these fields. Local failures where cc-lb stopped reading upstream itself (for example a transform, affinity, or framing error), clean streams, and client disconnects do not produce them.

| Field | Meaning |
| --- | --- |
| `upstream_http_version` | Upstream response HTTP version, such as `HTTP/1.1` or `HTTP/2.0`. |
| `upstream_request_id` | Anthropic `request-id` response header. |
| `upstream_content_encoding` | Upstream `Content-Encoding` header. |
| `upstream_content_length` | Upstream `Content-Length` header, when present. |
| `upstream_body_bytes` | Raw (still compressed) body bytes received, including the failing chunk. |
| `upstream_body_end` | How the body ended: `transport_error`, `decode_error_after_clean_end` (the transport ended cleanly but final decoding failed), or `decode_error_mid_body`. |
| `upstream_body_error_cause` | Stream termination cause category, such as `unexpected_eof`. |
| `upstream_body_error_io_kind` | I/O error kind of a transport failure, when available. |
| `upstream_body_error_h2_reason` | HTTP/2 reset reason of a transport failure, when available. |
| `upstream_stream_warning_type` | `upstream_response_decode_error` or `upstream_response_body_error` on an accepted stream. |
| `upstream_stream_warning_message` | Bounded error text the failure would otherwise have recorded. |

`stream_message_stop_ms` is also recorded on failed rows when cc-lb parsed `message_stop`. `request_id` is cc-lb's own request identifier; `upstream_request_id` is Anthropic's, for provider support cases. These fields, including warnings, are available in the request event payload returned by the admin API. The admin dashboard does not display them yet, and request list filters do not match them.

## Safe diagnosis

Start with the termination outcome, then compare upstream and proxy logs using the request correlation fields available in your deployment. Treat a client disconnect as different from an upstream failure, even when both end an HTTP stream early.
