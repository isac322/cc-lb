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

With `observability.request_diagnostics_log = true`, cc-lb writes one compact JSON line to stdout for each request event it stores. Request events are stored in `request_events_v1` with all 99 operational, latency-breakdown, and abnormal-stream-error scalar fields in typed relational columns. The `payload` column is dropped, so request-event reads have zero TOAST overhead.

The line carries five deep diagnostic fields:

| Key | Type | Meaning |
| --- | --- | --- |
| `cache_breakpoints` | array | Prompt-cache breakpoints; `lookback_prefixes` is omitted from the line. |
| `cache_control_message_indices` | array | Message indices that carry `cache_control`; may be empty. |
| `routing_trace` | object or null | All-candidate routing trace. |
| `iterations` | any JSON or null | Recorded routing iterations. |
| `internal_errors` | array | Redacted internal errors; may be empty. |

It also carries eight self-contained outcome and session-correlation fields:

| Key | Type | Meaning |
| --- | --- | --- |
| `thread_id` | string or null | Upstream or provider thread identifier. |
| `observed_session_id` | string or null | Session identifier observed for the request. |
| `input_tokens` | integer or null | Input token count. |
| `output_tokens` | integer or null | Output token count. |
| `cache_read_input_tokens` | integer or null | Cache-read input token count. |
| `cache_creation_input_tokens` | integer or null | Cache-creation input token count. |
| `duration_ms` | integer | Event duration in milliseconds. |
| `upstream_ttfb_ms` | integer or null | Upstream time to first byte in milliseconds. |

When routing produced a prompt-cache match prediction, the line also includes `matched_v3_cache_key`, `breakpoint_content_block_index`, `matched_content_block_index`, `lookback_distance`, `predicted_cache_read_tokens`, `predicted_cache_creation_tokens_5m`, and `predicted_cache_creation_tokens_1h`. Each line starts with `{"schema":"cc_lb.request_diagnostics.v1",` and includes event identifiers and routing context needed to correlate it with the relational row. Cache-keepalive renewal rows whose lifecycle events were dropped or finalized after the assembler stopped may have a stored row without a line.

### Collecting the lines

- **Routing.** Route records by the `cc_lb.request_diagnostics.v1` schema prefix. Reassemble container-runtime partial records; lines can exceed 16 KiB.
- **Redaction.** `observability.log_redaction` does not apply to these lines. Producers redact internal error text and cap routing traces before persistence.
- **Duplicates.** A late terminal lifecycle event can produce a second line for an `event_id` that the database already accepted. Keep the earliest-ingested line per `event_id`; analytics use first-write semantics.
- **Reconciliation.** Compare distinct `event_id` values per UTC day with committed relational rows, excluding zero event timestamps on both sides.

### Delivery

The line is queued after the row insert commits and before the admin event bus publishes the final update. A dedicated thread drains a 1,024-line queue and writes each line with one write call, so lines do not interleave with tracing output in the process.

- **Shared stdout.** Diagnostics share the stdout pipe and its in-process lock with tracing logs. A stalled collector can delay request-path tracing writes.
- **Best effort.** Queue overflow, encoding failures, write failures, worker failures, and shutdown deadlines are counted as bounded diagnostics outcomes. The committed relational row remains available when stdout delivery fails.
- **Process death.** Graceful shutdown drains the queue for up to two seconds. A `SIGKILL` or OOM kill can lose queued lines; those lines are not a second source of the event fields.

### Metrics and alerts

| Series | Labels |
| --- | --- |
| `cc_lb_request_diagnostics_lines_total` | `outcome`: `written`, `encode_error`, `write_error` |
| `cc_lb_dropped_events_total` | `reason`: diagnostics queue, worker, and shutdown reasons |

Alert on increases in diagnostics drop reasons and on `encode_error` or `write_error`. The relational event row is the source of truth; the stdout line is an operational observation stream.

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
