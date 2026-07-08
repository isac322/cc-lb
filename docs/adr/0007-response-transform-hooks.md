# ADR 0007 — Plugin response transform hooks

- Status: Accepted
- Date: 2026-07-08
- Plan: [.omo/plans/response-transform-hooks.md](../../.omo/plans/response-transform-hooks.md)
- Supersedes: none

## Context

cc-lb plugin hooks currently cover request filtering, upstream-bound request shaping, and observation. They do not cover upstream response mutation.

The immediate defect class is tool-name laundering. A shape plugin can rewrite request tool definitions and tool-result references from OpenCode-style lower-case names such as `bash` and `read` to Claude-style PascalCase names such as `Bash` and `Read`. Upstream responses can then return `tool_use.name: "Bash"`. Without a response-side reverse mapping, downstream OpenCode dispatch still expects `bash`, so the request-side mapping can make tool dispatch fail.

The existing `ShapeResponse` type is not an HTTP response. It is the output of the `shape` hook and represents the upstream-bound request produced by the plugin. Renaming or extending it would change the rkyv schema fingerprint and break existing shape plugins.

The response path has two distinct execution models:

- buffered non-SSE responses, usually JSON bodies; and
- streaming `text/event-stream` responses, where correctness depends on complete SSE event framing, flushing, cancellation, and backpressure.

A publish-grade interface must keep host-owned transport correctness while giving plugins a typed semantic payload to rewrite.

## Decision

Add two new additive plugin hook kinds. Do not change the existing `Filter`, `Shape`, or `Observe` contracts.

1. Add a buffered response transform hook exported as `cc_lb_transform_response`.
2. Add an SSE event transform hook exported as `cc_lb_transform_sse_event`.

Dispatch uses the actual upstream response framing, not the client request's `stream` flag. A response with `Content-Type: text/event-stream` uses the SSE event hook. Other buffered bodies use the buffered response hook when body size and decoding constraints allow it. Non-SSE binary or indefinite streaming responses pass through unless a future hook explicitly supports them.

### Buffered response hook

The buffered hook receives a decoded semantic body and trimmed stable context:

```rust
pub struct TransformResponseRequest {
    pub request_id: Box<str>,
    pub principal: Principal,
    pub upstream: Upstream,
    pub request_method: Box<str>,
    pub request_path: Box<str>,
    pub canonical_model_id: Box<str>,
    pub response_status: u16,
    pub response_headers: Box<[Header]>,
    pub body: Box<[u8]>,
}

pub enum TransformResponseResult {
    Unchanged,
    Replace {
        status: Option<u16>,
        headers: Option<Box<[Header]>>,
        body: Option<Box<[u8]>>,
    },
}
```

The host owns response safety:

- decode compressed upstream payloads before invoking the plugin;
- never expose compressed bytes to the plugin;
- strip or recompute `Content-Length` when the body changes;
- strip hop-by-hop, signing, `x-cc-lb-*`, and proxy-owned rate-limit/limit headers from plugin output;
- reattach proxy-owned headers after transform; and
- normalize transformed output to identity encoding for V1, with future recompression allowed as an optimization.

The hook can see upstream-produced 2xx and upstream-produced 4xx/5xx responses by default. Proxy-generated 502/503/bulkhead/timeout failures are not transformable by default. Buffered plugin trap/error fails open and returns the original upstream response.

### SSE event hook

The SSE hook receives exactly one complete parsed SSE event at a time. It does not receive raw TCP chunks and it never buffers the whole stream.

```rust
pub struct TransformSseEventRequest {
    pub request_id: Box<str>,
    pub principal: Principal,
    pub upstream: Upstream,
    pub request_method: Box<str>,
    pub request_path: Box<str>,
    pub canonical_model_id: Box<str>,
    pub response_status: u16,
    pub response_headers: Box<[Header]>,
    pub event: SseEvent,
}

pub struct SseEvent {
    pub event: Box<str>,
    pub data: Box<[u8]>,
}

pub enum TransformSseEventResult {
    Unchanged,
    Replace { events: Box<[SseEvent]> },
    Drop,
}
```

The host owns SSE boundary detection, framing, flushing, cancellation, backpressure, and `\n\n` re-emission. Plugins rewrite semantic event data only. For the tool-name laundering use case, the plugin rewrites `event: content_block_start` data when `data.content_block.name == "Bash"`; it does not rewrite `input_json_delta`, because that event carries argument fragments and no tool name.

For V1, SSE streams use a pinned guest instance per in-flight stream with a hard concurrent stream instance cap/backpressure. Opaque state blobs per event are rejected for V1 because they add rkyv serialization overhead and poor plugin-author ergonomics.

If an SSE transform fails before any transformed bytes are emitted, the host may fail open to raw passthrough. After any transformed event has been emitted, raw fallback is forbidden; the host terminates the stream with a classified transform error. Mixed transformed/raw tool names are unsafe.

### Accounting and observability

Usage, billing, quota, and cache accounting parse upstream truth before transformation. Post-transform client-visible bytes/events are valid for logs, debugging, and end-to-end contract checks, but not as the billing source of truth.

### ABI and versioning

The implementation adds new hook kinds and independent V1 schemas:

- `HookKind::TransformResponse`;
- `HookKind::TransformSseEvent`;
- `HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS`;
- `HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS`;
- schema custom sections such as `cc_lb.schema.transform_response.v1`; and
- schema custom sections such as `cc_lb.schema.transform_sse_event.v1`.

Each request/result type has owned and borrowed `*Ref<'a>` twins with identical archived bytes, following the existing filter/shape/observe rkyv pattern. PDK support includes `run_transform_response`, `run_transform_sse_event`, `#[handler(transform_response)]`, and `#[handler(transform_sse_event)]`.

Adding these hook kinds is backward-compatible. Existing plugins do not declare these exports, so the host treats absence as no transform and preserves existing byte-equivalent passthrough behavior. Existing `ShapeRequest` and `ShapeResponse` are not renamed, extended, or field-modified.

## Consequences

### Positive

- Fixes the response half of tool-name laundering without coupling it to request shaping.
- Keeps response transport correctness in the host, where framing, headers, compression, and backpressure are already owned.
- Preserves deployed plugin compatibility by adding new hooks instead of mutating existing schemas.
- Gives plugin authors two precise surfaces instead of a misleading raw-stream API.
- Keeps accounting trustworthy by billing upstream truth rather than plugin-mutated bytes.

### Negative

- The response lifecycle must retain a trimmed request/upstream context until response processing.
- SSE transforms add per-event guest-call latency and may occupy pinned guest instances for long-lived streams.
- V1 identity output can change client-visible `Content-Encoding` when a compressed upstream response is transformed.
- Mid-stream transform failure is necessarily asymmetric: fallback is possible only before transformed bytes are emitted.

### Neutral

- Original request bodies are not passed to response hooks by default. A future opt-in capability can be added if a response plugin genuinely needs them.
- Header/status replacement is allowed only through host sanitization; plugins propose end-to-end fields, while the host keeps proxy-owned fields authoritative.

## Alternatives considered

| Alternative | Rejection reason |
| --- | --- |
| Extend `shape()` or rename `ShapeResponse` | Breaks existing rkyv fingerprints and conflates upstream-bound request shaping with downstream response mutation. |
| One generic `cc_lb_transform_stream` hook | Implies raw chunk/transport control. The safe abstraction is complete SSE events under host-owned framing and backpressure. |
| Transform raw TCP chunks | Tool-use JSON can span chunks or multiple SSE lines; raw chunk transforms corrupt framing and break UTF-8/event boundaries. |
| Buffer whole SSE streams | Destroys streaming latency and backpressure. |
| Per-event opaque state blobs | Adds serialization overhead and poor ergonomics; pinned per-stream guest instances are simpler for V1. |
| Account post-transform bytes | Lets plugins alter billing/quota truth and diverge from upstream provider usage. |
| Require upstream identity encoding at the API level | Too restrictive. Compressed upstream responses are allowed; the host must decode before plugin input and normalize/re-encode output. |

## Rollout and QA requirements

- Add wire schema, PDK, macro, runtime admission, and conformance tests for both new hooks.
- Add engine tests proving no transform preserves existing passthrough behavior.
- Add buffered JSON tests proving a plugin can rewrite `content[].type == "tool_use"` names from PascalCase to lower-case and that `Content-Length`/encoding headers remain correct.
- Add SSE tests proving a plugin can rewrite `content_block_start.content_block.name` and that event framing, order, cancellation, and terminal events remain valid.
- Add failure tests proving buffered fail-open and SSE no-raw-after-transformed behavior.
- Add accounting tests proving billing/quota reads upstream truth even when client-visible bytes are transformed.
- Run proxy-path QA through `/v1/messages` for buffered and streaming responses using the client-visible proxy surface.
- Before creating the implementation PR, show the PR title/body and receive explicit final approval; after PR creation, monitor checks until the PR is mergeable or an exact blocker is reported.

## Out of scope

- A raw arbitrary-stream transform hook.
- Passing the full original request body to response hooks by default.
- Transforming proxy-generated internal errors by default.
- Changing existing filter/shape/observe schemas.
- Implementing the subscription-launderer reverse mapper itself in this ADR; this ADR defines the interface it will use.
