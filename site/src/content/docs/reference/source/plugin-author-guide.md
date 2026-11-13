---
title: "Plugin author source notes"
description: "Curated source sections for the Wasmtime plugin contract."
slug: docs/reference/source/plugin-author-guide
---

## Runtime model

The host loads each plugin under a single wasmtime engine configured
with `signals_based_traps` and request-path timeout/backpressure
controls rather than Wasmtime instruction metering. Local execution
defaults to on-demand allocation; each hook call gets a fresh `Store`
with `StoreLimits` derived from `runtime.wasmtime.memory_max_pages`,
and the runtime holds a process-wide store budget derived from
`pool_total_core_instances` so on-demand execution cannot instantiate
unbounded concurrent stores. Operators that need Wasmtime's pooling
allocator can opt in with `[runtime.wasmtime] allocation_strategy =
"pooling"` and tune the reservation/guard/pool totals in the same
   section.

## Quick Start

Create a Rust library crate that builds to `wasm32-unknown-unknown`:

A plugin upload travels through three gates before any user request
can reach it:

1. **`inspect_wasm` (host)** — walks raw `.wasm` bytes with
   `wasmparser`. Enforces imports allow-list (no host imports
   accepted), required exports per slot kind, and 32-byte BLAKE3
   schema hash custom section.
2. **`Engine::precompile_module` + `Module::deserialize`** —
   in-process compile (no .cwasm trust-boundary crossing).
3. **`Linker::instantiate_pre`** — produce reusable `InstancePre`
   for hot-path dispatch.

`Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
cc-lb-plugin-wire = "0.9"
cc-lb-pdk-wasmtime = "0.2"
```

Minimal Anthropic filter plugin:

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse, PerCandidateReason};

#[cc_lb_plugin(
    name = "accept-all",
    version = "0.1.0",
    description = "Accepts every upstream candidate.",
    usage = "Use as a smoke-test router filter.",
)]
mod accept_all {
    use super::*;

    #[handler(
        filter,
        provider = "anthropic",
        wire = 1,
        description = "Accepts all candidates without modification.",
        usage = "Attach to a router filter chain for baseline validation.",
    )]
    pub fn filter(req: FilterRequest) -> FilterResponse {
        let results = req
            .candidates
            .into_vec()
            .into_iter()
            .map(|candidate| PerCandidateReason {
                upstream_id: candidate.upstream_id,
                decision: Box::from("accept"),
                reason: Box::from(""),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        FilterResponse { results }
    }
}
```

Build it:

```bash
cargo build --release --target wasm32-unknown-unknown
```

## Authoring a Plugin

The PDK expects one inline module annotated with `#[cc_lb_plugin(...)]`.
Handlers live inside that module and are annotated with `#[handler(...)]`.

Required plugin metadata:

- `name`: stable plugin identity; must match the admin upload `name` field.
- `version`: plugin package or artifact version.
- `description`: short operator-facing summary.
- `usage`: operator-facing deployment guidance.

The macro emits allocator exports, hook exports, one schema custom section per
distinct hook/wire version, and one metadata section. Plugins with only legacy
handlers emit `cc_lb.plugin.v1`. Adding any provider-qualified handler selects
`cc_lb.plugin.v2`, including when the module also contains legacy handlers.

### Provider-qualified handlers

Split provider-specific behavior into separate functions with
`provider = "anthropic"`, `provider = "openai"`, or `provider = "gemini"`.
The attribute selects the export; Rust function names remain author-controlled.
For example, this complete plugin exports request shaping for Anthropic and
OpenAI without claiming any response-transform or Gemini capability:

```rust
extern crate alloc;

use alloc::{format, string::String};
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v2::{ShapeRequest, ShapeResponse};
#[cc_lb_plugin(
    name = "provider-request-shaper",
    version = "0.1.0",
    description = "Routes Anthropic and OpenAI HTTP requests to their API hosts.",
    usage = "Attach to a shape slot; response bodies remain unchanged.",
)]
mod provider_request_shaper {
    use super::*;

    #[handler(
        shape,
        provider = "anthropic",
        wire = 1,
        description = "Builds Anthropic requests.",
        usage = "Use for Anthropic upstreams.",
    )]
    pub fn anthropic_request(req: ShapeRequest) -> ShapeResponse {
        shape_at("https://api.anthropic.com", req)
    }

    #[handler(
        shape,
        provider = "openai",
        wire = 1,
        description = "Builds OpenAI HTTP requests.",
        usage = "Use for OpenAI upstreams.",
    )]
    pub fn openai_request(req: ShapeRequest) -> ShapeResponse {
        // OpenAI OAuth (Codex) uses `{base}/responses`, not `{base}/v1/responses`.
        shape_at("https://chatgpt.com/backend-api/codex", req)
    }

    fn shape_at(base: &str, req: ShapeRequest) -> ShapeResponse {
        let suffix = if base.contains("chatgpt.com/backend-api/codex") && req.path == "/v1/responses" {
            "/responses"
        } else {
            req.path.as_str()
        };
        let mut url: String = format!("{base}{suffix}");
        if let Some(query) = req.query.as_deref().filter(|query| !query.is_empty()) {
            url.push('?');
            url.push_str(query);
        }
        ShapeResponse {
            url: url.into_boxed_str(),
            method: req.method,
            headers: req.headers,
            body: req.body,
        }
    }
}
```

The generated exports are `cc_lb_shape_anthropic` and `cc_lb_shape_openai`;
metadata keys use the matching hook/provider form, such as `shape_anthropic`
and `shape_openai`. Provider selection and wire version are independent:
provider-qualified handlers may declare V1 or V2. A selected V1 handler uses
the unchanged V1 request and response layout; a selected V2 shape,
buffered-response, or SSE handler receives the V2 upstream and upstream
identity fields. V2 filter requests use the V2 filter schema and do not carry
an upstream-identity field. Filter, buffered-response, and SSE handlers use
the same provider suffix convention.
For Gemini OAuth Code Assist requests, a provider-qualified shape plugin may
also declare `shape_envelope_gemini` with `wire = 2`. The host invokes this
optional export after native Code Assist wrapping and before credentials are
applied. It may replace the envelope body or headers, but never the host;
missing export means no envelope call.
Principal claims are currently empty on this wire boundary because the
authenticated principal record does not expose a cheap claims projection;
plugins must not infer claims from the principal id or credential headers.

Each provider/hook combination is optional. Omit unsupported hooks instead of
writing placeholder no-ops. Without a request hook, the host uses the selected
upstream's native request dialect; independent buffered-response, SSE, or
Realtime/Live frame hooks can still run. Without a response hook, that response
passes through without a guest call. OpenAI Responses WebSocket turns use the
OpenAI shape hook for each `response.create` frame. The shaped body is sent
upstream; the shaped URL and
method are ignored because the host owns the WebSocket connection. Shaped
headers are applied when the provider connection opens when possible. The
OpenAI SSE-event hook also receives each upstream WebSocket frame as an SSE
event whose `data` is the frame JSON and whose event name is the frame's
`type`; `Unchanged`, `Replace`, and `Drop` map to unchanged, replacement, and
dropped WebSocket frames. Buffered-response transforms do not apply to frames.
Shape failures fall back to the raw frame as they do for HTTP. SSE transform
failures emit a WebSocket error event and terminate that turn.

### Realtime and Live frame transforms

Provider-qualified `transform_realtime_frame` handlers use wire version 2 and
receive `TransformRealtimeFrameRequest`. The request identifies the provider
upstream, principal, direction (`setup`, `client_to_provider`, or
`provider_to_client`), payload kind (`json`, `text`, or `binary`), and raw
payload bytes. A handler returns `Unchanged`, `Replace { kind, payload }`, or
`Drop`. The host owns the WebSocket URL, method, credentials, and connection;
the hook can only transform or drop individual frames. Setup and every data
frame are transformed independently, so the host never buffers a whole
session. Invalid replacement encoding or a hook error terminates the relay
with a proxy error.

An active frame hook alone is sufficient for a provider-qualified Shape plugin.
It is selected only for its declared provider; absent and `mode: noop` frame
hooks do not install a transform. No request, buffered-response, or SSE handler
is required.

For Gemini OAuth, Code Assist wrapping is removed before a custom buffered
response hook runs, including `streamGenerateContent` JSON-array responses.

OpenAI audio endpoints also stream non-JSON output without a request `stream`
flag. They relay chunks immediately without buffering or invoking an installed
`transform_response` hook. JSON audio responses (`application/json` or `+json`)
retain the ordinary buffered response behavior and invoke the hook when installed.

### Legacy compatibility and migration

Omitting `provider` preserves the unqualified export and V1 metadata. All
legacy exports, including `cc_lb_filter`, are Anthropic-only. Valid existing
artifacts need no rebuild for Anthropic; a legacy artifact intentionally used
for OpenAI or Gemini must add qualified functions to keep handling that traffic.

To migrate, replace each legacy handler's attribute with the corresponding
`provider = "anthropic"` attribute, then add separate OpenAI or Gemini functions
only for hooks you support. Reuse ordinary Rust helpers where logic is shared.
The macro rejects duplicate `(hook, provider)` pairs and legacy/qualified
Anthropic overlap. A legacy hook may coexist with qualified OpenAI or Gemini
hooks. V1 wire layouts and fingerprints are unchanged; provider qualification
does not require a new `wire` version.

Operators receive the non-blocking `legacy_plugin_anthropic_only` chain warning
when a legacy-only attached artifact coexists with enabled, allowed OpenAI or
Gemini upstreams. Migrate the artifact rather than relying on cross-provider
legacy dispatch.

## Hook Contracts

cc-lb supports two plugin slot kinds. A wasm artifact may implement one or more hooks; upload registers the artifact for every slot whose hook it exports.

| Slot | Legacy export | Provider-qualified exports | Request type | Response type | Use |
|---|---|---|---|---|---|
| `filter` | `cc_lb_filter` | `cc_lb_filter_{anthropic,openai,gemini}` | `v1::FilterRequest` | `FilterResponse` | Keep or reject upstream candidates. |
| `shape` | `cc_lb_shape`<br>`cc_lb_transform_response`<br>`cc_lb_transform_sse_event` | `cc_lb_{shape,transform_response,transform_sse_event}_{anthropic,openai,gemini}` | `ShapeRequest`<br>`TransformResponseRequest`<br>`TransformSseEventRequest` | `ShapeResponse`<br>`TransformResponseResult`<br>`TransformSseEventResult` | Provider-specific request and response processing. Legacy shape is Anthropic-only. |

Before encoding filter requests on V1, V2, or V3, the host removes query
segments whose parameter name is exactly `key`, case-insensitively. Other
segments retain their order and encoding; names are not percent-decoded.
An absent query stays absent, and an all-`key` query remains a present empty
string. Provider credential headers are also removed before dispatch.

Filter example:

```rust
use alloc::boxed::Box;
use alloc::vec::Vec;
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse, PerCandidateReason};

#[handler(
    filter,
    wire = 1,
    description = "Rejects candidates without observed cache state.",
    usage = "Attach before fallback filters in router chains.",
)]
pub fn filter(req: FilterRequest) -> FilterResponse {
    let results = req
        .candidates
        .into_vec()
        .into_iter()
        .map(|candidate| PerCandidateReason {
            upstream_id: candidate.upstream_id,
            decision: if candidate.observed_at_unix_secs > 0 {
                Box::from("accept")
            } else {
                Box::from("reject")
            },
            reason: Box::from("cache-observation-required"),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();

    FilterResponse { results }
}
```

The host keeps a candidate only when `decision` is `"accept"`. Every other
candidate is dropped, whether the plugin rejects it or leaves it out of
`results`, so an empty `results` rejects the whole candidate set. The host
writes each non-empty `reason` to the router audit log as `{upstream_id}:
{reason}`; pass an empty string when the decision needs no explanation.

Shape example (legacy unified slot):

The unqualified `shape` handler is a legacy Anthropic-only contract. A legacy
Shape plugin must implement all three wire hook functions: request shaping,
buffered response transform, and SSE event transform. Provider-qualified
handlers are independent: implement only the `(hook, provider)` combinations
your plugin supports, and omit unused request or response hooks.

When a request asks for streaming and the upstream returns a finite non-SSE JSON
body (`application/json` or `+json`, including chunked JSON without `Content-Length`),
the host buffers the complete body within the configured response cap and calls
the installed buffered response-transform hook once. A body over the cap becomes
a 502 response without invoking the hook. SSE responses (`text/event-stream`) use
the SSE event-transform hook. Non-SSE binary or non-JSON streaming responses
(`audio/*`, `application/octet-stream`, etc.) relay chunks immediately without
buffering or invoking the buffered hook; responses without a buffered hook also
relay as streams.
Buffered `transform_response` hooks run only on semantic JSON bodies
(`application/json` or `+json`) and Gemini OAuth envelopes. Other non-JSON bodies
skip the buffered hook on both streaming and non-streaming routes.

Prompt-cache identity is computed before shaping. For a deterministic shape plugin,
the shaped request must preserve the provider-effective cached prefix for the
modeled upstream. For Anthropic API-key/OAuth upstreams on
`https://api.anthropic.com` with the default port and no userinfo, the first exact
`system[0]` billing-header attribution block is ignored for cache identity even
when a shape plugin is configured. Do not copy that ignored attribution into
other cached content or redirect the route to a custom provider; configure and
model the actual provider upstream.

Here is an example of a complete shape plugin that shapes requests and provides explicit no-op handlers for response transformation:

```rust
use cc_lb_pdk_wasmtime::{cc_lb_plugin, handler};
use cc_lb_plugin_wire::v1::{
    Header, ShapeRequest, ShapeResponse,
    TransformResponseRequest, TransformResponseResult,
    TransformSseEventRequest, TransformSseEventResult,
};

#[cc_lb_plugin(
    name = "request-only-shaper",
    version = "0.1.0",
    description = "Shapes requests and passes responses through unchanged.",
    usage = "Attach to shape slots.",
)]
mod request_only_shaper {
    use super::*;

    #[handler(
        shape,
        wire = 1,
        description = "Builds the upstream request.",
        usage = "Attach to shape slots.",
    )]
    pub fn shape(req: ShapeRequest) -> ShapeResponse {
        ShapeResponse {
            url: Box::from(format!("https://api.anthropic.com{}", req.path)),
            method: req.method,
            headers: Box::from([Header {
                name: Box::from("content-type"),
                value: Box::from(&b"application/json"[..]),
            }]),
            body: req.body,
        }
    }

    #[handler(
        transform_response,
        wire = 1,
        description = "No-op buffered response transform.",
        usage = "Required by the unified shape slot.",
    )]
    pub fn transform_response(_req: TransformResponseRequest) -> TransformResponseResult {
        TransformResponseResult::Unchanged
    }

    #[handler(
        transform_sse_event,
        wire = 1,
        description = "No-op SSE event transform.",
        usage = "Required by the unified shape slot.",
    )]
    pub fn transform_sse_event(_req: TransformSseEventRequest) -> TransformSseEventResult {
        TransformSseEventResult::Unchanged
    }
}
```

Each handler declares its own `wire = N`. Legacy handlers use `wire = 1`;
provider-qualified handlers use `wire = 2` and the V2 request types.

## Metadata Contract

Plugin and per-hook metadata is required. Legacy-only artifacts emit
`cc_lb.plugin.v1`; any artifact with provider-qualified handlers emits
`cc_lb.plugin.v2`. The host rejects uploads when the selected section is
absent, malformed, incomplete, or contains an unknown hook key. Bare keys in
V2 remain legacy Anthropic-only entries.

Required top-level fields:

- `name`
- `version`
- `description`
- `usage`
- `hooks`

Required per-hook fields:

- `wire_version`
- `description`
- `usage`

Upload rejection names relevant to plugin authors include:

- `missing_part`: a required multipart field is absent.
- `invalid_wasm_magic`: uploaded bytes do not start with the wasm magic.
- `invalid_wasm_length`: uploaded bytes are too short to be wasm.
- `wasm_too_large`: the wasm exceeds the 32 MiB upload limit.
- `invalid_wasm`: wasmtime admission rejected exports, metadata, wire versions,
  fingerprints, imports, or the runtime probe.

## Wire Versioning

Wire versions are independent per hook. The plugin declares the version on each
handler:

```rust
use alloc::boxed::Box;
use alloc::vec::Vec;
use cc_lb_plugin_wire::v1::{FilterRequest, FilterResponse, PerCandidateReason};

#[handler(
    filter,
    wire = 1,
    description = "Filters candidates.",
    usage = "Attach to router filter chains.",
)]
pub fn filter(req: FilterRequest) -> FilterResponse {
    let results = req
        .candidates
        .into_vec()
        .into_iter()
        .map(|candidate| PerCandidateReason {
            upstream_id: candidate.upstream_id,
            decision: Box::from("accept"),
            reason: Box::from(""),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    FilterResponse { results }
}
```

The filter example under [Hook Contracts](#hook-contracts) shows a complete
handler. `service_tier` is the optional tier requested in the inbound request
body; a pre-request filter cannot observe the tier ultimately reported by an
upstream response.

The host maintains supported-version lists per hook:

- `HOST_SUPPORTED_FILTER_VERSIONS`
- `HOST_SUPPORTED_SHAPE_VERSIONS`
- `HOST_SUPPORTED_TRANSFORM_RESPONSE_VERSIONS`
- `HOST_SUPPORTED_TRANSFORM_SSE_EVENT_VERSIONS`

Admission rejects a plugin when the declared hook version is not in the host's
supported list. This rejection happens before the plugin is persisted into a
chain or called on user traffic.
