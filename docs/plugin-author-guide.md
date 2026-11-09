# cc-lb Plugin Author Guide

This guide describes the current wasmtime plugin contract for cc-lb plugin
authors. It covers the published crates, metadata requirements, per-hook wire
versioning, layout fingerprints, upload admission, and conformance testing.

The focused crate READMEs are also useful while building plugins:

- [`cc-lb-plugin-wire`](../crates/cc-lb-plugin-wire/README.md)
- [`cc-lb-pdk-wasmtime`](../crates/cc-lb-pdk-wasmtime/README.md)
- [`cc-lb-pdk-wasmtime-macros`](../crates/cc-lb-pdk-wasmtime-macros/README.md)
- [`cc-lb-plugin-conformance`](../crates/cc-lb-plugin-conformance/README.md)

## Contents

1. [Runtime model](#runtime-model)
2. [Authoring a plugin](#authoring-a-plugin)
3. [Provider-qualified handlers](#provider-qualified-handlers)
4. [Hook contracts](#hook-contracts)
5. [Owned vs view mode](#owned-vs-view-mode)
6. [Pure vs stateful dispatch](#pure-vs-stateful-dispatch)
7. [Schema hash + bumping the wire](#schema-hash--bumping-the-wire)
8. [Admin upload](#admin-upload)
9. [Operational invariants](#operational-invariants)

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
flag. They relay chunks immediately when no buffered hook is installed; a
buffered hook receives the complete body within the request path's body cap.
JSON audio responses retain the ordinary buffered response behavior.

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

When a request asks for streaming but the upstream returns a non-SSE body,
the host buffers the complete body within the configured response cap and
calls the buffered response-transform hook once. Genuine SSE responses still
use the SSE event-transform hook, and responses without a buffered hook are
relayed as streams. A body over the cap becomes a 502 response without
invoking the hook.

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

## Layout Fingerprint (`WireSchema`)

Each hook wire type carries a layout fingerprint. `cc-lb-plugin-wire` builds a
canonical descriptor string from the type name, fields, variants, and field
types, and embeds `BLAKE3(descriptor)` as a 32-byte fingerprint.
`#[cc_lb_plugin]` embeds the fingerprint of each handler's request type in the
plugin.

Plugin authors do not edit schema tags or manual hashes. A field edit naturally
changes the descriptor and the embedded fingerprint. During admission, the host
compares the plugin's embedded fingerprint with the current host fingerprint for
the requested hook and wire version. Mismatches are rejected as `invalid_wasm`.

## Runtime Probe (`admit_wasm`)

Static metadata and fingerprints do not catch every integration bug. The host
therefore runs `admit_wasm` for uploads, which compiles the module and executes
a canonical sample payload for each declared hook and wire version. Filter
probes use a V1 request with `service_tier = Some("priority")`.

The runtime probe catches issues such as:

- missing allocator exports
- mismatched `cc_lb_alloc` or `cc_lb_free` behavior
- guest encoding mistakes
- hook export traps
- response buffers that cannot be decoded as the expected rkyv type

The probe runs before the upload is accepted for dispatch.

## Building the Wasm

Install the target once in your development environment:

```bash
rustup target add wasm32-unknown-unknown
```

Build the plugin artifact:

```bash
cargo build --release --target wasm32-unknown-unknown
```

The artifact is usually under:

```text
target/wasm32-unknown-unknown/release/<crate_name>.wasm
```

## Uploading

Upload through the admin API:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F "bytes=@target/wasm32-unknown-unknown/release/my_plugin.wasm" \
  -F "original_filename=my-plugin.wasm" \
  "http://localhost:$ADMIN_PORT/admin/v1/plugins/wasm"
```

Successful upload responses include:

```json
{
  "sha256_hex": "...",
  "id": "...",
  "size_bytes": 12345,
  "original_filename": "my-plugin.wasm",
  "revision": 1,
  "idempotent": false
}
```

The registry list endpoint surfaces the persisted author metadata, including
`description`, `usage`, and `hook_metadata`:

```json
{
  "entries": [
    {
      "id": "...",
      "sha256_hex": "...",
      "name": "my-plugin",
      "description": "Accepts every upstream candidate.",
      "usage": "Use as a smoke-test router filter.",
      "hook_metadata": {
        "filter": {
          "wire_version": 1,
          "description": "Accepts all candidates without modification.",
          "usage": "Attach to a router filter chain for baseline validation."
        }
      }
    }
  ]
}
```

## Conformance Test

Add the conformance harness as a dev-dependency:

```toml
[dev-dependencies]
cc-lb-plugin-conformance = "0.4"
```

Use `ConformanceSuite::from_wasm` when the artifact exports one hook:

```rust
use cc_lb_plugin_conformance::ConformanceSuite;

fn wasm_bytes() -> Vec<u8> {
    std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/target/wasm32-unknown-unknown/release/my_plugin.wasm"
    ))
    .expect("build plugin wasm first")
}

#[test]
fn plugin_is_recognisable_by_current_host() {
    let wasm = wasm_bytes();
    ConformanceSuite::from_wasm(&wasm)
        .with_plugin_name("my-plugin")
        .assert_recognisable_by_current_host();
}

#[test]
fn plugin_passes_boundary_smoke() {
    let wasm = wasm_bytes();
    ConformanceSuite::from_wasm(&wasm)
        .with_plugin_name("my-plugin")
        .run();
}
```

`assert_recognisable_by_current_host()` is the admission gate. It proves the
current host can inspect, compile, fingerprint-check, and probe the plugin.

`run()` builds a live runtime session and performs ABI round-trips using a
canonical payload for each hook's declared wire version. Legacy
`ConformanceSuite::for_filter` and `for_shape` calls retain the Anthropic V1
fixtures; their filter plugin receives the V1 request with
`service_tier = Some("priority")`. Provider-selected suites use the
provider-native fixture for each selected hook's declared V1 or V2 wire
version. It proves the boundary works; it does not replace semantic tests for
your plugin's routing or shaping behavior.

### Provider-qualified conformance workflow

Use the same conformance harness for each provider you support. Select
`ProviderFamily::Anthropic`, `ProviderFamily::Openai`, or
`ProviderFamily::Gemini` on the suite builder, then run the admission check
and boundary smoke. The selected provider resolves only that provider's
qualified exports. For Anthropic, the harness also falls back to the bare
legacy export names.

For example, a shape-slot test can select OpenAI explicitly:

```rust
use cc_lb_plugin_conformance::prelude::*;

let suite = ConformanceSuite::for_shape_provider(&wasm, ProviderFamily::Openai);
suite.run();
```

Use `for_filter_provider` for a filter slot, or use `with_provider` after
`for_filter` or `for_shape`. The prelude exports provider fixture builders for
OpenAI Responses and Chat Completions, Gemini `generateContent` and
`streamGenerateContent`, and the V2 request/result types. The generic
`sample_*_for` builders select the provider-specific V1 request, response, or
SSE payload when a test needs to exercise a declared V1 hook directly.

Frame-only Shape plugins use the same provider-selected constructor. The
boundary smoke invokes active V2 frame hooks. For semantic tests, pass
`sample_transform_realtime_frame_request_v2(provider)` to
`session.call_transform_realtime_frame` and assert the decoded `Unchanged`,
`Replace`, or `Drop` result. The fixture contains the provider's WebSocket path
and synthetic upstream identity; its direction and payload can be changed.

Provider selection does not choose the wire layout. The harness reads each
selected hook's declared wire version and encodes and decodes the matching
V1 or V2 contract:

- A V1 hook uses the unchanged V1 request and response types.
- V2 shape, buffered-response, and SSE fixtures include a synthetic principal
  and matching `Upstream` and `UpstreamIdentity` values.
- V2 filter fixtures use the V2 filter schema and provider-appropriate
  candidates; that schema has no upstream-identity field.

The provider fixture set exercises the request, response, and streaming
boundaries without contacting a provider:

- **OpenAI** covers Responses and Chat Completions request/response/SSE
  payloads, including the Responses path and representative
  `response.created` event.
- **Gemini** covers native `generateContent` and
  `streamGenerateContent` payloads, response bodies, and streaming events.
  Fixtures use synthetic upstream identities and contain no credential
  material.

For Gemini OAuth Code Assist, a shape plugin may additionally export the
optional `shape_envelope_gemini` hook. The Gemini-selected session calls it
with a `ShapeEnvelopeGeminiRequest` fixture only when the export is present.
When it is absent, the harness skips that call and still requires the
provider's required shape boundary to pass.

Keep the existing `ConformanceSuite::for_filter` and
`ConformanceSuite::for_shape` calls for legacy Anthropic V1 coverage; they
remain source-compatible and default to the Anthropic V1 behavior. Add
provider-selected coverage for every qualified hook you implement, and omit
unsupported hooks instead of adding placeholder handlers. Conformance checks
boundary encoding, decoding, registration, and traps; provider semantics,
network behavior, credentials, and billing belong in separate tests.

## Wire Version Bump Policy

Bump a hook's wire version when the hook request or response layout changes
incompatibly. Examples include adding fields, removing fields, renaming fields,
reordering fields, changing field types, or changing enum variants.

When a future public plugin boundary needs a new incompatible layout, update the
affected hook's version, supported-version list, admission fingerprints, probe
fixtures, and conformance coverage together.
