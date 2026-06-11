# cc-lb Plugin Author Guide

This guide is for engineers writing or porting a wasm plugin against the cc-lb plugin handshake protocol. It covers the plugin lifecycle, the procedural-macro entry points, every wire function the host calls, the identity / handshake / self-check contracts, and the admin surface for shipping a plugin into a running cc-lb deployment.

The canonical sources are the crates under `crates/cc-lb-pdk` (proc-macros) and `crates/cc-lb-plugin-wire` (no_std wire types + handshake protocol). When this document and the code disagree, the code wins.

## Contents

1. [Quick start](#quick-start)
2. [Plugin lifecycle](#plugin-lifecycle)
3. [PDK macros](#pdk-macros)
4. [Identity and custom section](#identity-and-custom-section)
5. [Handshake protocol](#handshake-protocol)
6. [Self-check](#self-check)
7. [Wire functions reference](#wire-functions-reference)
8. [Fallback policy](#fallback-policy)
9. [Guardrail limits](#guardrail-limits)
10. [Building the wasm artifact](#building-the-wasm-artifact)
11. [Registering and operating a plugin](#registering-and-operating-a-plugin)
12. [Anti-patterns](#anti-patterns)
13. [Source-of-truth index](#source-of-truth-index)

---

## Quick start

The reference implementation is the cache-aware filter at `plugins/router/cache-aware/src/lib.rs`. Minimal plugin skeleton:

```rust
use cc_lb_plugin_wire::v3::filter::{FilterRequest, FilterResponse, PerCandidateReason};
use std::convert::Infallible;
use uuid::Uuid;

#[cc_lb_pdk::plugin(name = "my-router", version = "0.1.0")]
mod plugin {
    use super::*;

    #[cc_lb_pdk::handler(name = "filter", versions = [1])]
    pub(super) fn filter(request: FilterRequest) -> Result<FilterResponse, Infallible> {
        let kept_upstream_ids = request
            .candidates
            .iter()
            .filter_map(|candidate| Uuid::parse_str(&candidate.upstream_id).ok())
            .collect::<Vec<_>>();
        Ok(FilterResponse {
            kept_upstream_ids,
            reason: "accept all candidates".to_owned(),
            per_candidate_reasons: request
                .candidates
                .iter()
                .filter_map(|candidate| {
                    Some(PerCandidateReason {
                        upstream_id: Uuid::parse_str(&candidate.upstream_id).ok()?,
                        kept: true,
                        reason: "accepted by example filter".to_owned(),
                    })
                })
                .collect(),
        })
    }
}
```

`Cargo.toml`:

```toml
[package]
name = "cc-lb-router-my-router"
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"
publish = false

[dependencies]
cc-lb-pdk = { path = "../../../crates/cc-lb-pdk" }
cc-lb-plugin-wire = { path = "../../../crates/cc-lb-plugin-wire" }

[lib]
crate-type = ["cdylib"]
```

Build:

```bash
cargo build -p cc-lb-router-my-router --target wasm32-unknown-unknown --release
```

Register against a running cc-lb:

```bash
curl -X POST http://127.0.0.1:9091/admin/plugins \
  -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  -F bytes=@target/wasm32-unknown-unknown/release/cc_lb_router_my_router.wasm \
  -F name=my-router \
  -F original_filename=cc_lb_router_my_router.wasm
```

## Plugin lifecycle

A plugin moves through five host-driven phases. The host enforces each phase strictly; failing any phase rejects the plugin instead of partially activating it.

1. **Static identity (L1).** Before any wasm instantiation the host reads the `cc_lb.plugin.v1` custom section with `wasmparser`. The section must appear exactly once, fit within `CUSTOM_SECTION_MAX_SIZE` (64 KiB), and carry an `8`-byte magic that matches `CC_LB_PLUGIN_MAGIC` plus a JSON identity object. Names must match `^[a-z][a-z0-9_-]*$` and be at most 64 bytes; versions are free-form strings up to 32 bytes.
2. **Handshake (L2).** The host instantiates the wasm under Extism with `disallow_all_hosts()` and fuel + wall-time caps, then calls the `cc_lb_handshake` export with a serialized `HandshakeOffer`. The plugin must reply with a `HandshakeAccept` whose `chosen_versions` are the highest version in the intersection of host offer and plugin support for each declared function. The host re-computes `max(intersection)` and rejects accepts that select anything lower; it also cross-checks declared functions against the wasm export table with `Plugin::function_exists`.
3. **Self-check (L3).** The host calls the `cc_lb_self_check` export with the list of negotiated function names. The plugin serializes and deserializes the `dry_run_sample()` request and response of each wire function in turn and reports `status = "success"` if every round-trip is byte-identical and free of side effects. The host rejects `status = "failure"` outright; success with any failures is also rejected. Handler functions themselves are NOT invoked during self-check — only the wire types' serde round-trip is exercised.
4. **Persist (L4).** On successful identity + handshake + self-check, the host computes the wasm SHA-256, stores the blob in `plugin_registry_blobs`, and upserts an active `PluginRegistryRecord` (postgres `plugin_registry` table or redb `PLUGIN_REGISTRY` table). The host records `host_offer_hash` so a later cc-lb upgrade that changes the offer triggers a re-handshake automatically.
5. **Dispatch (L5).** At request time the host loads the registered `AugmentedMetadata` (handshake-derived) and routes each wire-function call through `cc-lb-runtime-extism::dispatch`. Every dispatch is wrapped in `catch_unwind` and timed; on panic, error, or timeout the host applies the compile-time `FALLBACK` policy for that function (see [Fallback policy](#fallback-policy)).

At server startup the host replays handshake for every persisted plugin (`startup_handshake` loop). If `host_offer_hash` matches and `--skip-handshake-if-fresh` is set (default `true`), the plugin is loaded from the in-process cache without re-running the wasm; otherwise the loop performs a fresh handshake under a parallelism cap (`STARTUP_HANDSHAKE_PARALLEL_MAX = 8`) and budget. The optional `--force-handshake` flag bypasses the freshness fast-path and forces every plugin to re-handshake on boot.

When cc-lb is restarted onto a fresh data directory but the operator already had plugins uploaded via the legacy `POST /admin/v1/plugins/wasm` path, the startup `bridge_legacy_wasm_registry` step picks up those rows from `wasm_registry_v2`, fetches each blob, and runs the full handshake + self-check + register pipeline, promoting them into the new validated registry with no manual action.

## PDK macros

The proc-macro crate `cc-lb-pdk` exposes exactly two attributes.

### `#[cc_lb_pdk::plugin]`

Place on an inline module that contains your `#[handler]` functions.

```rust
#[cc_lb_pdk::plugin(name = "my-plugin", version = "0.1.0", requires = ["log"])]
mod plugin { ... }
```

Arguments:

- `name` (required): plugin identity name. Must match `^[a-z][a-z0-9_-]*$` and be at most 64 bytes.
- `version` (required): plugin identity version. Free-form string up to 32 bytes; semver is conventional but not enforced.
- `requires` (optional): array of host capability names this plugin needs at runtime. Each name matches `^[a-z][a-z0-9_]*$`, max 64 bytes, deduplicated, and is sent as part of the handshake `required_capabilities`.

The macro generates: the `cc_lb.plugin.v1` custom section with serialized `PluginIdentity`, the `cc_lb_handshake` Extism export with downgrade-safe version negotiation, the `cc_lb_self_check` Extism export that round-trips every handler's wire types, and per-handler envelope wrappers.

### `#[cc_lb_pdk::handler]`

Place on a free function inside the `#[plugin]` module.

```rust
#[cc_lb_pdk::handler(name = "route", versions = [1])]
fn route(request: RouteRequest) -> Result<RouteResponse, Infallible> { ... }
```

Arguments:

- `name` (required): wire function name. Must match one of the canonical names (`route`, `shape`, `sign`, `build_signer`, `normalize_error`, `on_unauthorized`, `observe`).
- `versions` (required): array of supported envelope versions for this function (e.g. `[1]`). At least one version is required.

Signature constraints (enforced by the macro):

- Exactly one parameter, whose type is the wire function's `Request` (see [Wire functions reference](#wire-functions-reference)).
- Return type `Result<Response, E>` where `E: std::fmt::Display`. The handler's error is converted to a wire-level error by the generated envelope wrapper.
- Free function, no `&self`. Module-level statics for state are allowed.

You may declare multiple `#[handler]` functions in one `#[plugin]` module; each registers an additional wire function in the handshake.

## Identity and custom section

The plugin's identity travels in a single `cc_lb.plugin.v1` custom section. The PDK emits this for you; the shape is:

```rust
pub struct PluginIdentity {
    pub magic: [u8; 8],          // must equal CC_LB_PLUGIN_MAGIC
    pub abi_envelope: u32,
    pub plugin_name: String,
    pub plugin_version: String,
}
```

with `CC_LB_PLUGIN_MAGIC = [0xCC, 0x1B, 0x70, 0x10, 0x00, 0x01, 0x00, 0x00]`.

Validation rules enforced by `IdentityError`:

- `magic` must equal `CC_LB_PLUGIN_MAGIC` exactly.
- `plugin_name` non-empty, max 64 bytes, matches `^[a-z][a-z0-9_-]*$`.
- `plugin_version` non-empty, max 32 bytes.
- `abi_envelope` is currently `1` (the v1 wire envelope).
- Section payload (JSON) is at most `CUSTOM_SECTION_MAX_SIZE` (64 KiB) and contains exactly four top-level fields (`deny_unknown_fields` is enforced).
- The custom section appears exactly once. Duplicates fail L1.

## Handshake protocol

The host calls `cc_lb_handshake` with a JSON-encoded `HandshakeOffer`:

```rust
pub struct HandshakeOffer {
    pub handshake_schema_version: u32,                   // currently HANDSHAKE_SCHEMA_VERSION_V1 (1)
    pub envelope_version: u32,
    pub function_versions: BTreeMap<String, Vec<u32>>,   // host-supported versions per function
    pub host_capabilities: BTreeSet<String>,
}
```

The plugin (PDK-generated) replies with:

```rust
pub struct HandshakeAccept {
    pub handshake_schema_version: u32,                   // must equal offer's schema version
    pub envelope_version: u32,
    pub chosen_versions: BTreeMap<String, u32>,          // chosen[fn] = max(host_versions ∩ plugin_versions)
    pub plugin_supported: BTreeMap<String, Vec<u32>>,    // plugin's full version map
    pub implemented_functions: BTreeSet<String>,         // matches the #[handler] set
    pub required_capabilities: BTreeSet<String>,         // mirrors #[plugin(requires = ...)]
}
```

Host-side `HandshakeAccept::validate_against_offer` rejects:

- Mismatched `handshake_schema_version`.
- A function in `chosen_versions` whose key is missing from either `plugin_supported` or the offer's `function_versions`.
- A `chosen` version not contained in the offer's version list for that function.
- A `chosen` version strictly less than `max(offer_versions ∩ plugin_supported[fn])` — this is the downgrade guard; the host recomputes the maximum intersection and rejects any lower selection.

Additional host checks before accepting:

- `disallow_all_hosts()` is enabled for the handshake call, so any host function call from the plugin during handshake traps.
- `Plugin::function_exists` is called for every function in `chosen_versions` to ensure the wasm actually exports it.
- The handshake output is capped at `HANDSHAKE_OUTPUT_MAX_BYTES` (32 KiB), with a `HANDSHAKE_WALL_MS` (200 ms) wall-clock limit and `HANDSHAKE_FUEL` (10 M instructions) ceiling.

Plugin authors do not write the handshake themselves; the PDK macro emits a correct one. Just declare your handlers and capabilities accurately.

## Self-check

The host calls `cc_lb_self_check` after handshake with a `SelfCheckRequest` containing the negotiated function names. The PDK-generated implementation:

1. For each function name, serializes and deserializes `Request::dry_run_sample()` and asserts the round-trip is byte-identical.
2. Does the same for `Response::dry_run_sample()`.
3. Returns:

```rust
pub struct SelfCheckResponse {
    pub status: SelfCheckStatus,                         // Success or Failure
    pub failures: Vec<SelfCheckFailure>,                 // empty on Success
    pub completed_at: i64,                               // unix seconds
}
```

The host (`crates/cc-lb-runtime-extism/src/self_check.rs`) rejects in any of these cases:

- `status = Success` but `failures` non-empty.
- `status = Failure` (the executor itself errors with `SelfCheckExecutionError::FailureStatus`; this is the only successful self-check outcome).
- The response exceeds `SELF_CHECK_OUTPUT_MAX_BYTES` (8 KiB).
- The plugin attempts a host call (`disallow_all_hosts` is in effect for self-check too).
- The call exceeds `SELF_CHECK_WALL_MS` (200 ms) or `SELF_CHECK_FUEL` (10 M instructions).

If you supply a custom wire type via a fork, ensure your `dry_run_sample` returns a value that round-trips through your serde derives. Avoid `Default::default()` for fields like `HeaderName` whose `Default` is meaningless; the canonical wire types use explicit sentinel values (e.g. `String::new()`, zero UUIDs).

## Wire functions reference

The seven wire functions are defined under `crates/cc-lb-plugin-wire/src/v1/`. Each declares a `WireFunction` impl whose constants the host enforces.

| Function | Constant FALLBACK | Purpose |
|---|---|---|
| `route` | `UseDefault` | Pick an `upstream_id` from the candidate list. Failure falls back to host default routing. |
| `shape` | `FailRequest` | Translate a downstream request into the URL/method/headers/body the host will send upstream. Failure aborts the request. |
| `sign` | `FailRequest` | Apply credentials to a shaped request. Failure aborts the request (never silently drop signing). |
| `build_signer` | `FailRequest` | Construct or refresh a signer's opaque `signer_state` blob. Failure aborts. |
| `normalize_error` | `PassThrough` | Optionally rewrite an upstream error body. Failure leaves the original body unchanged. |
| `on_unauthorized` | `PassThrough` | React to a 401 from the upstream (e.g. refresh credentials). Failure passes the 401 through. |
| `observe` | `SilentSkip` | Receive lifecycle events (request started, chunk, finished, error). Failure is swallowed. |

Authoritative struct definitions live in:

- `route` — [route.rs](../crates/cc-lb-plugin-wire/src/v1/route.rs): `RouteRequest { request_id, headers, method, path, query, body_base64, principal, candidates }` → `RouteResponse { upstream_id, dialect, upstream }`.
- `shape` — [shape.rs](../crates/cc-lb-plugin-wire/src/v1/shape.rs): `ShapeRequest { request, upstream, principal }` → `ShapeResponse { url, method, headers, body_base64 }`.
- `sign` — [sign.rs](../crates/cc-lb-plugin-wire/src/v1/sign.rs): `SignRequest { shaped, signer_state }` → `SignResponse { url, method, headers, body_base64 }` (each field optional; `None` means "leave shaped value unchanged").
- `build_signer` — [build_signer.rs](../crates/cc-lb-plugin-wire/src/v1/build_signer.rs): `BuildSignerRequest { upstream, factory_state }` → `BuildSignerResponse { signer_state }`.
- `normalize_error` — [normalize_error.rs](../crates/cc-lb-plugin-wire/src/v1/normalize_error.rs): `NormalizeErrorRequest { status, body_base64 }` → `NormalizeErrorResponse { body_base64: Option<String> }`.
- `on_unauthorized` — [on_unauthorized.rs](../crates/cc-lb-plugin-wire/src/v1/on_unauthorized.rs): `OnUnauthorizedRequest { error, signer_state }` → `OnUnauthorizedResponse { decision, signer_state }`.
- `observe` — [observe.rs](../crates/cc-lb-plugin-wire/src/v1/observe.rs): `ObserveRequest { events }` → `ObserveResponse {}`.

Common subtypes (in [common.rs](../crates/cc-lb-plugin-wire/src/v1/common.rs)):

- `HeaderWire { name, value_base64 }` — header value is always base64-encoded so binary headers survive JSON transport.
- `Principal { id, kind, claims }` — `kind` is one of `api_key`, `oauth_subject`, etc.; `claims` is the only field where `serde_json::Value` is permitted (free-form per-deployment auth claims).
- `RequestWire { request_id, headers, method, path, query, body_base64 }` — the canonical downstream request shape (no host headers).
- `CandidateWire { upstream_id, name, kind, observed_rate_limits, observed_at_unix_secs }` — an upstream candidate; `observed_rate_limits` carries recent host observations so the router can avoid throttled hops.
- `UpstreamWire` — `AnthropicDirect`.
- `DialectBinding` — currently only `SelfReferenced`; the dialect lives with the plugin.
- `ShapedRequestWire { url, method, headers, body_base64 }` — the post-`shape` request, before signing.
- `UpstreamErrorWire { status, body_base64, category }` with `UpstreamErrorCategory` ∈ `{Unauthorized, Retryable, Failed}`.
- `ObserveEventWire` — enum of `RequestStarted | AuthnComplete | UpstreamChosen | Chunk | RequestFinished | Error`.

All wire types use `#[serde(deny_unknown_fields)]`; do not add fields locally, fork the crate to extend.

## Fallback policy

The `FALLBACK` constant on each `WireFunction` impl is enforced by the host at runtime; it is not configurable. The four policies (`crates/cc-lb-plugin-wire/src/wire_function.rs`):

- `UseDefault` — the host runs its built-in default for that function (e.g. accepting all candidates for `filter`).
- `FailRequest` — the host aborts the request with a 5xx and surfaces the failure in observability. Used for everything where silent failure would be a security or correctness violation (`shape`, `sign`, `build_signer`).
- `PassThrough` — the host proceeds as if the plugin returned an "unchanged" response (`normalize_error`: keep the original error body; `on_unauthorized`: surface the 401 to the downstream).
- `SilentSkip` — used by `observe` only; lossy event delivery is acceptable.

The host applies fallback on any of: serialized request rejected by the wasm guest, panic inside the guest (caught via `catch_unwind`), serialization error of the response, wall-clock or fuel exhaustion, or oversized output.

## Guardrail limits

All limits are compile-time constants in `crates/cc-lb-plugin-wire/src/limits.rs`. The most relevant for authors:

| Constant | Value | What it caps |
|---|---|---|
| `CUSTOM_SECTION_MAX_SIZE` | 64 KiB | The `cc_lb.plugin.v1` custom section payload. |
| `PLUGIN_NAME_MAX_BYTES` | 64 | Identity name length. |
| `PLUGIN_VERSION_MAX_BYTES` | 32 | Identity version length. |
| `HANDSHAKE_WALL_MS` | 200 | Wall-clock budget for the handshake call. |
| `HANDSHAKE_FUEL` | 10,000,000 | Fuel ceiling for the handshake call. |
| `HANDSHAKE_OUTPUT_MAX_BYTES` | 32 KiB | Max accepted handshake response. |
| `SELF_CHECK_WALL_MS` | 200 | Wall-clock budget for self-check. |
| `SELF_CHECK_FUEL` | 10,000,000 | Fuel ceiling for self-check. |
| `SELF_CHECK_OUTPUT_MAX_BYTES` | 8 KiB | Max accepted self-check response. |
| `IMPLEMENTED_FUNCTIONS_MAX` | 16 | Max distinct wire functions per plugin. |
| `FUNCTION_VERSIONS_PER_FN_MAX` | 16 | Max versions declared per function. |
| `CAPABILITIES_MAX_COUNT` | 32 | Max `required_capabilities`. |
| `CAPABILITY_NAME_MAX_BYTES` | 64 | Max capability name length. |
| `AUGMENTED_METADATA_MAX_BYTES` | 256 KiB | Cap on the per-plugin metadata blob the host derives from handshake+self-check. |
| `STARTUP_HANDSHAKE_TOTAL_BUDGET_MS` | 60,000 | Total wall budget for the boot-time re-handshake loop. |
| `STARTUP_HANDSHAKE_PARALLEL_MAX` | 8 | Max concurrent boot-time handshakes. |
| `SKIP_HANDSHAKE_IF_FRESH_TTL_SECS` | 7 days | Freshness window for `--skip-handshake-if-fresh`. |

Per-request dispatch enforces these too; the host catches the timeout / fuel-exhaust trap and applies the function's `FALLBACK`. Plan your handlers to finish well below 200 ms.

## Building the wasm artifact

The PDK targets either `wasm32-unknown-unknown` (lighter, no WASI) or `wasm32-wasip1`. Both are accepted by the host. The `wasm32-unknown-unknown` toolchain is the recommended default because the plugin-wire crate is `no_std`-friendly and does not require WASI.

```bash
rustup target add wasm32-unknown-unknown
cargo build -p cc-lb-router-my-router --target wasm32-unknown-unknown --release
```

The resulting wasm is at `target/wasm32-unknown-unknown/release/cc_lb_router_my_router.wasm`. Confirm the custom section is present:

```bash
wasm-tools dump target/wasm32-unknown-unknown/release/cc_lb_router_my_router.wasm \
  | grep cc_lb.plugin.v1
```

For deterministic builds (so a re-upload produces an identical SHA-256), pin `RUSTFLAGS="--remap-path-prefix=$PWD=." CARGO_PROFILE_RELEASE_DEBUG=0` before the build.

## Registering and operating a plugin

### Upload

`POST /admin/plugins` accepts a multipart body with three parts:

- `bytes`: the wasm artifact bytes (required).
- `name`: declared plugin name; must match the identity in the custom section, else the host returns `409 Conflict` with `code = "name_mismatch"`.
- `original_filename`: free-form filename for auditing (required).

The endpoint requires the admin Bearer token (`Authorization: Bearer $CC_LB_ADMIN_TOKEN`). Response codes:

| Code | Meaning |
|---|---|
| `201 Created` | New plugin handshake-validated and persisted. Body is the full `PluginRegistryRecord`. |
| `200 OK` | A record with the same SHA-256 already exists and is active; the response returns the existing record (idempotent re-upload). |
| `400 Bad Request` | Identity, handshake, or self-check rejected the wasm. `code` field disambiguates (`invalid_plugin`, etc.). |
| `409 Conflict` | The SHA-256 already maps to a different `plugin_name`. |
| `413 Payload Too Large` | Multipart payload exceeds the admin body cap. |
| `404 Not Found` | The blob SHA-256 referenced by the registry record is absent (extremely rare; indicates a corrupted store). |
| `500 Internal Server Error` | Repository, hash, or clock failure. |

### List, fetch, delete

- `GET /admin/plugins` — JSON array of all active records.
- `GET /admin/plugins/{sha256}` — single record or `404 Not Found`.
- `DELETE /admin/plugins/{sha256}` — `204 No Content`. Deleting a record also removes the blob. Idempotent.

### Server runtime flags and config

CLI:

- `--skip-handshake-if-fresh <bool>` — default `true` via config. When omitted on the command line, falls back to `[runtime.startup_handshake] skip_if_fresh` in `cc-lb.toml`.
- `--force-handshake <bool>` — default `false` via config. Forces every registered plugin to re-handshake on startup, ignoring `host_offer_hash` freshness.

`cc-lb.toml`:

```toml
[runtime.startup_handshake]
skip_if_fresh = true
force = false
```

CLI values, when provided, always win over the config file.

### Chain attachment

`/admin/plugins` only registers the plugin. To make it actually serve traffic for a principal you still attach it to that principal's plugin chain via the existing `PUT /admin/v1/principals/{id}/plugin-chain` admin API. See [runtime-management.md](runtime-management.md) for the chain-attachment surface.

## Anti-patterns

- Do not invoke host functions during `cc_lb_handshake` or `cc_lb_self_check`. Both phases run under `disallow_all_hosts()`; any host call traps and rejects the plugin.
- Do not bypass the PDK macros to emit your own custom section or `cc_lb_handshake` export by hand. The macro version is the only one tested for downgrade safety; the host re-computes `max(intersection)` and will reject hand-written accepts that pick anything lower.
- Do not depend on the wire types' `Default` impl. The canonical wire types deliberately use sentinel values in `dry_run_sample` because some sub-types (e.g. `HeaderName`-like) have no meaningful `Default`.
- Do not assume `FALLBACK` is configurable. It is a compile-time constant on each `WireFunction` impl and the host honors it without runtime override.
- Do not return `SelfCheckStatus::Failure` "to be safe" if the round-trip succeeded — `Failure` aborts registration outright. If you really cannot self-check, return `Success` with empty `failures`; otherwise produce a meaningful `Failure` payload that explains what went wrong.
- Do not put secrets in `signer_state` or `factory_state` payloads. These travel as JSON blobs to the host registry. They are intended for opaque per-signer cursor state (e.g. token refresh metadata), not for credentials.
- Do not change wire-type field names locally. All v1 structs use `#[serde(deny_unknown_fields)]`; extending requires a wire-protocol bump and a coordinated host upgrade.

## Source-of-truth index

When this guide and the code disagree, prefer the code.

| Topic | Primary source |
|---|---|
| Protocol architecture and rationale | [.omo/plans/wasm-plugin-handshake-protocol.md](../.omo/plans/wasm-plugin-handshake-protocol.md) |
| `#[plugin]` / `#[handler]` macro surface | [crates/cc-lb-pdk/src/parse.rs](../crates/cc-lb-pdk/src/parse.rs), [crates/cc-lb-pdk/src/codegen/](../crates/cc-lb-pdk/src/codegen/) |
| Identity custom section | [crates/cc-lb-plugin-wire/src/identity.rs](../crates/cc-lb-plugin-wire/src/identity.rs) |
| Handshake offer/accept and downgrade defense | [crates/cc-lb-plugin-wire/src/handshake/mod.rs](../crates/cc-lb-plugin-wire/src/handshake/mod.rs), [canonical.rs](../crates/cc-lb-plugin-wire/src/handshake/canonical.rs) |
| Self-check request/response and status semantics | [crates/cc-lb-plugin-wire/src/self_check.rs](../crates/cc-lb-plugin-wire/src/self_check.rs), [crates/cc-lb-runtime-extism/src/self_check.rs](../crates/cc-lb-runtime-extism/src/self_check.rs) |
| Wire function types (7 functions + common) | [crates/cc-lb-plugin-wire/src/v1/](../crates/cc-lb-plugin-wire/src/v1/) |
| Fallback policy + WireFunction trait | [crates/cc-lb-plugin-wire/src/wire_function.rs](../crates/cc-lb-plugin-wire/src/wire_function.rs) |
| Limits and guardrails | [crates/cc-lb-plugin-wire/src/limits.rs](../crates/cc-lb-plugin-wire/src/limits.rs) |
| Admin upload / list / delete API | [crates/cc-lb-server/src/admin_plugins.rs](../crates/cc-lb-server/src/admin_plugins.rs) |
| Startup re-handshake loop + freshness fast path | [crates/cc-lb-server/src/startup_handshake.rs](../crates/cc-lb-server/src/startup_handshake.rs) |
| Reference plugin implementation | [plugins/router/cache-aware/src/lib.rs](../plugins/router/cache-aware/src/lib.rs) |
| Static identity reader (host side) | [crates/cc-lb-runtime-extism/src/identity.rs](../crates/cc-lb-runtime-extism/src/identity.rs) |
| Dispatch + catch-and-skip + metrics | [crates/cc-lb-runtime-extism/src/dispatch.rs](../crates/cc-lb-runtime-extism/src/dispatch.rs) |
| Registry orchestration (L1-L4) | [crates/cc-lb-runtime-extism/src/registry.rs](../crates/cc-lb-runtime-extism/src/registry.rs) |
| Postgres schema for the registry | [crates/cc-lb-storage-postgres/migrations/0017_plugin_registry.sql](../crates/cc-lb-storage-postgres/migrations/0017_plugin_registry.sql), [0022_plugin_registry.sql](../crates/cc-lb-storage-postgres/migrations/0022_plugin_registry.sql) |
