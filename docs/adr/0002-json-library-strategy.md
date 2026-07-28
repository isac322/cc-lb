# ADR 0002 — Hot-Path JSON Parsing Library: sonic-rs

Status: Accepted

## Context

The workspace uses `serde_json` across 177 files. On the proxy hot path
(SSE relay per-event, streaming usage extraction, request-body inspection,
lifecycle metadata) the parser is invoked hundreds of times per streaming
request. `serde_json` is a scalar parser and dominates CPU time in
production profiles.

Faster alternatives evaluated:

- **sonic-rs 0.5** — SIMD parser + serializer, `Bytes` friendly (accepts
  `&[u8]`), drop-in `serde::Deserialize` support, ~2.8× parse and ~2×
  serialize vs `serde_json`. Known gap: `#[serde(flatten)]` (issue #114).
- **simd-json 0.17** — Requires `&mut [u8]` for in-place unescaping. hyper
  gives us read-only `Bytes`; forced clone erases the SIMD win.
- **jiter** — No general `serde::Deserializer`. Pydantic-specific.
- **serde_json_lenient / json-rust / struson / deser-hjson** — Slower or
  wrong niche.

## Decision

Adopt **sonic-rs for hot-path *parsing only*** (`from_slice`, `from_str`).
Everything else — the `Value` AST type, `serde_json::Map`, `json!`
macro, `to_vec`/`to_string` serialization — stays on `serde_json`.

Rationale for the split:

1. **Byte-parity of prompt-cache hashes.** `SerializationScratch::serialize`,
   `DigestBlockSerializer`, and `PrefixSerializer` in
   `crates/cc-lb-engine/src/prompt_cache_simulator.rs` serialize block-hash
   inputs and prefix token-count inputs with `serde_json`; block bytes then
   feed BLAKE3.
   `serde_json` (without the `preserve_order` feature) uses `BTreeMap`, so
   object keys are emitted in sorted order regardless of insertion order.
   `sonic_rs::Object` preserves insertion order. Swapping serialization
   would silently change cache-affinity routing keys. Keeping `serde_json`
   on this path preserves the canonical bytes.
2. **Frameworks forcing `serde_json`.** `sqlx` (the `json` feature)
   encodes/decodes JSON columns through `serde_json::Value`. `schemars`
   generates schemas as `serde_json::Value`. Neither has a `sonic_rs`
   adapter.
3. **Public plugin ABI.** `cc-lb-plugin-api::types::RequestContext` and
   related types expose `serde_json::Value` in the crates.io surface.
   Changing them is a breaking release for every plugin author.
4. **`#[serde(flatten)]` on 2 structs.**
   `crates/cc-lb-storage-api/src/warmup_attempts.rs:102` and
   `crates/cc-lb-admin/src/v1/upstreams.rs:194`. `sonic-rs` cannot
   deserialize `flatten` yet. Both are cold path.
5. **Test infrastructure.** `insta` snapshot testing, `serde_json::json!`
   macro in test fixtures, and `serde_json::to_value` / `to_string_pretty`
   for assertions all stay on `serde_json`. No perf pressure.

## Migration Scope

### Sites migrated to `sonic_rs::from_slice` / `sonic_rs::from_str`

| File | Sites | Reason |
| --- | --- | --- |
| `crates/cc-lb-engine/src/sse_relay.rs` | 2 | Per-SSE-event usage decode + fallback full-body decode |
| `crates/cc-lb-engine/src/usage_parser.rs` | 3 | SSE `accumulate_sse_usage`, non-stream `usage_from_json_body`, mid-stream error detection |
| `crates/cc-lb-engine/src/lifecycle.rs` | 1 | New `parse_body_json` helper; the main request handler now parses `ctx.body_bytes` **exactly once** and threads the cached `Option<Value>` to `request_cache_metadata_from_value`, `extract_model`, and `reserve_limit` / `LimitRequest::from_value`. Previously the same body was re-parsed up to 5× per request. |
| `crates/cc-lb-engine/src/error_normalizer.rs` | 1 | Upstream event-data JSON |
| `crates/cc-lb-server/src/scheduler_dispatch/usage.rs` | 1 | OAuth usage rollup body |

Cross-check command:

```
rg 'serde_json::(from_slice|from_str)' \
  crates/cc-lb-engine/src/sse_relay.rs \
  crates/cc-lb-engine/src/usage_parser.rs \
  crates/cc-lb-engine/src/lifecycle.rs \
  crates/cc-lb-engine/src/error_normalizer.rs \
  crates/cc-lb-server/src/scheduler_dispatch/usage.rs
```

Only test-fixture parses (`sse_relay.rs:1004`, `lifecycle.rs:3982`) may
remain.

### Sites kept on `serde_json` (intentional)

- **Serialization on hash paths** — `SerializationScratch::serialize`,
  `DigestBlockSerializer`, and `PrefixSerializer` in
  `prompt_cache_simulator.rs` use `serde_json::to_writer`. See Constraint 1
  above.
- **Serialization elsewhere in hot files** — e.g. `tokenizer.rs`,
  `audit_payload.rs`, `error_format.rs`, `sse_error_frame.rs`,
  `warmup/dialect.rs`. `serde_json::to_vec` / `to_string` output is what
  goes on the wire; keeping the same serializer keeps upstream bytes
  identical.
- **`serde_json::Value`, `serde_json::Map`, `serde_json::json!`** —
  used as language primitives; no replacement needed.
- **Warm-path fetchers** (`anthropic_metadata/fetchers.rs`,
  `anthropic_compat/fetchers.rs`, `signer-anthropic-oauth/src/lib.rs`) —
  periodic, not per-request. Migration is available but was scoped out to
  keep this PR focused on the hot path.
- **Storage adapters** (`cc-lb-storage-sqlite/**`,
  `cc-lb-storage-postgres/**`) — `sqlx` JSON columns require
  `serde_json`.
- **Config / schemars** (`cc-lb-config/**`) — `schemars` emits
  `serde_json::Value`.
- **Plugin API surface** (`cc-lb-plugin-api/src/types.rs`) — breaking
  ABI change if swapped.

## Rules for future contributors

1. **Do not swap `serde_json::to_vec` / `to_string` for
   `sonic_rs::to_vec` / `to_string`** in any file that feeds a hash,
   cache key, or bytes-on-the-wire comparison. The default `serde_json`
   dependency here has no `preserve_order` feature, so its output is
   sorted; `sonic_rs::Object` is insertion-ordered.
2. **Do not migrate parse sites in structs that use
   `#[serde(flatten)]`.** sonic-rs will reject them at runtime.
3. **New hot-path JSON parsing** (per-request, per-SSE-event) should use
   `sonic_rs::from_slice` / `sonic_rs::from_str`. New cold-path parsing
   should stay on `serde_json` for consistency with the storage /
   config / plugin surface.
4. **Cache parsed `Value`s at the request boundary** and thread them to
   helpers (as `Option<&Value>`) rather than re-parsing the same
   `Bytes`. The `lifecycle.rs::parse_body_json` pattern is the
   reference.

## Verification

- Existing unit tests (`cc-lb-engine` `usage_parser`, `sse_relay`,
  `lifecycle`, `error_normalizer`) exercise all migrated call sites and
  must remain green — they are the byte-parity assertion for parsing.
- `prompt_cache_byte_oracle_matches_current_behavior_over_wide_corpus` in
  `crates/cc-lb-engine/tests/prompt_cache_byte_oracle.rs` compares the live
  simulator's block and prefix bytes with independent `serde_json` oracles
  across the retained `tests/fixtures/hash_golden/` corpus. The companion
  `prompt_cache_byte_oracle_key_reordering_is_byte_stable` test explicitly
  verifies key-order stability. An accidental serializer swap would fail
  these comparisons.
- `v5_blake3_prefix_key_golden` in
  `crates/cc-lb-engine/tests/prompt_cache_structural_properties.rs` pins a
  schema-5 prefix key.
- The byte-oracle corpus includes `base_request_trailing_space.json`, but no
  current assertion compares it with `base_request.json` to prove whitespace
  sensitivity directly. Add that explicit comparison before relying on the
  corpus as a whitespace-sensitivity golden.

## Future work (out of scope for this ADR)

- Warm-path fetchers migration (behind a `--all` flag on the freshen
  sweep).
- Evaluate `sonic_rs::LazyValue` / `get_from_slice` for
  single-field extraction on the request path (e.g. reading only
  `stream` and `model` for the very early observability event).
- Consider setting `target-cpu=x86-64-v3` for release binaries where the
  deploy CPU baseline supports AVX2, to unlock sonic-rs SIMD without
  breaking musl portability.
