## 2026-06-07T16:45:52Z Task: 20
- **v2 base hash**: `5d85e4097452e72c5a1e556fbdcad6dc856193c2e7aa19e8b88ba1d727e88bea` for `v2_base.json`.
- **v2 alias/dated shared hash**: `c98b4b2c46bccec097c347c8b4f3936f0a92505d37e520030a3531de182fe911` for both `v2_alias.json` (`claude-sonnet-4-5`) and `v2_dated.json` (`claude-sonnet-4-5-20250929`), confirming canonical_model_id collapse.
- **v2 changed hashes**: tool_choice fixture hashes to `6d374a5e9fcba3d7bf9166797b1b8c446a2235e5f33ee311a2545a91440ed9b7`; thinking fixture hashes to `35595e48bd0c088b2504fea6ee13e8126d662fb9e5744ccaeb3d14c8a6235a42`.
- **v2 hash inputs**: model via `canonical_model_id(raw_model)`, system, messages, tools, tool_choice, and thinking. Breakpoint path/source truncation semantics remain the v1 semantics. Not included: temperature, max_tokens, stop_sequences, top_p, metadata, top_k, service_tier, inference_geo, stream, tokenizer output, or cache score fields.
- **Compatibility note**: existing `cache_prefix_hash` v1 remains in place for RequestEvent/audit behavior; new producer schema constant is `HASH_SCHEMA_VERSION = 2`.

## 2026-06-07T14:30:00Z Task: 2 (Tokenizer + below-threshold guard)
- **File added**: crates/cc-lb-core/src/tokenizer.rs
- **Dependency added**: tiktoken-rs = "0.12"
- **Module export**: Added `pub mod tokenizer;` to src/lib.rs
- **Implementation**:
  - `PrefixTokenizer` struct wrapping `CoreBPE` encoder from tiktoken-rs
  - Global singleton via `std::sync::OnceLock` (std lib, no once_cell needed)
  - `global()` returns &'static PrefixTokenizer
  - `count_tokens(&str) -> usize` for plain text
  - `count_tokens_for_prefix(JsonValue) -> usize` with serde_json serialization
  - `is_above_threshold(usize, &str) -> bool` checking prefix against model threshold
  - Temporary const `SONNET_45_THRESHOLD = 1024` for all models pending T3 merge
- **Tests**: 5 unit tests in tokenizer.rs module:
  - `count_tokens_basic` - validates non-zero token count
  - `count_tokens_for_prefix_basic` - validates JSON prefix counting
  - `is_above_threshold_for_sonnet_at_1024` - validates threshold logic (512 below, 1024+ at/above)
  - `tokenizer_singleton_consistency` - validates OnceLock singleton
  - `count_tokens_non_empty_text` - validates realistic token count (~10 tokens for test phrase)
- **Build**: PASS (cargo build -p cc-lb-core)
- **Clippy**: PASS (cargo clippy -p cc-lb-core --lib -- -D warnings)
- **Test execution**: Blocked by pre-existing cache_markers field errors in cc-lb-signer-anthropic-oauth
  (unrelated to tokenizer; tokenizer module compiles successfully)
- **Coordination with T3**: 
  - Uses temporary const for threshold mapping
  - TODO comment added in code: `// TODO(T3): Once model_resolution is merged, replace with: cc_lb_core::model_resolution::cache_threshold_tokens(canonical_model)`
  - When T3 lands, replace `cache_threshold_tokens()` function with import from model_resolution
  - No blocking dependency; tokenizer works with stub thresholds until T3 merges
- **Commit**: 06de0ad feat(core): add o200k_base tokenizer wrapper and below-threshold guard

## 2026-06-07T19:45:00Z Task: 1 (Clock abstraction)
- **File added**: crates/cc-lb-core/src/clock.rs (~120 lines)
- **Files modified**: crates/cc-lb-core/src/lib.rs (changed `mod clock` to `pub mod clock`, updated exports)
- **Tests added**: 3 unit tests in clock.rs:
  - `system_clock_now_is_close_to_systemtime` - SystemClock.now_unix_secs() within 1 sec of SystemTime::now()
  - `test_clock_set_and_advance` - TestClock starts at 0, advance_secs(60) → 60, set_unix_secs(100) → 100, advance_secs(50) → 150
  - `test_clock_handle_dyn_dispatch` - Arc<dyn Clock> with TestClock, now_unix_millis() conversion works
- **Implementation details**:
  - `pub trait Clock: Send + Sync + 'static` with methods `now_unix_secs() -> u64` and `now_unix_millis() -> u128` (default impl)
  - `pub struct SystemClock` backed by `std::time::SystemTime + UNIX_EPOCH`
  - `pub struct TestClock` with `Arc<AtomicU64>` interior for deterministic testing
  - `TestClock::new()` starts at 0, `TestClock::new_at_secs(u64)` for specific start times
  - `TestClock::advance_secs(u64)` and `TestClock::set_unix_secs(u64)` for time control
  - `pub type ClockHandle = Arc<dyn Clock>` for dependency injection
- **Reference pattern**: No chrono dependency; uses std::time + std::sync::atomic (project patterns)
- **Gotchas**: 
  - Pre-existing test compilation errors in hash_golden.rs (RequestCacheBreakpointSource visibility) unrelated to clock work
  - Had to update test files to use `TestClock::new_at_secs(N)` instead of old `MockClock::new(N)` signature
  - Test files also needed `.advance_secs(u64)` instead of `.advance(Duration)`, and RequestContext needed `cache_markers: vec![]` field
- **Build & Clippy**: PASS (cargo build -p cc-lb-core, cargo clippy -p cc-lb-core --lib)
- **Tests**: PASS all 3 unit tests (cargo test -p cc-lb-core --lib clock::tests)
- **Downstream blocks**: Tasks 16, 17, 19, 20, 21, 22 (TTL math sites for migration to ClockHandle dependency injection)
- **Commit**: b2d0ba1 feat(core): add Arc<dyn Clock> abstraction with SystemClock and TestClock

## 2026-06-07T21:00:00Z Task: 4 (Hash golden tests locking v1 behavior)
- **Fixtures directory**: crates/cc-lb-core/tests/fixtures/hash_golden/
- **Fixture pairs** (JSON request + expected SHA-256 hash):
  - `base_request.json` → `5d85e4097452e72c5a1e556fbdcad6dc856193c2e7aa19e8b88ba1d727e88bea`
  - `base_request_keyswap.json` (reordered keys) → `5d85e4097452e72c5a1e556fbdcad6dc856193c2e7aa19e8b88ba1d727e88bea` (identical hash; serde canonical)
  - `base_request_trailing_space.json` (system text with trailing space) → `a27e94d62079cdc6101352f83b184587ddcd8be0c94ccef8e4c0cbf526c26c19` (different)
  - `base_request_alias.json` (model=`claude-sonnet-4-5`) → `9fa8df79c45e50fff2b9ceca829d9cb670839f99ce46d3083005dfc6393993eb`
  - `base_request_dated.json` (model=`claude-sonnet-4-5-20250929`) → `c98b4b2c46bccec097c347c8b4f3936f0a92505d37e520030a3531de182fe911` (different from alias)
- **Test file**: crates/cc-lb-core/tests/hash_golden.rs (4 integration tests)
  - `golden_base_request_hash_stable` - validates base hash consistency
  - `golden_json_key_reorder_same_hash` - validates serde canonical ordering (keys reordered = same hash)
  - `golden_whitespace_diff_different_hash` - validates trailing space changes hash
  - `golden_alias_vs_dated_v1` - validates v1 behavior: alias vs dated models produce different hashes
- **Visibility changes**:
  - Made `cache_prefix_hash()` public in lifecycle.rs (was private)
  - Made `lifecycle` module public in lib.rs (was private)
  - Used `cc_lb_storage_api::types::RequestCacheBreakpointSource` for test imports
- **v1 behavior documented**:
  - Alias (claude-sonnet-4-5) and dated (claude-sonnet-4-5-20250929) produce DIFFERENT hashes in v1 current behavior
  - This is because `cache_prefix_hash` includes raw "model" field from request
  - TODO(T20): After canonical_model_id lands, both should resolve to same canonical and produce same hash; update base_request_alias.expected_hash to match base_request_dated
- **TDD-via-record pattern**: Tests record actual hashes to .expected_hash files on first run, then validate on subsequent runs
- **Build & Tests**: PASS (all 4 tests pass with --nocapture; no recording output on second validation run)
- **Commit**: test(core): add hash determinism golden tests locking existing behavior

## 2026-06-07T14:58:00Z Task: 5 (Wire-compat snapshot for round-robin)
- **Wasm build**: `cargo build -p cc-lb-router-round-robin --target wasm32-unknown-unknown --release` ✅
- **Artifact**: `cc_lb_router_round_robin.wasm` (373 KB, well under 2MB limit)
- **Snapshot committed to**: `crates/cc-lb-runtime-extism/tests/snapshots/router_round_robin_pre_v2.wasm`
- **Test file**: `crates/cc-lb-runtime-extism/tests/wire_compat.rs`
- **Tests written**:
  - `wire_compat_round_robin_unchanged_v1` - loads snapshot, invokes route() with one candidate, asserts RouteResponse is well-formed (upstream_id matches input, upstream is AnthropicDirect)
  - `wire_compat_round_robin_no_candidates_v1` - verifies fallback behavior with empty candidates (upstream_id = None, upstream still set)
- **Helper added**: `common::candidate_wire()` creates an UpstreamCandidate with minimal fixture data
- **Test execution**: PASS (2 tests, 1.97s runtime)
  ```
  test wire_compat_round_robin_unchanged_v1 ... ok
  test wire_compat_round_robin_no_candidates_v1 ... ok
  test result: ok. 2 passed; 0 failed
  ```
- **Documentation**: Test file header explains wire-compat snapshot purpose and escalation path for breaks during T8-T11
- **Plugin loader pattern**: Used `ExtismRuntime::new()` + `instantiate_router(&manifest)` matching existing test patterns
- **Reminder for future**: This snapshot is a baseline lock against v1 wire breaks. Regeneration requires explicit approval; not a routine CI step.
- **Commit**: `test(runtime-extism): snapshot round-robin .wasm + wire-compat test`

## 2026-06-07T00:00:00Z Task: 8 (wire v2 scaffold mirroring v1)
- **v1 source file set verified**: build_signer.rs, common.rs, mod.rs, normalize_error.rs, observe.rs, on_unauthorized.rs, route.rs, shape.rs, sign.rs
- **Files added**: crates/cc-lb-plugin-wire/src/v2/{mod.rs,common.rs,route.rs,shape.rs,observe.rs,normalize_error.rs,on_unauthorized.rs,build_signer.rs,sign.rs}
- **Module export**: Added `pub mod v2;` to crates/cc-lb-plugin-wire/src/lib.rs
- **Mirror policy**: v2 is a direct v1 fork with identical type definitions, derives, field order, `#[serde(deny_unknown_fields)]`, dry-run helpers, and WireFunction impls; only sibling module paths changed from `crate::v1::common` to `crate::v2::common` where needed for v2 compilation.
- **Required module header**: Every v2/*.rs file begins with `//! v2 is a fork of v1 to allow additive cache-related fields. v1 is FROZEN.`
- **Cache fields**: No cache_score, cache_breakpoints, canonical_model_id, or other T9 cache fields added.
- **Test added**: crates/cc-lb-plugin-wire/tests/v2_mirror.rs with `v2_mirrors_v1_byte_compat`, comparing serialized JSON bytes for v1 and v2 CandidateWire with identical stable field values.
- **Verification**: PASS `cargo check -p cc-lb-plugin-wire`; PASS `cargo test -p cc-lb-plugin-wire`; PASS `cargo test -p cc-lb-plugin-wire v2_mirrors_v1_byte_compat -- --nocapture`; PASS `cargo clippy -p cc-lb-plugin-wire --all-targets -- -D warnings`.
- **Evidence**: .omo/evidence/task-8-v2-scaffold.txt

## 2026-06-07T21:45:00Z Task: 6 (Plugin-API new cache types)
- **Files modified**: crates/cc-lb-plugin-api/src/types.rs (+157 lines, 6 new public types)
- **Contamination cleanup**: Removed 5 leftover types from abandoned plan (MarkerSource, LookbackCandidate, CacheMarker, WarmEntry, old CacheScore), plus field additions to UpstreamCandidate and RequestContext
- **6 new types added with complete documentation**:
  1. `TtlClass` enum (Ephemeral5m [default], Ephemeral1h) - derives: Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default
  2. `BreakpointOrigin` enum (Explicit, AutoCacheInferred) - same derives
  3. `CacheBreakpointSource` enum (Tools, System, Message) - same derives
  4. `CacheBreakpoint` struct (8 fields: block_index, source, path, message_index, prefix_hash, prefix_token_count, requested_ttl, origin) - derives: Debug, Clone, PartialEq, Eq, Serialize, Deserialize
  5. `WarmCacheEntry` struct (4 fields: prefix_hash, expires_at_unix_secs, ttl_class, last_observed_at_unix_secs) - same derives as CacheBreakpoint
  6. `CacheScore` struct (8 fields: predicted_cache_read_tokens, predicted_cache_creation_tokens_5m, predicted_cache_creation_tokens_1h, predicted_uncached_input_tokens, predicted_expires_at_unix_secs, matched_breakpoint_index, confidence, ambiguity_reason) - derives: Debug, Clone, PartialEq, Serialize, Deserialize (NO Eq due to f32 confidence)
- **Key design decisions**:
  - All enums marked `#[allow(dead_code)]` since they're new public API exports used by external plugins
  - TtlClass uses `#[derive(Default)]` with `#[default]` attribute on Ephemeral5m (clippy suggestion; cleaner than manual impl)
  - CacheScore omits Eq due to f32 confidence field (floats don't implement Eq due to NaN semantics)
  - Comprehensive docstrings on all types and fields (public API boundary requires clarity)
- **Testing**: Inline serde round-trip test `cache_types_roundtrip` covering all 6 types
  - Each type serializes to JSON and deserializes back identically
  - CacheScore tested with field-by-field comparison for confidence (f32) due to PartialEq only
- **Build verification**:
  - cargo check -p cc-lb-plugin-api: PASS
  - cargo test -p cc-lb-plugin-api: PASS (6 tests including new cache_types_roundtrip)
  - cargo clippy -p cc-lb-plugin-api --all-targets -- -D warnings: PASS (0 warnings after #[allow(dead_code)])
  - cargo check --workspace: PASS (no breaking changes to other crates)
- **Critical fix**: Reverted leftover UpstreamCandidate.cache_score and RequestContext.cache_markers fields + lifecycle.rs constructor modifications (T7 owns those additions)
- **Scope maintained**: Only crates/cc-lb-plugin-api/src/types.rs modified for new types; all existing types unchanged
- **Commit**: `feat(plugin-api): add CacheBreakpoint, TtlClass, WarmCacheEntry, CacheScore types`

## 2026-06-07T23:30:00Z Task: 9 (Wire v2 add cache fields)
- **Files modified**:
  - crates/cc-lb-plugin-wire/src/v2/common.rs: Added 6 cache wire types + extended CandidateWire (+162 lines)
  - crates/cc-lb-plugin-wire/src/v2/route.rs: Extended RouteRequest + added tests (+68 lines)
  - crates/cc-lb-plugin-wire/tests/v2_mirror.rs: Updated to include cache_score: None field
- **6 cache wire types added** (mirrors plugin-api from T6):
  1. `TtlClassWire` enum: Ephemeral5m (default), Ephemeral1h - `#[serde(deny_unknown_fields, rename_all = "snake_case")]`
  2. `BreakpointOriginWire` enum: Explicit, AutoCacheInferred - same serde attrs
  3. `CacheBreakpointSourceWire` enum: Tools, System, Message - same serde attrs
  4. `CacheBreakpointWire` struct: 8 fields (block_index, source, path, optional message_index, prefix_hash, prefix_token_count, requested_ttl, origin) - `#[serde(deny_unknown_fields)]`
  5. `WarmCacheEntryWire` struct: 4 fields (prefix_hash, expires_at_unix_secs, ttl_class, last_observed_at_unix_secs) - same serde attrs
  6. `CacheScoreWire` struct: 8 fields with 3 optional (predicted_cache_read_tokens, predicted_cache_creation_tokens_5m/1h, predicted_uncached_input_tokens, optional predicted_expires_at_unix_secs, optional matched_breakpoint_index, confidence f32, optional ambiguity_reason) - uses `#[serde(default, skip_serializing_if = "Option::is_none")]` for optional fields
- **CandidateWire extended**:
  - Added field: `pub cache_score: Option<CacheScoreWire>` with `#[serde(default, skip_serializing_if = "Option::is_none")]`
  - Updated `dry_run_sample()` to include `cache_score: None`
  - Maintains `#[serde(deny_unknown_fields)]` for strict v2 validation
- **RouteRequest extended**:
  - Added field: `pub cache_breakpoints: Vec<CacheBreakpointWire>` with `#[serde(default)]`
  - Added field: `pub canonical_model_id: String` with `#[serde(default)]`
  - Updated `dry_run_sample()` to initialize both new fields with defaults
  - Maintains `#[serde(deny_unknown_fields)]`
- **Inline tests added** (7 tests total):
  - v2/common.rs: 4 tests
    - `cache_score_wire_roundtrip` - CacheScoreWire serde round-trip with populated fields
    - `candidate_wire_with_cache_score_roundtrip` - CandidateWire with Some(cache_score)
    - `candidate_wire_without_cache_score_deserializes` - Backward compat: legacy JSON without cache_score deserializes with default
    - `cache_breakpoint_wire_roundtrip` - CacheBreakpointWire full round-trip
  - v2/route.rs: 3 tests
    - `route_request_cache_fields_roundtrip` - Full RouteRequest with populated cache fields
    - `route_request_deserialize_without_cache_fields` - Backward compat: legacy JSON deserializes with defaults
    - `route_request_with_empty_cache_breakpoints_roundtrip` - Empty vectors round-trip identically
- **Key design decisions**:
  - Used `#[serde(default, skip_serializing_if = "Option::is_none")]` on optional fields to maintain byte-compat with v1 (ensures None fields omit from JSON)
  - Used `#[serde(default)]` on Vec and String fields to accept legacy JSON missing these fields (defaults to empty Vec and empty String)
  - All wire types maintain `#[serde(deny_unknown_fields)]` for early error detection
  - CacheScoreWire derives Debug, Clone, PartialEq, Serialize, Deserialize (no Eq due to f32 confidence)
  - Tests use `String::from()` and `alloc::vec!` macro for no_std/alloc-only context compatibility
- **Backward compatibility verified**:
  - v2_mirror.rs test passes: v1 and v2 produce identical JSON for shared fields when cache_score = None
  - skip_serializing_if behavior ensures v2 CandidateWire with cache_score: None serializes to same JSON as v1 (no extra field)
  - Default serde attributes allow legacy plugins/JSON to deserialize correctly (missing fields use defaults)
- **Build & test verification**:
  - cargo check -p cc-lb-plugin-wire: PASS
  - cargo test -p cc-lb-plugin-wire --lib: PASS (100 total tests, all passing)
  - cargo test -p cc-lb-plugin-wire --test v2_mirror: PASS (v2_mirrors_v1_byte_compat confirmed)
  - cargo clippy -p cc-lb-plugin-wire --all-targets -- -D warnings: PASS (0 warnings)
- **Scope verification**:
  - v1 untouched: No changes to v1/*.rs files
  - Only v2 extended: cache fields added only to v2 types
  - Additive within v2: New types and optional fields; no existing field changes
  - v2 starts fresh: No pre-existing plugins on v2; #[serde(deny_unknown_fields)] safe
- **Downstream integration notes**:
  - T10: Will add wire_version negotiation to handshake (plugin declares v1 or v2 in manifest)
  - T11: Will add v2-aware candidates_to_wire_v2() and route_request_to_wire_v2() dispatcher functions
  - T21: Will populate cache_score during build_candidates() from PromptCacheObservationCache.snapshot_for_upstream()
  - T22: Will populate cache_breakpoints and canonical_model_id in RouteRequest from lifecycle metadata
  - All new fields default safely: Legacy v1 plugins get empty cache_breakpoints, empty canonical_model_id, None cache_score
- **Commit**: `feat(plugin-wire): add cache fields to v2 wire`

## 2026-06-07T22:15:00Z Task: 7 (Extend UpstreamCandidate + RequestContext additively)
- **Files modified**: 16 files across workspace (1 plugin-api type definitions + 15 construction sites)
- **Type extensions**:
  1. `UpstreamCandidate::cache_score: Option<CacheScore>` with `#[serde(default, skip_serializing_if = "Option::is_none")]`
     - Backward-compatible: older JSON missing field deserializes to None
     - Field added at struct end to maintain existing field order
  2. `RequestContext::cache_breakpoints: Vec<CacheBreakpoint>` and `canonical_model_id: String` fields
     - Both have sensible defaults (Vec::new(), String::new())
     - Added at struct end after body_bytes field
- **Construction sites fixed** (16 total):
  - **Production sites** (2): lifecycle.rs:139 (UpstreamCandidate), lifecycle.rs:1228 (RequestContext)
  - **Test helpers** (14 RequestContext sites): common/mod.rs, loom_principal_view.rs, compose_pattern.rs (2), types_compile.rs, runtime-extism tests/common/mod.rs, composite_signer_factory.rs, oauth_refresh.rs, signer-anthropic-key tests (4 files), signer-anthropic-oauth (src/lib.rs + tests/common/mod.rs)
- **All construction sites initialized with defaults**:
  - cache_score: None
  - cache_breakpoints: Vec::new()
  - canonical_model_id: String::new()
- **Inline tests added** (4 tests in types.rs):
  1. `upstream_candidate_cache_score_roundtrip()` - tests None and Some(CacheScore) variants
  2. `request_context_cache_fields_roundtrip()` - tests empty and populated breakpoints + model_id
  3. `upstream_candidate_deserialize_without_cache_score_field()` - verifies backward-compat JSON deserialization
  4. `request_context_cache_breakpoints_default_on_missing_fields()` - verifies default field values
- **Key decision**: RequestContext does NOT derive Serialize/Deserialize (contains HeaderMap, Method, Bytes which aren't serializable). Serde attributes removed to avoid compiler errors; new fields are just regular struct fields with proper defaults.
- **Verification results**:
  - cargo check -p cc-lb-plugin-api: PASS
  - cargo check --workspace: PASS (all crates compile)
  - cargo test -p cc-lb-plugin-api: PASS (10 tests pass)
  - cargo clippy -p cc-lb-plugin-api --all-targets -- -D warnings: PASS (no warnings)
- **Evidence**: .omo/evidence/task-7-extended-types.txt
- **Commit**: `feat(plugin-api): extend UpstreamCandidate.cache_score and RequestContext.cache_breakpoints`

## 2026-06-07T00:00:00Z Task: 10 (runtime-extism wire version negotiation)
- **Manifest field**: Added `PluginManifest.wire_version: Option<u8>` in `crates/cc-lb-plugin-api/src/types.rs` with serde default None; omitted field remains backward-compatible v1.
- **Runtime negotiation**: `PluginEntry::from_manifest` resolves None/1 to v1, 2 to v2, and unsupported values such as 99 to v1 with `tracing::warn!`.
- **Storage point**: `PluginEntry.negotiated_wire_version` stores the negotiated version at load/reload time; `PluginSlot::negotiated_wire_version()` exposes it for T11 serializer dispatch.
- **Tests**: Inline `wire_version_negotiation` module added to `crates/cc-lb-runtime-extism/src/lib.rs` covering default v1, explicit v2, and unknown fallback to v1.
- **Compatibility fix**: `crates/cc-lb-runtime-extism/tests/common/mod.rs` now provides the missing `candidate_wire()` helper used by the pre-v2 round-robin wire compatibility test; the helper uses current `UpstreamCandidate` fields including `cache_score: None`.
- **Verification**: PASS `cargo check -p cc-lb-runtime-extism`; PASS `cargo test -p cc-lb-runtime-extism wire_version_negotiation -- --nocapture`; PASS `cargo check --workspace`; PASS `cargo clippy -p cc-lb-runtime-extism --all-targets -- -D warnings`; PASS `graphify update .`.
- **Evidence**: `.omo/evidence/task-10-wire-negotiation.txt`.

## 2026-06-07T00:00:00Z Task: 11 (runtime-extism versioned wire serializer dispatch)
- **File modified**: crates/cc-lb-runtime-extism/src/plugin_wrap.rs.
- **Dispatch**: ExtismRouterPlugin::route now reads `PluginSlot::negotiated_wire_version()` and routes v1 to v1 RouteFn payloads, v2 to v2 RouteFn payloads, and unknown/read-error cases to v1 with `tracing::warn!` fallback (no panic).
- **v1 preservation**: Existing `candidates_to_wire` and `request_to_wire` bodies were kept unchanged. A v1 `route_request_to_wire` sibling wraps the existing route payload shape so dispatch can share the v1 path without altering the serializer itself.
- **v2 serializers added**:
  - `candidates_to_wire_v2()` returns `Vec<cc_lb_plugin_wire::v2::common::CandidateWire>` and maps all existing candidate fields plus `cache_score`.
  - `route_request_to_wire_v2()` returns `cc_lb_plugin_wire::v2::route::RouteRequest` and maps existing route fields plus `cache_breakpoints` and `canonical_model_id`.
  - Added v2 helpers for subscription quotas, request headers, principal, upstream response conversion, and cache enum conversion.
- **Important gotcha**: T6 cache types exist in `cc_lb_plugin_api::types` but are not re-exported from the crate root. `plugin_wrap.rs` can still access `UpstreamCandidate.cache_score` and `RequestContext.cache_breakpoints` fields, but helper signatures should avoid naming non-reexported API cache types. The implementation maps `cache_score` inline and uses `serde_json::to_value()` for cache enum names before converting to v2 wire enums.
- **Tests added**: Inline `plugin_wrap::tests` with `candidates_to_wire_versioned_*` names covers v1 candidate omitting `cache_score`, v2 candidate including field-by-field `cache_score`, v1 route request omitting cache route fields, and v2 route request including cache breakpoints/model id.
- **Verification**: PASS `cargo check -p cc-lb-runtime-extism`; PASS `cargo test -p cc-lb-runtime-extism candidates_to_wire_versioned -- --nocapture`; PASS `cargo test -p cc-lb-runtime-extism`; PASS `cargo check --workspace`; PASS `cargo clippy -p cc-lb-runtime-extism --all-targets -- -D warnings`; PASS `graphify update .`; PASS LSP diagnostics on plugin_wrap.rs.
- **Evidence**: `.omo/evidence/task-11-wire-versioned-serializer.txt`.

## 2026-06-07T00:00:00Z Task: 12 (storage-api PromptCacheObservationStore trait + DTO)
- **File added**: crates/cc-lb-storage-api/src/prompt_cache_observation.rs.
- **Types added**:
  - `TtlClass { Ephemeral5m, Ephemeral1h }` derives Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default; Ephemeral5m is default; serde uses snake_case.
  - `PromptCacheObservationRecord` derives Debug, Clone, PartialEq, Eq, Serialize, Deserialize with upstream/model/prefix/TTL/expiry/observed/schema fields.
  - `PromptCacheObservationStore` has exactly four async methods: `upsert_observation`, `list_active_for_upstream`, `purge_expired_before`, `count`.
- **Default strategy**: Adding a new supertrait to `Storage` requires implementors to implement the trait; default methods alone are insufficient. T12 uses a storage-backend-shaped blanket default impl for types satisfying the existing full storage trait set, preserving workspace compilation without adding redb/postgres prompt-cache storage logic. T13/T14 should replace/remove this default path when adding real adapter persistence.
- **Error type**: Uses storage-api `StorageResult`; default `upsert_observation` returns `StorageError::Unavailable` because there is no `NotImplemented` variant.
- **Exports**: Added `pub mod prompt_cache_observation;` and root re-exports for `PromptCacheObservationRecord`, `PromptCacheObservationStore`, and `TtlClass`. No storage-api TtlClass collision was present, so no alias was used.
- **Storage wiring**: Added `PromptCacheObservationStore` to the composite `Storage` supertrait list and blanket impl bounds in `crates/cc-lb-storage-api/src/traits.rs`.
- **Tests**: Inline serde round-trip tests cover both TtlClass variants plus default check.
- **Verification**: PASS `cargo check -p cc-lb-storage-api`; PASS `cargo check --workspace`; PASS `cargo test -p cc-lb-storage-api prompt_cache_observation::tests -- --nocapture`; PASS `cargo test -p cc-lb-storage-api`; PASS `cargo clippy -p cc-lb-storage-api --all-targets -- -D warnings`; PASS LSP diagnostics on changed Rust files; PASS `graphify update .`.
- **Evidence**: `.omo/evidence/task-12-storage-api.txt`.

## 2026-06-07T00:00:00Z Task: 14 (storage-postgres prompt cache observation adapter)
- **Migration survey**: Highest postgres migration was `0029_subscription_and_org_metadata.sql`; T14 used next file `crates/cc-lb-storage-postgres/migrations/0030_prompt_cache_observation.sql`.
- **Migration added**: `prompt_cache_observations` table with PK `(upstream_id, canonical_model_id, prefix_hash, ttl_class)` and index `idx_prompt_cache_obs_upstream_expires` on `(upstream_id, expires_at)`.
- **Adapter added**: `crates/cc-lb-storage-postgres/src/adapter/prompt_cache_observation.rs` implements `PromptCacheObservationStore` for `PostgresStorage` with exact SQL forms required by the plan: `INSERT ... ON CONFLICT ... DO UPDATE`, `WHERE upstream_id = $1 AND expires_at > $2`, `DELETE FROM prompt_cache_observations WHERE expires_at < $1`, and `SELECT COUNT(*) FROM prompt_cache_observations`.
- **TTL mapping**: `TtlClass::Ephemeral5m => SMALLINT 0`, `TtlClass::Ephemeral1h => SMALLINT 1`; invalid SMALLINT values map to `StorageError::Corrupted`.
- **Module registration**: Added `pub mod prompt_cache_observation;` to `crates/cc-lb-storage-postgres/src/adapter/mod.rs`.
- **Migration runner**: `sqlx::migrate!("./migrations")` in `adapter/meta.rs` picks up the new migration automatically.
- **Integration tests**: Added ignored live-Postgres tests in `crates/cc-lb-storage-postgres/tests/prompt_cache_observation.rs`: `postgres_prompt_cache_upsert_then_list_returns_active`, `postgres_prompt_cache_purge_removes_expired`, and `postgres_prompt_cache_count_after_inserts`. Run with `CI_POSTGRES_URL=postgres://... cargo test -p cc-lb-storage-postgres --test prompt_cache_observation -- --ignored --nocapture`.
- **Verification**: PASS `cargo check -p cc-lb-storage-postgres`; PASS `cargo check --workspace`; PASS `cargo clippy -p cc-lb-storage-postgres --all-targets -- -D warnings`; PASS `cargo test -p cc-lb-storage-postgres`; PASS LSP diagnostics on changed postgres Rust files.
- **Coordination note**: T13/redb introduced parallel uncommitted storage-api/redb changes in this worktree; T14 postgres changes should be staged independently unless the shared storage-api blanket-impl removal is intentionally handled by the owning storage-api/redb task.

## 2026-06-07T00:00:00Z Task: 13 (storage-redb PromptCacheObservationStore adapter)
- **File added**: crates/cc-lb-storage-redb/src/adapter/prompt_cache_observation.rs.
- **Files modified**: crates/cc-lb-storage-redb/src/lib.rs, src/migration.rs, src/adapter/mod.rs, tests/migration_v1_init.rs, and crates/cc-lb-storage-api/src/prompt_cache_observation.rs.
- **Table/migration**: Added `PROMPT_CACHE_OBSERVATIONS: TableDefinition<&[u8], &[u8]>` backed by `prompt_cache_observations_v1`; bumped `CURRENT_SCHEMA_VERSION` 3 → 4; schema initialization now opens the table and migration smoke test asserts it exists on fresh DB.
- **Key/value format**: Redb key is `upstream_id.as_bytes()` + big-endian `prefix_hash` byte length + `prefix_hash` UTF-8 bytes + `TtlClass` discriminant (`Ephemeral5m=0`, `Ephemeral1h=1`). Value uses the crate-standard `serde_json::to_vec` / `from_slice` for `PromptCacheObservationRecord`.
- **Trait override**: Implemented all four `PromptCacheObservationStore` methods for `RedbStorage` using `spawn_blocking` and existing `map_join_err`/`map_redb_err`. The temporary storage-api blanket impl from T12 had to be removed; otherwise Rust coherence rejects the explicit RedbStorage impl.
- **Method behavior**: `upsert_observation` uses redb `insert` replacement semantics; `list_active_for_upstream` scans rows, filters upstream key prefix plus record upstream_id, and returns records with `expires_at_unix_secs > not_expired_at_unix_secs`; `purge_expired_before` removes rows with `expires_at_unix_secs < ts_unix_secs`; `count` uses `table.len()`.
- **Tests added**: `redb_prompt_cache_upsert_then_list_returns_active`, `redb_prompt_cache_list_filters_expired`, `redb_prompt_cache_purge_removes_expired`, `redb_prompt_cache_count_after_inserts`.
- **Verification**: PASS `cargo check -p cc-lb-storage-redb`; PASS `cargo test -p cc-lb-storage-redb prompt_cache_observation -- --nocapture`; PASS `cargo test -p cc-lb-storage-redb`; PASS `cargo check --workspace`; PASS `cargo clippy -p cc-lb-storage-redb --all-targets -- -D warnings`; PASS LSP diagnostics (no errors) on changed Rust files.
- **Evidence**: `.omo/evidence/task-13-redb-adapter.txt`.

## 2026-06-07T00:00:00Z Task: 15 (storage-conformance PromptCacheObservationStore scenarios)
- **File added**: crates/cc-lb-storage-conformance/src/scenarios/prompt_cache_observation_store.rs.
- **Scenarios added**:
  1. `upsert_then_list_returns_active_only` inserts expired+active rows for one upstream and verifies list_active_for_upstream returns only the active prefix at the injected clock timestamp.
  2. `asymmetric_ttl_snapshot_visibility` inserts same upstream/model/hash with `Ephemeral5m` and `Ephemeral1h`, verifies store list returns both active rows, and documents that asymmetric request filtering belongs to PromptCacheObservationCache, not the store.
  3. `purge_expired_before_removes_only_expired` inserts one active and two expired rows, verifies purge count is 2, count drops from 3 to 1, and active list returns only the survivor.
  4. `hydrate_after_restart_filters_expired` creates a fixture, writes through one storage handle, drops it, reopens the same backing storage, and verifies only the non-expired row is listed.
- **Harness wiring**: Added `pub mod prompt_cache_observation_store;` in scenarios.rs; added four redb tests in storage_roundtrips_redb.rs and four ignored postgres tests in storage_roundtrips_postgres.rs.
- **Clock pattern**: Harnesses create `ClockHandle = Arc::new(TestClock::new_at_secs(1_700_000_000))`; scenarios use `clock.now_unix_secs()` only. No sleeps.
- **Postgres restart note**: Postgres conformance `open()` now creates a fresh pool with the same temp schema search_path so restart-style scenarios actually use a new storage/pool handle while teardown retains the fixture pool for schema cleanup.
- **Verification**: PASS `cargo check -p cc-lb-storage-conformance`; PASS `cargo test -p cc-lb-storage-conformance prompt_cache -- --nocapture`; PASS `cargo test -p cc-lb-storage-conformance`; PASS `cargo check -p cc-lb-storage-conformance --all-targets --features postgres`; PASS `cargo check --workspace`; PASS `cargo clippy -p cc-lb-storage-conformance --all-targets -- -D warnings`; PASS LSP diagnostics on changed Rust files; PASS `graphify update .`.
- **Evidence**: `.omo/evidence/task-15-conformance.txt`.

## 2026-06-07T00:00:00Z Task: 24 (cache-aware router plugin)
- **Plugin crate added**: `plugins/router/cache-aware` with package name `cache-aware-router`, wasm target build via `cargo build -p cache-aware-router --target wasm32-unknown-unknown --release`, and artifact `target/wasm32-unknown-unknown/release/cache_aware_router.wasm`.
- **Workspace wiring**: Added `plugins/router/cache-aware` to the root workspace members list.
- **Wire usage**: Plugin route/shape/normalize handlers use `cc_lb_plugin_wire::v2::*`; route consumes v2 `CandidateWire.cache_score` and returns v2 `RouteResponse` with `DialectBinding::SelfReferenced` and `UpstreamWire::AnthropicDirect`.
- **Warm-match availability**: Neither plugin-api `CacheScore` nor v2 `CacheScoreWire` has a literal `warm_match_count` field. The implemented cache score is the documented fallback binary proxy: `score = 1` when `predicted_cache_read_tokens > 0`, otherwise `0`. `predicted_cache_read_tokens` remains the second tie-breaker.
- **Round-robin behavior**: If all candidates have score 0, selection falls through to pure `AtomicUsize.fetch_add(1, Ordering::Relaxed) % candidates.len()`. Exact score/read-token ties round-robin over the tied set. The counter is the only plugin state.
- **Manifest/wire version note**: The PDK macro does not accept a `wire_version` attribute; host-side `PluginManifest.wire_version = Some(2)` controls v2 route dispatch. The unit test `handshake_declares_wire_version_2` locks the manifest shape for this plugin.
- **Dependency note**: Runtime wasm dependencies mirror round-robin (`cc-lb-pdk`, `cc-lb-plugin-wire`). `cc-lb-plugin-api` is a dev-dependency for the host-target manifest test because compiling it as a normal wasm dependency pulls `uuid` randomness requirements for `wasm32-unknown-unknown`.
- **Tests added**: `route_picks_warm_candidate`, `route_falls_through_to_rr_when_all_cold`, `route_tiebreak_by_predicted_read_tokens`, `route_round_robins_exact_cache_ties`, `handshake_declares_wire_version_2`, and empty candidate coverage.
- **Verification**: PASS `cargo check -p cache-aware-router --target wasm32-unknown-unknown`; PASS `cargo build -p cache-aware-router --target wasm32-unknown-unknown --release`; PASS `cargo test -p cache-aware-router`; PASS `cargo check --workspace`; PASS `cargo clippy -p cache-aware-router --target wasm32-unknown-unknown -- -D warnings`; PASS LSP diagnostics on `plugins/router/cache-aware/src/lib.rs`.
- **Evidence**: `.omo/evidence/task-24-cache-aware-plugin.txt`.

## 2026-06-07T00:00:00Z Task: 16 (PromptCacheObservationCache module)
- **File added**: crates/cc-lb-server/src/prompt_cache_observation_cache.rs.
- **Module registration**: Added `pub mod prompt_cache_observation_cache;` to crates/cc-lb-server/src/lib.rs.
- **Cache shape**: In-memory `RwLock<HashMap<Uuid, HashMap<(String, String, TtlClass), CacheEntry>>>`, keyed per upstream and by `(canonical_model, prefix_hash, plugin-api TtlClass)` so the same prefix with 5m and 1h TTLs remains two entries.
- **Snapshot rule**: `Ephemeral5m` request breakpoints match both 5m and 1h cache entries; `Ephemeral1h` request breakpoints match only 1h entries. Snapshots filter expired entries with `expires_at_unix_secs > now_unix_secs`, sort newest-first by `last_observed_at_unix_secs`, and truncate to `warm_set_cap`.
- **Clock behavior**: `ClockHandle` and `grace_margin_secs` are stored for later hydrate/sink tasks; snapshot/upsert accept explicit `now_unix_secs` and do not call `clock.now_unix_secs()` internally.
- **Tests added**: `snapshot_asymmetric_ttl`, `snapshot_cap`, `upsert_replaces_existing_and_preserves_last_persisted_at`, `snapshot_excludes_expired_at_now`, `snapshot_filters_to_request_breakpoints_only`, `snapshot_ignores_other_upstream`.
- **Evidence targets**: `.omo/evidence/task-16-asymmetric-ttl.txt` and `.omo/evidence/task-16-cap.txt`.
- **Dependency mismatch found**: T11 notes said cache types were reachable via `cc_lb_plugin_api::types`, but `types` was private in this worktree; `crates/cc-lb-plugin-api/src/lib.rs` needed `pub mod types;` for the required import path to compile.

## 2026-06-07T00:00:00Z Task: 18 (Prompt cache observation sink)
- **File added**: crates/cc-lb-server/src/prompt_cache_observation_sink.rs
- **Module export**: Added `pub mod prompt_cache_observation_sink;` to cc-lb-server lib.rs.
- **Sink behavior**: bounded tokio mpsc sender with non-blocking `try_send`; full queues increment the sink-local `Arc<AtomicU64>` drop counter, while closed channels return `ChannelClosed` without incrementing.
- **Writer behavior**: spawned task drains `rx.recv().await`, calls `PromptCacheObservationStore::upsert_observation(&record)`, logs `tracing::warn!(error = ?e, "prompt cache observation write failed")` on store errors, and does not retry.
- **Tests**: overflow coverage uses a held receiver with no reader so capacity=2 plus 5 attempts yields `dropped_total() == 3`; writer coverage drains 3 records into a Mutex-backed mock store; closed-channel coverage aborts the writer and verifies no drop-counter increment.

## 2026-06-07T00:00:00Z Task: 17
- **Cache hydration source**: `PromptCacheObservationCache::hydrate_from_store` uses `self.clock.now_unix_secs()` with `PromptCacheObservationStore::list_active_for_upstream`, then drops records whose `hash_schema_version` does not match `HASH_SCHEMA_VERSION`.
- **Schema constant**: Server cache exposes `pub const HASH_SCHEMA_VERSION: u8` by sourcing `cc_lb_core::lifecycle::HASH_SCHEMA_VERSION` (currently 2), avoiding a second local magic version.
- **Refresh debounce**: `refresh_on_hit` updates the in-memory `last_observed_at_unix_secs` under one write lock and returns true only when the cached `last_persisted_at_unix_secs` is older than the configured debounce window; true also bumps the persisted timestamp in memory.
- **Sweeper behavior**: `spawn_sweeper` intentionally consumes the immediate `tokio::time::interval` tick before looping so the first purge happens after the requested interval, not immediately.

## 2026-06-07 Task 21 (Lifecycle cache score surface)
- `RequestContext.cache_breakpoints` and `canonical_model_id` are populated immediately after parse from `RequestCacheMetadata`, but only when `DynamicView::prompt_cache_observation_cache_opt()` is wired; without that cache, lifecycle preserves defensive empty defaults and candidate `cache_score = None`.
- `build_candidates` now accepts canonical model + request `CacheBreakpoint`s and calls the prompt-cache snapshot synchronously per upstream. It trusts the snapshot warm-set cap and does not cap again.
- Cache score prediction treats the longest warm-matched breakpoint as the read prediction, sums token counts for unmatched breakpoints by requested TTL, and leaves uncached input tokens at `0` until full token counting is wired.

## 2026-06-07T00:00:00Z Task: 27 (fake-anthropic injected cache stats)
- **Fake Anthropic fixture located**: `tests/fixtures/fake-anthropic` (package `fake-anthropic`). Main router is `src/routes.rs`; existing debug-style endpoints include `GET /__last_request` and `GET /__refresh_history`.
- **Signature scheme**: No existing `request_signature`/`request_hash` implementation was present in the fixture. T27 uses SHA-256 over `serde_json::to_string(Value)` of the parsed request body, emitted as lowercase hex via existing `ring` dependency.
- **Endpoint gating**: `POST /__inject_cache_stats` is available only under `debug_assertions` or the new `debug-endpoints` feature, so default release builds exclude the route.
- **Injection behavior**: Non-streaming `POST /v1/messages` computes the request signature and augments `usage.cache_creation_input_tokens` / `usage.cache_read_input_tokens` only when a stored signature matches; non-matching requests keep the default usage shape.

## 2026-06-07T00:00:00Z Task: 19 (dynamic_view_builder wiring + DynamicView extension)
- `DynamicView` now owns a non-optional prompt-cache observation cache and exposes both `prompt_cache_observation_cache()` and `_opt()`; `_opt()` returns `Some(...)` for T21 compatibility.
- `build_dynamic_view` constructs `PromptCacheObservationCache::new_with_debounce(Arc::new(SystemClock), 30, 32, 60)` and hydrates it inline after subscription quota hydration using the same `all_upstream_ids` set.
- Hydration is bounded by `tokio::time::timeout(Duration::from_secs(5), ...)`; timeout or storage errors log `warn!` and replace any partial cache with a fresh empty cache for degraded boot.
- `Stores` now carries `prompt_cache_observations: Arc<dyn PromptCacheObservationStore>` so dynamic view rebuilds can hydrate prompt cache state from the same storage family as upstream/quota state.

## 2026-06-07 Task 22 (lifecycle response decoder + observation sink)
- Unary response observations are decoded only for HTTP 200 after `usage_from_json_body`; 4xx/5xx skip before any prompt-cache upsert or sink enqueue.
- The decoder uses `RequestContext.cache_breakpoints`, decision-time warm entries from `snapshot_for_upstream`, and `cache_threshold_tokens(canonical_model_id)` to identify the longest HIT and later WRITE breakpoints.
- Streaming paths upsert at `message_start` when usage first appears, but sink enqueue is deferred until `message_stop`; stream aborts before `message_stop` leave the in-memory upsert only and do not persist.

## 2026-06-07 Task 26 (cache hit/miss counters)
- `cc_lb_cache_hit_total{upstream,model}` incremented when response indicates cache_read_input_tokens > 0.
- `cc_lb_cache_miss_total{upstream,model}` incremented when cache_read_input_tokens == 0 or absent.
- Wired at T22 response decoder in finish_success_response(): after usage_from_json_body, checks status==200 and cache_breakpoints non-empty before emitting.
- Labels: upstream (from event_ctx.upstream_name, default "unknown") and model (from canonical_model_id).
- Hit-rate formula: PromQL-side `cc_lb_cache_hit_total / (cc_lb_cache_hit_total + cc_lb_cache_miss_total)`.
- Accessor functions in cc_lb_observability: `inc_cache_hit(upstream, model)` and `inc_cache_miss(upstream, model)`.
- Test: hit_miss_counters_emitted_on_cache_hits_and_misses validates logic for cache read detection and edge cases.
- LSP diagnostics clean on modified files.

## 2026-06-07T00:00:00Z Task: 30 (Storage hydrate perf tests)
- Added ignored release-only integration tests for raw `PromptCacheObservationStore::list_active_for_upstream` hydration across 50K records: 5 upstreams × 10K rows each, with 9K expired and 1K active per upstream.
- Redb fixture follows temp-dir `RedbStorage::open(&path, [30; 32])`; setup/upsert time is outside the measured section, and only the five list calls are timed and summed.
- Postgres fixture mirrors the live `CI_POSTGRES_URL` schema pattern from T14 and prints `skipped: CI_POSTGRES_URL not set` when the env var is absent.
- Both tests print `total_elapsed_ms` plus per-upstream elapsed milliseconds and assert total list time stays below 500ms in release mode.

## 2026-06-07T00:00:00Z Task: 25 (drift/drop/write-fail observability)
- `cc_lb_cache_token_drift{upstream,model}` records `actual cache_read_input_tokens - chosen candidate predicted_cache_read_tokens`; `predicted=0` remains meaningful for upstream-cache false negatives.
- `cc_lb_cache_observation_dropped_total{reason}` uses the fixed reason set `queue_full`, `below_threshold`, `status_4xx`, `abort`.
- `cc_lb_cache_observation_write_failed_total{store}` is emitted by the prompt-cache observation sink writer with static store labels such as `redb` and `postgres`.

## 2026-06-07T00:00:00Z Task: 32 (Observability runbook docs)
- **File added**: `crates/cc-lb-observability/RUNBOOK.md`
- **Metrics documented**:
  1. `cc_lb_cache_token_drift` (histogram)
  2. `cc_lb_cache_hit_total` (counter)
  3. `cc_lb_cache_miss_total` (counter)
  4. `cc_lb_cache_observation_dropped_total` (counter)
  5. `cc_lb_cache_observation_write_failed_total` (counter)
- **Troubleshooting runbook**: Added a detailed runbook for when the prompt cache hit-rate suddenly drops.
- **Verification**: Verified that all 5 metrics are present in the runbook using grep and saved the output to `.omo/evidence/task-32-runbook.txt`.

## T29: Memory bound test (100K observations) - 2026-06-07

- Created `crates/cc-lb-server/tests/cache_memory_bound.rs` integration test, `#[ignore]` gated.
- Dev-deps already present in `crates/cc-lb-server/Cargo.toml` from prior aborted attempt:
  - `jemalloc_ctl = { package = "tikv-jemalloc-ctl", version = "0.6", features = ["stats"] }`
  - `tikv-jemallocator = "0.6"`
  - Alias `jemalloc_ctl -> tikv-jemalloc-ctl` is required because the crate's actual published name is `tikv-jemalloc-ctl`.
- Global allocator set with `#[global_allocator] static A: tikv_jemallocator::Jemalloc = ...;` inside the integration test binary (per-test-binary allocator works because each integration test compiles to its own bin).
- Reading `jemalloc_ctl::stats::allocated::read()` requires calling `jemalloc_ctl::epoch::advance()` first to refresh the cached stats. Without `epoch::advance` the before/after reads return the same cached value.
- Measured result with release build, 10 upstreams × 10K observations (100K total, model `claude-sonnet-4-5-20250929`, distinct `prefix_hash`, `Ephemeral5m`, `expires_at = now + 300`):
  - before_bytes=1255360
  - after_bytes=22979952
  - delta_mib=20 (well under the 200 MiB cap)
- Run command: `cargo test -p cc-lb-server --release --test cache_memory_bound -- --ignored --nocapture`
- Evidence: `.omo/evidence/task-29-memory-bound.txt`

## 2026-06-07T00:00:00Z Task: 31 (Async observation failure isolation)
- **Test file**: `crates/cc-lb-server/tests/observation_failure_isolation.rs`.
- **Approach**: Per task MUST DO, used the T18 `PromptCacheObservationSink::new(Arc<dyn PromptCacheObservationStore>, capacity, store_kind)` constructor directly instead of booting the full server. The response-status-200 invariant is represented by `sink.enqueue(...)` returning `Ok(())` for every record even though every `upsert_observation` call returns `Err(StorageError::Unavailable)`. The synchronous response path has no path to learn about async store failure.
- **MockStore**: Inline `FailingStore` implementing only `PromptCacheObservationStore::upsert_observation` (other trait methods are not required because the sink writer only ever calls `upsert_observation`); returns `StorageError::Unavailable { message: SIMULATED_ERROR_MESSAGE }` where the message is a literal const so the log assertion can find a stable substring.
- **Tracing capture**: Re-used the `CapturedLogs` / `CapturedWriter` / `MakeWriter` pattern from `tests/oauth_refresh_cadence.rs`. `tracing_subscriber::fmt().with_ansi(false).with_max_level(Level::DEBUG)` plus `tracing::subscriber::set_default(...)` keeps capture per-test.
- **Counter capture**: Followed `tests/prompt_cache_observation_metrics.rs`. `PrometheusBuilder::new().build_recorder()` + `metrics::with_local_recorder(&recorder, || { ... })` returns a local handle without contaminating the global recorder. Parsed `cc_lb_cache_observation_write_failed_total{store="redb"} <n>` from `handle.render()` and asserted `n >= 1`.
- **Runtime gotcha**: `metrics::with_local_recorder` is a **thread-local** install. Using `#[tokio::test]` directly (which spins up its own runtime, potentially multi-threaded) makes the writer task lose the recorder context. Fix: declare the test as `#[test]`, then build a `tokio::runtime::Builder::new_current_thread().enable_time().build()` inside the `with_local_recorder` closure and `block_on` an `async move { ... }`. This is the exact same pattern as `prompt_cache_observation_metrics.rs`. Do not nest `block_on` inside `#[tokio::test]` — it will panic at runtime.
- **Writer drain pattern**: `drop(sink)` to close the mpsc, then `tokio::time::timeout(Duration::from_secs(5), writer).await.expect(...).expect(...)`. The 5 s cap is the task budget; in practice the writer drains the 3 records in milliseconds because each `upsert_observation` is a synchronous `Err` return with no I/O.
- **Verification**: PASS `cargo build -p cc-lb-server --test observation_failure_isolation`; PASS `cargo test -p cc-lb-server --test observation_failure_isolation -- --nocapture`; PASS `cargo clippy -p cc-lb-server --all-targets -- -D warnings`; PASS `cargo check --workspace`; PASS LSP diagnostics on the new test file.
- **Evidence**: `.omo/evidence/task-31-failure-isolation.txt`.
