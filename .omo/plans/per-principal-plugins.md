# Per-PrincipalSpec Plugin Configuration

## TL;DR
> **Quick Summary**: Move Extism plugin configuration (`router_plugin`, `observability_hooks`) from the single global `PluginsConfig` into `PrincipalSpec` as inline `Option<PluginRef>` / `Option<Vec<PluginRef>>`. Global PluginsConfig remains as default. Dispatch resolves the per-principal chain via `PrincipalView` using a `load-once-and-bind` snapshot. `ExtismRuntime` keys both `instances` and `manifests` by `(principal_id_or_global, plugin_name)`. Validation/preflight/hot-reload are all-or-nothing across the whole graph. TDD throughout.
>
> **Deliverables**:
> - New `router_plugin: Option<PluginRef>` and `observability_hooks: Option<Vec<PluginRef>>` fields on `PrincipalSpec`
> - `PrincipalView::from_config` becomes fallible: T4 introduces `Result<Arc<PrincipalView>, ConfigError>` (signature-only, no plugin instantiation), T8 extends to `Result<(Arc<PrincipalView>, Vec<StagedSlot>), ConfigError>` once per-principal plugin staging is added
> - `ExtismRuntime` instance/manifest maps keyed by `(principal_id_or_global, plugin_name)` via a new `SlotKey` newtype
> - `PrincipalSpecCached` carries resolved `Arc<dyn RouterPlugin>` / `Vec<Arc<dyn ObservabilityHook>>` (or `Inherit` markers)
> - `Lifecycle::handle` resolves per-principal plugin chain exactly once per request via `principal_view.load()` snapshot
> - All-or-nothing validation/preflight/hot-reload extended across global + every principal
> - Admin `/status` response keeps legacy `plugins: [...]` and adds new `principals: { id -> { router_plugin, observability_hooks } }`
> - Hot-reload eviction of stale `(principal_id, plugin_name)` slots after `PrincipalView` swap
> - Backward-compat regression: zero-principal-plugin configs produce byte-for-byte identical observe event streams
>
> **Estimated Effort**: Large
> **Parallel Execution**: YES — 5 implementation waves + FINAL
> **Critical Path**: T3 → T7 → T8 → T9 → T10 → T15 → T16 → T17 → F1-F4 → user okay

---

## Context

### Original Request
"api key마다 plugin을 설정하고싶어. 그렇게 구조를 바꾸자"

### Interview Summary
**Key Discussions**:
- **Scoping unit**: User confirmed `PrincipalSpec` (config-static), NOT `ApiKeyRecord` (storage-dynamic). All keys belonging to the same principal share the plugin chain.
- **Global vs per-principal combination**: Global default + per-principal full override (no concat/merge).
- **Field shape**: Inline `PluginRef` directly inside `PrincipalSpec` (no named-library indirection).
- **Plugin instance lifecycle**: Per-principal Extism plugin instances. `ExtismRuntime` instance/manifest maps keyed by `(principal_id_or_global, plugin_name)`. Single-tenant plugin assumption preserved.
- **Test strategy**: TDD. Existing infra (cargo test, property, loom, integration) reused; no test framework rewrite.
- **Failure policy**: All-or-nothing across validation/preflight/hot-reload.

**Research Findings (file:line landmarks)**:
- `crates/cc-lb-config/src/types.rs:49` — `Config.principals: HashMap<String, PrincipalSpec>`
- `crates/cc-lb-config/src/types.rs:309-332` — `PrincipalSpec` (no plugin fields yet)
- `crates/cc-lb-config/src/types.rs:336-355` — `PluginsConfig`, `PluginRef`
- `crates/cc-lb-config/src/validation.rs:212-218` — `validate_plugin_ref` (global only today)
- `crates/cc-lb-core/src/api_keys/principal_view.rs:32-74` — `PrincipalView::from_config` (infallible today; uses `.expect`)
- `crates/cc-lb-core/src/api_keys/builtin_authn.rs:84-138` — `BuiltinAuthn::authenticate` returns `principal_id`
- `crates/cc-lb-core/src/lifecycle.rs:294-307` — pre-authn observe sites (no principal_id yet)
- `crates/cc-lb-core/src/lifecycle.rs:316-320` — `Principal` struct construction (best place to bind per-principal chain)
- `crates/cc-lb-core/src/lifecycle.rs:327, 748, 790` — current global `self.router.route(...)` and `observe_many(&self.observability_hooks, ...)` (must become per-principal-resolved)
- `crates/cc-lb-server/src/app.rs:257-265` — startup wiring (global plugin instantiation)
- `crates/cc-lb-server/src/preflight.rs:95-106` — dry-load preflight (global only today)
- `crates/cc-lb-server/src/reload.rs:101-104, 244-251` — hot-reload `principal_view.store(...)` + path-change warnings
- `crates/cc-lb-runtime-extism/src/lib.rs:105-110, 141-200, 291-313` — `ExtismRuntime { instances, manifests }`, `register_slot`, `reload`, `PluginSlot { ArcSwap<PluginCell> }`
- `crates/cc-lb-admin/src/status.rs:130-135`, `crates/cc-lb-admin/src/settings.rs:477` — admin status enumeration
- `crates/cc-lb-storage-api/src/types.rs:233-247` — `ApiKeyRecord`/`StoredApiKeyRecord` (OUT OF SCOPE — do not touch)

### Metis Review
**Identified Gaps (all addressed)**:
- `PrincipalView::from_config` must become fallible — propagated through all callers.
- `ExtismRuntime.manifests` map (not just `instances`) needs widening; both keys → `SlotKey`.
- ArcSwap publication order: register all new `PluginSlot`s **before** publishing the new `PrincipalView`.
- `observability_hooks` needs `Option<Vec<PluginRef>>` to distinguish "absent (inherit)" from "explicit empty".
- Pre-authn observe paths always go through global hooks; silent no-op if global empty.
- Dispatch is `load-once-and-bind` to eliminate the auth/reload race entirely (no fallback metric needed).
- Eviction: drop stale `SlotKey`s after view swap; Arc refcount handles in-flight safety.
- Whole-graph validation/preflight: any single PluginRef failure aborts the entire reload, previous view kept.
- Backward compat: zero-principal-plugin configs must produce byte-for-byte identical observe streams.

---

## Work Objectives

### Core Objective
Make Extism plugin configuration (`router_plugin` + `observability_hooks`) attachable per `PrincipalSpec` with the global `PluginsConfig` serving as the default chain; dispatch resolves the active chain from a request-bound `PrincipalView` snapshot.

### Concrete Deliverables
- `crates/cc-lb-config/src/types.rs`: new fields on `PrincipalSpec`
- `crates/cc-lb-config/src/validation.rs`: whole-graph PluginRef validation
- `crates/cc-lb-core/src/api_keys/principal_view.rs`: fallible `from_config`, cache of resolved plugin handles
- `crates/cc-lb-core/src/lifecycle.rs`: load-once-and-bind dispatch, pre-authn global-only observe
- `crates/cc-lb-runtime-extism/src/lib.rs`: `SlotKey` newtype, widened maps, register/reload/lookup site updates
- `crates/cc-lb-server/src/app.rs`: per-principal instantiation wiring at startup
- `crates/cc-lb-server/src/preflight.rs`: per-principal dry-load
- `crates/cc-lb-server/src/reload.rs`: per-principal warnings + post-swap eviction
- `crates/cc-lb-admin/src/status.rs`: per-principal status surface (config Value redacted)
- `crates/cc-lb-admin/src/settings.rs`: per-principal plugin count
- Test fixtures under `crates/cc-lb-config/tests/fixtures/per_principal_plugins/`
- New tests across `cc-lb-config`, `cc-lb-core`, `cc-lb-runtime-extism`, `cc-lb-server`, `tests/integration`, `tests/property`, `tests/loom`

### Definition of Done
- [ ] `cargo build --workspace` passes
- [ ] `cargo test --workspace` passes (all existing + new tests)
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes
- [ ] `RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view` passes
- [ ] `cargo test --test hot_reload_race` passes
- [ ] Backward-compat regression test passes: zero-principal-plugin config produces byte-identical observe stream vs. baseline
- [ ] Memory ceiling test: 200 principals × 3 plugins → `instances.len() == 600`, RSS under recorded ceiling
- [ ] Admin `/status` retains legacy `plugins` field AND adds new `principals` map (curl + jq verified)
- [ ] All Wave Final agent reviews APPROVE

### Must Have
- `router_plugin: Option<PluginRef>` + `observability_hooks: Option<Vec<PluginRef>>` on `PrincipalSpec`
- `PrincipalView::from_config(&Config, principal_chains: HashMap<String, (RouterPluginCache, ObservabilityHooksCache)>) -> Result<Arc<PrincipalView>, ConfigError>` (responsibility split per Oracle phase-3 decision: cc-lb-core stays plugin-api-only - no cc-lb-runtime-extism dependency. Server (T11/T15) iterates `config.principals`, calls `runtime.instantiate_*_for(...)` on the staging API, builds the `principal_chains` map AND accumulates a separate `Vec<StagedSlot>` itself, then calls `runtime.commit_staged(staged)` BEFORE `principal_view.store(new_view)`. Global router/hooks are owned by `Lifecycle` directly, NOT by `PrincipalView`.)
- `ExtismRuntime` `instances` and `manifests` both keyed by `SlotKey(principal_id_or_global, plugin_name)`
- `Lifecycle::handle` loads `principal_view` exactly once and binds the per-principal chain to that snapshot
- Validation, preflight, and hot-reload run over the **whole graph** (global + every principal) and are **all-or-nothing**
- New per-principal slots registered in `ExtismRuntime` **before** `principal_view.store(new)`
- Stale slots dropped from `ExtismRuntime` **after** `principal_view.store(new)`; Arc refcount keeps in-flight safe
- Global `PluginsConfig` preserved with byte-for-byte identical behavior when no principal sets plugin fields
- TDD: every implementation task has a preceding failing test commit

### Must NOT Have (Guardrails)
- ❌ Any change to `ApiKeyRecord` / `StoredApiKeyRecord` / storage-api crate
- ❌ Any new abstraction layer: NO `PluginRegistry`, `PluginManager`, `PluginResolver`, `PluginChain` trait, `PrincipalPluginResolver` trait, `Box<dyn ...>` wrappers around the plugin chain
- ❌ Named-library indirection (`plugin_library: { name -> PluginRef }`) — inline only
- ❌ Renaming `PluginsConfig`, `PluginRef`, `router_plugin`, `observability_hooks`, or any existing public field
- ❌ Changing global plugin instantiation order, lifetime, or sharing semantics for the global path
- ❌ Admin API mutation endpoints (`POST /admin/principals/{id}/plugins` etc.)
- ❌ Dedup of identical `PluginRef`s across principals (single-tenant assumption preserved)
- ❌ Per-principal expansion of `signer_factory`, `error_normalizer`, dialect plugin, or any plugin slot besides `router_plugin` / `observability_hooks`
- ❌ New crate creation (e.g. `cc-lb-plugin-routing`)
- ❌ `principal_id` missing fallback metric/counter (load-once-and-bind makes the race impossible)
- ❌ Per-principal degradation marking (D5 = all-or-nothing)
- ❌ Reachability scans / GC sweeps for eviction (use Arc refcount + diff drop only)
- ❌ Test framework restructure or fixture file consolidation
- ❌ Premature `metrics::counter!` additions beyond what acceptance criteria explicitly demand
- ❌ Premature documentation rewrites (touch only README/CHANGELOG entries this plan demands)
- ❌ JSDoc/comment bloat or generic field names (`data`, `result`, `item`, `temp`)

### Spec Framework Integration
No `openspec/` or `.specify/` directory detected. Section omitted.

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — all acceptance criteria are agent-executable.

### Test Decision
- **Infrastructure exists**: YES
- **Automated tests**: YES (TDD)
- **Framework**: `cargo test` (workspace), `proptest`, `loom`
- **TDD pattern**: RED commit (failing test) → GREEN commit (minimal implementation passing the test) → REFACTOR commit (if needed) per task

### QA Policy
Every task includes Agent-Executed QA Scenarios that the executing agent MUST run.
Evidence saved to `.omo/evidence/task-{N}-{scenario-slug}.{ext}`.

- **Cargo tests**: `cargo test -p <crate> <test_name> -- --exact --nocapture` → capture stdout to evidence file
- **Integration tests**: `cargo test --workspace --test <name>` → capture stdout
- **Loom tests**: `RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test <name>` → capture stdout
- **API (admin status)**: `curl -s http://127.0.0.1:PORT/admin/status | jq` → save JSON
- **Build/lint**: `cargo build --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — type + key changes, mostly compile-time):
├── T1: Add failing JSON schema regression test for PrincipalSpec new fields [quick]
├── T2: Add router_plugin + observability_hooks fields to PrincipalSpec [quick]
├── T3: SlotKey newtype + ExtismRuntime instances/manifests widening [unspecified-high]
├── T4: PrincipalView::from_config -> Result + caller updates (no plugin instantiation yet) [unspecified-high]
├── T5: PrincipalSpecCached resolved-chain cache fields (Inherit | Explicit markers) [quick]
└── T6: validate_plugin_ref whole-graph extension + fixtures + RED test [unspecified-high]

Wave 2 (Per-principal instantiation + dispatch — needs Wave 1 foundation):
├── T7: ExtismRuntime per-principal register/lookup API + tests (depends: T3) [unspecified-high]
├── T8: PrincipalView::from_config instantiates per-principal plugins (depends: T4, T5, T7) [deep]
├── T9: Lifecycle stops owning self.router/self.observability_hooks; resolves via PrincipalView (depends: T5, T8) [deep]
├── T10: Lifecycle::handle load-once-and-bind + per-request dispatch via Principal chain + pre-authn global-only observe (depends: T9) [deep]
├── T11: app.rs startup wiring uses fallible PrincipalView::from_config with ExtismRuntime arg (depends: T4, T8) [quick]
├── T12: preflight.rs whole-graph dry-load all-or-nothing (depends: T6, T7) [unspecified-high]
├── T13: Property test for SlotKey uniqueness under arbitrary principal sets (depends: T7) [unspecified-high]
└── T14: Loom test: PrincipalView swap + concurrent handle() safety (depends: T8, T10) [unspecified-high]

Wave 3 (Hot-reload prep, admin, regression — needs Wave 2 wiring; 5 parallel):
├── T15: reload.rs per-principal path-change warnings + all-or-nothing reload abort (depends: T8, T12) [unspecified-high]
├── T18: Admin /status response: keep legacy plugins + add principals map (redacted config) (depends: T8) [unspecified-high]
├── T19: settings.rs plugin_count covers global + per-principal sum (depends: T2) [quick]
├── T20: Backward-compat regression: zero-principal-plugin config observe stream byte-diff (depends: T10) [unspecified-high]
└── T21: SSE batching divergence integration test (depends: T7, T10) [unspecified-high]

Wave 4 (Eviction + docs — needs Wave 3 reload prep; 2 parallel, dependency-forced):
├── T16: Hot-reload eviction of stale (principal_id, plugin_name) slots after view swap (depends: T15) [deep]
└── T23: CHANGELOG + per-principal-plugin docs entry (depends: T2, T15) [writing]
> Wave 4 has only 2 parallel tasks because T16 and T23 BOTH depend on T15 but each must
> precede different downstream consumers (T16 → T17/T22; T23 → F1). Splitting them into
> a single combined task would mix code + docs concerns; merging into Wave 3 violates the
> "wave members must be independently parallel" rule. The 2-task wave is dependency-forced.

Wave 5 (Final integration tests — needs eviction; 2 parallel, dependency-forced before FINAL):
├── T17: Fault-injection integration test: per-principal instantiation failure aborts reload (depends: T15, T16) [unspecified-high]
└── T22: Memory ceiling test (200 principals × 3 plugins) + hot-reload race test (depends: T8, T16) [unspecified-high]
> Wave 5 has only 2 parallel tasks because both are integration verifications that exercise
> the post-eviction runtime. They cannot run before T16 lands and cannot be merged into a
> single test (separate test files, separate harnesses, separate failure modes). This is
> the integration wave directly preceding FINAL; per planner template's exception, a 2-task
> wave at this position is acceptable.

Wave FINAL (4 parallel reviews; user okay required to complete):
├── F1: Plan compliance audit [oracle]
├── F2: Code quality review (build/clippy/test, slop sweep) [unspecified-high]
├── F3: Real manual QA (boot server, curl per principal, verify per-principal hook events) [unspecified-high]
└── F4: Scope fidelity check (diff vs spec, "Must NOT" sweep) [deep]
→ Present results → wait for user's explicit okay

Critical Path: T3 → T7 → T8 → T9 → T10 → T15 → T16 → T17 → F1-F4 → user okay
Max Concurrent: 8 (Wave 2 fully parallel)
```

### Dependency Matrix

- **T1**: — → T2
- **T2**: T1 → T19, T23, T6, T8 (PrincipalSpec must carry fields before validation/cache wiring)
- **T3**: — → T7, T13
- **T4**: — → T8, T11
- **T5**: T2 → T8, T9
- **T6**: T2 → T12, T15
- **T7**: T3 → T8, T12, T21
- **T8**: T4, T5, T7 → T9, T11, T14, T18, T15, T20, T22
- **T9**: T5, T8 → T10
- **T10**: T9 → T14, T20, T21
- **T11**: T4, T8 → preflight/server boot
- **T12**: T6, T7 → T15
- **T13**: T7 → final
- **T14**: T8, T10 → final
- **T15**: T8, T12 → T16, T17, T23
- **T16**: T15 → T17, T22
- **T17**: T15, T16 → final
- **T18**: T8 → F3
- **T19**: T2 → final
- **T20**: T10 → F1
- **T21**: T7, T10 → final
- **T22**: T8, T16 → final
- **T23**: T2, T15 → F1

### Agent Dispatch Summary

- **Wave 1 (6)**: T1 → `quick`, T2 → `quick`, T3 → `unspecified-high`, T4 → `unspecified-high`, T5 → `quick`, T6 → `unspecified-high`
- **Wave 2 (8)**: T7 → `unspecified-high`, T8 → `deep`, T9 → `deep`, T10 → `deep`, T11 → `quick`, T12 → `unspecified-high`, T13 → `unspecified-high`, T14 → `unspecified-high`
- **Wave 3 (5)**: T15 → `unspecified-high`, T18 → `unspecified-high`, T19 → `quick`, T20 → `unspecified-high`, T21 → `unspecified-high`
- **Wave 4 (2, dependency-forced)**: T16 → `deep`, T23 → `writing`
- **Wave 5 (2, dependency-forced)**: T17 → `unspecified-high`, T22 → `unspecified-high`
- **Wave FINAL (4)**: F1 → `oracle`, F2 → `unspecified-high`, F3 → `unspecified-high`, F4 → `deep`

---

## TODOs

- [x] 1. Add failing JSON schema regression test for new `PrincipalSpec` fields

  **What to do**:
  - Add a new test in `crates/cc-lb-config/src/types.rs` (or a sibling `tests/` file under `cc-lb-config`) that asserts the JSON Schema generated for `PrincipalSpec` contains optional `router_plugin` and `observability_hooks` fields with the correct shape (`Option<PluginRef>` and `Option<Vec<PluginRef>>`).
  - Add a TOML fixture that exercises a principal carrying both new fields, parsed via existing `figment` loader; assert parsed `PrincipalSpec` matches expected struct.
  - The test MUST fail today (RED) because the fields do not yet exist.

  **Must NOT do**:
  - Do not modify `PrincipalSpec` itself in this task — only the test code + fixtures.
  - Do not add fields to `Config` or `ApiKeyRecord`.
  - Do not introduce a new helper crate or trait.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Pure test/fixture authoring against existing patterns; no production code touched.
  - **Skills**: none
    - Reason: Existing test patterns in `crates/cc-lb-config/src/types.rs` (line 600+ has `#[cfg(test)] mod tests`) are self-evident.

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with T2-T6)
  - **Blocks**: T2 (T2 must turn this test GREEN)
  - **Blocked By**: None — can start immediately

  **References**:

  **Pattern References**:
  - `crates/cc-lb-config/src/types.rs:800-950` — existing `#[cfg(test)] mod tests` for parsing Config from TOML; copy the TOML/figment pattern.
  - `crates/cc-lb-config/src/types.rs:836-840` — assertion style for parsed config values (e.g. `assert_eq!(config.downstream_auth.mode, DownstreamAuthMode::ApiKey)`).

  **API/Type References**:
  - `crates/cc-lb-config/src/types.rs:309-332` — current `PrincipalSpec` struct (no plugin fields yet).
  - `crates/cc-lb-config/src/types.rs:344-368` — `PluginRef` struct (the type used in new fields).
  - `crates/cc-lb-config/src/types.rs:336-340` — `PluginsConfig` for reference shape.

  **External References**:
  - `schemars` v1 docs: `https://docs.rs/schemars/latest/schemars/` — for asserting generated schema shape if a schema-based assertion is preferred.

  **WHY each reference matters**: existing test module shows how to load TOML/TOML and assert on struct fields via figment. The PluginRef definition is the canonical shape that must round-trip.

  **Acceptance Criteria**:
  - [ ] New test file/test fn exists under `crates/cc-lb-config/`
  - [ ] `cargo test -p cc-lb-config -- principal_spec_plugin_fields` FAILS today with a clear message naming `router_plugin`/`observability_hooks` as missing — capture stderr.

  **QA Scenarios**:
  ```
  Scenario: RED commit — failing test
    Tool: Bash (cargo)
    Preconditions: New test committed, PrincipalSpec unchanged
    Steps:
      1. Run: cargo test -p cc-lb-config principal_spec_plugin_fields --no-fail-fast 2>&1 | tee .omo/evidence/task-1-red.txt
      2. Inspect output for a compile error or assertion failure naming `router_plugin` or `observability_hooks`
    Expected Result: Non-zero exit code; output contains "router_plugin" or "observability_hooks" reference
    Failure Indicators: Test passes (no RED) → test does not actually exercise the new fields
    Evidence: .omo/evidence/task-1-red.txt

  Scenario: TOML fixture round-trip (forward-looking assertion)
    Tool: Bash (cargo)
    Preconditions: Fixture file committed
    Steps:
      1. Run: cargo test -p cc-lb-config principal_spec_plugin_fields_toml_fixture --no-fail-fast 2>&1 | tee .omo/evidence/task-1-fixture-red.txt
    Expected Result: Compile or assertion failure; once T2 lands, this test will go green
    Evidence: .omo/evidence/task-1-fixture-red.txt
  ```

  **Commit**: YES
  - Message: `test(config): add failing schema test for principal plugin fields`
  - Files: `crates/cc-lb-config/src/types.rs` (test module), `crates/cc-lb-config/tests/fixtures/per_principal_plugins/principal_with_plugins.toml`
  - Pre-commit: `cargo fmt --check && cargo test -p cc-lb-config principal_spec_plugin_fields` (expect failure — commit anyway as RED)

- [x] 2. Add `router_plugin` + `observability_hooks` fields to `PrincipalSpec`

  **What to do**:
  - Edit `crates/cc-lb-config/src/types.rs` `PrincipalSpec` (line 309-332):
    - Add `pub router_plugin: Option<PluginRef>` with `#[serde(default, skip_serializing_if = "Option::is_none")]`.
    - Add `pub observability_hooks: Option<Vec<PluginRef>>` with `#[serde(default, skip_serializing_if = "Option::is_none")]`.
  - Update `impl Default for PrincipalSpec` (line 322-331) to set both new fields to `None`.
  - Keep `#[serde(default, deny_unknown_fields)]` on the struct (intentional — typos like `router-plugin` should be hard errors).
  - Make T1's RED test go GREEN.

  **Must NOT do**:
  - Do not rename existing fields.
  - Do not change `PluginRef` shape.
  - Do not add `principal_id` to `PluginRef`.
  - Do not add a `default_plugins`/`global_plugins` helper field to `PrincipalSpec`.
  - Do not introduce a new enum like `PluginInherit { Inherit, Explicit(...) }` in `cc-lb-config` — keep raw `Option<Vec<PluginRef>>`; the `Inherit | Explicit` semantic lives in `PrincipalSpecCached` (T5), not here.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Two field additions, default impl update, no logic.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO (in same wave but starts when T1 lands)
  - **Parallel Group**: Wave 1
  - **Blocks**: T5 (PrincipalSpecCached extension), T6 (validation), T8 (per-principal instantiation), T19 (admin plugin_count), T23 (docs)
  - **Blocked By**: T1

  **References**:

  **Pattern References**:
  - `crates/cc-lb-config/src/types.rs:336-340` — `PluginsConfig` field declarations (the model to follow).
  - `crates/cc-lb-config/src/types.rs:322-331` — current `impl Default for PrincipalSpec` (must add new field defaults).
  - `crates/cc-lb-config/src/types.rs:297-306` — `UpstreamSpec` field pattern with `#[serde(default, skip_serializing_if = "Option::is_none")]` (apply identically).

  **API/Type References**:
  - `crates/cc-lb-config/src/types.rs:344-368` — `PluginRef` (referenced type; must NOT change).

  **WHY each reference matters**: existing PluginsConfig and UpstreamSpec demonstrate the canonical serde attribute combo for optional/inheritable fields. Following them preserves config-file UX.

  **Acceptance Criteria**:
  - [ ] `crates/cc-lb-config/src/types.rs` `PrincipalSpec` has both new fields with correct attributes
  - [ ] `cargo test -p cc-lb-config principal_spec_plugin_fields` PASSES (T1's RED test is now GREEN)
  - [ ] `cargo test -p cc-lb-config` full crate passes (existing tests untouched)
  - [ ] `cargo build --workspace` passes

  **QA Scenarios**:
  ```
  Scenario: GREEN commit — T1 test now passes
    Tool: Bash (cargo)
    Preconditions: T1 committed, T2 implementation applied
    Steps:
      1. Run: cargo test -p cc-lb-config principal_spec_plugin_fields --no-fail-fast 2>&1 | tee .omo/evidence/task-2-green.txt
    Expected Result: Exit 0; "test result: ok" in output
    Failure Indicators: Test still fails → field shape or default impl wrong
    Evidence: .omo/evidence/task-2-green.txt

  Scenario: Reject typo in TOML (deny_unknown_fields preserved)
    Tool: Bash (cargo)
    Preconditions: Fixture with `router-plugin` (hyphenated typo) under tests/fixtures
    Steps:
      1. Add fixture `crates/cc-lb-config/tests/fixtures/per_principal_plugins/principal_typo.toml` containing a `[principals.alice]` block with `router-plugin = { name = "foo" }` (hyphenated key — should be rejected by `deny_unknown_fields`)
      2. Add test `principal_spec_typo_rejected` that loads the fixture and expects parse error
      3. Run: cargo test -p cc-lb-config principal_spec_typo_rejected 2>&1 | tee .omo/evidence/task-2-typo.txt
    Expected Result: Test asserts an error mentioning "unknown field" or "deny_unknown_fields"
    Evidence: .omo/evidence/task-2-typo.txt
  ```

  **Commit**: YES
  - Message: `feat(config): add router_plugin + observability_hooks to PrincipalSpec`
  - Files: `crates/cc-lb-config/src/types.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-config -- -D warnings && cargo test -p cc-lb-config`

- [x] 3. `SlotKey` newtype + `ExtismRuntime` `instances`/`manifests` widening

  **What to do**:
  - In `crates/cc-lb-runtime-extism/src/lib.rs`:
    - Introduce `pub(crate) struct SlotKey { principal: String, plugin: String }` (or a tuple newtype `pub(crate) struct SlotKey(pub String, pub String);`) with `#[derive(Clone, Eq, PartialEq, Hash, Debug)]`.
    - Add a `const GLOBAL_PRINCIPAL: &str = "__global__"` sentinel.
    - Change `instances: RwLock<HashMap<String, Arc<PluginSlot>>>` → `RwLock<HashMap<SlotKey, Arc<PluginSlot>>>` (line ~107).
    - Change `manifests: RwLock<HashMap<String, PluginEntry>>` → `RwLock<HashMap<SlotKey, PluginEntry>>` (line ~108).
    - Update every call site inside the crate: `register_slot` (line 141-171), `reload` (line 173-200), all `.get`/`.insert`/`.remove` accesses. Preserve the existing `ArcSwap<PluginCell>` swap semantics on `PluginSlot::replace`.
    - Add a precise RED test BEFORE implementation: a unit test that registers `SlotKey { principal: "alice", plugin: "router" }` and `SlotKey { principal: "bob", plugin: "router" }` and asserts both coexist; today's `String` key would collide.

  **Must NOT do**:
  - Do not change `PluginSlot` or `PluginCell` internals.
  - Do not change `ArcSwap` semantics.
  - Do not change the public crate API except where required to widen the key (the existing public `instantiate_router(&PluginManifest)` etc. should remain — they internally translate to `SlotKey { principal: GLOBAL_PRINCIPAL, plugin: name }`).
  - Do not split this into a new submodule unless `lib.rs` size mandates it (Metis guardrail: minimize structural churn).
  - Do not introduce dedup across SlotKeys.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Multi-site refactor in a concurrency-critical crate; needs careful enumeration of call sites.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T7 (per-principal API surface), T13 (property test), T14 (loom test)
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/cc-lb-runtime-extism/src/lib.rs:141-200` — current `register_slot` + `reload` (single-key model).
  - `crates/cc-lb-runtime-extism/src/lib.rs:291-313` — `PluginSlot { ArcSwap<PluginCell> }` — preserve unchanged.

  **API/Type References**:
  - `crates/cc-lb-runtime-extism/src/lib.rs:105-110` — `ExtismRuntime` struct (target for key widening).
  - `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` — wrappers that hold `Arc<PluginSlot>`; check signatures.

  **Test References**:
  - `crates/cc-lb-runtime-extism/tests/` — existing test patterns.
  - `crates/cc-lb-plugin-api/src/traits.rs` — `RouterPlugin`, `ObservabilityHook` trait shapes (handles returned by `instantiate_*`).

  **WHY each reference matters**: the `ArcSwap` semantic is the lynchpin of hot-reload safety — preserving it untouched while widening the key proves the refactor is purely structural. Existing single-key tests anchor the regression surface.

  **Acceptance Criteria**:
  - [ ] `SlotKey` defined and used as the key type for both maps
  - [ ] `GLOBAL_PRINCIPAL` sentinel exported within the crate (not as public API)
  - [ ] RED test (two same-name plugins under different principals) committed first, then GREEN
  - [ ] `cargo test -p cc-lb-runtime-extism` passes
  - [ ] No public-API breakage outside the crate (verify with `cargo build --workspace`)
  - [ ] No new `unsafe` blocks introduced

  **QA Scenarios**:
  ```
  Scenario: Coexistence of same-name plugins under different principals
    Tool: Bash (cargo)
    Preconditions: SlotKey applied, both maps widened
    Steps:
      1. Run: cargo test -p cc-lb-runtime-extism slotkey_coexistence -- --nocapture 2>&1 | tee .omo/evidence/task-3-coexist.txt
    Expected Result: Both Arc<PluginSlot> entries observable via instances.read(); .len() == 2
    Evidence: .omo/evidence/task-3-coexist.txt

  Scenario: Existing single-key tests still pass
    Tool: Bash (cargo)
    Preconditions: Refactor complete
    Steps:
      1. Run: cargo test -p cc-lb-runtime-extism 2>&1 | tee .omo/evidence/task-3-regression.txt
    Expected Result: Exit 0; all existing tests pass
    Evidence: .omo/evidence/task-3-regression.txt

  Scenario: Workspace build clean
    Tool: Bash (cargo)
    Preconditions: All crates consume new crate API
    Steps:
      1. Run: cargo build --workspace --all-targets 2>&1 | tee .omo/evidence/task-3-build.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-3-build.txt
  ```

  **Commit**: YES (RED + GREEN as separate commits)
  - RED: `test(runtime-extism): add failing same-name-across-principals coexistence test`
  - GREEN: `refactor(runtime-extism): widen instance/manifest maps via SlotKey newtype`
  - Files: `crates/cc-lb-runtime-extism/src/lib.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-runtime-extism -- -D warnings && cargo test -p cc-lb-runtime-extism && cargo build --workspace`

- [x] 4. `PrincipalView::from_config` → `Result<Arc<PrincipalView>, ConfigError>` (caller updates only; no plugin instantiation yet)

  **What to do**:
  - Add (or reuse) a `ConfigError` variant suitable for principal_view failures in the smallest scoped location (likely a new error type in `cc-lb-core/src/api_keys/principal_view.rs` or extending an existing error enum). Do NOT add new variants beyond what this signature change strictly requires.
  - Change `PrincipalView::from_config(&Config) -> Arc<PrincipalView>` → `PrincipalView::from_config(&Config) -> Result<Arc<PrincipalView>, ConfigError>` (signature only — body still infallible; replace `.expect("invalid principal allowed_models glob")` (line ~43, 51) with `?` returning the new error).
  - Update ALL caller sites:
    - `crates/cc-lb-server/src/app.rs:257-265` (and wherever startup builds the initial view)
    - `crates/cc-lb-server/src/reload.rs:101-104` (hot-reload swap)
    - `crates/cc-lb-server/src/preflight.rs:95-106`
    - Any test that calls `PrincipalView::from_config` (run `rg "PrincipalView::from_config"` across the workspace first; use the output as a checklist).
  - For startup, propagate the error to `anyhow::Result<()>` at `main`; the server refuses to start with a clear error message.
  - For hot-reload, log the error at WARN level and preserve the previous view (do not swap).

  **Must NOT do**:
  - Do not start instantiating plugins inside `PrincipalView::from_config` in this task — that is T8.
  - Do not add an `ExtismRuntime` parameter to `from_config` yet — that is added in T8 to keep this diff strictly scoped to signature changes.
  - Do not introduce `Box<dyn Error>`; use a concrete error type.
  - Do not add a `Result<>` to any other `PrincipalView` method that doesn't already need it.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Wide-impact signature change across multiple crates; needs careful caller enumeration and error type design.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T8 (plugin instantiation), T11 (startup wiring)
  - **Blocked By**: None

  **References**:

  **Pattern References**:
  - `crates/cc-lb-core/src/api_keys/principal_view.rs:32-72` — current `from_config` infallible body.
  - `crates/cc-lb-core/src/api_keys/principal_view.rs:43,51` — `.expect` sites to convert.
  - `crates/cc-lb-server/src/reload.rs:101-104` — `principal_view.store(PrincipalView::from_config(&new_config))` site (must handle `Result`).
  - `crates/cc-lb-server/src/app.rs:257-265` — startup site.

  **API/Type References**:
  - `crates/cc-lb-config/src/types.rs` — `Config` shape (input to `from_config`).
  - `globset` crate — current source of the `expect` failures.

  **Test References**:
  - Use `mcp__oc__lsp_find_references` on `PrincipalView::from_config` before editing to enumerate ALL caller sites; commit the list to the task description so reviewers can verify.

  **WHY each reference matters**: signature changes that miss a caller produce build errors; an LSP-driven enumeration prevents silent breakage.

  **Acceptance Criteria**:
  - [ ] `PrincipalView::from_config` returns `Result<Arc<PrincipalView>, ConfigError>`
  - [ ] All `.expect` panics replaced with `?` and a `ConfigError` variant
  - [ ] Every caller updated (verified via `rg "PrincipalView::from_config" --type rust`)
  - [ ] Startup propagates error to `anyhow::Result<()>` (server refuses to start on bad principal config)
  - [ ] Hot-reload preserves previous view on error, logs at WARN
  - [ ] `cargo test -p cc-lb-core` passes
  - [ ] `cargo build --workspace` passes

  **QA Scenarios**:
  ```
  Scenario: Bad allowed_models glob fails reload, preserves previous view
    Tool: Bash (cargo)
    Preconditions: Test fixture with invalid glob like `**[invalid` in a principal's allowed_models
    Steps:
      1. Add integration test `principal_view_invalid_glob_keeps_old_view` that sets up a valid initial view, calls reload with the bad fixture, asserts:
         - new view is NOT published
         - error log emitted at WARN
         - original view still returned by `arc_swap.load()`
      2. Run: cargo test -p cc-lb-core principal_view_invalid_glob_keeps_old_view 2>&1 | tee .omo/evidence/task-4-bad-glob.txt
    Expected Result: Exit 0; test asserts WARN log + view unchanged
    Evidence: .omo/evidence/task-4-bad-glob.txt

  Scenario: Startup refuses to start with bad config
    Tool: Bash (cargo + cc-lb binary)
    Preconditions: Test config file with invalid principal
    Steps:
      1. Write bad config to /tmp/bad-principal.toml
      2. Run: cargo run -p cc-lb-server -- --config /tmp/bad-principal.toml 2>&1 | tee .omo/evidence/task-4-startup-refuse.txt
    Expected Result: Non-zero exit; stderr contains the ConfigError variant name and principal identifier
    Evidence: .omo/evidence/task-4-startup-refuse.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(core): add failing PrincipalView::from_config error propagation test`
  - GREEN: `refactor(core): make PrincipalView::from_config fallible`
  - Files: `crates/cc-lb-core/src/api_keys/principal_view.rs`, `crates/cc-lb-server/src/app.rs`, `crates/cc-lb-server/src/reload.rs`, `crates/cc-lb-server/src/preflight.rs`, any updated tests
  - Pre-commit: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo build --workspace && cargo test -p cc-lb-core`

- [x] 5. `PrincipalSpecCached` resolved-chain cache fields

  **What to do**:
  - In `crates/cc-lb-core/src/api_keys/principal_view.rs`, extend `PrincipalSpecCached` with:
    - `pub router_plugin: RouterPluginCache` where `pub enum RouterPluginCache { Inherit, Explicit(Arc<dyn RouterPlugin>) }`.
    - `pub observability_hooks: ObservabilityHooksCache` where `pub enum ObservabilityHooksCache { Inherit, Explicit(Vec<Arc<dyn ObservabilityHook>>) }`.
  - Define these enums in the same module (private to crate). Keep them concrete — do NOT use `Box<dyn ...>` wrappers.
  - In this task, leave `PrincipalView::from_config` returning `RouterPluginCache::Inherit` and `ObservabilityHooksCache::Inherit` for every principal — the actual instantiation logic lives in T8. This task is purely scaffolding.
  - Add a method `pub fn resolved_router<'a>(&'a self, global: &'a Arc<dyn RouterPlugin>) -> &'a Arc<dyn RouterPlugin>` and `pub fn resolved_hooks<'a>(&'a self, global: &'a [Arc<dyn ObservabilityHook>]) -> &'a [Arc<dyn ObservabilityHook>]` that implement the inherit-vs-explicit resolution by borrowing — no per-request Vec clones.

  **Must NOT do**:
  - Do not instantiate any Extism plugin here (T8 territory).
  - Do not store `PluginRef` directly on the cache — only resolved handles or `Inherit`.
  - Do not add a third enum variant like `Disabled` — `Explicit(vec![])` already represents that case.
  - Do not introduce a `PluginChain` struct or trait.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Local additive change in one file.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T8, T9
  - **Blocked By**: T2 (needs `PrincipalSpec` fields to exist)

  **References**:

  **Pattern References**:
  - `crates/cc-lb-core/src/api_keys/principal_view.rs:32-74` — current `PrincipalSpecCached`, `from_config`, `get` patterns.
  - `crates/cc-lb-plugin-api/src/traits.rs` — `RouterPlugin`, `ObservabilityHook` trait signatures.

  **API/Type References**:
  - `crates/cc-lb-core/src/lifecycle.rs:327, 748, 790` — call sites that will consume `resolved_router` / `resolved_hooks` in T10. Plan the borrow lifetimes accordingly.

  **WHY each reference matters**: lifecycle's per-request dispatch is the consumer; designing the resolver to return borrowed references avoids per-request Vec cloning that Metis flagged.

  **Acceptance Criteria**:
  - [ ] `RouterPluginCache` and `ObservabilityHooksCache` enums defined with exactly two variants each
  - [ ] `PrincipalSpecCached` carries both
  - [ ] `resolved_router` and `resolved_hooks` methods compile and return borrowed refs (no `Arc::clone` inside)
  - [ ] Existing `PrincipalView` tests still pass (cache fields are unused so far; just scaffolding)
  - [ ] `cargo test -p cc-lb-core` passes

  **QA Scenarios**:
  ```
  Scenario: Inherit case returns global
    Tool: Bash (cargo)
    Preconditions: Unit test in principal_view.rs constructing a cached spec with Inherit
    Steps:
      1. Add unit test `principal_spec_cached_inherit_resolves_to_global` that builds a dummy global router Arc, a `PrincipalSpecCached { router_plugin: Inherit, .. }`, and asserts `Arc::ptr_eq(global, cached.resolved_router(&global))`.
      2. Run: cargo test -p cc-lb-core principal_spec_cached_inherit_resolves_to_global -- --nocapture 2>&1 | tee .omo/evidence/task-5-inherit.txt
    Expected Result: Exit 0; ptr_eq is true
    Evidence: .omo/evidence/task-5-inherit.txt

  Scenario: Explicit empty hooks returns empty slice (not global)
    Tool: Bash (cargo)
    Preconditions: Unit test
    Steps:
      1. Add unit test `principal_spec_cached_explicit_empty_hooks_returns_empty` that builds `ObservabilityHooksCache::Explicit(vec![])` and global with two hooks, asserts `resolved_hooks(&global).len() == 0`.
      2. Run: cargo test -p cc-lb-core principal_spec_cached_explicit_empty_hooks_returns_empty 2>&1 | tee .omo/evidence/task-5-explicit-empty.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-5-explicit-empty.txt
  ```

  **Commit**: YES
  - Message: `feat(core): add resolved-plugin-chain cache to PrincipalSpecCached`
  - Files: `crates/cc-lb-core/src/api_keys/principal_view.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-core -- -D warnings && cargo test -p cc-lb-core`

- [x] 6. `validate_plugin_ref` whole-graph extension + fixtures + RED test

  **What to do**:
  - In `crates/cc-lb-config/src/validation.rs:212-218`, extend validation so it iterates `config.principals.values()` and validates each `principal.router_plugin` and each entry in `principal.observability_hooks` via `validate_plugin_ref` with a path string like `principals.{name}.router_plugin` / `principals.{name}.observability_hooks.{index}`.
  - Add these specific rejection rules (all-or-nothing per D5):
    - Empty `PluginRef.name` → reject.
    - Missing `PluginRef.wasm_path` file on disk → reject (use existing canonicalization helper if present in validation.rs).
    - Duplicate plugin `name` within a single principal's `observability_hooks` → reject.
    - Same plugin `name` across principals with **different** `wasm_path` or `config` is **allowed** (each principal gets its own slot — see Metis "single-tenant assumption preserved").
    - Same plugin `name` between global and a principal with **different** `wasm_path` or `config` is **allowed** for the same reason; document this in a comment.
  - Add RED tests in `crates/cc-lb-config/tests/fixtures/per_principal_plugins/`:
    - `principal_with_invalid_wasm.toml` — references a non-existent path; expect rejection.
    - `principal_with_duplicate_hooks.toml` — `[ {name:a}, {name:a} ]` in one principal; expect rejection.
    - `principal_with_empty_name.toml` — empty name; expect rejection.
    - `principal_with_same_name_different_config.toml` — same name across principals with different `config`; expect ACCEPT (positive).

  **Must NOT do**:
  - Do not introduce a new `PluginRefValidator` struct or trait.
  - Do not validate global and per-principal independently — must run as one pass that aborts on the first error (with a clear path message).
  - Do not skip validation when `config.principals` is empty (zero principals must still pass).
  - Do not write a single `validate_principal_plugins` helper if inlining 6-10 lines into the existing `validate` function is clearer.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Mixes file-system checks, error-path enumeration, and fixture authoring; care needed to keep failure messages actionable.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1
  - **Blocks**: T12 (preflight uses these rules), T15 (reload uses these rules)
  - **Blocked By**: T2

  **References**:

  **Pattern References**:
  - `crates/cc-lb-config/src/validation.rs:212-218` — current global `validate_plugin_ref` call.
  - `crates/cc-lb-config/src/validation.rs` (file) — existing principle validation walks (find: `principal`, `default_limits`, `allowed_models` validation patterns).

  **API/Type References**:
  - `crates/cc-lb-config/src/types.rs:344-368` — `PluginRef` field names.

  **Test References**:
  - `crates/cc-lb-config/tests/fixtures/` — existing fixture directory layout.

  **External References**:
  - None.

  **WHY each reference matters**: existing validation already walks `principals` for `allowed_models` glob checks; the new plugin walks should slot in next to those for locality.

  **Acceptance Criteria**:
  - [ ] All four new fixtures present and used by tests
  - [ ] `cargo test -p cc-lb-config validation_per_principal_plugin` PASSES (each RED test now GREEN via implementation in the same commit pair)
  - [ ] Error messages include the path `principals.{name}.router_plugin` or `principals.{name}.observability_hooks.{i}`
  - [ ] Zero-principal config still passes validation unchanged
  - [ ] `cargo test -p cc-lb-config` full crate passes

  **QA Scenarios**:
  ```
  Scenario: Invalid wasm path rejected with actionable error
    Tool: Bash (cargo)
    Preconditions: Fixture committed
    Steps:
      1. Run: cargo test -p cc-lb-config validation_principal_invalid_wasm_path 2>&1 | tee .omo/evidence/task-6-invalid-wasm.txt
    Expected Result: Test asserts error string contains both `principals.alice.router_plugin` and the missing path
    Evidence: .omo/evidence/task-6-invalid-wasm.txt

  Scenario: Same name + different config across principals ACCEPTED
    Tool: Bash (cargo)
    Preconditions: Positive fixture committed
    Steps:
      1. Run: cargo test -p cc-lb-config validation_same_plugin_name_different_principals_ok 2>&1 | tee .omo/evidence/task-6-same-name-ok.txt
    Expected Result: Validation passes; test asserts loaded principals have distinct PluginRef.config values
    Evidence: .omo/evidence/task-6-same-name-ok.txt

  Scenario: Duplicate names within one principal's hooks rejected
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test -p cc-lb-config validation_duplicate_hook_names_rejected 2>&1 | tee .omo/evidence/task-6-dup.txt
    Expected Result: Error contains `principals.bob.observability_hooks` and the duplicate name
    Evidence: .omo/evidence/task-6-dup.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(config): add failing per-principal PluginRef validation fixtures`
  - GREEN: `feat(config): validate per-principal PluginRef whole-graph`
  - Files: `crates/cc-lb-config/src/validation.rs`, fixtures under `crates/cc-lb-config/tests/fixtures/per_principal_plugins/`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-config -- -D warnings && cargo test -p cc-lb-config`

- [x] 7. `ExtismRuntime` per-principal slot register / lookup API + tests

  **What to do**:
  - In `crates/cc-lb-runtime-extism/src/lib.rs`, add per-principal entry points that complement the existing global ones. **Critical: instantiation must be STAGING-ONLY — do NOT mutate the live `instances`/`manifests` maps or call `PluginSlot::replace` until an explicit commit step.** This is the foundation for T8's all-or-nothing rollback:
    - `pub fn instantiate_router_for(&self, principal_id: &str, plugin_name: &str, manifest: &PluginManifest) -> Result<(Arc<dyn RouterPlugin>, StagedSlot), RuntimeError>` — builds a brand-new `PluginSlot` + `PluginCell` on the heap and returns the trait handle paired with a `StagedSlot`. Does NOT touch `self.instances` or `self.manifests`.
    - `pub fn instantiate_observability_for(&self, principal_id: &str, plugin_name: &str, manifest: &PluginManifest) -> Result<(Arc<dyn ObservabilityHook>, StagedSlot), RuntimeError>` — same staging contract.
    - `pub fn instantiate_router_global(&self, plugin_name: &str, manifest: &PluginManifest) -> Result<(Arc<dyn RouterPlugin>, StagedSlot), RuntimeError>` — convenience wrapper that calls `instantiate_router_for(<crate-internal global sentinel>, plugin_name, manifest)`. Lets external crates stage global slots without naming the sentinel.
    - `pub fn instantiate_observability_global(&self, plugin_name: &str, manifest: &PluginManifest) -> Result<(Arc<dyn ObservabilityHook>, StagedSlot), RuntimeError>` — same for hooks.
    - `pub struct StagedSlot { /* opaque */ }` — pub but field-opaque (carries internal `SlotKey` + `Arc<PluginSlot>`). Consumers do not destructure it.
    - `pub fn commit_staged(&self, staged: Vec<StagedSlot>)` — atomically inserts each entry into `instances` AND `manifests` (replaces existing entries for the same `SlotKey`; the replaced entries are dropped, which is safe because in-flight `Arc<PluginSlot>` references keep the OLD cell alive for the duration of any bound request). Uses `instances.write()` and `manifests.write()` exactly once each for the whole batch.
    - `pub fn evict_slot(&self, principal_id: &str, plugin_name: &str)` (consumers: T16 post-swap eviction in `cc-lb-server`). Internally constructs `SlotKey { principal: principal_id.to_string(), plugin: plugin_name.to_string() }` and removes the entry from both `instances` and `manifests` maps.
    - `pub fn registered_slot_keys(&self) -> Vec<(String, String)>` returning `(principal_id, plugin_name)` pairs (consumer: T16 diff computation). Reads under `instances.read()`.
  - `SlotKey` newtype itself stays `pub(crate)` — it is an implementation detail; consumers use `(principal_id, plugin_name)` string pairs at the API boundary and `StagedSlot` as the opaque carrier between `instantiate_*_for` and `commit_staged`.
  - Existing public global functions (`instantiate_router`, `instantiate_observability`) keep their **immediate-insert** semantics for now (server startup pre-T11 still uses them; once T11 lands, the new wiring uses the staging variants exclusively).
  - **Why staging matters**: under hot-reload, if a per-principal `(principal_id, plugin_name)` already exists, `PluginSlot::replace`'s `ArcSwap::store(new_cell)` would commit the new cell before T8 knows the overall reload succeeds. A later failure on a different principal would leave us with a partially-swapped runtime that `evict_slot` cannot un-do (the old cell is gone). Staging defers all swaps to a single post-validation commit point, preserving all-or-nothing.
  - Reuse the same `PluginSlot` internal mechanism for the staged slot — no new locking discipline beyond the existing `RwLock<HashMap<...>>`.
  - Add unit tests: register two staged slots with same plugin name under different principals, commit, verify both `Arc<dyn RouterPlugin>` handles work independently (instantiate twice, observe distinct host_state per slot if any).

  **Must NOT do**:
  - Do not change `register_slot`'s contract — only its key.
  - Do not introduce a per-principal lock; the existing `RwLock<HashMap<SlotKey, _>>` is sufficient.
  - Do not change the `RouterPlugin` / `ObservabilityHook` trait shapes.
  - Do not implement eviction in this task (T16 territory) — `evict_slot` only deletes the entry from the map without further cleanup beyond Arc drop.
  - Do not implement lazy instantiation — all instantiation is eager and explicit.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Public API surface change in a runtime crate; must preserve all existing test signatures.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO (in same wave but depends on T3)
  - **Parallel Group**: Wave 2
  - **Blocks**: T8, T12, T21
  - **Blocked By**: T3

  **References**:

  **Pattern References**:
  - `crates/cc-lb-runtime-extism/src/lib.rs:141-200` — `register_slot` + `reload` (model for the per-principal variants).
  - `crates/cc-lb-runtime-extism/src/lib.rs:291-313` — `PluginSlot` reuse.

  **API/Type References**:
  - Existing `instantiate_router` / `instantiate_observability` signatures (find via `rg "pub fn instantiate"`).

  **WHY each reference matters**: keeping the global wrappers ensures global-only configs still produce the same instances at the same `SlotKey { principal: "__global__", plugin: name }`, preserving backward compat at the runtime layer.

  **Acceptance Criteria**:
  - [ ] New `_for` variants compile and pass test
  - [ ] Global wrappers delegate to `_for` with sentinel
  - [ ] `cargo test -p cc-lb-runtime-extism` passes (including T3's coexistence test)
  - [ ] `cargo build --workspace`

  **QA Scenarios**:
  ```
  Scenario: Two principals same plugin name produce two independent instances
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test -p cc-lb-runtime-extism per_principal_independent_instances --nocapture 2>&1 | tee .omo/evidence/task-7-independent.txt
    Expected Result: Two distinct Arc<PluginSlot> pointers observed; calling each instance's `extism::Plugin::call` does not interfere (use a counter-style fixture plugin)
    Evidence: .omo/evidence/task-7-independent.txt

  Scenario: Global wrapper still works
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test -p cc-lb-runtime-extism global_wrapper_unchanged 2>&1 | tee .omo/evidence/task-7-global.txt
    Expected Result: Pre-T7 test fixtures pass; SlotKey internally is `("__global__", name)`
    Evidence: .omo/evidence/task-7-global.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(runtime-extism): add failing per-principal instantiate_for tests`
  - GREEN: `feat(runtime-extism): per-principal slot register/lookup API`
  - Files: `crates/cc-lb-runtime-extism/src/lib.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-runtime-extism -- -D warnings && cargo test -p cc-lb-runtime-extism`

- [x] 8. `PrincipalView::from_config` instantiates per-principal plugins

  **What to do**:
  - Change `PrincipalView::from_config(&Config) -> Result<...>` (post-T4) to `from_config(&Config, principal_chains: HashMap<String, (RouterPluginCache, ObservabilityHooksCache)>) -> Result<Arc<PrincipalView>, ConfigError>`. **Oracle phase-3 decision: responsibility split.** cc-lb-core does NOT depend on cc-lb-runtime-extism. The caller (server `app.rs` at T11; reload at T15) is responsible for: (1) iterating `config.principals`, (2) calling `runtime.instantiate_router_for(...)` / `instantiate_observability_for(...)` for each principal with plugin overrides, (3) accumulating both the `principal_chains` map AND a separate `Vec<StagedSlot>`, (4) calling `runtime.commit_staged(staged)` BEFORE `principal_view.store(new_view)`. Global router/hooks live on `Lifecycle` (T9), NOT on `PrincipalView`.
  - Maintain a local `let mut staged: Vec<StagedSlot> = Vec::new();` accumulator.
  - For each principal:
    - If `principal.router_plugin.is_some()`: call `let (handle, slot) = runtime.instantiate_router_for(principal_name, &plugin.name, manifest)?;` (staging-only — does NOT touch live maps). Push `slot` into `staged`. Store `RouterPluginCache::Explicit(handle)` on the cached spec.
    - Else: store `RouterPluginCache::Inherit`.
    - If `principal.observability_hooks.is_some()`: for each entry, call `let (handle, slot) = runtime.instantiate_observability_for(principal_name, &plugin.name, manifest)?;` (in declared order), push `slot`, accumulate `handle`s into `Vec<Arc<dyn ObservabilityHook>>`. Store `ObservabilityHooksCache::Explicit(Vec<Arc<_>>)`.
    - Else: store `ObservabilityHooksCache::Inherit`.
  - **Responsibility split (Oracle phase-3 decision)**: cc-lb-core does NOT take an `&ExtismRuntime` parameter and does NOT depend on cc-lb-runtime-extism. The server (T11/T15) owns the staging loop and the global chain. The data flow is:
    1. server: `let mut staged: Vec<StagedSlot> = Vec::new();`
    2. server builds global router (either `BuiltinRouter::new(&config)` if no global plugin OR `runtime.instantiate_router_global(&plugin.name, &manifest)?` then push staged) and global hooks (each via `instantiate_observability_global`, push staged for each).
    3. server iterates `config.principals` and for each principal builds `(RouterPluginCache, ObservabilityHooksCache)`: `None` -> `Inherit`; `Some(plugin)` -> call `runtime.instantiate_*_for(principal_id, &plugin.name, &manifest)`, push the returned StagedSlot into `staged`, wrap the returned handle into `Explicit(handle)`. Collect these into `principal_chains: HashMap<String, (RouterPluginCache, ObservabilityHooksCache)>`.
    4. server: `let view = PrincipalView::from_config(&config, principal_chains)?;` — this validates allowed_models globs and stitches `PrincipalSpecCached` entries; pure data assembly, no runtime calls.
    5. server: `runtime.commit_staged(staged)?` THEN `principal_view.store(view)` THEN `lifecycle.update_global_chain(global_router, global_hooks)` (T9 will add a setter; or rebuild Lifecycle if simpler).
  - `PrincipalView` holds only `specs: HashMap<String, PrincipalSpecCached>`. It does NOT store global router/hooks. The `cached.resolved_router(&global)` / `resolved_hooks(&global)` resolver methods (T5) take the global as a borrow from whoever owns it (Lifecycle, per T9).
  - **Crucial ordering**: build all per-principal staged slots BEFORE constructing the `Arc<PrincipalView>` that holds the resolved chain. Once the `Arc<PrincipalView>` is built and returned alongside `staged`, the caller (T11/T15) MUST `runtime.commit_staged(staged)` BEFORE publishing the view via `principal_view.store(new_view)`. This guarantees every `Arc<dyn ...>` handle the new view references is reachable in `ExtismRuntime.instances` by the time the view becomes visible.
  - **Cleanup on failure (D5 all-or-nothing)**: because `instantiate_*_for` is staging-only and does NOT mutate `ExtismRuntime`'s live maps, failure handling is trivial: on the `?` propagation site, the local `staged: Vec<StagedSlot>` is automatically dropped together with the rest of the function frame. Live `instances` and `manifests` were never modified during this attempt, so `ExtismRuntime` is byte-for-byte identical to its pre-call state. No explicit `evict_slot` calls are required for rollback.
  - Update T4's caller updates to pass `&ExtismRuntime`.

  **Must NOT do**:
  - Do not store `PluginRef` on `PrincipalSpecCached` (T5 already enforces resolved-handle storage).
  - Do not perform partial instantiation — any failure returns `Err`, the caller MUST keep the previous view (T15 enforces this for hot-reload).
  - Do not call `principal_view.store(...)` from this function.
  - Do not introduce a builder pattern (`PrincipalViewBuilder`); inline construction is sufficient.
  - Do not implement eviction of stale slots here; T16 owns that.

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: This is the core mechanism touching three crates' invariants (config, core, runtime); ordering correctness directly impacts hot-reload safety.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: T9, T11, T14, T15, T18, T20, T22
  - **Blocked By**: T4, T5, T7

  **References**:

  **Pattern References**:
  - `crates/cc-lb-core/src/api_keys/principal_view.rs:32-72` — `from_config` walk pattern.
  - `crates/cc-lb-runtime-extism/src/lib.rs:141-200` — `register_slot` Arc handling (instantiation is idempotent for the same SlotKey via `reload`).

  **API/Type References**:
  - `crates/cc-lb-config/src/types.rs:336-340, 344-355` — `PluginsConfig`, `PluginRef`.

  **Test References**:
  - `crates/cc-lb-core/` existing `principal_view` tests for regression coverage.

  **WHY each reference matters**: instantiation order is the single most important correctness property in this refactor (Metis flag). Reusing `register_slot`'s ArcSwap semantics inside `from_config` keeps the in-flight safety property intact.

  **Acceptance Criteria**:
  - [ ] `from_config(&Config, &ExtismRuntime, Arc<dyn RouterPlugin>, Vec<Arc<dyn ObservabilityHook>>) -> Result<(Arc<PrincipalView>, Vec<StagedSlot>), ConfigError>` signature exactly as in Must Have
  - [ ] Each principal's resolved cache populated correctly per the inherit/explicit rules
  - [ ] Global router + hooks instantiated and stored on `PrincipalView`
  - [ ] All per-principal slots STAGED in the returned `Vec<StagedSlot>` before return; `runtime.registered_slot_keys()` is UNCHANGED across the call (staging-only). Caller (T11/T15) commits before publishing the view.
  - [ ] On any per-principal instantiation failure: `from_config`'s local `staged: Vec<StagedSlot>` is dropped via stack unwinding when `?` propagates the error. Live `instances` and `manifests` were never mutated. After Err returns, `runtime.registered_slot_keys()` is byte-for-byte identical to its pre-call snapshot. This honors D5 (all-or-nothing) by construction — no explicit cleanup or T16 diff-eviction needed.
  - [ ] `cargo test -p cc-lb-core` passes
  - [ ] `cargo build --workspace`

  **QA Scenarios**:
  ```
  Scenario: Per-principal handles are distinct from global
    Tool: Bash (cargo)
    Steps:
      1. Add test `principal_view_per_principal_router_distinct_from_global`: build a Config with global router R_G and principal A's router R_A (same plugin name, different `config`); call from_config, assert `!Arc::ptr_eq(global_router, principal_a_router)`.
      2. Run: cargo test -p cc-lb-core principal_view_per_principal_router_distinct_from_global -- --nocapture 2>&1 | tee .omo/evidence/task-8-distinct.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-8-distinct.txt

  Scenario: Inherit principal sees global router
    Tool: Bash (cargo)
    Steps:
      1. Add test `principal_view_inherit_router_is_global`: build a Config with global router and a principal with `router_plugin: None`; assert `resolved_router` returns the global handle.
      2. Run: cargo test -p cc-lb-core principal_view_inherit_router_is_global 2>&1 | tee .omo/evidence/task-8-inherit.txt
    Expected Result: Exit 0; ptr_eq true
    Evidence: .omo/evidence/task-8-inherit.txt

  Scenario: Failure rolls back — ExtismRuntime byte-identical pre/post attempt (staging never commits on failure)
    Tool: Bash (cargo)
    Steps:
      1. Add test using a bad wasm path on principal B (positioned between principal A and C). Snapshot `runtime.registered_slot_keys().into_iter().collect::<HashSet<_>>()` (the public API) before calling `from_config`. Call `from_config`; assert Err. Snapshot via the same public API again; assert the two HashSets are equal.
      2. Run: cargo test -p cc-lb-core principal_view_partial_failure_rolls_back -- --nocapture 2>&1 | tee .omo/evidence/task-8-rollback.txt
    Expected Result: Exit 0; Err returned with B's principal name + wasm path; pre-snapshot equals post-snapshot via `registered_slot_keys()`
    Evidence: .omo/evidence/task-8-rollback.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(core): add failing per-principal plugin instantiation tests`
  - GREEN: `feat(core): instantiate per-principal plugins in PrincipalView::from_config`
  - Files: `crates/cc-lb-core/src/api_keys/principal_view.rs` (+ caller updates to pass `&ExtismRuntime`)
  - Pre-commit: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo test -p cc-lb-core && cargo build --workspace`

- [x] 9. `Lifecycle` stops owning `self.router` / `self.observability_hooks`; resolves via `PrincipalView`

  **What to do**:
  - In `crates/cc-lb-core/src/lifecycle.rs`:
    - Remove fields `self.router: Arc<dyn RouterPlugin>` and `self.observability_hooks: Vec<Arc<dyn ObservabilityHook>>` from `Lifecycle` struct.
    - `Lifecycle` retains `principal_view: Arc<ArcSwap<PrincipalView>>`. Global router + hooks now live on `PrincipalView` itself (T8).
    - Adjust `Lifecycle::new` constructor signature: drop the `router` + `observability_hooks` args (callers must change accordingly).
    - All call sites inside `handle()` that previously used `self.router` / `self.observability_hooks` now use the bound chain (which T10 will actually wire). For this task: introduce a temporary local helper `fn current_global_router(&self) -> &Arc<dyn RouterPlugin>` reading from `principal_view.load()` — used at pre-authn paths only.
  - Update `app.rs:257-265` to not pass `router_plugin` + `observability_hooks` into `Lifecycle::new`; instead these are passed to `PrincipalView::from_config` (T11 wires this fully).

  **Must NOT do**:
  - Do not change the per-request entry point signature (`handle(request) -> Response`).
  - Do not introduce a `Lifecycle::dispatch_with_plugins` overload.
  - Do not change observability host_state passing semantics.

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Lifecycle is the request-handling hot path; struct churn must preserve all timing and ownership semantics.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: T10
  - **Blocked By**: T5, T8

  **References**:

  **Pattern References**:
  - `crates/cc-lb-core/src/lifecycle.rs:316-320` — current `Principal` construction (will be the anchor for T10's load-once-and-bind).
  - `crates/cc-lb-core/src/lifecycle.rs:327, 748, 790` — current global call sites.

  **API/Type References**:
  - `crates/cc-lb-server/src/app.rs:257-265` — startup construction site.

  **WHY each reference matters**: removing direct `self.router` ownership without breaking call sites requires every site to switch in one commit, which this task does.

  **Acceptance Criteria**:
  - [ ] `Lifecycle` struct no longer has `router` or `observability_hooks` fields
  - [ ] `Lifecycle::new` signature dropped those args
  - [ ] `app.rs` passes the new signature
  - [ ] `cargo test -p cc-lb-core` passes
  - [ ] `cargo build --workspace`

  **QA Scenarios**:
  ```
  Scenario: Struct shape audited
    Tool: Bash (rg)
    Steps:
      1. Run: rg -n "self\.router|self\.observability_hooks" crates/cc-lb-core/src/lifecycle.rs > .omo/evidence/task-9-grep.txt
    Expected Result: File empty (no occurrences)
    Evidence: .omo/evidence/task-9-grep.txt

  Scenario: Workspace still builds
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo build --workspace --all-targets 2>&1 | tee .omo/evidence/task-9-build.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-9-build.txt
  ```

  **Commit**: YES
  - Message: `refactor(core): Lifecycle resolves plugin chain via PrincipalView`
  - Files: `crates/cc-lb-core/src/lifecycle.rs`, `crates/cc-lb-server/src/app.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo build --workspace && cargo test -p cc-lb-core`

- [x] 10. `Lifecycle::handle` load-once-and-bind + per-request dispatch + pre-authn global-only observe

  **What to do**:
  - At the top of `Lifecycle::handle` (`crates/cc-lb-core/src/lifecycle.rs:286+`), call `let view = self.principal_view.load_full();` exactly once. Bind that `Arc<PrincipalView>` for the entire request lifetime. Every subsequent lookup uses `&*view`.
  - **Thread the bound view into authentication and limit reservation** so they cannot independently re-load:
    - Change `BuiltinAuthn::authenticate` (`crates/cc-lb-core/src/api_keys/builtin_authn.rs:84`) to accept `view: &PrincipalView` as an explicit parameter. Remove any internal `self.principal_view.load()` / `view.load()` calls inside `authenticate`. Internal `view.get(...)` and `view.principal_status(...)` calls now use the passed reference.
    - Change `LimitEngine::reserve` (`crates/cc-lb-core/src/api_keys/limit_engine.rs:170`) the same way: accept `view: &PrincipalView` and use it for `principal_status`, `is_model_allowed`, `default_limits` (lines ~170-195). Remove any internal load.
    - `Lifecycle::handle` passes `&*view` into both `self.authn.authenticate(&headers, &*view)` and `self.limit_engine.reserve(&*view, ...)`. This guarantees auth, limit reservation, and dispatch all operate on the same `PrincipalView` snapshot; the auth/dispatch race is impossible by construction (no fallback metric needed).
    - Update all existing tests that call `BuiltinAuthn::authenticate` / `LimitEngine::reserve` to pass the view explicitly.
  - Pre-authn observe sites (`lifecycle.rs:294-297, 307`):
    - These run before `principal_id` exists. Use `view.global_observability_hooks()` (added in T8) and call `observe_many` with that slice. If `global_observability_hooks()` returns empty, silently no-op.
    - Add an inline comment: `// Pre-authn observe: global hooks only (no principal context). Silent no-op when global is empty. Do not introduce per-principal fallback here.`
  - After successful auth (`lifecycle.rs:316-320`) where `principal_id` is known:
    - Look up `let cached = view.get(&principal_id)?;` (this cannot miss because auth already verified existence via the same view snapshot — T10 enforces this via the bound `view` variable).
    - Resolve `let router = cached.resolved_router(view.global_router());` and `let hooks = cached.resolved_hooks(view.global_observability_hooks());`.
    - Use `router` and `hooks` for the remainder of the request. Replace `self.router.route(...)` → `router.route(...)`. Replace `observe_many(&self.observability_hooks, ...)` → `observe_many(hooks, ...)` at lines 748, 790.
  - Borrow-only — no `Arc::clone` of the hooks vec per request. The hooks slice is borrowed from the bound `view`.

  **Must NOT do**:
  - Do not call `principal_view.load()` a second time anywhere in `handle()`.
  - Do not introduce a `metrics::counter!("principal_view_miss", 1)` — load-once-and-bind eliminates the miss.
  - Do not allow pre-authn observe to fall through to per-principal hooks under any circumstance.
  - Do not split `handle` into multiple functions just for this refactor; inline.
  - Do not clone the hooks Vec to extend its lifetime — match lifetimes to the bound view.

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Hot path; lifetime and ownership require careful borrow analysis. Pre-authn vs post-authn branching must be visually unambiguous in code.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: T14, T20, T21
  - **Blocked By**: T9

  **References**:

  **Pattern References**:
  - `crates/cc-lb-core/src/lifecycle.rs:286-320` — full pre-authn → auth → principal binding region (the rewrite target).
  - `crates/cc-lb-core/src/lifecycle.rs:327, 748, 790` — every dispatch call site (rewrite to use bound chain).
  - `arc_swap` docs: `https://docs.rs/arc-swap/latest/arc_swap/struct.ArcSwap.html#method.load_full` — confirm `load_full` returns owned Arc (correct here vs `.load()` which returns guard).

  **API/Type References**:
  - `crates/cc-lb-core/src/api_keys/principal_view.rs` — `get`, `global_router`, `global_observability_hooks` methods (T8 must expose these).

  **WHY each reference matters**: `load_full` is the right primitive — it owns the snapshot for the duration of the request. `load` returns a `Guard` that's cheaper but more lifetime-restrictive; `load_full` simplifies borrows for the rest of `handle()`.

  **Acceptance Criteria**:
  - [ ] `Lifecycle::handle` body contains exactly one `principal_view.load_full()` call (verified by grep)
  - [ ] `BuiltinAuthn::authenticate` signature accepts `view: &PrincipalView` as explicit parameter
  - [ ] `LimitEngine::reserve` signature accepts `view: &PrincipalView` as explicit parameter
  - [ ] Zero `principal_view.load` / `principal_view.load_full` calls inside `builtin_authn.rs` and `limit_engine.rs` (verified by grep)
  - [ ] Pre-authn observe path explicitly uses `view.global_observability_hooks()`
  - [ ] Post-authn dispatch uses bound `router` + `hooks` from `cached.resolved_*`
  - [ ] No `Arc::clone` of `Vec<Arc<dyn ObservabilityHook>>` per request (verified by grep `hooks.clone()` and `Arc::clone(&self.observability`)
  - [ ] `cargo test -p cc-lb-core` passes including new tests
  - [ ] `cargo build --workspace`

  **QA Scenarios**:
  ```
  Scenario: load_full called exactly once
    Tool: Bash (rg)
    Steps:
      1. Run: rg -nC2 "principal_view\.(load|load_full)\(\)" crates/cc-lb-core/src/lifecycle.rs > .omo/evidence/task-10-load-count.txt
      2. Confirm exactly one `load_full()` call inside the `handle` function block
    Expected Result: Exactly 1 match within `Lifecycle::handle` body
    Evidence: .omo/evidence/task-10-load-count.txt

  Scenario: Pre-authn observe with empty global is no-op
    Tool: Bash (cargo)
    Steps:
      1. Add test `lifecycle_pre_authn_observe_empty_global_no_op`: build PrincipalView with empty global hooks; trigger a pre-authn error path; assert observe events emitted == 0.
      2. Run: cargo test -p cc-lb-core lifecycle_pre_authn_observe_empty_global_no_op -- --nocapture 2>&1 | tee .omo/evidence/task-10-pre-authn-noop.txt
    Expected Result: Exit 0; recorded event count == 0
    Evidence: .omo/evidence/task-10-pre-authn-noop.txt

  Scenario: Per-principal dispatch hits per-principal hook
    Tool: Bash (cargo)
    Steps:
      1. Add integration-ish test in `crates/cc-lb-core/tests/`: build view with principal A having a hook that emits a known marker; trigger a request as A; assert marker emitted; same for principal B with different marker.
      2. Run: cargo test -p cc-lb-core lifecycle_per_principal_dispatch_hits_correct_hook -- --nocapture 2>&1 | tee .omo/evidence/task-10-dispatch.txt
    Expected Result: Exit 0; markers match expected principal
    Evidence: .omo/evidence/task-10-dispatch.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(core): add failing load-once-and-bind dispatch tests`
  - GREEN: `feat(core): load-once-and-bind dispatch + pre-authn global-only observe`
  - Files: `crates/cc-lb-core/src/lifecycle.rs`, `crates/cc-lb-core/src/api_keys/principal_view.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo test -p cc-lb-core && cargo build --workspace`

- [x] 11. `app.rs` startup wiring: pass `ExtismRuntime` into `PrincipalView::from_config`

  **What to do**:
  - In `crates/cc-lb-server/src/app.rs:257-265`, change startup so it:
    1. Creates `ExtismRuntime` first (as today).
    2. Builds the **global router** via the staging API so global commit defers until the whole graph succeeds: `let global_router: Arc<dyn RouterPlugin> = match &config.plugins.router_plugin { Some(plugin) => { let (h, slot) = runtime.instantiate_router_global(&plugin.name, &manifest_from_plugin(plugin)?)?; global_staged.push(slot); h }, None => Arc::new(BuiltinRouter::new(&config)?) };`. The `BuiltinRouter` path stays as-is (it does not touch `ExtismRuntime`). The `_global` wrapper hides the internal sentinel.
    3. Builds the **global hooks** the same way: iterate `config.plugins.observability_hooks`, call `runtime.instantiate_observability_global(&plugin.name, &manifest)?` for each, push staged slots into `global_staged`, accumulate handles.
    4. Extract steps 2-3 into a `pub(crate) fn build_global_chain(config: &Config, runtime: &ExtismRuntime, staged: &mut Vec<StagedSlot>) -> Result<(Arc<dyn RouterPlugin>, Vec<Arc<dyn ObservabilityHook>>), GlobalChainError>` placed at the top of `crates/cc-lb-server/src/app.rs` (or a new sibling module `crates/cc-lb-server/src/plugin_chain.rs` if `app.rs` size demands it — `app.rs` and `reload.rs` are in the same crate so `pub(crate)` is reachable from both). Define `pub(crate) enum GlobalChainError { Builtin(BuiltinError), Runtime(RuntimeError), Config(ConfigError) }` with `From` impls so callers can `?` cleanly.
    5. Calls `let (view, per_principal_staged) = PrincipalView::from_config(&config, &runtime, global_router.clone(), global_hooks.clone())?;`.
    6. Concatenate: `let mut all_staged = global_staged; all_staged.extend(per_principal_staged);`.
    7. On any Err from step 2-5, propagate to `main`'s `anyhow::Result<()>` (server exits with non-zero); all `StagedSlot`s on the stack are dropped, `ExtismRuntime`'s live maps were never touched.
    8. On Ok: call `runtime.commit_staged(all_staged);` FIRST (every slot becomes live in `instances`/`manifests` atomically), THEN store `view` into the existing `ArcSwap<PrincipalView>` cell.
    9. **Never** call the legacy `runtime.instantiate_router(...)` / `instantiate_observability(...)` (immediate-insert) variants after T11 lands — they remain in the crate only for backward compatibility with any tests not yet migrated. Production wiring uses the staging variants exclusively.
  - Adjust `Lifecycle::new` invocation to match T9's new signature (no `router`/`observability_hooks` args).
  - **Do not modify `ConfigWatcher` in this task** — `app.rs` continues to call the existing `ConfigWatcher::new(...)` signature. T15 will add the `runtime: Arc<ExtismRuntime>` field on `ConfigWatcher` AND update `app.rs` to pass `Arc::clone(&runtime)` at the same time, since both changes are atomic and trivially compile-tied.

  **Must NOT do**:
  - Do not change the startup ordering of TLS / listener / signer setup.
  - Do not move `ExtismRuntime` construction into `PrincipalView`.
  - Do not add log lines beyond the existing startup logs.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Wiring change; depends mostly on T4/T8/T9.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: preflight (T12), server boot
  - **Blocked By**: T4, T8

  **References**:

  **Pattern References**:
  - `crates/cc-lb-server/src/app.rs:257-265` — current global plugin wiring (removed/replaced here).

  **WHY each reference matters**: this is the single source of truth for application bring-up; an incorrect order silently breaks startup.

  **Acceptance Criteria**:
  - [ ] `app.rs` compiles with the new fallible `from_config` call
  - [ ] No duplicate `runtime.instantiate_router/observability(...)` calls remain (verified by grep)
  - [ ] `cargo build --workspace`
  - [ ] `cargo test -p cc-lb-server`

  **QA Scenarios**:
  ```
  Scenario: Server boots with a sample config containing global + per-principal plugins
    Tool: Bash (cargo + ephemeral process)
    Preconditions: tests/fixtures plugin wasm available
    Steps:
      1. Write a test fixture config to /tmp/per-principal-config.toml (one global hook, one principal-A hook)
      2. Spawn: cargo run -p cc-lb-server -- --config /tmp/per-principal-config.toml >.omo/evidence/task-11-boot.log 2>&1 &
      3. Wait 3s, then `curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status >/dev/null && echo OK`
      4. Kill the process.
    Expected Result: "OK" printed; .omo/evidence/task-11-boot.log shows no panics
    Evidence: .omo/evidence/task-11-boot.log

  Scenario: Server refuses to boot when a per-principal PluginRef is invalid
    Tool: Bash
    Steps:
      1. Write bad config to /tmp/bad-per-principal.toml (principal alice has nonexistent wasm_path)
      2. Run: cargo run -p cc-lb-server -- --config /tmp/bad-per-principal.toml 2>&1 | tee .omo/evidence/task-11-refuse.txt; echo "exit=$?"
    Expected Result: Non-zero exit; stderr contains "principals.alice" and the bad path
    Evidence: .omo/evidence/task-11-refuse.txt
  ```

  **Commit**: YES
  - Message: `feat(server): wire fallible PrincipalView with ExtismRuntime at startup`
  - Files: `crates/cc-lb-server/src/app.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo build --workspace && cargo test -p cc-lb-server`

- [x] 12. `preflight.rs` whole-graph dry-load (all-or-nothing)

  **What to do**:
  - In `crates/cc-lb-server/src/preflight.rs:95-106`, after global dry-load, iterate `config.principals` and dry-load each principal's `router_plugin` and `observability_hooks` via the same `dry_load_plugin` helper.
  - Any failure returns `Err` from `preflight`; the server refuses to start.
  - Error report (the existing `preflight::Report` struct) gains entries identifying the path `principals.{name}.router_plugin` or `principals.{name}.observability_hooks.{i}`.

  **Must NOT do**:
  - Do not introduce a "principal-degraded mode" or similar partial-success path.
  - Do not change `Report` field names or remove existing fields.
  - Do not parallelize the preflight loop (serial dry-load is fine and avoids surprising error ordering).

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Loops + error reporting; must preserve existing report semantics.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 2
  - **Blocks**: T15
  - **Blocked By**: T6, T7

  **References**:

  **Pattern References**:
  - `crates/cc-lb-server/src/preflight.rs:95-106` — existing dry-load loop for global.
  - `crates/cc-lb-server/src/preflight.rs` (top of file) — `Report`, `dry_load_plugin`, `PluginLoadKind` signatures.

  **WHY each reference matters**: keeping the same `dry_load_plugin` helper preserves report formatting and test expectations.

  **Acceptance Criteria**:
  - [ ] Preflight iterates every principal
  - [ ] A single failing principal causes preflight Err
  - [ ] Path string in error includes principal name
  - [ ] `cargo test -p cc-lb-server preflight`

  **QA Scenarios**:
  ```
  Scenario: One principal's bad plugin aborts preflight
    Tool: Bash (cargo)
    Steps:
      1. Add integration test `preflight_aborts_on_principal_plugin_failure`: build a Config with valid global + valid principal A + invalid principal B; call preflight; assert Err with B's path.
      2. Run: cargo test -p cc-lb-server preflight_aborts_on_principal_plugin_failure -- --nocapture 2>&1 | tee .omo/evidence/task-12-abort.txt
    Expected Result: Exit 0; error string contains `principals.bob`
    Evidence: .omo/evidence/task-12-abort.txt

  Scenario: All-valid preflight succeeds
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test -p cc-lb-server preflight_all_valid 2>&1 | tee .omo/evidence/task-12-ok.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-12-ok.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(server): add failing preflight per-principal dry-load tests`
  - GREEN: `feat(server): whole-graph preflight dry-load all-or-nothing`
  - Files: `crates/cc-lb-server/src/preflight.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-server -- -D warnings && cargo test -p cc-lb-server`

- [x] 13. Property test for `SlotKey` uniqueness under arbitrary principal sets

  **What to do**:
  - Add `crates/cc-lb-runtime-extism/tests/property/slot_key_uniqueness.rs` (or extend `tests/property/` per workspace layout).
  - Generate arbitrary inputs via `proptest`:
    - `Vec<(String, String)>` representing `(principal_id, plugin_name)` pairs, with bounded alphabet + bounded length, set size up to 256.
    - For each generated input, register all slots, then assert `instances.read().len() == unique_pairs.len()` (de-dup on identical `SlotKey`s allowed, distinct pairs must produce distinct entries).
    - Also assert: two different `principal_id`s with the same `plugin_name` produce two distinct `Arc<PluginSlot>` (i.e. ptr_eq false).
  - Use a stub plugin manifest that doesn't require real WASM (mock via `extism::Manifest::new(vec![])` or workspace-internal fake).

  **Must NOT do**:
  - Do not run real WASM in this test — keeps it fast.
  - Do not test eviction here (T16/T22 territory).
  - Do not introduce a new test crate.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: proptest generation + invariant assertion; bounded but mildly non-obvious.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: final
  - **Blocked By**: T7 (test consumes the public staging + `registered_slot_keys()` API, not private `instances`/`PluginSlot` internals — must run after the public surface lands)

  **References**:

  **Pattern References**:
  - `tests/property/` (workspace member; check its existing harness) — use as the test home if it exposes `extism` test fixtures; else colocate under `cc-lb-runtime-extism/tests/`.
  - Public API only: `instantiate_router_for` + `instantiate_observability_for` + `commit_staged` + `registered_slot_keys` (T7). The test must NOT depend on the private `instances` field or the `SlotKey` newtype directly.

  **API/Type References**:
  - `proptest` v1 docs.

  **WHY each reference matters**: existing property test layout dictates fixture sharing and CI invocation; following it avoids touching CI config.

  **Acceptance Criteria**:
  - [ ] Property test runs `cargo test -p cc-lb-runtime-extism --test property slot_key_uniqueness` (or `-p ... --features property` per existing convention)
  - [ ] Test passes with default proptest config

  **QA Scenarios**:
  ```
  Scenario: Property holds under proptest default cases
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test -p cc-lb-runtime-extism slot_key_uniqueness 2>&1 | tee .omo/evidence/task-13-property.txt
    Expected Result: Exit 0; proptest summary shows N successful cases
    Evidence: .omo/evidence/task-13-property.txt
  ```

  **Commit**: YES
  - Message: `test(runtime-extism): property test for SlotKey uniqueness`
  - Files: `crates/cc-lb-runtime-extism/tests/...` (or `tests/property/...`)
  - Pre-commit: `cargo fmt --check && cargo test -p cc-lb-runtime-extism slot_key_uniqueness`

- [x] 14. Loom test: `PrincipalView` swap + concurrent `handle()` safety

  **What to do**:
  - Add `crates/cc-lb-core/tests/loom_principal_view.rs` gated on `#[cfg(loom)]`.
  - Model two threads:
    - Thread A: `principal_view.load_full()` + dispatch through a bound `view` snapshot (mimicking T10's flow). Call a dummy `route()` and a dummy `observe()` on the resolved chain.
    - Thread B: `principal_view.store(new_view)` swapping the snapshot.
  - Loom invariants:
    - Thread A never observes a `Lifecycle` dispatch where the resolved `Arc<dyn RouterPlugin>` points to a `PluginSlot` that's no longer in `ExtismRuntime.instances`. (Slot must remain reachable via the view's resolved cache.)
    - No data race or torn read.
  - Wire into the workspace's existing loom test harness (see `tests/loom/Cargo.toml`).

  **Must NOT do**:
  - Do not exercise real Extism WASM under loom (too slow + non-deterministic).
  - Do not assert eviction safety here (T22 covers in-flight + reload with full Tokio runtime).

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Loom test design requires precise model and bounded interleaving.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: final
  - **Blocked By**: T8, T10

  **References**:

  **Pattern References**:
  - `tests/loom/` (workspace member) — existing loom harness configuration.
  - `arc_swap` crate docs — loom guidance for `ArcSwap`.

  **WHY each reference matters**: the workspace already has a loom test target; reusing it avoids CI config changes.

  **Acceptance Criteria**:
  - [ ] Test compiles under `--cfg loom`
  - [ ] `RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view` passes (default loom iterations or bounded sample)

  **QA Scenarios**:
  ```
  Scenario: Loom check passes
    Tool: Bash (cargo)
    Steps:
      1. Run: RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view -- --nocapture 2>&1 | tee .omo/evidence/task-14-loom.txt
    Expected Result: Exit 0; "test result: ok" present
    Evidence: .omo/evidence/task-14-loom.txt
  ```

  **Commit**: YES
  - Message: `test(core): loom test for PrincipalView swap + concurrent handle()`
  - Files: `crates/cc-lb-core/tests/loom_principal_view.rs` (or `tests/loom/...`)
  - Pre-commit: `RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view`

- [x] 15. `reload.rs` per-principal path-change warnings + all-or-nothing reload abort

  **What to do**:
  - In `crates/cc-lb-server/src/reload.rs`:
    - **Add `runtime: Arc<ExtismRuntime>` field to `ConfigWatcher`** (`reload.rs:17-23`). Update `ConfigWatcher::new` to accept and store it. **In the same commit**, update the `ConfigWatcher::new(...)` call site in `crates/cc-lb-server/src/app.rs` to pass `Arc::clone(&runtime)` so the workspace compiles (T11 intentionally did not modify this call).
    - **Introduce `LastReloadStatus` in `crates/cc-lb-admin/src/`** (not in `cc-lb-server`) because `cc-lb-admin` is the boundary `cc-lb-server` already depends on (`reload.rs:167-169` impls `cc_lb_admin::CurrentConfig`), and `cc-lb-admin` cannot depend on `cc-lb-server`. Define:
      ```rust
      // crates/cc-lb-admin/src/lib.rs (or a new sibling module)
      #[derive(Clone, Debug, Serialize)]
      pub struct LastReloadStatus {
          pub timestamp_unix_secs: u64,
          pub outcome: ReloadOutcome,
          pub config_path: Option<String>,
      }
      #[derive(Clone, Debug, Serialize)]
      pub enum ReloadOutcome {
          Success,
          Failure { reason: String, principal: Option<String>, plugin: Option<String> },
      }
      ```
      Extend the existing `pub trait CurrentConfig` (`cc-lb-admin` crate) with `fn last_reload_status(&self) -> Option<LastReloadStatus>;`.
    - In `crates/cc-lb-server/src/reload.rs`, `import cc_lb_admin::{LastReloadStatus, ReloadOutcome}`. Store on `ConfigWatcher` as `last_reload_status: Arc<ArcSwap<Option<LastReloadStatus>>>` (initial `None`). Update via `self.last_reload_status.store(Arc::new(Some(LastReloadStatus { .. })))` on every reload attempt (both success and failure).
    - Implement the new `CurrentConfig::last_reload_status` method on `ConfigWatcher` by returning `self.last_reload_status.load().as_ref().clone()`. T18's admin `/status` and T17's fault-injection test consume the value through the trait.
    - Extend `warn_plugin_path_change` and `warn_observability_hook_path_changes` (around line 244-251) to also walk per-principal plugin fields. Emit warnings keyed by `principals.{name}.router_plugin.wasm_path` / `principals.{name}.observability_hooks.{i}.wasm_path`.
    - Around line 101-104, where the new view is constructed:
      1. `let mut all_staged: Vec<StagedSlot> = Vec::new();`
      2. Call `let (new_global_router, new_global_hooks) = build_global_chain(&new_config, &self.runtime, &mut all_staged)?;` (the T11 helper, staging global slots too).
      3. Call `let (new_view, per_principal_staged) = PrincipalView::from_config(&new_config, &self.runtime, new_global_router, new_global_hooks)?;`.
      4. `all_staged.extend(per_principal_staged);` so a single batch covers global + per-principal.
    - On any `Err` from either step:
      - Do NOT call `principal_view.store(...)`.
      - Do NOT call `commit_staged(...)` — `all_staged` is dropped via stack unwinding.
      - Log error at WARN with the principal name + plugin name + reason.
      - `self.last_reload_status.store(Arc::new(Some(LastReloadStatus { timestamp_unix_secs: now, outcome: ReloadOutcome::Failure { reason, principal, plugin }, config_path })))` so `/admin/status` (T18) surfaces the failure.
      - Return without affecting in-flight requests.
    - On `Ok`: call `self.runtime.commit_staged(all_staged);` FIRST so global + per-principal slots all become live in `instances`/`manifests` atomically, THEN `principal_view.store(new_view)` so the new view's resolved `Arc<dyn ...>` handles are guaranteed reachable. After the swap, trigger T16's eviction (which itself happens in a separate task; in this task, leave a `TODO(T16):` comment marker that T16 will replace). Finally, `self.last_reload_status.store(Arc::new(Some(LastReloadStatus { timestamp_unix_secs: now, outcome: ReloadOutcome::Success, config_path })))`.

  **Must NOT do**:
  - Do not perform any cleanup on `ExtismRuntime` here — T16 owns eviction.
  - Do not record per-principal degradation state; D5 is all-or-nothing.
  - Do not add new metrics beyond the existing reload status struct.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Touches the SIGHUP / file-watch hot path; correctness on failure means previous view stays live.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 3
  - **Blocks**: T16, T17, T23
  - **Blocked By**: T8, T12

  **References**:

  **Pattern References**:
  - `crates/cc-lb-server/src/reload.rs:101-104, 244-251` — existing reload + warning sites.
  - `crates/cc-lb-config/src/hot_reload.rs:12-34` — config file watch (no change here, just consumer).

  **API/Type References**:
  - `LastReloadStatus` (or similar) — find via `rg "LastReloadStatus\|reload_status" crates/cc-lb-server`.

  **WHY each reference matters**: the admin status field is the only place a user sees a failed reload — losing this signal silently is unacceptable.

  **Acceptance Criteria**:
  - [ ] Reload with valid new config succeeds and swaps view
  - [ ] Reload with invalid principal plugin aborts cleanly; previous view still active; status surfaces the error
  - [ ] Per-principal path-change warnings emitted with correct path strings
  - [ ] `cargo test -p cc-lb-server reload`

  **QA Scenarios**:
  ```
  Scenario: Reload with invalid principal plugin preserves previous view
    Tool: Bash (cargo + ephemeral process)
    Steps:
      1. Boot server with /tmp/initial.toml (valid global + principal A).
      2. Write /tmp/bad-reload.toml introducing principal B with invalid wasm_path.
      3. Send SIGHUP to the running server pointing at /tmp/bad-reload.toml.
      4. curl /admin/status to confirm last reload status shows failure with `principals.bob`.
      5. curl /v1/messages with principal A's key — request still succeeds.
    Expected Result: Step 4 returns failure with bob's name; step 5 returns 200; previous view unchanged.
    Evidence: .omo/evidence/task-15-bad-reload.json + .omo/evidence/task-15-still-serving.txt

  Scenario: Per-principal path-change warning emitted
    Tool: Bash (cargo)
    Steps:
      1. Add unit test `reload_warn_per_principal_path_change` constructing old + new config with changed wasm_path on principal alice; assert log contains `principals.alice.router_plugin.wasm_path` substring (use `tracing-test` or capture via `tracing_subscriber::registry`).
      2. Run: cargo test -p cc-lb-server reload_warn_per_principal_path_change 2>&1 | tee .omo/evidence/task-15-warn.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-15-warn.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(server): add failing per-principal reload warning + abort tests`
  - GREEN: `feat(server): per-principal reload path warnings + all-or-nothing abort`
  - Files: `crates/cc-lb-server/src/reload.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-server -- -D warnings && cargo test -p cc-lb-server`

- [ ] 16. Hot-reload eviction of stale `(principal_id, plugin_name)` slots after view swap

  **What to do**:
  - In `crates/cc-lb-server/src/reload.rs` (post-swap point identified by the `TODO(T16):` marker from T15):
    - Compute the set of `(principal_id_or_global, plugin_name)` pairs referenced by the **new** `Config` (NOT the `PrincipalView` — the view stores trait handles only and intentionally has no plugin-name metadata, per T5).
    - Diff against the current `runtime.registered_slot_keys()` snapshot.
    - For each pair not present in the new referenced set, call `runtime.evict_slot(&principal_id, &plugin_name)` (the helper added in T7).
  - Evict happens AFTER `principal_view.store(new_view)` returns — Arc refcount from in-flight requests guarantees safety because they bound the OLD view via `load_full` (T10).
  - Do not implement a "grace period" wait — Arc refcount alone is sufficient.
  - Add internal helper `pub(crate) fn referenced_slot_keys(config: &Config) -> HashSet<(String, String)>` (in `crates/cc-lb-server/src/reload.rs` or a sibling module). It walks:
    - For each entry in `config.plugins.router_plugin` and `config.plugins.observability_hooks`: insert `("__global__".to_string(), plugin.name.clone())` (uses the public string sentinel; matches what `instantiate_*_global` registered internally).
    - For each `(principal_name, spec)` in `config.principals`: if `spec.router_plugin.is_some()` insert `(principal_name.clone(), spec.router_plugin.as_ref().unwrap().name.clone())`; for `spec.observability_hooks.as_ref().unwrap_or(&vec![]).iter()` insert `(principal_name.clone(), plugin.name.clone())`.

  **Must NOT do**:
  - Do not call `evict_slot` BEFORE the view swap.
  - Do not implement a reachability scan over `Arc` graph — diff over the new view's keys only.
  - Do not retain stale slots "in case future requests need them".
  - Do not block the reload thread on in-flight request completion; the eviction call is synchronous-and-cheap (just `HashMap::remove`).

  **Recommended Agent Profile**:
  - **Category**: `deep`
    - Reason: Correctness depends on event ordering and Arc refcount lifetimes; getting this wrong drops in-flight WASM execution.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 4 (with T23)
  - **Blocks**: T17, T22
  - **Blocked By**: T15

  **References**:

  **Pattern References**:
  - `crates/cc-lb-runtime-extism/src/lib.rs:291-313` — `PluginSlot { ArcSwap<PluginCell> }` (Arc refcount lifecycle).
  - `crates/cc-lb-server/src/reload.rs:101-104` — post-swap site.

  **WHY each reference matters**: the in-flight safety property *depends on the consumer holding an Arc*; if eviction were synchronous-and-blocking on actual instance drop, that's overengineering. Plain `HashMap::remove` is correct because the slot's Arc is kept alive by the request.

  **Acceptance Criteria**:
  - [ ] After successful reload, `runtime.registered_slot_keys().len()` equals `referenced_slot_keys(&new_config).len()` (both as `(String, String)` pair counts)
  - [ ] In-flight request bound to old view completes successfully even after eviction
  - [ ] `cargo test -p cc-lb-server reload_evicts_stale`

  **QA Scenarios**:
  ```
  Scenario: Stale slots dropped after reload
    Tool: Bash (cargo)
    Steps:
      1. Add integration test `reload_evicts_stale_slots`: boot with config containing global + principal A; reload removing principal A; assert `runtime.registered_slot_keys().len() == referenced_slot_keys(&new_config).len()` (both as `(String, String)` pair counts via the public API).
      2. Run: cargo test -p cc-lb-server reload_evicts_stale_slots -- --nocapture 2>&1 | tee .omo/evidence/task-16-evict.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-16-evict.txt

  Scenario: In-flight request completes after eviction
    Tool: Bash (cargo + tokio runtime)
    Steps:
      1. Add async test `reload_inflight_request_unaffected_by_eviction`: start a long-running mock request bound to old view; reload to a view that no longer references that principal; assert request completes 200 OK and observe events emitted to the OLD hook chain (not the new one).
      2. Run: cargo test -p cc-lb-server reload_inflight_request_unaffected_by_eviction -- --nocapture 2>&1 | tee .omo/evidence/task-16-inflight.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-16-inflight.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(server): add failing post-swap eviction tests`
  - GREEN: `feat(runtime-extism): post-swap eviction of stale slots`
  - Files: `crates/cc-lb-server/src/reload.rs`, `crates/cc-lb-runtime-extism/src/lib.rs` (small helper if needed)
  - Pre-commit: `cargo fmt --check && cargo clippy --workspace -- -D warnings && cargo test -p cc-lb-server && cargo test -p cc-lb-runtime-extism`

- [x] 17. Fault-injection integration test: per-principal instantiation failure aborts reload

  **What to do**:
  - Add `tests/integration/per_principal_reload_fault_injection.rs` (or extend the existing integration suite at `tests/integration/`).
  - Boot a full cc-lb instance with config including principal A (good) and principal B (good).
  - Trigger a hot-reload to a config where principal C has an invalid PluginRef (e.g. `wasm_path` points at a non-existent file).
  - Assert:
    1. Reload aborts; `principal_view.load()` still returns the previous view (A + B intact).
    2. `/admin/status` `.last_reload.outcome` equals `Failure { reason: ..., principal: "charlie", plugin: ... }` (assert via jq).
    3. `runtime.registered_slot_keys()` is unchanged from pre-reload snapshot.
    4. Requests against principal A and principal B succeed end-to-end (curl-style assertions inside the test).
  - Use the existing `tests/fixtures/fake-anthropic` upstream as the dialect target.

  **Must NOT do**:
  - Do not mock `PrincipalView::from_config` directly — drive failure through real config IO.
  - Do not assert on log line counts (flaky); assert on `LastReloadStatus` API surface.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Full integration with config IO + tokio runtime + admin API; many moving parts but well-defined invariants.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 5 (with T22)
  - **Blocks**: final
  - **Blocked By**: T15, T16

  **References**:

  **Pattern References**:
  - `tests/integration/` — existing integration test layout (find a similar SIGHUP-based reload test to model on).
  - `tests/fixtures/fake-anthropic/` — upstream fixture.

  **WHY each reference matters**: reusing the existing integration harness keeps CI runtime predictable and isolates this test from infrastructure drift.

  **Acceptance Criteria**:
  - [ ] Test boots full server, drives reload, verifies invariants via admin API + request flow
  - [ ] `cargo test --test per_principal_reload_fault_injection` passes

  **QA Scenarios**:
  ```
  Scenario: Bad reload preserves last-known-good
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test --test per_principal_reload_fault_injection -- --nocapture 2>&1 | tee .omo/evidence/task-17-faultinjection.txt
    Expected Result: Exit 0; test logs show reload Err and continued service
    Evidence: .omo/evidence/task-17-faultinjection.txt
  ```

  **Commit**: YES
  - Message: `test(integration): fault-injection per-principal reload abort`
  - Files: `tests/integration/per_principal_reload_fault_injection.rs`
  - Pre-commit: `cargo fmt --check && cargo test --test per_principal_reload_fault_injection`

- [x] 18. Admin `/status` response: keep legacy `plugins` + add `principals: { id -> { router_plugin, observability_hooks } }` (redacted config)

  **What to do**:
  - In `crates/cc-lb-admin/src/status.rs:130-135` (the response builder):
    - Keep the existing global `plugins: [...]` array (for backward compat).
    - Add a new top-level `principals: { <name>: { router_plugin: PluginRefView?, observability_hooks: [PluginRefView] } }` map.
    - Define `PluginRefView` as a redacted projection of `PluginRef`: include `name`, `wasm_path`, `sse_per_event`, `batched_events_per_flush`, `batched_flush_ms`. Replace `config: Value` with `config_hash: String` (sha256 hex of canonical JSON form, 16-char prefix).
    - Add a new top-level `last_reload: Option<LastReloadStatus>` field. `LastReloadStatus` and `ReloadOutcome` are defined in `cc-lb-admin` (T15) — the same crate as `status.rs` — so no new dependency edges are needed. Read via `self.current_config.last_reload_status()` (the trait method T15 added on `CurrentConfig`).
  - In `crates/cc-lb-admin/src/routes.rs`:
    - **Register the `/admin/status` HTTP route** mounting the status handler from `status.rs`. The repo currently exposes status via the response builder but does NOT have an `/admin/status` route registered. Add `Router::new().route("/admin/status", get(status::handler))` (or the equivalent axum form already used by neighboring routes like `/admin/health`, `/admin/config/current`). Without this step the QA `curl` commands cannot reach the endpoint.
  - No mutation endpoints. No additional routes.
  - In `crates/cc-lb-admin/src/settings.rs:477`, `plugin_count` now sums global + per-principal counts (this is also covered by T19; coordinate so the two tasks don't conflict).

  **Must NOT do**:
  - Do not remove or rename the existing global `plugins` field.
  - Do not add a `POST` or `PATCH` endpoint for per-principal plugin mutations.
  - Do not expose unredacted `config: Value`.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: API surface change with backward-compat constraint; redaction must be correct.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: F3 (manual QA depends on this surface)
  - **Blocked By**: T8

  **References**:

  **Pattern References**:
  - `crates/cc-lb-admin/src/status.rs:130-135` — current `plugins` building.
  - `crates/cc-lb-admin/src/status.rs` (response structs) — for adding the new `principals` field.

  **API/Type References**:
  - `serde_json::Value` canonicalization — use `serde_json::to_string` then sha256; if a canonicalization helper already exists in the workspace, reuse it.

  **WHY each reference matters**: keeping the legacy field stable + adding the new field is the simplest path to non-breaking API evolution.

  **Acceptance Criteria**:
  - [ ] `/admin/status` response includes both legacy `plugins` and new `principals` fields
  - [ ] `config_hash` is hex string, exactly 16 chars long
  - [ ] `cargo test -p cc-lb-admin status`

  **QA Scenarios**:
  ```
  Scenario: /admin/status surfaces per-principal plugin info
    Tool: Bash (curl + jq)
    Preconditions: Server booted with config containing global + principal A (with router_plugin) + principal B (without)
    Steps:
      1. Run: curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.principals' > .omo/evidence/task-18-principals.json
      2. Run: curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.plugins' > .omo/evidence/task-18-legacy.json
      3. Assert via jq: `.principals.alice.router_plugin.name == "my-router"`, `.principals.bob.router_plugin == null`, `.principals.alice.router_plugin.config_hash | length == 16`.
    Expected Result: All jq assertions pass; legacy `plugins` still present
    Evidence: .omo/evidence/task-18-principals.json + .omo/evidence/task-18-legacy.json

  Scenario: config Value never leaked
    Tool: Bash (curl + jq)
    Steps:
      1. Run: curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.. | objects | select(has("config"))' > .omo/evidence/task-18-no-leak.txt
    Expected Result: File empty (no object with `config` field exists in response)
    Evidence: .omo/evidence/task-18-no-leak.txt
  ```

  **Commit**: YES (RED + GREEN)
  - RED: `test(admin): add failing per-principal /admin/status response tests`
  - GREEN: `feat(admin): expose per-principal plugin status with redacted config`
  - Files: `crates/cc-lb-admin/src/status.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-admin -- -D warnings && cargo test -p cc-lb-admin`

- [x] 19. `settings.rs` `plugin_count` covers global + per-principal sum

  **What to do**:
  - Update `plugin_count` in `crates/cc-lb-admin/src/settings.rs:477` to include per-principal plugin entries:
    ```
    fn plugin_count(config: &Config) -> usize {
        let global = usize::from(config.plugins.router_plugin.is_some())
            + config.plugins.observability_hooks.len();
        let per_principal: usize = config.principals.values().map(|p| {
            usize::from(p.router_plugin.is_some())
                + p.observability_hooks.as_ref().map_or(0, |v| v.len())
        }).sum();
        global + per_principal
    }
    ```
  - Update unit test asserting the new sum.

  **Must NOT do**:
  - Do not change the function signature.
  - Do not add caller-side counts elsewhere.

  **Recommended Agent Profile**:
  - **Category**: `quick`
    - Reason: Single function, single test.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: final
  - **Blocked By**: T2

  **References**:

  **Pattern References**:
  - `crates/cc-lb-admin/src/settings.rs:477` — current `plugin_count`.

  **WHY each reference matters**: this function feeds settings history summaries; missing per-principal plugins skews the metric.

  **Acceptance Criteria**:
  - [ ] `plugin_count` returns global + per-principal sum
  - [ ] Existing tests updated to reflect the new sum
  - [ ] `cargo test -p cc-lb-admin`

  **QA Scenarios**:
  ```
  Scenario: plugin_count returns correct sum
    Tool: Bash (cargo)
    Steps:
      1. Add unit test `plugin_count_sums_global_and_per_principal`: build Config with global router + 2 hooks + principal A with 1 router + 3 hooks + principal B with 0 + 2; assert `plugin_count(config) == 1+2+1+3+0+2`.
      2. Run: cargo test -p cc-lb-admin plugin_count_sums_global_and_per_principal 2>&1 | tee .omo/evidence/task-19-count.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-19-count.txt
  ```

  **Commit**: YES
  - Message: `feat(admin): include per-principal plugins in plugin_count`
  - Files: `crates/cc-lb-admin/src/settings.rs`
  - Pre-commit: `cargo fmt --check && cargo clippy -p cc-lb-admin -- -D warnings && cargo test -p cc-lb-admin`

- [x] 20. Backward-compat regression: zero-principal-plugin config observe stream byte-diff

  **What to do**:
  - Add `tests/integration/backward_compat_observe.rs`.
  - Construct a Config that uses ONLY the global `PluginsConfig` (no principal has `router_plugin` or `observability_hooks`).
  - Boot the cc-lb instance, send a deterministic sequence of N requests (use the `tests/fixtures/fake-anthropic` upstream and a fixed seed for any randomness).
  - Collect observe events emitted (via a test hook that records to a `Vec<serde_json::Value>` or similar).
  - Serialize to JSON and compare against a checked-in golden file `tests/fixtures/per_principal_plugins/backward_compat_observe_stream.json` (this golden file is captured ONCE before the refactor and checked in as part of T20).
  - Assertion: diff must be empty.

  **Must NOT do**:
  - Do not include timestamps in the comparison (record them but strip before diff).
  - Do not record `request_id` UUIDs literally — normalize to placeholders (`req-1`, `req-2`...).
  - Do not skip the golden capture step — without a baseline this test is meaningless.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Requires careful normalization design and golden file capture from the pre-refactor baseline.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: F1
  - **Blocked By**: T10

  **References**:

  **Pattern References**:
  - `tests/integration/` — existing golden file conventions.
  - `insta` crate — if the workspace uses it for snapshots, reuse it (preferred over hand-rolled diff).

  **WHY each reference matters**: `insta` snapshot tests are the workspace's standard for byte-stable assertions; following the existing pattern keeps reviewability high.

  **Acceptance Criteria**:
  - [ ] Golden file committed under `tests/fixtures/`
  - [ ] Test passes; running it again against current code shows empty diff
  - [ ] `cargo test --test backward_compat_observe`

  **QA Scenarios**:
  ```
  Scenario: Zero-principal config observe stream identical to baseline
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test --test backward_compat_observe -- --nocapture 2>&1 | tee .omo/evidence/task-20-backwardcompat.txt
    Expected Result: Exit 0; insta accepts snapshot or hand-rolled diff is empty
    Evidence: .omo/evidence/task-20-backwardcompat.txt
  ```

  **Commit**: YES
  - Message: `test(server): backward-compat byte-diff observe stream`
  - Files: `tests/integration/backward_compat_observe.rs`, `tests/fixtures/per_principal_plugins/backward_compat_observe_stream.json`
  - Pre-commit: `cargo fmt --check && cargo test --test backward_compat_observe`

- [ ] 21. SSE batching divergence integration test

  **What to do**:
  - Add `tests/integration/sse_batching_per_principal.rs`.
  - Config: same plugin name `"my-hook"` in two principals A and B, with:
    - Principal A: `sse_per_event: true`, `batched_events_per_flush: 1`, `batched_flush_ms: 0`
    - Principal B: `sse_per_event: false`, `batched_events_per_flush: 16`, `batched_flush_ms: 200`
  - Send streaming requests via principal A and principal B simultaneously.
  - Assert:
    - Principal A's hook receives one observe call per SSE event (`call_count >= event_count`).
    - Principal B's hook receives one batched call per flush window (`call_count <= ceil(event_count / 16)`).
    - The two principals' counters are completely independent (no contamination).
  - Use a test plugin that increments per-call counters exposed via host function.

  **Must NOT do**:
  - Do not deduplicate the two slots — the whole point of the test is that they differ.
  - Do not allow the test to share a single batcher across principals.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: SSE timing + parallel principal load; needs careful synchronization to avoid flakiness.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 3
  - **Blocks**: final
  - **Blocked By**: T7, T10

  **References**:

  **Pattern References**:
  - `crates/cc-lb-runtime-extism/src/sse_batch.rs` — SSE batching impl (verify per-slot batcher state).
  - `benches/extism_sse_overhead` — for fixture plugin used in SSE testing; reuse if compatible.

  **WHY each reference matters**: SSE batching is the obvious place to find a hidden cross-principal-leak bug, and the bench's fixture plugin is the right shape for a fast integration test.

  **Acceptance Criteria**:
  - [ ] Test passes consistently across 5 runs
  - [ ] Per-principal counters never bleed into each other
  - [ ] `cargo test --test sse_batching_per_principal`

  **QA Scenarios**:
  ```
  Scenario: Divergent batching settings produce divergent call counts
    Tool: Bash (cargo)
    Steps:
      1. Run 5 times: for i in 1 2 3 4 5; do cargo test --test sse_batching_per_principal -- --nocapture 2>&1 | tee .omo/evidence/task-21-sse-run${i}.txt; done
    Expected Result: All 5 runs exit 0
    Evidence: .omo/evidence/task-21-sse-run1.txt ... .omo/evidence/task-21-sse-run5.txt
  ```

  **Commit**: YES
  - Message: `test(integration): SSE batching per principal divergence`
  - Files: `tests/integration/sse_batching_per_principal.rs`
  - Pre-commit: `cargo fmt --check && cargo test --test sse_batching_per_principal`

- [x] 22. Memory ceiling test (200 principals × 3 plugins) + hot-reload race test

  **What to do**:
  - Add `tests/integration/memory_ceiling.rs`:
    - Build a synthetic Config with 200 principals, each with 1 router_plugin + 2 observability_hooks (3 plugins per principal).
    - Boot a cc-lb instance with this config.
    - Assert `runtime.instances.read().len() == 600` (200 principals × 3 plugins) plus global slots (note: if config has no global plugins, baseline is `0`; if it does, `+ N_global`).
    - Read process RSS via `procfs::process::Process::myself()?.status()?.vmrss` (or platform-equivalent helper if procfs not in deps; otherwise read `/proc/self/status` directly). Assert RSS delta from a baseline-config boot is under a documented ceiling (capture the value once, hard-code it, e.g. `200 MiB` — adjust based on first measurement).
  - Add `tests/integration/hot_reload_race.rs`:
    - Boot a cc-lb instance with 10 principals.
    - Spawn 50 concurrent client tasks each issuing 10 requests (500 total).
    - In parallel, trigger 5 SIGHUP reloads that swap principal config but keep schema valid.
    - Assert: zero non-2xx responses, zero panics, observe event count matches expected (500 × events-per-request).

  **Must NOT do**:
  - Do not run real WASM through `extism::Plugin::call` 600 times in the test — register slots via stub manifests that succeed at instantiation but never actually execute.
  - Do not assert RSS without first capturing a baseline; flakiness from ceiling tests is a real risk.
  - Do not increase the test timeout to mask race conditions — flake = bug.

  **Recommended Agent Profile**:
  - **Category**: `unspecified-high`
    - Reason: Scale + concurrency; needs careful baselining and timing.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 5 (with T17)
  - **Blocks**: final
  - **Blocked By**: T8, T16

  **References**:

  **Pattern References**:
  - `tests/load/` — workspace member for load tests; reuse its harness if applicable.
  - `tests/crash-recovery/` — for SIGHUP-based test patterns.

  **WHY each reference matters**: existing scale-oriented tests provide harness helpers (spawn server, send concurrent requests, collect counters) that should be reused, not reinvented.

  **Acceptance Criteria**:
  - [ ] Memory ceiling test passes with `instances.len() == 600`
  - [ ] Hot-reload race test: 0 non-2xx, 0 panics, observe count exact
  - [ ] `cargo test --test memory_ceiling --test hot_reload_race`

  **QA Scenarios**:
  ```
  Scenario: Memory ceiling
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo test --test memory_ceiling -- --nocapture 2>&1 | tee .omo/evidence/task-22-mem.txt
    Expected Result: Exit 0; output contains `instances.len() = 600`
    Evidence: .omo/evidence/task-22-mem.txt

  Scenario: Hot-reload race (5 runs to detect flakiness)
    Tool: Bash (cargo)
    Steps:
      1. for i in 1 2 3 4 5; do cargo test --test hot_reload_race -- --nocapture 2>&1 | tee .omo/evidence/task-22-race-run${i}.txt; done
    Expected Result: All 5 runs exit 0
    Evidence: .omo/evidence/task-22-race-run1.txt ... .omo/evidence/task-22-race-run5.txt
  ```

  **Commit**: YES
  - Message: `test(server): memory ceiling + hot-reload race`
  - Files: `tests/integration/memory_ceiling.rs`, `tests/integration/hot_reload_race.rs`
  - Pre-commit: `cargo fmt --check && cargo test --test memory_ceiling --test hot_reload_race`

- [ ] 23. CHANGELOG + per-principal-plugin docs entry

  **What to do**:
  - Add a section under `docs/` (e.g. `docs/per-principal-plugins.md`) covering:
    - When and why to use per-principal plugin configuration.
    - The inherit-vs-explicit semantic for `Option<Vec<PluginRef>>` (`None` = inherit, `Some(vec![])` = explicit empty).
    - All-or-nothing validation/preflight/reload behavior.
    - Backward compatibility note: zero-principal-plugin configs are byte-for-byte identical to today.
    - Example config snippet showing global + per-principal mixed.
  - Update root `README.md` if it currently references the global plugin model — link to the new doc instead of inlining details.
  - Update `CHANGELOG.md` (if present) or create one with a `[Unreleased]` entry describing the new fields, the fallible `PrincipalView::from_config`, the SlotKey widening, and the admin status surface addition.

  **Must NOT do**:
  - Do not rewrite the existing global PluginsConfig docs.
  - Do not add tutorial-style walkthroughs beyond the example snippet.
  - Do not document internal types like `SlotKey` in user-facing docs (internal-only).

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Documentation only.
  - **Skills**: none

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 4 (with T16)
  - **Blocks**: F1 (Oracle plan-compliance audit reads docs to confirm Must-Have coverage)
  - **Blocked By**: T2, T15

  **References**:

  **Pattern References**:
  - `docs/` (workspace docs dir) — existing format/style.
  - `README.md` — for the link target update.
  - `CHANGELOG.md` if exists; otherwise use Keep-a-Changelog format.

  **WHY each reference matters**: external surface stability — the doc serves both human users and the F4 scope fidelity reviewer who diffs intent vs delivery.

  **Acceptance Criteria**:
  - [ ] `docs/per-principal-plugins.md` exists with the listed sections
  - [ ] Example TOML/TOML snippet parses cleanly (verify by including it as a test fixture in T6's set if shape matches)
  - [ ] `CHANGELOG.md` has a `[Unreleased]` entry referencing the user-visible changes

  **QA Scenarios**:
  ```
  Scenario: Docs build / lint
    Tool: Bash (cargo)
    Steps:
      1. Run: cargo doc --workspace --no-deps 2>&1 | tee .omo/evidence/task-23-cargodoc.txt
    Expected Result: Exit 0; no missing-link warnings
    Evidence: .omo/evidence/task-23-cargodoc.txt

  Scenario: Example snippet round-trips through config loader
    Tool: Bash (cargo)
    Steps:
      1. Add a test that loads the snippet from docs and asserts it parses to an expected Config (use `figment` like other config tests).
      2. Run: cargo test -p cc-lb-config docs_example_per_principal_snippet 2>&1 | tee .omo/evidence/task-23-snippet.txt
    Expected Result: Exit 0
    Evidence: .omo/evidence/task-23-snippet.txt
  ```

  **Commit**: YES
  - Message: `docs(config): per-principal plugin configuration guide + CHANGELOG`
  - Files: `docs/per-principal-plugins.md`, `README.md` (if updated), `CHANGELOG.md`
  - Pre-commit: `cargo fmt --check && cargo doc --workspace --no-deps`

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
>
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback → fix → re-run → present again → wait for okay.

- [ ] F1. **Plan Compliance Audit** — `oracle`

  Read this plan end-to-end. For each "Must Have": verify implementation exists (read file, curl endpoint, run `cargo test`). For each "Must NOT Have": search codebase for forbidden patterns (`PluginRegistry`, `PluginManager`, `PluginChain` trait impls, ApiKeyRecord modifications, `signer_factory` per-principal changes, new crate roots, dedup helpers, principal-miss counter, named library config) — reject with file:line if found. Check evidence files exist in `.omo/evidence/`. Compare deliverables vs plan list.

  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **Code Quality Review** — `unspecified-high`

  Run `cargo build --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `cargo deny check` if `deny.toml` rules apply, `RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view`. Review all changed files for: `as any`/`unwrap()` in non-test code, `unsafe` blocks introduced, empty `match _ => {}` arms hiding errors, `eprintln!`/`println!` in non-test/non-CLI code, commented-out code, unused imports. Slop sweep: excessive comments, generic names (`data`, `result`, `item`, `temp`), premature abstraction, premature `metrics::counter!` additions.

  Output: `Build [PASS/FAIL] | Lint [PASS/FAIL] | Tests [N pass/N fail] | Loom [PASS/FAIL] | Files [N clean/N issues] | VERDICT`

- [ ] F3. **Real Manual QA** — `unspecified-high`

  Start a clean cc-lb instance from a fresh config containing: (1) global default plugin chain, (2) principal `A` with its own `router_plugin` + `observability_hooks`, (3) principal `B` with NO plugin fields. Issue requests with each principal's api-key via `curl` and confirm observe event JSON shows the correct `plugin` name and `principal` id per request. Then trigger a hot-reload via SIGHUP changing only principal `A`'s plugin config; verify in-flight request to `A` completes against the old chain and a new request to `A` uses the new chain. Then trigger a reload introducing an invalid PluginRef in principal `C`; verify reload aborts and prior view stays live (assert no traffic disruption to `A` or `B`). Save curl outputs and admin `/status` snapshots to `.omo/evidence/final-qa/`.

  Output: `Scenarios [N/N pass] | Integration [N/N] | Edge Cases [N tested] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`

  For each task: read "What to do", read actual diff via `git log --stat` + `git diff`. Verify 1:1 — every Must Have shipped, no extras beyond plan. Confirm "Must NOT Have" compliance via codebase greps (`PluginRegistry`, `PluginManager`, `PluginChain`, `principal_miss`, named-library config keys, ApiKeyRecord field additions). Detect cross-task contamination (Task N touching Task M's files). Flag any unaccounted changes.

  Output: `Tasks [N/N compliant] | Contamination [CLEAN/N issues] | Unaccounted [CLEAN/N files] | VERDICT`

---

## Commit Strategy

> TDD enforced: each task has a RED commit (failing test) before the GREEN commit (implementation).

- **T1**: `test(config): add failing schema test for principal plugin fields` → none required (test-only)
- **T2**: `feat(config): add router_plugin + observability_hooks to PrincipalSpec` → `cargo test -p cc-lb-config`
- **T3**: `refactor(runtime-extism): widen instance/manifest maps via SlotKey newtype` → `cargo test -p cc-lb-runtime-extism`
- **T4**: `refactor(core): make PrincipalView::from_config fallible` → `cargo test -p cc-lb-core`
- **T5**: `feat(core): add resolved-plugin-chain cache to PrincipalSpecCached` → `cargo test -p cc-lb-core`
- **T6**: `feat(config): validate per-principal PluginRef whole-graph` → `cargo test -p cc-lb-config`
- **T7**: `feat(runtime-extism): per-principal slot register/lookup API` → `cargo test -p cc-lb-runtime-extism`
- **T8**: `feat(core): instantiate per-principal plugins in PrincipalView::from_config` → `cargo test -p cc-lb-core`
- **T9**: `refactor(core): Lifecycle resolves plugin chain via PrincipalView` → `cargo test -p cc-lb-core`
- **T10**: `feat(core): load-once-and-bind dispatch + pre-authn global-only observe` → `cargo test -p cc-lb-core`
- **T11**: `feat(server): wire fallible PrincipalView with ExtismRuntime at startup` → `cargo build --workspace`
- **T12**: `feat(server): whole-graph preflight dry-load all-or-nothing` → `cargo test -p cc-lb-server`
- **T13**: `test(runtime-extism): property test for SlotKey uniqueness` → `cargo test -p cc-lb-runtime-extism --test property`
- **T14**: `test(core): loom test for PrincipalView swap + concurrent handle()` → `RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view`
- **T15**: `feat(server): per-principal reload path warnings + all-or-nothing abort` → `cargo test -p cc-lb-server`
- **T16**: `feat(runtime-extism): post-swap eviction of stale slots` → `cargo test -p cc-lb-runtime-extism`
- **T17**: `test(integration): fault-injection per-principal reload abort` → `cargo test --test per_principal_reload_fault_injection`
- **T18**: `feat(admin): expose per-principal plugin status with redacted config` → `cargo test -p cc-lb-admin`
- **T19**: `feat(admin): include per-principal plugins in plugin_count` → `cargo test -p cc-lb-admin`
- **T20**: `test(server): backward-compat byte-diff observe stream` → `cargo test --test backward_compat_observe`
- **T21**: `test(integration): SSE batching per principal divergence` → `cargo test --test sse_batching_per_principal`
- **T22**: `test(server): memory ceiling + hot-reload race` → `cargo test --test memory_ceiling --test hot_reload_race`
- **T23**: `docs(config): per-principal plugin configuration guide + CHANGELOG` → `cargo doc --workspace --no-deps`

Pre-commit gate: `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p <changed_crate>`

---

## Success Criteria

### Verification Commands
```bash
# Full workspace
cargo build --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

# Targeted
RUSTFLAGS="--cfg loom" cargo test -p cc-lb-core --test loom_principal_view
cargo test --test hot_reload_race -- --nocapture
cargo test --test backward_compat_observe -- --nocapture
cargo test --test sse_batching_per_principal -- --nocapture
cargo test --test memory_ceiling -- --nocapture
cargo test --test per_principal_reload_fault_injection -- --nocapture

# Admin status (boot server first; PORT from .run/local.toml)
curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.plugins'                                   # legacy global field stays for back-compat
curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.principals'                                # new per-principal map: { <name>: { router_plugin, observability_hooks } }
curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.principals.alice.router_plugin'            # spot check: principal's router (null when inheriting global)
curl -s http://127.0.0.1:${ADMIN_PORT}/admin/status | jq '.principals.alice.observability_hooks'      # spot check: principal's hooks list
```

### Final Checklist
- [ ] All "Must Have" items shipped
- [ ] All "Must NOT Have" items absent (verified via grep)
- [ ] All workspace tests pass (existing + new)
- [ ] Loom tests pass
- [ ] Backward-compat byte-diff observe stream: empty diff
- [ ] Memory ceiling test: `instances.len() == 600` for 200×3 fixture
- [ ] Admin `/status` JSON shape verified via curl + jq
- [ ] F1-F4 wave APPROVE + user explicit okay
