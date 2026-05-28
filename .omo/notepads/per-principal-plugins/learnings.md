## 2026-05-28 T9

- Oracle phase-3 override remains authoritative over the stale T9 plan body: global default router/hooks stay on `Lifecycle`, not `PrincipalView`.
- `Lifecycle` now names those globals explicitly as `global_router` and `global_observability_hooks`; T10 should pass `&self.global_router` and `&self.global_observability_hooks` into `PrincipalSpecCached::{resolved_router,resolved_hooks}`.
- `PrincipalView` shape was not changed; startup still constructs and passes the same global plugin instances through `Lifecycle::new`.
- `rust-analyzer` is not installed in this environment, so LSP rename/reference/diagnostics commands currently report the missing server instead of semantic results.

## 2026-05-28 T13

- `crates/cc-lb-runtime-extism/tests/slot_key_uniqueness.rs` covers SlotKey uniqueness through the public staging API only: `instantiate_router_for`, `commit_staged`, and `registered_slot_keys`.
- The proptest input is bounded to `[a-z0-9_]` identifiers of length 1-16 and vectors of 1-256 `(principal_id, plugin_name)` pairs; cases are capped at 4 because each generated pair instantiates an Extism plugin, while default shrinking remains enabled and no fixed seed is used.
- The test also checks that `alice/shared` and `bob/shared` stage as distinct `Arc<dyn RouterPlugin>` handles with `!Arc::ptr_eq`, then commit to two public registered keys.
- Verification: `cargo test -p cc-lb-runtime-extism --test slot_key_uniqueness` passed, `cargo clippy --workspace --all-targets -- -D warnings` passed, and `rustfmt --edition 2024 --check crates/cc-lb-runtime-extism/tests/slot_key_uniqueness.rs` passed. Workspace `cargo fmt --check` still reports unrelated pre-existing formatting diffs outside T13 scope.

# T12 Preflight Whole-Graph Dry-Load

- 2026-05-28: `crates/cc-lb-server/src/preflight.rs` now dry-loads global plugins first, then per-principal plugin overrides in sorted principal-name order for deterministic first-failure behavior.
- 2026-05-28: Principal dry-load errors are wrapped as `PreflightError::Plugin` strings prefixed with `principals.{name}.router_plugin` or `principals.{name}.observability_hooks.{i}`, preserving the existing `dry_load_plugin` helper unchanged.
- 2026-05-28: Unit coverage uses tiny valid Wasm fixtures for global and per-principal router/observability plugins plus a missing Bob router artifact to assert all-or-nothing abort semantics.
- 2026-05-28: Verification passed for `cargo test -p cc-lb-server preflight -- --nocapture`, `cargo build --workspace`, and `cargo clippy -p cc-lb-server -- -D warnings`; `rust-analyzer` was unavailable for LSP diagnostics.

## 2026-05-28 T10

- `Lifecycle::handle` now binds a single `PrincipalView` snapshot with `load_full()` before parse/auth dispatch and passes that snapshot into builtin auth and limit reservation.
- Post-auth routing and observability resolve from `PrincipalSpecCached::{resolved_router,resolved_hooks}` using `&self.global_router` and `&self.global_observability_hooks`; streaming responses keep the snapshot alive instead of cloning a hooks Vec.
- `PrincipalView::from_config` synthesizes the configured none-mode principal when it is not listed under `principals`, preserving test-mode none auth while satisfying same-snapshot post-auth lookup.
- Verification passed: `cargo test -p cc-lb-core`, `cargo test -p cc-lb-server`, `cargo build --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings`; rust-analyzer was unavailable for LSP diagnostics.

## 2026-05-28 T11

- `crates/cc-lb-server/src/app.rs` stages global + per-principal plugin chains via the staging API and calls `runtime.commit_staged(all_staged)?` BEFORE wrapping the PrincipalView in ArcSwap.
- Added `pub(crate) fn build_global_chain(config, runtime, &mut staged) -> Result<(Arc<dyn RouterPlugin>, Vec<Arc<dyn ObservabilityHook>>), GlobalChainError>` for reuse by T15 in reload.rs.
- `pub(crate) enum GlobalChainError { Builtin, Runtime, Config }` with `thiserror::Error` derive and `#[from]` impls for `BuiltinError`, `RuntimeError`, `ConfigError`.
- `manifest_from_plugin` return type changed `Result<PluginManifest, BuildError>` → `Result<PluginManifest, ConfigError>`.
- ConfigWatcher and reload.rs intentionally untouched — T15 owns the runtime field + LastReloadStatus wiring.

## 2026-05-28 T20

- `crates/cc-lb-server/tests/backward_compat_observe.rs` exercises `Lifecycle::handle` directly with `BuiltinRouter`, two stub global observability hooks derived from `Config.plugins.observability_hooks`, and no Extism runtime.
- The zero-principal config relies on `PrincipalView::from_config` to synthesize the configured none-mode principal with inherited router/hooks; the companion config with an explicit principal but no plugin overrides must serialize to the same canonical event bytes.
- The golden file normalizes `request_id` to `req-N` and `duration_ms` to `0`; request ordering, hook ordering, principal id, upstream URL, statuses, and token counts remain byte-for-byte asserted against `crates/cc-lb-server/tests/fixtures/per_principal_plugins/backward_compat_observe_stream.json`.
- Verification passed for `cargo test -p cc-lb-server --test backward_compat_observe -- --nocapture`, `cargo build --workspace`, and `cargo clippy -p cc-lb-server --test backward_compat_observe -- -D warnings`; workspace clippy is currently blocked by unrelated dirty `cc-lb-admin/src/settings.rs` test code using field reassignment after `Default::default()`.

## 2026-05-28 T19

- `plugin_count` in `crates/cc-lb-admin/src/settings.rs` now iterates `config.principals` and sums per-principal `router_plugin` (if Some) and `observability_hooks.len()` entries alongside the global count.
- Implementation: `global_count` (router + hooks) + `per_principal_count` (each principal's router + hooks) = total.
- Three unit tests verify: (1) global-only backward compat (count unchanged with empty principals), (2) combined global + per-principal sum matching the example (1+2+1+3+0+2=9), (3) zero-principal equivalence.
- Verification: `cargo test -p cc-lb-admin --lib settings::tests` passed all 3 tests, `cargo build --workspace` passed, `cargo clippy -p cc-lb-admin -- -D warnings` passed with no warnings.
- Commit: `e3f0dcc` feat(admin): plugin_count includes per-principal plugins

## 2026-05-28 T15

- `ConfigWatcher` now owns an `Arc<ExtismRuntime>` and records `LastReloadStatus` through the `cc-lb-admin::CurrentConfig` trait; current admin routes are intentionally unchanged for T18 to surface later.
- Reload staging builds per-principal router/hook caches before publishing `PrincipalView`, commits staged runtime slots before `principal_view.store`, and records principal/plugin names on per-principal plugin failures.
- T15 keeps `Lifecycle` global plugin handles frozen; reload only stages configured global plugins when present and leaves a `TODO(T16)` marker after view publication for stale slot eviction.

## 2026-05-28 T14

- `crates/cc-lb-core/tests/loom_principal_view.rs` models the T10 same-snapshot dispatch pattern with `principal_view.load_full()` in one loom thread and `principal_view.store(new_view)` in another, then calls stub `RouterPlugin::route` and `ObservabilityHook::observe` through the resolved chain.
- The test avoids Extism entirely and uses generation-tagged stub router/hook instances so a torn or mismatched router/hook chain would fail the observed upstream generation assertion.
- `RUSTFLAGS="--cfg loom"` applies to dependencies too; `cc-lb-core` and `cc-lb-config` now keep loom builds on the minimal `PrincipalView`/config-type surface to avoid Tokio `net`/`signal` modules that are intentionally disabled under loom.
- Verification passed with isolated target dirs: `RUSTFLAGS="--cfg loom" CARGO_TARGET_DIR=/tmp/cc-lb-t14-loom-target cargo test -p cc-lb-core --test loom_principal_view -- --nocapture` and `CARGO_TARGET_DIR=/tmp/cc-lb-t14-build-target cargo build --workspace`.

## 2026-05-28 T18

- `/admin/status` is registered under the existing admin-auth protected routes and is backed by `cc_lb_admin::status::handler`, which reads `CurrentConfig::current_config()` plus `CurrentConfig::last_reload_status()`.
- The response keeps legacy `plugins` unchanged and adds `principals: { id -> { router_plugin, observability_hooks } }`; per-principal plugin refs expose `name`, `wasm_path`, and `config_hash` only.
- `config_hash` uses `sha2::Sha256` over `serde_json::to_string(&plugin_ref.config)` and emits the first 16 lowercase hex characters; raw `config` values are not serialized in `/admin/status`.
- Verification passed: `cargo build -p cc-lb-admin`, `cargo test -p cc-lb-admin`, and `cargo clippy -p cc-lb-admin -- -D warnings`.
