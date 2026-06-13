# cc-lb WASM Plugin Pipeline Follow-ups (post PR #119)

## How to read this document

You are a future agent picking up work in a **fresh worktree** with no prior context. This document is self-contained on purpose. Every claim is grounded in a `file:line` reference at the master commit listed below. If you find a discrepancy between this document and the code, **the code wins** — re-verify and update the doc rather than blindly trusting it.

## Provenance and starting state

- Repo: `isac322/cc-lb` on GitHub
- Master HEAD at writing: `33155995234` `fix(admin): filter plugin chain pickers by handshake-declared slot (#119)` (squash merge of PR #119)
- The merged PR introduced `WasmRegistryEntry.supported_slots: Vec<PluginSlot>`, ran handshake at upload to derive it, added a startup backfill, narrowed a legacy `route → Router` export fallback, and wired the field through admin API + admin UI pickers + chain-insert validation. All four follow-ups in this document were intentionally **scoped out** of #119 to keep the PR mergeable.
- Live service this work runs against: `cc-lb.service` (systemd user unit). Storage at `~/.local/share/cc-lb/storage.redb`. Admin port `52252`. Admin token in `~/.config/cc-lb/env` as `CC_LB_ADMIN_TOKEN`. `cc-lb.service` may be restarted freely; **do not** restart, stop, enable, disable, or mask `opencode.service`.
- AGENTS rules you must observe across every follow-up:
  - Show PR title/body to the user and wait for explicit approval before `gh pr create`.
  - Never add `Co-authored-by:`, `Ultraworked with Sisyphus`, or similar attribution to commit messages or PR bodies.
  - Commit body has no column limit; do not wrap lines for cosmetic reasons.
  - Do not merge a PR unless the user explicitly asks.
  - Do not run `sudo` unless the user explicitly provides the password.
  - When modifying state in postgres/redb storage for debugging, save raw dumps to `/tmp` and only emit aggregates back to stdout.

## Summary table

| ID | Title | Severity | Recommended PR scope | Touches |
|---|---|---|---|---|
| A | `/admin/v1/export` silently drops `supported_slots` | HIGH (regression) | Small standalone PR | admin/v1/status.rs, tests |
| B | `wire_version` hardcoded + not validated at chain-insert / dispatch | MEDIUM-HIGH | Larger PR; postgres migration required | storage-api, storage-redb, storage-postgres, runtime-extism, admin, server |
| C | Legacy `route`-only plugins dead-end at filter dispatch | MEDIUM (operational) | Investigation + targeted fix | runtime-extism, server backfill, possibly admin |
| D | Drift / trust hardening across re-handshake, view builder, warmup, preflight, rebalance, reorder, bootstrap, self-check, audit | LOW-MEDIUM bundle | Split into focused PRs per call site | many files; mostly additive validation |

Pick **one** follow-up per PR. Do not bundle.

---

## Follow-up A: `/admin/v1/export` drops `supported_slots`

### Symptom

`GET /admin/v1/export` produces a JSON payload meant to round-trip into a fresh cc-lb instance. The exported registry entries describe the wasm blobs (sha256, name, filename, label, size, refcount) but omit `supported_slots`. After import, every entry's `supported_slots` is empty, so every subsequent `POST /admin/v1/principals/{id}/plugin-chain` is rejected with `400 slot_metadata_unknown` (added in PR #119). The only way to recover is to manually re-upload every plugin, which defeats export/import.

### Evidence (current code)

- `crates/cc-lb-admin/src/v1/status.rs:103-115` defines the exported registry payload:

  ```rust
  registry: Vec<ExportRegistryEntry>,
  ...
  struct ExportRegistryEntry {
      sha256_hex: String,
      name: String,
      original_filename: String,
      label: Option<String>,
      size_bytes: u64,
      refcount: i64,
  }
  ```

  `supported_slots` is absent.

- `crates/cc-lb-admin/src/v1/status.rs:412-419` builds the value:

  ```rust
  fn export_registry_entry(entry: WasmRegistryEntry, size_bytes: u64) -> ExportRegistryEntry {
      ExportRegistryEntry {
          sha256_hex: hex_sha256(entry.sha256),
          ...
          refcount: entry.refcount,
      }
  }
  ```

  `entry.supported_slots` is in scope and discarded.

- For confirmation that the field is the canonical source: `crates/cc-lb-storage-api/src/plugin_registry.rs` declares `pub supported_slots: Vec<PluginSlot>` on `WasmRegistryEntry` with `#[serde(default)]` (so removing the export field on a future schema change would be silent). PR #119's chain-insert guard in `crates/cc-lb-admin/src/v1/plugins.rs` (around L300) explicitly rejects empty `supported_slots`.

### Why this was scoped out of #119

PR #119 focused on writing `supported_slots` into the registry, surfacing it through the admin API/UI, and enforcing it on chain insert. Touching the export endpoint would have widened the diff with no functional dependency on the rest of the fix. It is a separate, tightly scoped regression.

### Proposed fix

1. Add `pub supported_slots: Vec<String>` to `ExportRegistryEntry` (snake-case slot ids `"router" / "shape" / "observability_hook"`, same wire format as `RegistryEntryResponse.supported_slots` in `crates/cc-lb-admin/src/v1/plugins.rs`).
2. In `export_registry_entry`, map `entry.supported_slots.iter().map(|slot| slot.as_str().to_owned()).collect()`.
3. Confirm there is **no** corresponding import endpoint that already expects the field (search `import` in `crates/cc-lb-admin/src/v1/`). If an import endpoint exists, ensure it honors the new field with `#[serde(default)]` so legacy exports without it still parse.
4. Snapshot tests / golden files for the export payload (look under `crates/cc-lb-admin/tests/` for any `export_*.rs`) must be updated to assert the new field.

### Tests to add

- New integration test in `crates/cc-lb-admin/tests/` that uploads a fixture wasm (the `tests/fixtures/extism-echo-plugin/` produces `[router]`), calls the export endpoint, and asserts each registry entry contains the correct `supported_slots`.
- A round-trip test: upload → export → assert exported JSON, then if an import endpoint exists, import into a fresh harness and assert chain insert succeeds.

### Migration / operator impact

- Backwards compatible: legacy exports parse fine when consumed by the import path because the receiver should use `#[serde(default)]`.
- Operators with old exports may still hit `slot_metadata_unknown` on import until startup backfill re-derives slots from the imported blobs, which already exists in `crates/cc-lb-server/src/app.rs::backfill_supported_slots`.

### Verification

```sh
source ~/.config/cc-lb/env
curl -sS -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" http://localhost:52252/admin/v1/export | jq '.registry[].supported_slots'
```

Every entry must show a non-empty array.

---

## Follow-up B: `wire_version` is hardcoded and never validated at chain insert / dispatch

### Symptom

The host treats every registry entry as if it were the maximum supported wire version (`BUILTIN_CACHE_AFFINITY_WIRE_VERSION = 3`) regardless of what the plugin actually negotiates, and lets admins write any `wire_version` they want into a chain entry. If an admin chains a v2-only plugin with `wire_version: 3`, dispatch will compose v2 envelopes (`_v: 2` injected by `crates/cc-lb-runtime-extism/src/dispatch.rs:34-46`) against a host shape path that expects v3 (`crates/cc-lb-runtime-extism/src/plugin_wrap.rs:98` `use_v2 = matches!(self.slot.negotiated_wire_version(), Ok(2))`), corrupting requests instead of failing fast at insert time.

### Evidence

- `crates/cc-lb-storage-redb/src/adapter/plugin_registry.rs:277-278` hardcodes both fields on upload:

  ```rust
  kind: "filter".to_owned(),
  wire_version: cc_lb_storage_api::BUILTIN_CACHE_AFFINITY_WIRE_VERSION,
  ```

  (PR #119 left `kind` hardcoded intentionally as a dead field replaced by `supported_slots`; in this follow-up `kind` is in scope to either be dropped from the struct or re-derived.)

- `crates/cc-lb-storage-postgres/src/adapter/plugin_registry.rs:695-696` returns the same hardcoded values from postgres, because the `wasm_registry_v2` schema has no columns for `kind`, `wire_version`, `is_builtin`, or `metadata`. See migrations `crates/cc-lb-storage-postgres/migrations/0017_plugin_registry.sql`, `0022_plugin_registry.sql`, `0039_wasm_registry_supported_slots.sql`. This is a **schema parity gap** with redb's JSON blob.

- `crates/cc-lb-admin/src/v1/plugins.rs::insert_chain` (around L286) accepts `body.wire_version` and stores it on the chain entry without checking it against the registry entry or against the plugin's handshake-reported capabilities.

- The handshake response that the upload path already runs (see `derive_supported_slots` in `crates/cc-lb-admin/src/v1/plugins_wasm.rs:404-436`) carries `chosen_versions`, `plugin_supported`, and `required_capabilities`. PR #119 only consumed `implemented_functions` and discarded the rest, missing the wire_version source of truth.

### Why this was scoped out of #119

This requires a postgres migration (`wasm_registry_v2`), a storage-api struct update (`WasmRegistryEntry.wire_version` derivation rules + serde defaults), changes in both adapters' `registry_from_row` mappers, an upload-path change to persist the negotiated version, a chain-insert validator, and likely an admin UI surface for the negotiated version. Folding it into #119 would have at least doubled the diff and the regression surface.

### Proposed fix

Work in three commits:

**B1. Storage parity**

1. Add a postgres migration `crates/cc-lb-storage-postgres/migrations/0040_wasm_registry_wire_version.sql`:

   ```sql
   ALTER TABLE wasm_registry_v2
     ADD COLUMN IF NOT EXISTS wire_version SMALLINT NOT NULL DEFAULT 1;
   ```

   (`1` is the most conservative legacy value. The startup backfill will narrow this for real plugins; see B3.)

2. In `crates/cc-lb-storage-postgres/src/adapter/plugin_registry.rs`:
   - Bind `&wire_version: i16` (cast from `u8`) in the insert path next to `supported_slots`.
   - Read it back via `row.try_get::<i16, _>("wire_version")` and cast to `u8` in `registry_from_row`.

3. Optional but recommended: also add columns for `kind` (TEXT) and `metadata` (JSONB). If the team is fine deprecating `kind` outright, remove it from `WasmRegistryEntry` instead — see Follow-up C and D for the dispatch knock-ons.

**B2. Derive wire_version on upload**

1. In `crates/cc-lb-admin/src/v1/plugins_wasm.rs::upload_wasm_inner`, after `derive_supported_slots` returns, also extract `accept.chosen_versions` (highest negotiated `(name → version)`) and compute `wire_version = max version across supported slots`. Pass it through `WasmRegistryEntryInput` (new `wire_version: u8` field with `#[serde(default = "default_wire_version")]` returning `1`).
2. In `crates/cc-lb-storage-redb/src/adapter/plugin_registry.rs::persist_wasm_upload_sync`, replace the hardcoded `BUILTIN_CACHE_AFFINITY_WIRE_VERSION` with `input.wire_version`.
3. Heal the same way #119 healed `supported_slots`: on idempotent re-upload with stored `wire_version` that disagrees with the freshly negotiated one, call a new `update_wire_version` storage method.

**B3. Backfill + validate at chain insert and dispatch**

1. Mirror the existing `backfill_supported_slots` in `crates/cc-lb-server/src/app.rs` with a `backfill_wire_version` that runs the handshake (or, if it fails, leaves the stored default in place and logs a warning).
2. In `crates/cc-lb-admin/src/v1/plugins.rs::insert_chain` (around L286-320), reject a chain entry whose `wire_version` is greater than the registry entry's negotiated version or which is not in `chosen_versions[slot]`. Error code: `unsupported_wire_version` with hint pointing the operator at the registry entry's `wire_version`.
3. Also reject at runtime defensively in `crates/cc-lb-runtime-extism/src/dispatch.rs::dispatch_wire_call_inner` if the chain entry's `wire_version` is not in `augmented_metadata.negotiated_functions[F::NAME]` — analogous to the existing `MissingNegotiatedVersion` branch.

### Tests to add

- Storage-api round-trip + legacy default (mirror the `supported_slots` test pattern in `crates/cc-lb-storage-api/tests/plugin_registry_metadata.rs`).
- redb + postgres conformance: persist with explicit `wire_version`, read back equal; missing column / default falls back to 1.
- Admin: upload a plugin whose handshake advertises only v1 of `shape`, then chain it with `wire_version: 2` → expect 400 `unsupported_wire_version`.
- Server backfill: seed an entry with `wire_version: 1` and a v2-handshake-capable wasm; backfill bumps to `2`.

### Migration / operator impact

- Postgres migration is additive with a `DEFAULT 1`, safe on rolling deploys.
- Operators with chain entries currently pinned to `wire_version > negotiated` will start receiving 400s on the next admin write; provide a one-shot `cc-lb` subcommand or admin endpoint to surface the diff before the migration goes out.
- Redb storage gains a new field but `#[serde(default)]` keeps legacy blobs parsable.

### Verification

After deploy, the live registry must show non-3 `wire_version` for any non-cache-affinity plugin:

```sh
source ~/.config/cc-lb/env
curl -sS -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" http://localhost:52252/admin/v1/plugins/registry | jq '.entries[] | {name, wire_version}'
```

`cache-aware-router` should land on whatever its handshake reports (it exports `cc_lb_handshake` and `cc_lb_self_check` so it can do a real handshake; see live evidence in Follow-up C). New uploads of a wire-v3 plugin must report `3`.

---

## Follow-up C: Legacy `route`-only plugins dead-end at filter dispatch

### Background you need before touching this

`cache-aware-router` (live in `cc-lb.service` at `~/.local/share/cc-lb/plugins/wasm/cache/83fd1ae9e68aafa303a4275d1b0bab57c36eb05fa168b260a4d4bb1245f4a076.wasm`) exports `cc_lb_handshake`, `cc_lb_self_check`, `normalize_error`, `route`, `shape`. Wire v3 renamed the router slot's export from `route` to `filter`. The host runtime no longer recognizes `route` as a routable wire function:

- `crates/cc-lb-runtime-extism/src/lib.rs:277-285`:

  ```rust
  #[allow(deprecated)]
  pub fn instantiate_router_for(...) -> Result<...> {
      Err(router_wire_removed_error())
  }
  ```

- `crates/cc-lb-runtime-extism/src/lib.rs:287-296` instantiates the Router slot via `instantiate_filter_for`, which calls `stage_slot(..., "filter")` (`lib.rs:269`) and returns `InstantiateFailed { reason: "plugin {name} does not export filter" }` when the plugin lacks that export.

PR #119's narrow extism-export fallback in `crates/cc-lb-runtime-extism/src/handshake.rs` maps `route → Router`, so backfill persists `[Router]` for `cache-aware-router`. Chain insert then accepts it into a Router slot. **Dispatch fails on first call** with `does not export filter`.

### Open question this follow-up must answer first

Confirm whether `cache-aware-router` is currently in any principal's plugin chain on the live `cc-lb.service`. If yes, confirm whether requests actually exercise its Router slot or whether the runtime silently skips broken slots. Steps:

```sh
source ~/.config/cc-lb/env
curl -sS -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  http://localhost:52252/admin/v1/principals \
| jq '.principals[].id' \
| while read id; do
    id=${id//\"/}
    echo "principal=$id"
    curl -sS -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
      "http://localhost:52252/admin/v1/principals/$id/plugin-chain?slot=Router" \
    | jq '{slot: "router", entries: [.entries[] | {wasm_registry_id, wire_version}]}'
done
```

If `cache-aware-router`'s registry id appears, send a single real request through that principal and watch `journalctl --user -u cc-lb -f` for `InstantiateFailed`. If it appears and traffic does not blow up, the runtime has another bypass we are missing — read `crates/cc-lb-runtime-extism/src/lib.rs::stage_slot` carefully and update this document with what you learn before fixing.

### Symptom (assuming the open question confirms the dead-end)

Operators can insert a wire-v1/v2 router plugin into a Router slot via admin API, see it in the UI, and only discover at first request that it never instantiates.

### Proposed fix (choose one path, do not do both)

**Path 1: refuse silent legacy admission.** Drop `slot_set_from_extism_exports` entirely. Make `backfill_supported_slots` leave such entries with `supported_slots: []`, so chain insert keeps rejecting them with the existing `slot_metadata_unknown` 400. PR #119 already preserves the detail-drawer warn badge so operators can see them in registry/detail. This is the conservative path; legacy plugins must be re-built against wire v3 before they can be chained again.

Touchpoints:
- Delete the function in `crates/cc-lb-runtime-extism/src/handshake.rs` and its 5 negative tests `slot_set_from_extism_exports_*` (kept for clarity; safe to remove now).
- Remove the fallback call in `crates/cc-lb-server/src/app.rs::backfill_supported_slots`.
- Update the existing `backfill_recovers_legacy_route_plugin_via_extism_export_scan` test in `crates/cc-lb-server/tests/backfill_supported_slots.rs` to assert the slot remains empty (rename it accordingly).

**Path 2: thin compatibility shim.** Teach `instantiate_filter_for` to call `route` when `filter` is missing but `route` is present. This is a real wire migration: confirm the envelope and return-value layout match between `route` and `filter` (likely they do not, since v3 changed conventions). Requires runtime changes in `crates/cc-lb-runtime-extism/src/lib.rs::stage_slot`, careful audit of `dispatch.rs`, and migration tests. Likely too invasive for a follow-up; recommended only if Path 1 breaks a deployed customer.

### Tests to add

For Path 1:
- Update the existing fallback-success test as above.
- A negative test in the admin layer that asserts a re-uploaded `route`-only wasm is admitted to the registry but receives 400 `slot_metadata_unknown` on chain insert.

For Path 2 (if chosen):
- A dispatch integration test that calls `route` on a `route`-only plugin through the Router slot and verifies success.

### Migration / operator impact

- Path 1: any legacy plugin currently in a chain will start being rejected from chain mutation; existing chain entries continue to run until the next instantiation (where they already fail). Provide a `cc-lb` doctor command that lists chain entries whose registry entry has `supported_slots == []` so operators can scrub them.
- Path 2: silent migration. Provide release notes.

### Verification (Path 1)

```sh
source ~/.config/cc-lb/env
# 1) Force-clear cache-aware-router's supported_slots via a tiny one-shot binary
#    (mirror /tmp/cc-lb-reset pattern documented in this repo's PR history).
# 2) systemctl --user restart cc-lb
# 3) Confirm backfill leaves it empty:
curl -sS -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" http://localhost:52252/admin/v1/plugins/registry | jq '.entries[] | select(.name=="cache-aware-router") | .supported_slots'
# Must be [].
# 4) Attempt chain insert: expect 400 slot_metadata_unknown.
```

---

## Follow-up D: Drift / trust hardening bundle

These are smaller hardening items. **Split each into its own focused PR** unless reviewers ask to bundle.

### D1. `supported_slots` drift detection on re-handshake

`crates/cc-lb-server/src/startup_handshake.rs::process_record` (around L158-220, find `re_handshake_by_sha256`) updates `augmented_metadata` from the fresh handshake but does **not** compare `slot_set_from_handshake(&accept.implemented_functions)` against the stored `entry.supported_slots`. A plugin re-built with different exports drifts silently.

Fix: after re-handshake succeeds, compute the fresh slot set and:
- If equal to stored: no-op.
- If different: log `tracing::warn!(stored, fresh, "supported_slots drift on re-handshake")` and call `update_supported_slots`.

Tests: extend `crates/cc-lb-server/tests/backfill_supported_slots.rs` with a fixture that has been re-uploaded with new exports.

### D2. View-builder + warmup + preflight + bootstrap defensive checks

`crates/cc-lb-server/src/dynamic_view_builder.rs::build_principal_chains` (around L408 where it fetches `registry_entry`) trusts `entry.slot` blindly. After PR #119 the database should never contain a mismatched pair, but if redb is restored from an older snapshot or replicas diverge, this becomes silent. Add:

```rust
if !registry_entry.supported_slots.is_empty()
   && !registry_entry.supported_slots.contains(&entry.slot)
{
    tracing::warn!(
        plugin_id = %registry_entry.id,
        plugin_name = %registry_entry.name,
        chain_slot = ?entry.slot,
        supported_slots = ?registry_entry.supported_slots,
        "skipping chain entry: plugin does not support chain slot",
    );
    continue;
}
```

Same pattern in:
- `crates/cc-lb-server/src/warmup/dialect.rs:68` (`shape` slot check)
- `crates/cc-lb-server/src/preflight.rs:206` (entry.slot check)
- `crates/cc-lb-server/src/bootstrap.rs:230-249` (`apply_plugin_chain` — bootstrap is meant to fail closed; promote the check to a hard error rather than skip)

### D3. Chain rebalance / reorder defensive recheck

`crates/cc-lb-admin/src/v1/plugins.rs::reorder_chain` (L385) and `rebalance_chain` (L424) only shuffle `order`. They cannot create a mismatched (slot, plugin) pair on their own because they preserve `entry.slot`, but it costs almost nothing to re-validate. Add the same `supported_slots.contains(&entry.slot)` check as D2 and surface a 409 / 400 if a mismatch is found (treat as evidence of database drift; tell the operator to run a doctor).

### D4. Self-check coverage by `supported_slots`

`crates/cc-lb-runtime-extism/src/self_check.rs:64` passes `all_wire_functions()` to the plugin. After `supported_slots` exists, the self-check should pass exactly that set (or at minimum assert the plugin reports `ok` for each slot it claims to support). Change `functions_to_test` to be parameterized by the registry entry's `supported_slots` (mapped back via `wire_function_to_slot`'s inverse — encode as a small helper in `crates/cc-lb-runtime-extism/src/handshake.rs`).

### D5. Audit log enrichment

`crates/cc-lb-admin/src/v1/plugins.rs::emit_chain_audit` (L810) and `crates/cc-lb-core/src/audit_payload.rs::AuditPayload::PluginChainUpdate` (L68) record only `principal_id` and `slots_changed`. Add `wasm_registry_id: String`, `sha256_hex: String`, and `supported_slots: Vec<String>` so operators can trace drift after the fact. Pure observability, no behavior change.

### Tests to add (per item)

- D1: fixture wasm A with exports `{filter}`, fixture B with exports `{filter, shape}`. Persist registry with A, swap blob to B, run re-handshake, assert slots updated to `[Router, Shape]`.
- D2: seed a chain entry with `slot = Shape` and a registry entry with `supported_slots = [Router]`; assert `build_principal_chains` skips it and emits the warn log.
- D3: same fixture; call `reorder_chain` / `rebalance_chain` and assert the new defensive error path triggers.
- D4: update existing self-check tests to verify only declared slots are exercised.
- D5: snapshot test on the emitted `AuditPayload::PluginChainUpdate` JSON.

### Migration / operator impact

- D1–D3 are silent on healthy databases; they only fire on drift, which should never happen post-PR-#119 on a fresh database.
- D5 is additive on the audit payload; downstream consumers should already be using `#[serde(deny_unknown_fields)] = false` (verify before shipping).

---

## Out of scope / intentional behavior — do **not** "fix"

The audit that produced this document also flagged the following. They are intentional or false positives and should not be re-opened:

- **Frontend `pluginSupportsSlot()` returns `false` for empty `supported_slots`** (`crates/cc-lb-admin/web/src/routes/principals.tsx:86-92`). This was Oracle's explicit ask in PR #119 round 2: an empty-slot plugin must not appear in any picker. The detail drawer (`principals.tsx:810-818`) still shows a warn "Unknown slot" badge so operators are not blind to its existence. Do not weaken this.
- **`backfill_supported_slots` skips entries with non-empty `supported_slots`** (`crates/cc-lb-server/src/app.rs:1195`). This is the idempotency guarantee. D1 above adds the missing **drift detection on explicit re-handshake** without touching the idempotent skip.
- **`is_builtin` is hardcoded `false` outside the cache-affinity constructor.** The audit suggested adding schema support; in practice the storage layer cannot mark a non-builtin as builtin without code changes, so this is safe by construction. Revisit only if a second builtin plugin is introduced.
- **`refcount` / `revision` computation** (audit cleared as safe). Do not refactor.

---

## Operational notes you will need

### Live deploy you can use to verify

```sh
# Service status
systemctl --user status cc-lb --no-pager

# Logs
journalctl --user -u cc-lb -f

# Restart after a new binary
systemctl --user stop cc-lb
command cp target/release/cc-lb ~/.local/bin/cc-lb
systemctl --user start cc-lb
~/.local/bin/cc-lb --version    # must echo the new short SHA

# Admin API
source ~/.config/cc-lb/env
curl -sS -H "Authorization: Bearer $CC_LB_ADMIN_TOKEN" \
  http://localhost:52252/admin/v1/plugins/registry | jq .
```

`build.rs` only re-runs on env change, so always pass `GIT_SHA=$(git rev-parse --short HEAD)` to cargo when building for deploy:

```sh
GIT_SHA=$(git rev-parse --short HEAD) cargo build --release -p cc-lb-server
```

### Force-clearing `supported_slots` on a live entry

If you need to re-run backfill for a single plugin, the cleanest path is a one-shot Rust binary that reuses `cc-lb-storage-redb` (PR #119 used this pattern). Sketch:

```rust
// /tmp/cc-lb-reset/src/main.rs
use std::path::PathBuf;
use cc_lb_storage_api::PluginRegistryStore;
use cc_lb_storage_redb::Storage;

#[tokio::main]
async fn main() {
    let mk_hex = std::env::var("CC_LB_MASTER_KEY").unwrap();
    let mut master_key = [0u8; 32];
    for i in 0..32 {
        master_key[i] = u8::from_str_radix(&mk_hex[i * 2..i * 2 + 2], 16).unwrap();
    }
    let path = PathBuf::from("/home/example/.local/share/cc-lb/storage.redb");
    let storage = Storage::open(&path, master_key).unwrap();
    let entries = storage.list_registry(None, usize::MAX).await.unwrap();
    for entry in entries {
        if entry.name == "TARGET_PLUGIN_NAME" {
            storage.update_supported_slots(entry.id, Vec::new()).await.unwrap();
            println!("cleared {} ({})", entry.name, entry.id);
        }
    }
}
```

`Cargo.toml` for that crate carries path dependencies on `cc-lb-storage-api` and `cc-lb-storage-redb` in this worktree. Stop `cc-lb.service` first to release the redb lock. Delete the temp crate after use.

### Anti-patterns the agent must not commit

- Do **not** widen the upload handler to also persist `kind` from handshake. PR #119 left `kind` hardcoded as a deliberate compatibility layer; Follow-up B is the right place to address it, in a single coordinated change.
- Do **not** silently extend `slot_set_from_extism_exports` past `route → Router`. Oracle round 2 narrowed it intentionally. Follow-up C decides between deleting it (Path 1) or replacing it with a real shim (Path 2). Do not split the difference.
- Do **not** add `Co-authored-by:` or `Ultraworked with Sisyphus` to any commit body or PR description.
- Do **not** `git push --force` once a PR exists. Use additional commits or `git push --force-with-lease` only after explicit user approval.

### Reference: file:line citations cross-checked at master 33155995

| Topic | File | Line(s) |
|---|---|---|
| Export endpoint struct | `crates/cc-lb-admin/src/v1/status.rs` | 103, 108-115, 412-419 |
| Export endpoint reads `list_registry` | `crates/cc-lb-admin/src/v1/status.rs` | 272 |
| Redb hardcoded kind / wire_version | `crates/cc-lb-storage-redb/src/adapter/plugin_registry.rs` | 277-278 |
| Postgres `registry_from_row` hardcoded fields | `crates/cc-lb-storage-postgres/src/adapter/plugin_registry.rs` | 695-698 |
| Postgres registry schema migrations | `crates/cc-lb-storage-postgres/migrations/` | `0017_plugin_registry.sql`, `0022_plugin_registry.sql`, `0039_wasm_registry_supported_slots.sql` |
| Upload handler runs handshake, drops most fields | `crates/cc-lb-admin/src/v1/plugins_wasm.rs` | 404-436 |
| Chain insert validates slot but not wire_version | `crates/cc-lb-admin/src/v1/plugins.rs` | ~286-320 |
| Narrow extism export fallback | `crates/cc-lb-runtime-extism/src/handshake.rs` | 46-70 |
| Router instantiation deprecated stub | `crates/cc-lb-runtime-extism/src/lib.rs` | 277-285 |
| Filter instantiation requires `filter` export | `crates/cc-lb-runtime-extism/src/lib.rs` | 269-274, 287-296 |
| Wire version envelope inject | `crates/cc-lb-runtime-extism/src/dispatch.rs` | 34-46 |
| Dialect v2 vs v1 branch | `crates/cc-lb-runtime-extism/src/plugin_wrap.rs` | 98 |
| Startup handshake re-handshake (no slot drift check) | `crates/cc-lb-server/src/startup_handshake.rs` | 171 |
| Backfill skip on non-empty supported_slots | `crates/cc-lb-server/src/app.rs` | 1195 |
| View builder builds chain entries | `crates/cc-lb-server/src/dynamic_view_builder.rs` | 408 |
| Warmup dialect fetches entry | `crates/cc-lb-server/src/warmup/dialect.rs` | 68 |
| Preflight checks blob, not slot | `crates/cc-lb-server/src/preflight.rs` | 206 |
| Bootstrap apply chain | `crates/cc-lb-server/src/bootstrap.rs` | 230-249 |
| Reorder chain handler | `crates/cc-lb-admin/src/v1/plugins.rs` | 385 |
| Rebalance chain handler | `crates/cc-lb-admin/src/v1/plugins.rs` | 424 |
| Audit emit chain | `crates/cc-lb-admin/src/v1/plugins.rs` | 810 |
| Audit payload schema | `crates/cc-lb-core/src/audit_payload.rs` | 68 |
| Self check fixed function list | `crates/cc-lb-runtime-extism/src/self_check.rs` | 64 |

If any of these have shifted by a few lines when you read them, that is fine — search for the surrounding symbol or comment.

---

## Suggested PR sequencing

1. **PR-A** (small, ship first): export endpoint includes `supported_slots`.
2. **PR-D1** (small): drift detection on re-handshake (the smallest piece of D, ships independently).
3. **PR-D5** (small): audit log enrichment.
4. **PR-B** (large): `wire_version` end to end. Coordinate with the team because of the postgres migration and the chain-insert rejection.
5. **PR-C**: legacy `route` plugin policy. Send the open question first as a comment so the team chooses Path 1 vs Path 2 before you implement.
6. **PR-D2 / D3 / D4** as time allows.

Show each PR title and body to the user for approval before `gh pr create`. Use `gh pr checks --watch` after creation; do **not** declare the work done until CI is green and `mergeStateStatus=CLEAN`. Do **not** merge unless the user explicitly asks.
