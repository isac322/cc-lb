# Plugin Upload Replacement and Reference Lifecycle QA

Goal: functionally test the plugin registry lifecycle end to end — storage → admin API → admin-web UI → dynamic proxy/runtime rebind — so a zero-context agent can verify plugin upload replacement, warmup-inclusive reference counts, and referenced-plugin deletion.

Two equally weighted halves:
- **Part A — Point-in-time** (§3): a fixed backend state exposes the same plugin version, reference count, and reference list at storage, API, and UI layers.
- **Part B — State-transition** (§4): uploads, replacements, and deletes mutate every layer consistently without stale UI or broken runtime binding.

---

## 0. Environment & tooling

- Use an isolated cc-lb instance. Do not mutate a shared/prod DB for cascade-delete tests.
- Browser QA must use `agent-browser` only: run `agent-browser skills get core`, then drive the dashboard with `agent-browser` commands. Do not use Playwright/Puppeteer/Selenium directly.
- Admin auth: set `localStorage['cc-lb-admin-token']` to the test admin token; API calls use `Authorization: Bearer $TOKEN`.
- Build frontend changes before embedded-SPA QA: from `crates/cc-lb-admin/web`, run `bun run build`, then rebuild/restart the test server.
- For API assertions, save raw responses under `/tmp/cclb-plugin-qa/` and inspect with `jq`; do not paste full raw dumps into reports.

## 1. Fixture setup

1. Start a fresh SQLite-backed test instance on non-prod ports.
2. Build two wasm fixtures with identical plugin metadata `name` and versions `1.0.0` and `2.0.0`; build a third with identical version `1.0.0` but a different SHA.
3. Create one principal and one upstream.
4. Upload the first plugin as `slot_kind=shape`.
5. Bind the uploaded registry `id` to the principal plugin chain and to the upstream `warmup_dialect_plugin`.

Expected fixture invariant: storage/API/UI show one plugin row whose `refcount` is `2` and whose references include one `plugin_chain` and one `upstream_warmup_dialect`.

## 2. Point-in-time checks

### 2.1 Storage

- `wasm_registry_v2` has one stable registry `id`, plugin `name`, SHA, nullable semantic plugin version, original filename, and revision.
- `refcount` is not read from `wasm_blobs_v2`; it is computed from chain refs plus warmup refs.
- Reference fingerprint changes when any referenced chain/upstream revision changes.

### 2.2 Admin API

- `GET /admin/v1/plugins/registry` returns the plugin with `version`, `refcount: 2`, and supported slot `shape`.
- `GET /admin/v1/plugins/registry/{id}/references` returns `registry`, `refcount: 2`, `reference_fingerprint`, and both reference kinds.
- `DELETE /admin/v1/plugins/registry/{id}` without cascade returns `409 plugin_registry_referenced`.

### 2.3 Admin web UI

Using `agent-browser`:
- Open `/plugins`, Registry tab.
- Verify the upload card has an upload slot selector with Router, Observability, and Shape.
- Verify the registry row shows the plugin version and refs badge `2`.
- Click delete on the referenced plugin; verify the dialog lists both the chain/principal and warmup/upstream references and labels the action as cascade delete.

## 3. State-transition checks

### T1 — Same SHA upload is a noop

Upload the same wasm bytes again with the same `slot_kind`.

Expected:
- API returns `200`, `idempotent: true`, `action: "noop"`, same `id` and revision.
- UI success state does not duplicate the row; refs remain unchanged.

### T2 — Higher version replaces in place

Upload same metadata name with version `2.0.0` and a different SHA.

Expected:
- API returns `200`, `action: "replaced"`, same registry `id`, higher revision, version `2.0.0`.
- Chain and warmup references still point at the same registry `id`; refcount remains `2`.
- Runtime dynamic rebind is triggered; proxy requests using the shape plugin continue to route and observe the replacement behavior.

### T3 — Same/lower version requires confirmation

Upload same metadata name with same or lower semver and a different SHA.

Expected:
- First attempt returns `409 replacement_confirmation_required` with `replace_registry_id`, `expected_revision`, current/incoming versions, and SHA fragments.
- UI opens an alert dialog with those values.
- Confirming retries with `confirm_replacement=true`, `replace_registry_id`, and `expected_revision`; API returns `200 action:"replaced"`.

### T4 — Cascade delete removes references atomically

From the referenced plugin delete dialog, confirm cascade delete.

Expected:
- UI sends `DELETE /admin/v1/plugins/registry/{id}?cascade=references` with `If-Match` and `X-Reference-Fingerprint`.
- API returns `200` with deleted registry data and removed references.
- Storage removes plugin chain rows, clears upstream warmup dialect plugin, bumps upstream revision, deletes registry/blob/cache when orphaned.
- UI row disappears after query invalidation; principal chain and upstream warmup surfaces no longer show the plugin.
- Proxy/runtime rebind succeeds; subsequent proxy requests no longer execute the removed shape plugin.

### T5 — Stale reference fingerprint is blocked

Fetch references, mutate the chain or warmup reference, then attempt cascade delete with the old fingerprint.

Expected:
- API returns `409 references_changed`.
- UI reports the stale reference error and keeps the plugin row/references intact.

## 4. Verification commands

Minimum local gates before marking this scenario covered:

```bash
cargo fmt --check
cargo test -p cc-lb-storage-conformance --no-default-features --features sqlite plugin_registry -- --nocapture
cargo test -p cc-lb-admin --test integration --no-default-features --features sqlite wasm_upload -- --nocapture
cargo test -p cc-lb-admin --test integration --no-default-features --features sqlite v1_plugins::registry_ -- --nocapture
(cd crates/cc-lb-admin/web && bun run lint && bun run typecheck && bun run test src/routes/-plugins.test.tsx)
```

For browser evidence, include the `agent-browser` transcript or screenshots showing upload slot selection, replacement confirmation, references preview, and cascade delete completion.

## 5. PASS/FAIL report template

| Case | Storage | API | UI | Proxy/runtime | Verdict |
|---|---|---|---|---|---|
| Point-in-time refs |  |  |  | n/a |  |
| T1 noop |  |  |  | n/a |  |
| T2 higher-version replace |  |  |  |  |  |
| T3 confirmed replacement |  |  |  |  |  |
| T4 cascade delete |  |  |  |  |  |
| T5 stale fingerprint |  |  |  | n/a |  |
