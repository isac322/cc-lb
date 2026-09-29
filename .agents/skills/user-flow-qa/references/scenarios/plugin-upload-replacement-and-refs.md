# Plugin upload, library, replacement, and reference lifecycle QA

Goal: functionally test the plugin upload and library lifecycle end to end — storage → admin API → admin-web UI → dynamic proxy/runtime rebind — so a zero-context agent can verify that an operator can upload an arbitrary wasm file, review the detected capabilities, inspect where it is used, replace it safely, and delete it with reference handling.

Two equally weighted halves:
- **Part A — Point-in-time** (§2): a fixed backend state exposes the same plugin version, detected capabilities, reference count, and reference list at storage, API, and UI layers.
- **Part B — State-transition** (§3): browser uploads, replacements, reference changes, and deletes mutate every layer consistently without stale UI or broken runtime binding.

---

## 0. Environment & tooling

- Use an isolated cc-lb instance. Do not mutate a shared/prod DB for cascade-delete tests.
- Browser QA must use `agent-browser` only: run `agent-browser skills get core`, then drive the dashboard with `agent-browser` commands. Do not use Playwright/Puppeteer/Selenium directly.
- Admin auth: set `localStorage['cc-lb-admin-token']` to the test admin token; API calls use `Authorization: Bearer $TOKEN`.
- Build frontend changes before embedded-SPA QA: from `crates/cc-lb-admin/web`, run `bun run build`, then rebuild/restart the test server. For Vite QA, serve the current admin web with `bun run dev --host 0.0.0.0` and point it at the isolated admin API.
- For API assertions, save raw responses under `/tmp/cclb-plugin-qa/` and inspect with `jq`; do not paste full raw dumps into reports.

## 1. Fixture setup

1. Start a fresh SQLite-backed test instance on non-prod ports.
2. Build at least one valid wasm plugin fixture and keep it as a local file path for browser upload. Build two more fixtures with identical plugin metadata `name` and versions `1.0.0` and `2.0.0`; build a third with identical version `1.0.0` but a different SHA.
3. Create one principal and one upstream.
4. Upload the first plugin without choosing or sending a slot. The upload path must infer supported slots and hooks from embedded plugin metadata.
5. Bind the uploaded registry `id` to the principal plugin chain and to the upstream `warmup_dialect_plugin`.

Expected fixture invariant: storage/API/UI show one plugin row whose supported slots match embedded metadata, whose `refcount` is `2`, and whose references include one `plugin_chain` and one `upstream_warmup_dialect`.

## 2. Point-in-time checks

### 2.1 Storage

- `wasm_registry_v2` has one stable registry `id`, plugin `name`, SHA, nullable semantic plugin version, original filename, and revision.
- Supported slots and hook metadata are derived from the uploaded artifact, not from a user-selected slot field.
- `refcount` is not read from `wasm_blobs_v2`; it is computed from chain refs plus warmup refs.
- Reference fingerprint changes when any referenced chain/upstream revision changes.

### 2.2 Admin API

- `POST /admin/v1/plugins/wasm` accepts a valid multipart wasm upload and returns the detected registry entry.
- `GET /admin/v1/plugins/registry` returns the plugin with `version`, `refcount: 2`, detected `supported_slots`, and hook metadata.
- `GET /admin/v1/plugins/registry/{id}/references` returns `registry`, `refcount: 2`, `reference_fingerprint`, and both reference kinds.
- `DELETE /admin/v1/plugins/registry/{id}` without cascade returns `409 plugin_registry_referenced`.
- `POST /admin/v1/plugins/wasm/gc` removes only uploaded WASM blobs that no
  registered plugin references (orphan blobs). It never deletes a registered
  plugin, including registered uploads whose `refcount` is `0`.

### 2.3 Admin web UI

Using `agent-browser`:
- Open `/plugins` and authenticate with the isolated admin token in `localStorage['cc-lb-admin-token']`.
- Verify the upload card is user-centered: title `Upload plugin`, drop zone `Choose .wasm file`, and no slot selector or historical slot-selection copy.
- Upload a valid arbitrary wasm file through the file chooser. Verify the library row appears without a page reload, displays detected `Works in` badges, `File hash`, `Added`, and `Used By` values, and offers Inspect.
- Click Inspect. Verify the URL moves to `/plugins?plugin=<id>`, the browser Back button returns to the library instead of leaving Plugins, and the detail view shows `What this plugin does`, `Where it can run`, hooks, `Used by`, `Use this plugin`, `File details`, `Updating this plugin`, and `Manage this plugin`.
- For a referenced plugin, click delete and verify the dialog lists both the principal and upstream references in user-facing language and labels the destructive action as `Delete and remove uses`.
- The catalog cleanup action is labeled `Clean orphaned uploads` and stays
  enabled even when every registered upload reports `Used By` 0: its target is
  orphan blobs, not registered plugins. The `N not used anywhere` count still
  describes registered uploads only.
- After a successful delete from the plugin detail view, the UI returns to the
  catalog and clears the `?plugin=<id>` selection; the deleted plugin's
  references query cache is dropped so no stale or 404 references fetch
  remains.

## 3. State-transition checks

### T1 — Browser upload creates a detected library row

Use a real browser file chooser to upload a valid wasm plugin file.

Expected:
- UI sends the multipart upload without any slot field selected by the user.
- API returns `200` with a registry `id`, detected supported slots, hook metadata, and file hash.
- Storage writes exactly one registry row and blob/cache artifact for the SHA.
- UI immediately transitions to the uploaded plugin detail or shows the new row, with no duplicate rows and no stale loading state.
- A screenshot captures the new row/detail and the absence of slot-selection UI.

### T2 — Same SHA upload is a noop

Upload the same wasm bytes again through the browser.

Expected:
- API returns `200`, `idempotent: true`, `action: "noop"`, same `id` and revision.
- UI success state does not duplicate the row; refs remain unchanged.

### T3 — Higher version replaces in place

Upload same metadata name with version `2.0.0` and a different SHA.

Expected:
- API returns `200`, `action: "replaced"`, same registry `id`, higher revision, version `2.0.0`.
- Chain and warmup references still point at the same registry `id`; refcount remains `2`.
- Runtime dynamic rebind is triggered; proxy requests using the shape plugin continue to route and observe the replacement behavior.

### T4 — Same/lower version requires confirmation

Upload same metadata name with same or lower semver and a different SHA.

Expected:
- First attempt returns `409 replacement_confirmation_required` with `replace_registry_id`, `expected_revision`, current/incoming versions, and SHA fragments.
- UI opens an alert dialog with those values.
- Confirming retries with `confirm_replacement=true`, `replace_registry_id`, and `expected_revision`; API returns `200 action:"replaced"`.

### T5 — Delete and remove uses removes references atomically

From the referenced plugin delete dialog, confirm `Delete and remove uses`.

Expected:
- UI sends `DELETE /admin/v1/plugins/registry/{id}?cascade=references` with `If-Match` and `X-Reference-Fingerprint`.
- API returns `200` with deleted registry data and removed references.
- Storage removes plugin chain rows, clears upstream warmup dialect plugin, bumps upstream revision, deletes registry/blob/cache when orphaned.
- UI row disappears after query invalidation; principal chain and upstream warmup surfaces no longer show the plugin.
- Proxy/runtime rebind succeeds; subsequent proxy requests no longer execute the removed shape plugin.
- The detail view navigates back to the catalog, the `?plugin=<id>` URL
  selection is cleared, and the deleted plugin's references query cache is
  removed rather than refetched.

### T6 — Stale reference fingerprint is blocked

Fetch references, mutate the chain or warmup reference, then attempt cascade delete with the old fingerprint.

Expected:
- API returns `409 references_changed`.
- UI reports the stale reference error and keeps the plugin row/references intact.

### T7 — Orphan cleanup removes blobs, not registered plugins

With at least one registered upload whose `Used By` is `0` and at least one
orphan blob seeded in storage (a blob row no registry entry references):

Expected:
- The catalog offers `Clean orphaned uploads` and the action is enabled.
- Confirming sends `POST /admin/v1/plugins/wasm/gc`; the registry list is
  unchanged afterward — the refcount-0 registered plugin still exists.
- Storage loses only the orphan blob rows; registered plugin rows, blobs, and
  references are untouched.
- UI reports the removed orphan blob count (or that none were found).

## 4. Verification commands

Minimum local gates before marking this scenario covered:

```bash
cargo fmt --check
cargo test -p cc-lb-storage-conformance --no-default-features --features sqlite plugin_registry -- --nocapture
cargo test -p cc-lb-admin --test integration --no-default-features --features sqlite wasm_upload -- --nocapture
cargo test -p cc-lb-admin --test integration --no-default-features --features sqlite v1_plugins::registry_ -- --nocapture
(cd crates/cc-lb-admin/web && bun run lint && bun run typecheck && bun run test src/routes/-plugins.test.tsx)
```

For browser evidence, include the `agent-browser` transcript or screenshots showing direct wasm file upload, the new library row/detail, replacement confirmation, references preview, and `Delete and remove uses` completion.


## 5. PASS/FAIL report template

| Case | Storage | API | UI | Proxy/runtime | Verdict |
|---|---|---|---|---|---|
| Point-in-time refs |  |  |  | n/a |  |
| T1 browser upload |  |  |  | n/a |  |
| T2 noop |  |  |  | n/a |  |
| T3 higher-version replace |  |  |  |  |  |
| T4 confirmed replacement |  |  |  |  |  |
| T5 delete and remove uses |  |  |  |  |  |
| T6 stale fingerprint |  |  |  | n/a |  |
| T7 orphan cleanup |  |  |  | n/a |  |
