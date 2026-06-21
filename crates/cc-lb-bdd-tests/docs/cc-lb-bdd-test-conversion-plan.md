# cc-lb BDD v5.2 → Rust executable test conversion plan (v3.2, resolving the B3 PARTIAL from Momus v3.1 APPROVED-WITH-EDITS)

- Date: 2026-06-18
- Conversion scope: 4 files / 27 features / 321 scenarios (v5.2)
- **Scenario text language (decided in v3)**: **English**. The v5.2 reports are human-readable documents written in Korean; the Rust test code's step text, scenario titles, and doc comments are **English**. Persona names (Alice/Bob/Charlie/Dana) and domain vocabulary (upstream / warmup / replica / callback / readyz / drain / sweep / lease / token / quota) stay English as well. The Korean-source → English-step-text translation queue is an additional M0 deliverable.
- Target workspace: `/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf` (branch `opencode/crisp-wolf`)
- v3 changelog (v2 → v3):
  1. **Scenario source language locked to English.** v2's "Korean domain vocabulary" notation is dropped. Korean stays only in the human-readable v5.2 markdown reports; the Rust artifacts (step text, title, doc, jargon-rewrite gate) are all English.
  2. **BDD framework recomparison + decision matrix** (§3 rewritten end-to-end). Four candidates compared head-to-head: cucumber-rs 0.23, hand-rolled `bdd_scenario!` macro, gauge, gherkin-rs. **Conclusion unchanged (hand-rolled macro adopted)** — but the rejection rationale shifts from v2's "Korean step matching is fragile" to v3's "no external scenario-level filtering through nextest + heavy dependency cost + ID-based mapping wins".
  3. §4.2 scenario metadata: Korean doc comment → English doc comment (the Korean source path + line range is preserved as a backlink).
  4. §4.3's reasoning against Korean function names is naturally resolved by the English-source decision (§4.3 is folded into this §0).
  5. §7.1 persona bootstrap: persona English names stay; persona descriptions are one-line English.
  6. §12 jargon-rewrite gate is inverted in meaning: v2 = "check that English infrastructure words were swapped for Korean domain vocabulary" → v3 = "detect and block **Rust implementation jargon (Vec<u8>, HashMap, tokio::spawn, sqlx::query!, Arc, Mutex, etc.)** leaking into step text". Domain vocabulary (upstream/warmup/replica/callback/readyz) is all allowed.
  7. §13 R-jargon-rewrite risk: redefined under the English source policy. The detection grep pattern is spelled out.
  8. (The 10 Momus blocking gaps from v2 were resolved already in v2 and stay resolved in v3.)
- v3.1 changelog (v3 → v3.1, resolving the 3 new blocking issues raised in Momus's v3 REJECT):
  1. **§1.3 stale Korean hard constraint refreshed** — the line preserving "Korean scenario titles / personas / domain vocabulary", which conflicted with the v3 §0 English decision, is replaced by the English-source policy. Korean originals survive only as `///` doc backlinks; Korean is forbidden in panic / assert / Rust identifiers.
  2. **§12.1 forbidden jargon list significantly expanded** — the items Momus flagged as missing (`JoinHandle`, `JoinSet`, `select!`, `join!`, `mpsc`, `oneshot`, `broadcast`, `Semaphore`, `Runtime`, `block_on`, `Duration`, `Instant`, `sleep`, `serde_json::Value`, `Uuid`) plus `Notify`, `Stream`, `Cow`, `OnceCell`, `OnceLock`, `Lazy`, `task::yield_now`, `spawn_blocking`, `Transaction`, `PgPool`, `SqlitePool`, `Migrator`, `tower::`, `hyper::`, `http::`, `Json(`, `Response::`, `Request::`, `Method::`, `Uri`, `Deserialize`, `Serialize`, `json!`, `thiserror::`, ` ?;`, `bail!`, `ensure!`, `Err(`, `pub fn`, `pub async fn`, `pub struct`, `pub enum`, `impl `, `where `, `'a`, `'static` are added.
  3. **§3.5 macro gains a `description =` string literal argument + §12.1 lint targets are spelled out** — Momus's concern was that lint extractability was not guaranteed. The macro now requires a `description` string literal, so the grep targets are constrained to four named text slots (title literal + description literal + generated-fn doc comment + panic literal) rather than closure bodies. The lint implementation uses the `syn` crate to extract named-argument literals (avoiding any dependency on `cargo expand`).
  4. §13 R3 row reinforced (mandating the above + listing lint non-targets + a PR checklist row).
- v3.2 changelog (v3.1 → v3.2, resolving Momus's v3.1 B3 PARTIAL):
  1. §3.5 macro signature: `description =` moved from the "optional attributes" block to the "**required attributes**" block. Missing it is a compile error, with the Momus B3 rationale recorded inline.
  2. §12.1 lint-target text list: the `description` row's "optional" wording corrected to "**required argument**".
- Purpose of this document: **plan only**. Code lands in a separate round.
- Context inventory: [cc-lb-bdd-inventory-coverage-v5.2.md](/home/bhyoo/cc-lb-bdd/cc-lb-bdd-inventory-coverage-v5.2.md) (79.3% COVERED of the 1326-entry inventory).

---

## 0. Decision summary (TL;DR)

| Area | Decision | Rejected alternatives |
|---|---|---|
| Gherkin runner | **New `bdd_scenario!` macro** (hand-rolled, defined as a `macro_rules!` inside the `cc-lb-bdd-tests` crate). See §3.2 for the comparison matrix. | cucumber-rs 0.23 / gauge / gherkin-rs / hand-written `#[test]` per scenario |
| Scenario → Rust function | ID rule `f<n>_<m>[_suffix]` + `fast_f<n>_<m>` prefix for fast candidates. **English title / step / doc**, with a required macro `persona=` argument (`Alice` / `Bob` / `Charlie` / `Dana`). | Korean function names (rust-analyzer / nextest filter / git grep friction), Korean step text (auto-killed by the English-source decision) |
| Backend matrix | sqlite + postgres on both sides (reusing the conformance pattern verbatim). | sqlite-only |
| Anthropic upstream | **fake-anthropic's `MessageScript` is the primary tool** (FIFO response queue + header injection + `with_delay` + recorded-request retrieval + `wait_for_requests` async wait). A single narrow extension PR to fake-anthropic covers cases `MessageScript` cannot express — SSE streaming + mid-response drop — per §5.2. | a separate `ScenarioInjector` trait, wiremock-only |
| OAuth | Reuse mock-anthropic-oauth-server. | New mock |
| DB isolation | sqlite tempfile-per-test + postgres schema-per-test. | Shared DB + transaction rollback |
| Postgres URL env | **`CI_POSTGRES_URL` as the single canonical name** (matching postgres.yml). The local short alias `DATABASE_URL` is allowed only as a fallback; code and scripts always look up `CI_POSTGRES_URL` first. | Use `DATABASE_URL` directly |
| Persona | Four bootstrap helpers (`alice()`, `bob()`, `charlie()`, `dana()`). | Inline setup |
| Crate shape | Four top-level integration test binaries (one per writer): `tests/w1.rs`, `tests/w2.rs`, `tests/w3.rs`, `tests/w4.rs`. Each binary uses `mod w<n>_<feature>;` to pull in submodules (cargo auto-discovery works). | 27 binaries (one per feature), nested dirs (cargo auto-discovery breaks) |
| Fast subset | Function-name prefix `fast_` (e.g. `fast_f1_1a`). nextest filter `test(/^fast_/)`. | An `@fast` tag (Rust identifiers cannot contain `@`) |
| CI | New `.github/workflows/bdd.yml` + `bdd-nightly.yml`. | Extending ci.yml |
| PR vs nightly | PR = `fast_` prefix functions only, ≤5 minutes. Nightly = full suite, ≤30 minutes. | Full suite on every PR |
| No-real-API gate | (a) Build-time check `grep -r api.anthropic.com tests/`, (b) loopback-only HTTP wrapper in the integration fixture, panicking on violation. | Trust-based |
| Captures retention | `tests/__captures__/` is **gitignored**. Uploaded via `actions/upload-artifact` only on CI failure. | Commit to git |
| Progress unit | M0–M5 milestones. Every milestone gate enforces the invariant `converted + OoS-manual + blocked == 321`. | Feature-by-feature big-bang |

---

## 1. Goals and constraints (the contract)

### 1.1 Goals
1. Convert the 321 v5.2 scenarios into **individually executable Rust tests**.
2. Each converted test must run **locally with a single command** and **automatically in CI**.
3. On failure, every scenario must print **scenario ID + persona + failing step + binary-observable evidence**.
4. The same scenario must pass on **sqlite (default) + postgres (conformance)** — except for the explicitly marked OoS-manual items.
5. At every milestone gate the **321 invariant must hold**: `converted + OoS-manual + blocked == 321`.

### 1.2 Non-goals
- Backfilling the 1326-inventory's NC (82) and PARTIAL (95) items (v6 agenda).
- Real Anthropic API regression.
- Load / soak / 24-hour stability (`tests/soak/soak.sh`, `.github/workflows/soak.yml`).
- Web dashboard UI visual regression (Playwright / visual-qa is a separate track).
- "Human-look" persona-visibility scenarios are explicitly OoS-manual (exact ID list in §7.3 / the M0 deliverable).

### 1.3 Hard constraints
- No real Anthropic API calls. Every upstream response is generated by fake-anthropic or wiremock.
- The §13 no-real-API detection gate is enforced both at build time and in CI.
- Scenario step text, title, doc comment, and panic messages are **English**. The English domain vocabulary (`upstream`, `warmup`, `callback`, `replica`, `readyz`, `drain`, `sweep`, `lease`, `token`, `quota`, `principal`, `audit`, `signer`, `dialect`, `plugin`, `chain`, `terminal`, `filter`, `shape`, `observability`) is used as-is inside step text and doc comments. The Korean originals (v5.2 markdown) are preserved only as a `/// Original (Korean human-readable report): <path>#L<from>-L<to>` backlink doc comment. Korean must NOT appear in panic / assert messages or Rust identifiers.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo nextest run` must all pass.

---

## 2. Current asset inventory (reuse targets)

This conversion minimizes new infrastructure. The reused, already-verified assets:

| Asset | Path | Reuse mode |
|---|---|---|
| `ConformanceBackend` trait | [crates/cc-lb-storage-conformance/src/harness.rs](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/crates/cc-lb-storage-conformance/src/harness.rs) | Mirror trait shape → new `BddBackend` trait |
| `scenario!` / `plugin_registry_scenario!` macros | each scenarios file (anthropic_compatibility_kv_store.rs:22, organization_metadata_store.rs:19, ...) | **Not reused**. Pattern reference only; a fresh `bdd_scenario!` is defined inside cc-lb-bdd-tests |
| `MessageScript` + `ScriptedMessageResponse` + `RecordedMessageRequest` | [tests/fixtures/fake-anthropic/src/routes.rs:48-156](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/tests/fixtures/fake-anthropic/src/routes.rs) | **Main tool**. `push_response`, `pop_response`, `requests()`, `request_count()`, `wait_for_requests(expected, timeout)`, `with_delay`, `with_header` are all used as-is |
| fake-anthropic library form | `fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse, RecordedMessageRequest, app}` (lib.rs:8) | In-process spawn |
| mock-anthropic-oauth-server | tests/fixtures/mock-anthropic-oauth-server | OAuth (F5, F10) |
| wiremock 0.6 | Workspace dependency | Auxiliary upstream (LiteLLM price catalog — F24) |
| Existing E2E pattern | [pipeline_e2e.rs](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/crates/cc-lb-server/tests/pipeline_e2e.rs) | 5 fake-anthropic + sqlite + Lifecycle boot chain |
| Postgres CI pattern | [.github/workflows/postgres.yml](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.github/workflows/postgres.yml) | postgres:18 service + `CI_POSTGRES_URL` + `--test-threads=4` |
| Sqlite CI pattern | [.github/workflows/ci.yml](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.github/workflows/ci.yml) | nextest + sccache + `oracle4-cc-lb` runner + features matrix `[sqlite, postgres]` |

### 2.1 Asset gaps (must be built)

1. **`bdd_scenario!` macro** (defined locally inside cc-lb-bdd-tests). Auto-generates persona / `fast_` prefix / sqlite+postgres pairs.
2. **fake-anthropic library extension PR** (narrow scope):
   - SSE streaming response output option (current `into_response()` is `Json(body)` only).
   - Mid-response connection-drop option.
   - Both options are added as enum variants of `ScriptedMessageResponse` (backwards compatible).
3. **Persona bootstrap helpers**.
4. **Scenario-ID ↔ function-name mapping table** (321 rows, M0 deliverable). The 321 invariant is enforced on every milestone gate.
5. **No-real-API detection fixtures** (loopback-only `reqwest::Client` wrapper + `cargo test no_real_anthropic_in_fixtures`).

---

## 3. Decision 1: Gherkin runner (recompared under the English-source policy, v3)

### 3.1 Choice: new `bdd_scenario!` macro inside the `cc-lb-bdd-tests` crate (hand-rolled, `macro_rules!`)

### 3.2 Comparison matrix (4 candidates × 13 axes)

Survey date: 2026-06-18. Sources:
- cucumber-rs 0.23.0 `Cargo.toml` (`gherkin = 0.16`, `cucumber-codegen = 0.23.0`, `cucumber-expressions = 0.5`, `inventory = 0.3`, `clap = 4.3`, +9 transitive)
- cucumber-rs README + Tags doc + the IntelliJ libtest-integration doc (cucumber-rs/cucumber main branch book)
- Real-world uses: `leptos-rs/leptos` examples/*/e2e/, `eclipse-kuksa/kuksa-databroker`, `hit-box/hitbox`, `mandrean/cw-optimizoor`, `boxabirds/maw`
- gauge official site (gauge.org) supported-language matrix
- This workspace's `crates/cc-lb-storage-conformance/src/scenarios/*.rs` — 8 file-local `scenario!` `macro_rules!` definitions (`anthropic_compatibility_kv_store.rs:22`, `organization_metadata_store.rs:19`, `upstream_rate_limit_store.rs:23`, `upstream_subscription_metadata_store.rs:20`, `upstream_subscription_quota_store.rs:34`, `scenarios/upstream_store.rs:84`, `plugin_registry_store.rs`'s `plugin_registry_scenario!`)

| # | Axis | cucumber-rs 0.23 | hand-rolled `bdd_scenario!` (v3 choice) | gauge | gherkin-rs (parser only) |
|---|---|---|---|---|---|
| 1 | Rust native support | ✓ | ✓ | ✗ JS/C#/Java/Py/Ruby only | ✓ parser, runner is DIY |
| 2 | English `.feature` file parsing | ✓ (`.feature` auto) | ✗ Rust DSL only | n/a | ✓ parser API |
| 3 | Step text → fn binding | `#[given/when/then(expr=...)]` + cucumber-expressions + `inventory` linker collect | Explicit macro args (id, persona, title, given/when/then closures) | n/a | DIY |
| 4 | Backend matrix (sqlite/pg) | `Scenario Outline + Examples` or a backend field inside `World` | Macro attribute `backend = both` (default) / `sqlite_only` / `postgres_only` → expands to 2 fns | n/a | DIY |
| 5 | nextest **external** scenario-level filter | **✗** A single binary is exposed. `--tags @fast` is cucumber-rs's **internal** CLI / `CUCUMBER_FILTER_TAGS` env. `harness = false` is forced. | **✓** Every scenario is a native `#[tokio::test]` function. `cargo nextest -E 'test(/^fast_/)'`, `test(=f1_1a_sqlite)` all work as-is. | n/a | DIY (can choose nextest-friendly layout) |
| 6 | Fast-subset mechanism | tag `@fast` + cucumber-internal filter (unrelated to nextest) | Macro attribute `fast = true` → emitted fn name carries the `fast_` prefix | n/a | DIY |
| 7 | Single-scenario debugging (1-test nextest filter) | binary + `--name f1_1a` (cucumber-internal regex). nextest sees only one binary. | `cargo nextest run -E 'test(=f1_1a_sqlite)'` works directly. | n/a | possible |
| 8 | Compile time (321 sc, at M5) | Step fn registration uses `inventory` so size impact is small. But `cucumber-codegen` proc-macro + 13+ transitive deps add +60–120s cold build / +5–10s with workspace sccache. | macro_rules expansion gives 321 fns (642 with sqlite/postgres split). 4 binary units (W1–W4) → ~160 fns each. Zero external deps → cold build impact is minimal. | n/a | depends |
| 9 | `MessageScript` / fake-anthropic integration | `World` carries `Arc<MessageScript>`; in-process spawn happens in a hook. The `World` is global, so per-scenario isolation depends on `World::default()` being invoked per scenario. | One line in the macro body: `let bdd_ctx = BddCtx::spawn_fake_anthropic().await;`. Fresh instance per scenario is guaranteed. | n/a | possible |
| 10 | Persona preservation (English source) | "Alice", "Bob" appear in the step text directly (natural under English source). `World` carries the persona context. | Macro forces `persona = Alice` — compile error if missing. The generated fn's panic message auto-prefixes the persona. | n/a | DIY |
| 11 | Alignment with the conformance `scenario!` pattern | World/Cucumber runner is its own system. `ConformanceBackend` trait calls are possible inside `World`, but it is a **different paradigm coexisting**. | Same mental model as the workspace's 8 file-local `scenario!` macro_rules! (generated fn → backend matrix → conformance trait call). | n/a | possible |
| 12 | Added external dependencies | 13+ transitive deps (`gherkin 0.16`, `cucumber-codegen`, `cucumber-expressions`, `inventory`, `clap 4.3`, `globwalk`, `ref-cast`, `sealed`, `smart-default`, `derive_more`, `humantime`, ...) | 0 (workspace-internal) | n/a | gherkin (one crate) |
| 13 | Output / CI friendliness | `--format=json` (libtest-compatible, IntelliJ Rust only). JUnit XML / Cucumber JSON supported. Different stream from nextest's junit/json output (cucumber binary owns stdout). | nextest's standard libtest output + junit-output is reused as-is. `cargo nextest run --message-format=json` also works. | n/a | DIY |

### 3.3 Decision (under the English-source policy) and rejection rationale

**Adopted: hand-rolled `bdd_scenario!` macro.**

Even though the English-source policy auto-resolves some of cucumber-rs's weaknesses (e.g. unstable Korean step matching), the choice is unchanged because:

- **Axis 5 (no external scenario-level filtering) is decisive.** v2 §0's "fast subset = function prefix + `cargo nextest -E 'test(/^fast_/)'`" is the first-stage gate in our CI. cucumber-rs forces `harness = false`, so nextest sees only one binary. Fast subset, single-scenario isolation, retry strategy — all depend on cucumber's internal CLI (`--tags`, `--name`). **It cannot be integrated on equal terms with the external tool (nextest filter expression DSL).**
- **Axis 11 (conformance pattern alignment).** The workspace already has 8 file-local `scenario!` macro_rules! sharing the same pattern (id → backend matrix fn pair → conformance trait call). The new `bdd_scenario!` keeps the same mental model → zero learning-curve cost for adopting a new macro. cucumber-rs introduces the **different paradigm** of `World` / step attr / `cucumber-expressions`.
- **Axis 12 (external dependencies).** Adopting cucumber-rs adds `cucumber-codegen` proc-macro + `inventory` linker collect + `cucumber-expressions` regex DSL + 9 transitive deps. Possible conflicts with the workspace's sqlite/postgres feature matrix + extism runtime + axum + sqlx dependency tree. Hand-rolled = 0 added deps.
- **Axis 3 (step → fn binding).** cucumber-expressions' fuzzy step matching pays off when **natural-language variability is high**. Our 321 scenarios are already ID-keyed (F1.1a → f1_1a) + explicit macro arguments. Explicit ID mapping is stronger than fuzzy matching (it avoids regex collisions between "Alice creates a team" and "Alice creates a team named 'X'").

**Rejected**:
- **cucumber-rs 0.23**: axes 5/11/12 all incur cost. Even after the English-source decision, the conclusion is unchanged.
- **gauge**: no Rust support (supported languages: JS/C#/Java/Py/Ruby). Could be worked around by polyglot fan-out, but that is orthogonal to this workspace's ergonomics and only adds CI burden.
- **gherkin-rs (parser only)**: a subset of cucumber-rs. Building a runner on top of it would carry the same burden without any advantage over cucumber-rs.
- **Hand-written `#[test]` per scenario**: boilerplate explosion (321 fn × 2 backends × persona setup × MessageScript spawn ≈ 2,000 duplicated lines). One macro reduces this to 0 lines.

### 3.4 Relationship with the conformance `scenario!` (clarification)
- The workspace's `scenario!` and `plugin_registry_scenario!` are **helper macros defined locally inside each conformance file** (anthropic_compatibility_kv_store.rs:22, organization_metadata_store.rs:19, upstream_rate_limit_store.rs:23, upstream_subscription_metadata_store.rs:20, upstream_subscription_quota_store.rs:34, scenarios/upstream_store.rs:84, plugin_registry_store.rs).
- They are not a public reusable API and therefore are **not an extension target**. Only the pattern (signature, sqlite/postgres matrix generation, error path) is mirrored in a fresh definition inside cc-lb-bdd-tests.

### 3.5 New macro signature (proposed, English source)

```rust
// crates/cc-lb-bdd-tests/src/macros.rs (M0 deliverable)
//
// usage:
//   bdd_scenario!(
//     id = "F1.1a",
//     fn_name = f1_1a,                 // ASCII identifier (no non-ascii)
//     persona = Alice,                 // required attribute; compile error if missing
//     title = "Alice registers a new principal and sees active state + first key on one screen",
//     given = |ctx| async move { ctx.alice().await },
//     when  = |ctx, alice| async move { alice.create_principal("team-x").await },
//     then  = |ctx, _alice, result: PrincipalCreateResult| async move {
//       ctx.assert(result.is_active,
//         "[F1.1a · Alice] active flag missing. expected=true, actual={}",
//         result.is_active);
//       ctx.assert(result.first_key.is_some(),
//         "[F1.1a · Alice] first key missing. expected=Some, actual=None");
//     },
//   );
//
// required attributes (in addition to id/fn_name/persona/title/given/when/then above):
//   description = "Alice creates principal 'team-x' via admin POST. \
//                  Verify response carries active=true and first_key=Some. \
//                  Audit row written with actor=admin, action=PrincipalCreate.",
//                         // ↑ Natural-language Given/When/Then narrative as a string literal.
//                         //   THIS IS REQUIRED, not optional. Missing description = compile error.
//                         //   This is the primary scenario-text input to the §12.1 jargon-lint
//                         //   (along with `title =`). Closure bodies are excluded from lint.
//                         //   Multi-line literals concatenated with `\` continuation are OK.
//                         //   Rationale: guarantees grep-extractability (Momus v3 B3).
//
// optional attributes:
//   fast = true,           // → fn name gets prefix `fast_f1_1a`, picked up by nextest -E 'test(/^fast_/)'
//   backend = sqlite_only, // → skip postgres matrix (e.g. F17.x multi-replica scenarios)
//   oos_manual = "human visual assertion; promote to visual-qa track", // OoS-manual: compiles but #[ignore] + reason doc auto-attached
```

Macro body:
- `sqlite` feature active → `#[tokio::test] async fn <fn_name>_sqlite()` generated.
- `postgres` feature active && `backend != sqlite_only` → `#[tokio::test] async fn <fn_name>_postgres()` generated.
- `fast = true` → both functions emitted as `fast_<fn_name>_sqlite()` / `fast_<fn_name>_postgres()`.
- Failure → `panic!("[{id} · {persona}] {step}: {observable_failed}")` (English message format; no Korean characters in panic text — Korean is for the human report only).

---

## 4. Decision 2: scenario → Rust function mapping rules

### 4.1 ID rule

| Gherkin ID | Rust fn name (sqlite) | Rust fn name (postgres) | Location | Fast? |
|---|---|---|---|---|
| F1.1a | `f1_1a_sqlite` (`fast_f1_1a_sqlite`) | `f1_1a_postgres` (`fast_f1_1a_postgres`) | `crates/cc-lb-bdd-tests/tests/w1/f1_principal_create.rs` | YES |
| F11A.9 | `f11a_9_sqlite` | `f11a_9_postgres` | `tests/w2/f11a_warmup_target.rs` | NO |
| F11B.4 | `f11b_4_sqlite` | `f11b_4_postgres` | `tests/w2/f11b_warmup_execution.rs` | NO |
| F25.13 | `f25_13_sqlite` | `f25_13_postgres` | `tests/w3/f25_plugin_runtime.rs` | NO |

Rules:
- Dot → underscore (`F1.1a` → `f1_1a`).
- English suffix preserved (a/b/c/d).
- One pair of functions per backend, auto-generated by the macro.
- Fast subset entries gain the additional `fast_` prefix.
- File split: per feature (`f<N>_<short_name>.rs`).
- Directory split: per writer (`tests/w1/`, ..., `tests/w4/`).
- **cargo auto-discovery guaranteed**: see the §8.1 layout.

### 4.2 Scenario metadata preservation (English, v3)

```rust
/// # F1.1a — Alice (operator) registers a new principal and sees active state + first key on one screen
///
/// **Given** Alice is logged in to the cc-lb operator console
/// **When** she registers a new principal named "team-x"
/// **Then** the result screen shows the active flag and the first key together
///
/// Persona: Alice (operator)
/// Original (Korean human-readable report): cc-lb-true-bdd-1-team-traffic-v5.2.md L42-L58
bdd_scenario!(
    id = "F1.1a",
    fn_name = f1_1a,
    persona = Alice,
    fast = true,
    title = "Alice registers a new principal and sees active state + first key on one screen",
    ...
);
```

- Doc comment, title, and step text are all **English**.
- The Korean-report path + line range is preserved as a backlink in the doc (`Original ... L42-L58`).
- Panic / assert messages are English too (for example `"[F1.1a · Alice] active flag missing. expected=true, actual=false"`).

### 4.3 Identifier policy (§v2.4.3 naturally absorbed under the English-source decision)
The "why Korean function names are unused" section from v2 is auto-resolved once English is the source language and is therefore retired. No new policy is needed beyond: Rust identifiers use ASCII only, and step text plus doc are English.

---

## 5. Decision 3: Anthropic upstream mocking strategy

### 5.1 Primary tool: fake-anthropic `MessageScript` (estimated coverage ~270 scenarios)

`BddCtx::with_upstream` in cc-lb-bdd-tests spawns fake-anthropic in-process and returns a `MessageScript` handle.

What `MessageScript` can model (verified at M0):
- Status code / body / response headers (rate-limit and `x-anthropic-*` header injection).
- Response delay (`with_delay(Duration)`).
- Multi-response FIFO queue (e.g. first request 200, second 429, third 200).
- Recorded request retrieval (`requests()`, `request_count()`).
- Async wait (`wait_for_requests(expected, timeout)`).
- Error response shape (`error(status, type, msg)`).

Representative `MessageScript` scenarios:
- **F1.1b** call after key issuance — response 200, verifying `body_json.principal_id` on the recorded request.
- **F3.10** rate-limit header exposure — response header injection plus Bob's call result verification.
- **F5.2** OAuth refresh repeated failure notification — response 401 × N then 200, combined with mock-anthropic-oauth-server.
- **F6.10a** per-route body limit — normal response plus recorded-body verification.
- **F11A.9** warmup target — sweep verification by recorded-request count.
- **F18.2** per-call cost calculation — forcing `usage.input_tokens` in the response body and verifying the storage row.

### 5.2 Secondary tool: fake-anthropic extension PR (M0 deliverable, narrow scope)

Only the cases `MessageScript` cannot express are added:

1. **SSE streaming response** — the current `ScriptedMessageResponse::into_response()` only emits `Json(body).into_response()`. SSE is not emitted.
   - Change: add a `body_kind: ResponseKind` field to `ScriptedMessageResponse` (enum `Json(Value)` / `Sse(Vec<SseEvent>)`).
   - Affected scenarios: streaming verification in F3 response flows (~10 scenarios), F4 dashboard SSE subscriptions (~5 scenarios).
2. **Mid-response connection drop** — close the connection after the response has begun.
   - Change: add a `ResponseKind::DropAfterBytes(Vec<u8>, usize)` variant.
   - Affected scenarios: F3.8 (cc-lb dies mid-response), F8.6 (upstream dies mid-response) (~6 scenarios).
3. **Conditional response** — branch the response by request body (the current `pop_response` is FIFO only).
   - Change: `MessageScript::push_conditional(predicate: Fn(&RecordedMessageRequest) -> Option<ScriptedMessageResponse>)`.
   - Affected scenarios: F6 ACL per-model branching (~8 scenarios), F12 plugin-chain ordering (~10 scenarios).

Existing call sites affected by the extension: `cc-lb-server/tests/pipeline_e2e.rs`, `tests/integration/managed_api_key_full_flow.rs`, and similar. Adding an enum variant is **backwards compatible** (using `#[non_exhaustive]`).

This PR is part of the M0 gate. It may be tracked under a separate plan file (`.omo/plans/fake-anthropic-bdd-extensions.md`).

### 5.3 Tertiary tools: auxiliary upstreams (wiremock + mock-anthropic-oauth-server, ~21 scenarios)

- LiteLLM price catalog (F18, F24): `wiremock::MockServer` (already verified by [managed_api_key_full_flow.rs](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/tests/integration/managed_api_key_full_flow.rs)).
- OAuth (F5, F10): mock-anthropic-oauth-server, in-process spawn.

### 5.4 Rejected alternatives
- Real API VCR record/replay — rate limit / credential / reproducibility / CI cost.
- A new `ScenarioInjector` trait — `MessageScript` already plays the same role (Momus-verified). It would only add a duplicate dependency.

---

## 6. Decision 4: DB fixtures (reuse the existing pattern + standardize the env var)

### 6.1 sqlite
- Per-test: `tempfile::TempDir` → `<dir>/bdd-<scenario_id>.sqlite`.
- Auto-cleanup via RAII on drop.

### 6.2 postgres
- Per-test schema: `CREATE SCHEMA bdd_<scenario_id>_<uuid>`.
- Pool's `search_path` pinned.
- Teardown: `DROP SCHEMA IF EXISTS <name> CASCADE`.

### 6.3 Environment-variable policy (standardized)
- **Canonical env: `CI_POSTGRES_URL`** (matching [postgres.yml:52](/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.github/workflows/postgres.yml)).
- Local short alias `DATABASE_URL` is also recognized, but code always looks up `CI_POSTGRES_URL` first and falls back to `DATABASE_URL`.
- Neither set → the postgres matrix is auto-skipped (no panic; `eprintln!` + early return).

`cc-lb-bdd-tests/src/backends/postgres.rs` lookup:
```rust
let url = std::env::var("CI_POSTGRES_URL")
    .or_else(|_| std::env::var("DATABASE_URL"))
    .ok();
let Some(url) = url else {
    eprintln!("postgres bdd matrix skip: neither CI_POSTGRES_URL nor DATABASE_URL is set");
    return Ok(());
};
```

### 6.4 Migrations
`MetaStore::initialize(&storage, BackendKind::{Sqlite|Postgres})` (same as the conformance crate). `sqlx::migrate!` runs inside.

### 6.5 Isolation verification gate
proxy-e2e-qa skill cleanup gate — running two scenarios concurrently must yield separate sqlite files / postgres schemas and zero leaks after teardown.

---

## 7. Decision 5: personas, scenario classification, OoS handling

### 7.1 Persona bootstrap helpers (English source, v3)

```rust
// crates/cc-lb-bdd-tests/src/personas.rs
impl BddCtx {
    /// Alice — operator. Admin token; full operator privileges (principals, keys, dashboards, killswitch).
    pub async fn alice(&self) -> OperatorClient { /* admin token + operator role */ }
    /// Bob — developer / plugin author. Per-principal API key; can call upstream, upload plugins.
    pub async fn bob(&self) -> DeveloperClient { /* api key + developer role */ }
    /// Charlie — SRE. Admin token; incident response, drain, multi-replica, backend parity, warmup leases.
    pub async fn charlie(&self) -> SreClient    { /* admin + sre role */ }
    /// Dana — auditor. Read-only token; audit log access, redaction verification, no mutation.
    pub async fn dana(&self) -> AuditorClient   { /* read-only + auditor role */ }
}
```

- Persona names: English (Alice / Bob / Charlie / Dana), matching the v5.2 markdown.
- Persona role doc: an English one-liner per helper.
- The `persona = Alice|Bob|Charlie|Dana` macro argument is required; missing it is a compile error.
- Panic / assert messages use the English persona name (`[F1.1a · Alice] ...`), never Korean.

### 7.2 Scenario level classification (L0 / L1 / L2)

| Level | Definition | Boot cost | Estimated count |
|---|---|---|---|
| **L0** Storage | Direct storage-trait calls, no server boot. | <50ms | ~30 |
| **L1** Server | cc-lb-server in-process + fake-anthropic in-process. | ~500ms | ~200 |
| **L2** Full-stack | cc-lb-server + fake-anthropic + admin API + dashboard read. | ~1s | ~91 |
| **Total** | | | **321** |

Initial assignment happens at M0. A column is added to the mapping table.

### 7.3 OoS-manual handling (exact ID list gate)

**v1's "expected 5–10 scenarios" wording is dropped.** M0 must produce an **exact ID list**. If the table is incomplete, M1 is blocked (this is part of the M0 gate).

OoS-manual candidates (IDs are finalized in M0):
- W1 F4.1c "clicking a team row leads to the team's detailed view" — the "the human sees" assertion. The automatable portion is converted (verifying the JSON response); only the "sees" branch is OoS-manual.
- W4 keystroke / theme / locale (frontend-fanout-qa track).

M0 deliverable `bdd-oos-manual.md` shape:
```
| id | reason | replacement |
|---|---|---|
| F4.1c (manual portion) | Human-eye verification; not assertable from the JSON response alone. | visual-qa skill, after M4. |
| F4.11b (manual portion) | Tooltip text; only the accessible name is auto-assertable. | visual-qa skill. |
```

Each row carries (a) the exact scenario ID, (b) a one-line reason for "not automatable", and (c) the replacement verification track. A missing row blocks the M1 gate.

**Mandatory invariant at M0 close**:
```
converted_count + oos_manual_count + blocked_count == 321
```
- `converted`: status is `RED→GREEN` or `STABLE` in the mapping table.
- `oos_manual`: exact ID + reason + replacement track present in the OoS-manual table.
- `blocked`: scenario-definition gaps and similar cases that need to be retried in v6 — exact ID + reason.

If the sum is below or above 321, M0 is incomplete (the gate fails).

---

## 8. New crate structure (a layout cargo can auto-discover)

### 8.1 Directory layout

```
crates/cc-lb-bdd-tests/
├── Cargo.toml          # [features] sqlite (default), postgres
├── src/
│   ├── lib.rs          # public fixtures / ctx
│   ├── macros.rs       # bdd_scenario! macro (defined locally inside cc-lb-bdd-tests)
│   ├── ctx.rs          # BddCtx (admin client + upstream handle + storage)
│   ├── personas.rs     # alice/bob/charlie/dana
│   ├── upstream.rs     # fake-anthropic spawn helper + MessageScript wrapper
│   ├── upstream_safety.rs # loopback-only HTTP wrapper (§13 R11)
│   └── backends/
│       ├── sqlite.rs   # BddBackend impl
│       └── postgres.rs # BddBackend impl (feature-gated)
└── tests/
    ├── w1.rs           # top-level integration binary 1
    ├── w1/
    │   ├── mod.rs      # pub mod f1_principal_create; ... pub mod f26_health;
    │   ├── f1_principal_create.rs
    │   ├── f2_api_key.rs
    │   ├── f3_dispatch.rs
    │   ├── f4_dashboard.rs
    │   ├── f6_quota.rs
    │   ├── f19_prompt_cache.rs
    │   └── f26_health.rs
    ├── w2.rs           # top-level binary 2
    ├── w2/
    │   ├── mod.rs
    │   ├── f5_oauth_refresh.rs
    │   ├── f7_killswitch.rs
    │   ├── f8_upstream_outage.rs
    │   ├── f10_oauth_consent.rs
    │   ├── f11a_warmup_target.rs
    │   ├── f11b_warmup_execution.rs
    │   └── f11c_warmup_observability.rs
    ├── w3.rs           # top-level binary 3
    ├── w3/
    │   ├── mod.rs
    │   ├── f9_policy.rs
    │   ├── f12_plugin_registry.rs
    │   ├── f21_observability.rs
    │   ├── f25_plugin_runtime.rs
    │   ├── f27_admin_meta.rs
    │   └── f29_chaos.rs
    ├── w4.rs           # top-level binary 4
    ├── w4/
    │   ├── mod.rs
    │   ├── f13_retention.rs
    │   ├── f14_config_apply.rs
    │   ├── f15_lifecycle.rs
    │   ├── f17_multi_replica.rs
    │   ├── f18_cost_catalog.rs
    │   ├── f20_secret_redaction.rs
    │   └── f24_pricing_provenance.rs
    └── no_real_anthropic.rs # §13 R11 build-time grep + runtime wrapper gate
```

Each `tests/w<n>.rs` (example):
```rust
mod w1;
// bdd_scenario! emits #[tokio::test] async fn entries inside the modules.
// w1.rs itself is the top-level integration test binary cargo auto-discovers.
```

Each `tests/w<n>/mod.rs`:
```rust
pub mod f1_principal_create;
pub mod f2_api_key;
// ...
```

With this layout cargo auto-discovers **four test binaries** (`w1`, `w2`, `w3`, `w4`). nextest recognizes them identically.

### 8.2 `Cargo.toml` (feature-gated optional dependency pattern)

```toml
[package]
name = "cc-lb-bdd-tests"
version.workspace = true
edition.workspace = true
publish = false

[features]
default = ["sqlite"]
sqlite = ["dep:cc-lb-storage-sqlite"]
postgres = ["dep:cc-lb-storage-postgres"]

# Kept under [dependencies] because the dev-dependencies + optional combination
# does not interact correctly with cargo feature gates.
[dependencies]
tokio = { workspace = true, features = ["full"] }
async-trait = { workspace = true }
anyhow = { workspace = true }
reqwest = { workspace = true }
tempfile = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
fake-anthropic = { path = "../../tests/fixtures/fake-anthropic" }
mock-anthropic-oauth-server = { path = "../../tests/fixtures/mock-anthropic-oauth-server" }
cc-lb-server = { path = "../cc-lb-server" }
cc-lb-storage-api = { path = "../cc-lb-storage-api" }
cc-lb-storage-sqlite = { path = "../cc-lb-storage-sqlite", optional = true }
cc-lb-storage-postgres = { path = "../cc-lb-storage-postgres", optional = true }

[dev-dependencies]
wiremock = { workspace = true }
```

Specifically, `cc-lb-storage-sqlite` / `cc-lb-storage-postgres` are wired through `[dependencies]` with `optional = true` + the `dep:` mapping under `[features]` (v1's `[dev-dependencies] optional = true` pattern was removed because it does not interact with cargo feature gates correctly).

`publish = false` removes any publish burden.

---

## 9. Local execution procedure

### 9.1 sqlite (default)

```sh
cargo nextest run -p cc-lb-bdd-tests
```

### 9.2 postgres

```sh
# Assume a local postgres is already up.
export CI_POSTGRES_URL=postgres://postgres:testpw@localhost:5432/cc_lb_test
cargo nextest run -p cc-lb-bdd-tests --no-default-features --features postgres -- --test-threads=4
```

If neither `CI_POSTGRES_URL` nor the fallback `DATABASE_URL` is set, the postgres matrix is auto-skipped.

### 9.3 Single-scenario debugging

Each scenario expands to `<fn_name>_sqlite` or `<fn_name>_postgres` (or `fast_<fn_name>_*` for fast-subset entries).

```sh
# F1.1a, sqlite only:
cargo nextest run -p cc-lb-bdd-tests -E 'test(=f1_1a_sqlite)'

# F1.1a, sqlite + postgres (exact match):
cargo nextest run -p cc-lb-bdd-tests -E 'test(/^f1_1a_(sqlite|postgres)$/)'

# Entire fast subset:
cargo nextest run -p cc-lb-bdd-tests -E 'test(/^fast_/)'
```

(v1's `test(=f1_1a) and test(=f1_1a_sqlite)` was logically impossible and has been removed.)

### 9.4 Environment variables
- `CC_LB_BDD_LOG=trace` — enable tracing.
- `CC_LB_BDD_KEEP_TEMPFILE=1` — preserve the sqlite file / postgres schema on failure.
- `CC_LB_TEST_READY_TIMEOUT_SECS` — existing variable.
- `CI_POSTGRES_URL` (or `DATABASE_URL`) — see §6.3.

---

## 10. CI execution plan

### 10.1 New workflow: `.github/workflows/bdd.yml` (PR gate)

```yaml
name: bdd

on:
  pull_request:
    paths:
      - 'crates/cc-lb-bdd-tests/**'
      - 'tests/fixtures/fake-anthropic/**'
      - 'tests/fixtures/mock-anthropic-oauth-server/**'
      - 'crates/cc-lb-server/**'
      - 'crates/cc-lb-storage-*/**'
      - '.github/workflows/bdd.yml'
  push:
    branches: [master]
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

env:
  CC_LB_ADMIN_SKIP_SPA: "1"

jobs:
  bdd-sqlite-fast:
    name: BDD sqlite (fast_ subset, PR gate)
    runs-on: oracle4-cc-lb
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
        with: { rust-cache-key: bdd-sqlite }
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features sqlite \
            -E 'test(/^fast_/)'
      - name: Upload failure captures
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-sqlite-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 14

  bdd-postgres-fast:
    name: BDD postgres (fast_ subset, PR gate)
    runs-on: oracle4-cc-lb
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: testpw
          POSTGRES_DB: cc_lb_test
        ports: ['5432:5432']
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 3s --health-retries 10
    env:
      CI_POSTGRES_URL: postgres://postgres:testpw@localhost:5432/cc_lb_test
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
        with: { rust-cache-key: bdd-postgres }
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features postgres \
            -E 'test(/^fast_/)' -- --test-threads=4
      - name: Upload failure captures
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-postgres-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 14
```

### 10.2 New workflow: `.github/workflows/bdd-nightly.yml` (full suite)

```yaml
name: bdd-nightly

on:
  schedule:
    - cron: '0 18 * * *'  # 03:00 KST
  workflow_dispatch:

jobs:
  bdd-full-sqlite:
    runs-on: oracle4-cc-lb
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features sqlite
      - name: Upload captures on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-nightly-sqlite-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 30

  bdd-full-postgres:
    runs-on: oracle4-cc-lb
    timeout-minutes: 60
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: testpw
          POSTGRES_DB: cc_lb_test
        ports: ['5432:5432']
        options: >-
          --health-cmd "pg_isready -U postgres"
          --health-interval 5s --health-timeout 3s --health-retries 10
    env:
      CI_POSTGRES_URL: postgres://postgres:testpw@localhost:5432/cc_lb_test
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: ./.github/actions/setup-rust-cache
      - uses: taiki-e/install-action@v2
        with: { tool: cargo-nextest }
      - run: |
          cargo nextest run -p cc-lb-bdd-tests \
            --no-default-features --features postgres \
            -- --test-threads=4
      - name: Upload captures on failure
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: bdd-nightly-postgres-captures
          path: crates/cc-lb-bdd-tests/tests/__captures__/
          if-no-files-found: ignore
          retention-days: 30
```

### 10.3 Fast-subset policy (clarified)
- Function-name prefix `fast_` is mandatory — auto-attached by the macro when `fast = true`.
- nextest filter `test(/^fast_/)` (compatible with Rust identifiers).
- Selection rule: per feature, 1 happy + 1 critical edge + 1 high-regression-risk scenario.
- Target PR-gate wall clock < 5 minutes.
- Selection deliverable: `/home/bhyoo/cc-lb-bdd/bdd-fast-subset.md` (M0 deliverable, matches the `fast` column of the mapping table).

### 10.4 Cadence by trigger

| Trigger | Suite | Backend | Expected runtime |
|---|---|---|---|
| PR `paths` match | `fast_` prefix | sqlite + postgres | ≤5 min |
| `push master` | `fast_` prefix | sqlite + postgres | ≤5 min |
| nightly cron | full | sqlite + postgres | ≤30 min |
| `workflow_dispatch` manual | configurable (default `fast_`) | matrix selectable | variable |

### 10.5 CI runner / resources
- `oracle4-cc-lb` self-hosted runner.
- postgres:18 service container.
- sccache enabled.

### 10.6 Secrets
- **0**. No real Anthropic API key. DB password (`POSTGRES_PASSWORD=testpw`) is scoped to the service container.

---

## 11. Milestones (M0 → M5)

At every milestone end the following invariants are enforced:
- `converted + OoS-manual + blocked == 321` (equality is mandatory).
- All changed files pass `cargo fmt --check`.
- All changed crates pass `cargo clippy -- -D warnings`.
- §13 no-real-API gate passes.
- New scenarios carry RED→GREEN evidence (§12).

### M0 — Harness scaffold + W1 F1 pilot + OoS table complete (1 week)

**Deliverables**:
- `crates/cc-lb-bdd-tests/` crate created (per §8 layout).
- `BddCtx`, `BddBackend` trait, `bdd_scenario!` macro.
- `personas::{alice,bob,charlie,dana}`.
- fake-anthropic extension PR (§5.2): SSE + drop_after_bytes + push_conditional (may live under a separate plan).
- W1 F1 — 10 scenarios converted.
- Scenario mapping table v1 (`/home/bhyoo/cc-lb-bdd/bdd-test-conversion-map.md` — 321 rows).
- Exact OoS-manual ID list (`/home/bhyoo/cc-lb-bdd/bdd-oos-manual.md`).
- Fast-subset selection v1 (`/home/bhyoo/cc-lb-bdd/bdd-fast-subset.md`).
- §13 no-real-API gate (loopback wrapper + grep test).

**Verification gate (binary)**:
- [ ] `cargo nextest run -p cc-lb-bdd-tests` sqlite passes (10 F1 scenarios).
- [ ] `CI_POSTGRES_URL=... cargo nextest run -p cc-lb-bdd-tests --no-default-features --features postgres -- --test-threads=4` passes.
- [ ] `cargo clippy -p cc-lb-bdd-tests --no-default-features --features sqlite -- -D warnings` 0 warnings.
- [ ] `cargo clippy -p cc-lb-bdd-tests --no-default-features --features postgres -- -D warnings` 0 warnings.
- [ ] Mapping table invariant: converted=10, OoS-manual=finalized, blocked=finalized, sum=321 (everything outside W1 F1 is marked `blocked-pending-conversion`).
- [ ] Every OoS-manual row carries (id, reason, replacement).
- [ ] no_real_anthropic test passes.
- [ ] One intentional failure injection (deliberately break a F1 assertion) shows scenario ID + persona + step + observable in the output.

### M1 — W1 remaining 88 scenarios + CI sqlite gate (1 week)

**Deliverables**:
- W1 F2/F3/F4/F6/F19/F26 converted (88 scenarios).
- `.github/workflows/bdd.yml` sqlite job enabled.
- W1 rows filled in the mapping table (98 rows).

**Verification gate**:
- [ ] One pilot PR → bdd.yml sqlite job passes.
- [ ] PR-gate wall clock ≤ 5 minutes.
- [ ] Invariant: converted ≥ 98 (all of W1) + OoS-manual + blocked == 321.

### M2 — W2 66 scenarios + CI postgres gate (1.5 weeks)

**Deliverables**:
- W2 F5/F7/F8/F10/F11A/F11B/F11C converted (66 scenarios).
- OAuth scenarios (F5/F10) verified against mock-anthropic-oauth-server.
- `.github/workflows/bdd.yml` postgres job enabled.
- W2 rows filled in the mapping table.

**Verification gate**:
- [ ] postgres job wall clock ≤ 7 minutes.
- [ ] OAuth refresh scenarios use `tokio::time::pause()` + `advance()` (no wall-clock dependence).
- [ ] Invariant: converted ≥ 164 + OoS-manual + blocked == 321.

### M3 — W3 63 scenarios + use of the fake-anthropic extension (1.5 weeks)

**Deliverables**:
- W3 F9/F12/F21/F25/F27/F29 converted (63 scenarios).
- F29 (chaos): use `ScriptedMessageResponse::error` + the fake-anthropic extension (drop_after_bytes / push_conditional).
- Reuse the `plugin-handshake-spike` wasm asset (already built by ci.yml).

**Verification gate**:
- [ ] F29 chaos scenarios assert cc-lb recovery in < 5 seconds.
- [ ] The same scenario, run 100 times, has flake rate ≤ 1% (no automatic retry).
- [ ] Invariant: converted ≥ 227 + OoS-manual + blocked == 321.

### M4 — W4 94 scenarios + nightly full suite (2 weeks)

**Deliverables**:
- W4 F13/F14/F15/F17/F18/F20/F24 converted (94 scenarios).
- F17 (multi-replica) spins up 2 sqlite instances concurrently.
- `.github/workflows/bdd-nightly.yml` enabled.

**Verification gate**:
- [ ] One week of uninterrupted nightly green.
- [ ] Nightly wall clock < 30 minutes.
- [ ] Invariant: converted == 321 - OoS-manual - blocked (W4 fully converted).

### M5 — Wrap-up, tuning, stabilization (1 week)

**Deliverables**:
- nextest sharding tuned.
- Persona-fixture cache verified.
- Failure-diagnostic message format standardized.
- `fast_` subset re-selected (PR wall clock stays ≤ 5 minutes).
- Add a "BDD function name" column to [cc-lb-bdd-inventory-coverage-v5.2.md](/home/bhyoo/cc-lb-bdd/cc-lb-bdd-inventory-coverage-v5.2.md).

**Verification gate**:
- [ ] PR-gate wall clock ≤ 5 minutes (three consecutive runs).
- [ ] **Every converted scenario passes sqlite + postgres for 7 consecutive nightly runs.**
- [ ] Final mapping-table invariant: converted + OoS-manual + blocked == 321.
- [ ] 1326-inventory mapping table's BDD-function-name column is 100% filled in (for converted scenarios).

---

## 12. Verification gates (common across milestones, Proxy-E2E-QA skill basis)

Every scenario conversion must capture the following two pieces of evidence before being "done":

| Evidence | Source | Format | Storage |
|---|---|---|---|
| RED→GREEN evidence | `cargo nextest run -E 'test(=<fn>)'` output before fix + after fix | nextest output text | PR description or PR comment body (not in commit messages) |
| Surface artifact | L0 = storage row dump, L1 = admin API response + recorded-request log, L2 = dashboard JSON response | JSON file `tests/__captures__/<scenario_id>.json` | **gitignored** (no commit). Uploaded via `actions/upload-artifact` only on CI failure (14-day / 30-day retention) |

Rules:
- If either piece of evidence is missing, the scenario is not done.
- Mapping-table status column values: `RED→GREEN` (first pass), `STABLE` (1 nightly week with zero flake), `OoS-manual`, `blocked-<reason>`.
- The captures directory is added to `crates/cc-lb-bdd-tests/.gitignore`.
- CI workflows upload the captures directory as an artifact under `if: failure()`.

### 12.1 Step-text jargon-rewrite gate (v3, semantically inverted)

Under the English-source decision, v2's "swap English infrastructure words for Korean domain vocabulary" gate is dropped. v3 inverts the meaning: the gate now **detects Rust implementation jargon that has leaked into step text**.

- **Allowed vocabulary (domain language, used as-is)**: `upstream`, `warmup`, `replica`, `callback`, `readyz`, `drain`, `sweep`, `lease`, `token`, `quota`, `principal`, `audit`, `signer`, `dialect`, `plugin`, `chain`, `terminal`, `filter`, `shape`, `observability`.
- **Forbidden vocabulary (Rust implementation jargon, banned from lint-target text)**:
  - Types: `Vec`, `HashMap`, `BTreeMap`, `HashSet`, `BTreeSet`, `Arc`, `Rc`, `Mutex`, `RwLock`, `Option`, `Result`, `Box<dyn>`, `dyn Trait`, `Cow`, `String`, `&str`, `Cell`, `RefCell`, `OnceCell`, `OnceLock`, `Lazy`, `OnceLazy`
  - async / runtime: `tokio::spawn`, `tokio::time`, `tokio::sync`, `tokio::task`, `tokio::runtime`, `async fn`, `await`, `Pin<Box>`, `Future`, `Stream`, `JoinHandle`, `JoinSet`, `select!`, `join!`, `mpsc`, `oneshot`, `broadcast`, `Semaphore`, `Notify`, `Runtime`, `block_on`, `Duration`, `Instant`, `sleep`, `timeout(`, `task::yield_now`, `spawn_blocking`
  - DB / SQL: `sqlx::`, `sqlx::query`, `sqlx::query_as`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `WHERE`, `transaction`, `pool`, `connection`, `Transaction`, `PgPool`, `SqlitePool`, `Migrator`, `migrate!`
  - HTTP / wire: `axum::`, `tower::`, `reqwest::`, `hyper::`, `http::`, `StatusCode::`, `StatusCode::OK`, `HeaderMap`, `HeaderValue`, `Body`, `Bytes`, `Json(`, `Response::`, `Request::`, `Method::`, `Uri`
  - serde / JSON: `serde::`, `serde_json::`, `serde_json::Value`, `serde_yaml::`, `Deserialize`, `Serialize`, `json!`
  - UUID / IDs: `Uuid`, `uuid::`, `Uuid::new_v4`, `Uuid::parse_str`, `UlidGenerator`
  - Error handling: `unwrap()`, `expect(`, `unwrap_or`, `unwrap_err`, `panic!`, `anyhow::`, `thiserror::`, ` ?`, ` ?;` (Rust's `?` operator), `bail!`, `ensure!`, `Err(`
  - Module signatures: `pub fn`, `pub async fn`, `pub struct`, `pub enum`, `impl ` (followed by a trait name), `where ` clauses, lifetime parameters (`'a`, `'static` in prose).
- **Lint-target text** (grep-extractable, guaranteed by the macro signature):
  - The `title = "<string literal>"` argument to `bdd_scenario!`.
  - The `description = "<string literal>"` argument to `bdd_scenario!` — added in M0, **required argument**, used for the natural-language Given/When/Then narrative. Missing it is a compile error.
  - The doc comments (`///` lines) on the fn the macro emits.
  - The English step-text portion of panic / assert messages (for example in `"[F1.1a · Alice] active flag missing. expected=true, actual=false"` the `"active flag missing"` portion — only literal text, not formatter arguments).
- **Lint non-targets** (intentional Rust usage, not scanned):
  - The macro's `given = |...| { ... }` and similar closure bodies — Rust implementation code, every kind of jargon is allowed.
  - The macro's identifier arguments such as `fn_name = f1_1a`.
  - `Cargo.toml` dependencies.
  - Capture data under `tests/__captures__/*.json`.
- **Detection gate**: the `cc-lb-bdd-tests` build has a lint script (`scripts/bdd-jargon-lint.sh`) that extracts the four lint-target texts (title argument + description argument + generated-fn doc comment + panic literal) and runs a regex grep for forbidden tokens; any hit fails the build. Extraction is implemented (b) as a named-argument parser over the `bdd_scenario!\(` macro invocation in source (using the `syn` Rust syntax tree crate), chosen over (a) `cargo expand` output parsing in order to avoid the CI dependency on `cargo expand`.
- Policy-violation examples:
  - ❌ "Given the upstream pool has 3 connections" (`pool`, `connection` are implementation jargon).
  - ✅ "Given there are 3 active upstreams".
  - ❌ "When Alice spawns a tokio task to drain" (`tokio task` is implementation jargon).
  - ✅ "When Alice initiates a drain".
- Persona and domain vocabulary are all allowed (Alice/Bob/Charlie/Dana, upstream/warmup/...).

---

## 13. Risks + responses

| # | Risk | Detection signal | Response (concrete) |
|---|---|---|---|
| R1 | MessageScript expressiveness gap | A scenario cannot be expressed during the M2–M3 conversion. | Expand the §5.2 extension PR's enum variants (separate plan). |
| R2 | Postgres CI concurrency too low | postgres job > 10 min. | Bump `--test-threads` 4 → 8, or run the full postgres suite nightly only. |
| R3 | Rust implementation jargon (Vec/HashMap/tokio/sqlx/JoinHandle/select!/etc. — the §12.1 forbidden list) leaks into step text. | The build-time lint (`scripts/bdd-jargon-lint.sh`) extracts the four §12.1 lint-target texts (title literal + description literal + generated-fn doc comment + panic literal), accepts only domain vocabulary, and fails the build on any implementation-jargon hit. | (a) Macro signature requires a `description = "..."` string literal argument (§3.5) — explicit string for the step narrative (not a closure body), so it is grep-able. (b) Lint implementation uses the `syn` crate to extract named-argument literals from `bdd_scenario!\(...\)` macro invocations (avoiding the `cargo expand` dependency). (c) PR-review checklist row "step text uses domain language only" + (d) §12.1 spells out lint non-targets (closure bodies / Cargo.toml / captures). |
| R4 | Persona-permission pre-grant missing | F1/F2 scenarios return 403. | Guarantee the `personas::*` helper bootstraps the role. The M0 gate emits a permission-matrix verification table. |
| R5 | Anthropic SSE format change | A cluster of nightly failures. | Add a contract test in the fake-anthropic SSE module (`ScriptedMessageResponse::Sse` verification, M0-companion). |
| R6 | Scenario intent unclear | "given is ambiguous" emerges during M2–M4. | Classify in the mapping table as `blocked-ambiguous` and put into the v6 regression queue. |
| R7 | CI resources (oracle4-cc-lb) saturated | nightly exceeds 30 min on 3 occasions. | Strengthen sccache + split the nextest archive build (`cargo nextest archive` → `run --archive-file`). |
| R8 | OoS scenario miscategorized | A non-automatable scenario is attempted as automated. | M0 OoS-manual table row is mandatory; the M1 gate checks table completeness. |
| R9 | Persona-fixture dependency explosion | Setup time ≥ 2 seconds per scenario at M3. | Fixture cache (`once_cell::sync::OnceLock` + `Arc<Bootstrap>`). |
| R10 | Flake (time-dependent scenarios) | M3 chaos / M2 OAuth refresh fails intermittently. | Mandatory: use `tokio::time::pause()` + `advance()` for any time-progression need (no automatic retry). On flake, immediately demote the mapping-table status from `STABLE` back to `RED→GREEN` and re-investigate. |
| **R11** | **Real Anthropic API call leakage** | The literal `api.anthropic.com` or a non-loopback host appears in tests / fixtures. | (a) `tests/no_real_anthropic.rs` runs `grep -r 'api\.anthropic\.com' tests/` at build time and panics on any hit. (b) `upstream_safety::LoopbackOnlyClient` is a reqwest middleware that blocks non-loopback hosts and panics on violation. (c) CI workflow env never receives an Anthropic key secret (`rg secrets\.ANTHROPIC` in quality.yml). |

---

## 14. Deliverable locations

| Deliverable | Location | Created | Retention |
|---|---|---|---|
| This plan (canonical) | `/home/bhyoo/.local/share/opencode/worktree/8b031e50bfdcbbbec2b64be6945414766d5e93f2/crisp-wolf/.omo/plans/bdd-test-conversion.md` | Current turn | Permanent |
| This plan (web-visible copy) | `/home/bhyoo/cc-lb-bdd/cc-lb-bdd-test-conversion-plan.md` | Current turn | Permanent |
| Mapping table (321 rows) | `/home/bhyoo/cc-lb-bdd/bdd-test-conversion-map.md` | M0 deliverable, refreshed at every milestone | Permanent |
| OoS-manual table | `/home/bhyoo/cc-lb-bdd/bdd-oos-manual.md` | M0 deliverable (M1 entry gate) | Permanent |
| Fast-subset selection | `/home/bhyoo/cc-lb-bdd/bdd-fast-subset.md` | M0 deliverable, re-selected at M5 | Permanent |
| Crate code | `crates/cc-lb-bdd-tests/` | M0–M4 | Permanent |
| New workflows | `.github/workflows/bdd.yml`, `bdd-nightly.yml` | M1, M4 | Permanent |
| fake-anthropic extension plan | `.omo/plans/fake-anthropic-bdd-extensions.md` | M0 companion | Permanent |
| RED→GREEN evidence | PR description / PR comment body (not commit messages) | Per scenario | PR history |
| Surface artifact (captures) | `crates/cc-lb-bdd-tests/tests/__captures__/<scenario_id>.json` | At scenario execution | **gitignored**. CI failure → artifact upload (PR 14 days, nightly 30 days) |
| 1326-inventory mapping update | `/home/bhyoo/cc-lb-bdd/cc-lb-bdd-inventory-coverage-v5.2.md` (add the "BDD function name" column) | M5 | Permanent |

---

## 15. Out of scope (re-statement)

- The v6 round's NC (82) and PARTIAL (95) backfill.
- 1326-inventory corrections (definition gaps discovered during conversion → categorize as `blocked-pending-v6`).
- visual-qa / Playwright visual regression (frontend-fanout-qa).
- Security penetration testing (security-research).
- Real Anthropic API regression (entirely different track).
- Soak / load (soak.yml).

---

## 16. Acceptance gate for this plan

Code round M0 may start once all four of the following are satisfied:

1. The user has reviewed this document and explicitly approved it ("OK / start").
2. **Momus reviewer has returned APPROVED-WITH-EDITS or APPROVED, confirming that every blocking gap has been resolved** (v1's "unconditional approval" wording was incorrect and is replaced by the gap-resolution criterion).
3. `.omo/plans/bdd-test-conversion.md` and `/home/bhyoo/cc-lb-bdd/cc-lb-bdd-test-conversion-plan.md` are identical (synced this turn).
4. The sidebar / INDEX link to this plan (refreshed this turn).

Satisfying all four → publish a separate M0 work plan → start the code round.
