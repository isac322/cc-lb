---
name: web-qa-inventory-extractor
description: Extract and independently reconcile the complete cc-lb Admin Web UI, API, handler, storage, SQL, cache, and side-effect inventory from current source. Use before execution; this skill does not run browser cells or claim runtime PASS.
---

# cc-lb Admin Web source inventory extractor

Build the source-audited input to Admin Web QA. This skill owns **static extraction and bidirectional reconciliation**. It does not own browser execution, performance measurements, runtime entity counts, or PASS/FAIL claims; those belong to `web-performance-qa`. Feature-level state-transition scenarios remain in `user-flow-qa`, frontend implementation QA remains in `frontend-fanout-qa`, and proxy-path behavior remains in `proxy-e2e-qa`.

Take scope, environments, and authorization from the current user request and the active approval guards. `docs/admin-web-qa-completion-plan-2026-09-15.md` is a worked historical plan, not standing authorization for a future run. Do not silently narrow the current requested scope.

## Completion boundary

Source extraction is complete only when all independently derived sets reconcile:

- every current frontend interaction maps to one parent inventory item;
- every request branch produced by an interaction maps to a separate atomic request item;
- every registered Admin API method/path maps to exactly one backend item;
- every inventory item maps back to current source evidence;
- missing, extra, duplicate, stale-method, stale-path, and broken-source-reference sets are all empty.

A generated row count, an old catalog, a route mount, a representative entity, or a successful generator run is not evidence that current-source extraction is complete.

## 1. Freeze the source audit boundary

Record the source commit and the plan-defined UI/backend directories before extracting. A recorded `source_commit` may name the base commit a candidate was built on; it does not by itself prove which source produced the audited tree. Preserve the catalog's `source_state` object (`kind`, `base_commit`, `note`) and the actual file hashes or provenance manifest. For an approved uncommitted snapshot, the current catalog uses `kind: approved_uncommitted_candidate`. Keep candidate and deployed evidence distinguishable, and treat source as authoritative when it disagrees with an existing inventory.

Use two independent passes. Do not derive one pass from the output of the other:

1. **Frontend denominator:** routes, shared navigation/auth, pages, components, rendered branches, mounts, buttons, links, tabs, drawers, dialogs, forms, inputs, blur/submit behavior, filters, sorting, pagination, refresh, debounce, polling, SSE lifecycle, conditional actions, and empty/loading/error states.
2. **Backend denominator:** every registered Admin API method/path, handler, authorization operation, parameters, side effects, storage trait call, SQLite implementation, PostgreSQL implementation, direct SQL, cache/no-query path, stream, and external call.

Frontend actions without requests are `client_only` with a source-backed reason. Backend routes unavailable through the Admin Web are `backend_only`; they remain in the backend denominator.

## 2. Record parent actions and atomic requests

A user-visible action is a parent item. Each HTTP request it can cause is a distinct atomic child. Never combine methods or endpoints into one row, even when one click triggers several requests.

Each UI item must record:

```yaml
source_id: <file#symbol:trigger>
page: <route or shared surface>
component: <component>
source: { file: <path>, symbol: <symbol>, line: <line> }
name: <action name>
trigger_steps: []
preconditions: []
variants: []
scope: once | each_principal | each_upstream | each_plugin | each_row | each_filter_combination
iteration_source: <wire response field, cursor collection, or not_applicable>
risk_tier: read | reversible_write | destructive_write
expected_ui: <observable result>
requests: []
polling: <interval/lifecycle or not_applicable>
client_only_reason: <reason or null>
```

Each atomic request must include method, exact wire path, query fields, body fields, client symbol, source location, and side-effect notes. Record path variables and optional parameters as the client actually serializes them. Arbitrary numeric or text inputs use validation-equivalence classes; an infinite input domain is not enumerated, but one sample must never be reported as entity-population coverage.

## 3. Trace each backend item to the real data path

For every registered route, follow:

```text
method + path -> handler -> authorization -> storage trait/direct service
              -> SQLite implementation and SQL
              -> PostgreSQL implementation and SQL
```

Record both storage implementations when both exist. If a handler uses memory, a cache, streaming state, a direct connection, or no query, record the exact source evidence instead of inventing a SQL mapping.

Do not infer safety from HTTP method. A `GET` that refreshes state, invokes an upstream, writes observations, rotates data, performs billable work, or otherwise changes external/internal state must be classified by its actual side effect and flagged `unsafe_get` in `side_effect_notes`.

For cache-backed paths, name the actual cache layer and object from source, such as application snapshot cache, request coalescing/single-flight, HTTP browser cache, database buffer cache, or OS page cache. Do not use an unqualified `cache` label.

## 4. Declare dynamic dimensions without fabricating runtime counts

Static extraction identifies **where** runtime dimensions come from; it does not guess their values. For every entity, pagination, history, or poll dimension, record:

- the exact API response field or cursor that supplies the iteration set;
- inclusion/exclusion rules from the current plan and source;
- variant/filter dimensions and non-applicable cardinality-1 reasons;
- terminal pagination evidence required by the wire contract, normally `next_cursor == null`;
- a time-anchor strategy for continuously changing data: retrieval UTC time, monotonic observation window where available, query bounds, initial/final cursor, and change handling;
- required poll streams and cycles.

Do not encode a page-count cap as terminal pagination. Do not treat an old runtime count as a current denominator. The executor resolves actual IDs, counts, pages, and anchors immediately before a run.

## 5. Keep the generator subordinate to source evidence

`scripts/generate-qa-inventory.mjs` is a renderer/validator for audited inputs. It is not a source-discovery mechanism.

Before accepting generated YAML or Markdown:

1. prove every emitted stable ID came from current source evidence;
2. compare the generator's input IDs with the independent frontend/backend denominator sets;
3. fail reconciliation for missing, extra, duplicate, or stale items;
4. keep YAML and Markdown IDs and atomic request mappings in lockstep.

A generator `--check` exit or byte-identical output is not sufficient acceptance. Parse the generated YAML with a standard YAML parser and verify semantics: no empty mapping keys, no empty objects where a populated record is required, and every emitted field carrying its intended meaning.

If the generator merely reproduces a hard-coded historical catalog, a successful exit or unchanged row count proves only reproducibility of that catalog. Status remains `pending_full_reconciliation` until the independent current-source set comparison is empty in both directions.

## 6. Handoff contract to execution

Provide `web-performance-qa` with:

- source commit and extraction timestamp;
- the reconciled parent and atomic-item inventories;
- frontend and backend source denominators;
- zero/non-zero reconciliation sets;
- iteration sources, filter/state dimensions, pagination termination rules, time-anchor rules, polling requirements, and risk tiers;
- handler/storage/SQLite/PostgreSQL/cache/no-query mappings;
- unresolved gaps and the exact evidence needed to close them.

Do not create browser observations, latency numbers, runtime entity IDs, or PASS rows during source extraction. If any source set is unresolved, say so; do not reduce the denominator or relabel a partial inventory as exhaustive.

## Static completion report

Report:

1. audited source commit and directories;
2. frontend parent count and atomic-request count;
3. backend method/path count;
4. source-to-inventory and inventory-to-source missing/extra/duplicate sets;
5. SQL/cache/no-query and side-effect mapping gaps;
6. generator input/output reconciliation result;
7. unresolved blockers.

Only this static contract may be called complete. Runtime coverage and performance remain unverified until `web-performance-qa` executes the reconciled inventory.