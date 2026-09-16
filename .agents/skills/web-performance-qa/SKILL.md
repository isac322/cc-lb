---
name: web-performance-qa
description: Execute and measure every cell of the source-reconciled cc-lb Admin Web QA inventory across runtime entities, variants, pages, poll cycles, risk-isolated mutations, browser/direct requests, server handlers, SQL, pools, and explicit cache states.
---

# cc-lb Admin Web exhaustive execution and performance QA

Execute the inventory produced by `web-qa-inventory-extractor`. This skill owns **runtime denominator resolution, complete matrix execution, measurement, risk isolation, evidence correlation, and PASS/FAIL/BLOCKED accounting**. It does not replace the extractor's independent current-source audit and must not infer source completeness from a successful run.

Take scope and authorization from the current user request and the active approval guards. `docs/admin-web-qa-completion-plan-2026-09-15.md` is a worked historical plan, not standing authorization for a future run. Preserve the current run's full population rather than replacing it with a representative sample.

## 1. Entry gate

Do not freeze an execution matrix until the inventory records:

- a current source commit;
- stable parent and atomic request IDs;
- zero or explicitly unresolved source reconciliation sets;
- iteration sources, variants, pagination rules, poll requirements, and risk tiers;
- handler and storage/cache/no-query mappings.

Unresolved source gaps remain visible blockers. Running known rows does not make the inventory exhaustive.

## 2. Resolve dynamic denominators at a time anchor

Immediately before each run, query the audited iteration sources and record the actual in-scope IDs for every Principal, Upstream, Plugin, row collection, and other entity dimension. Traverse cursor pagination to the wire terminal condition; a fixed page cap is not a terminal condition.

For every denominator capture, record:

```yaml
run_id: <stable run ID>
environment: production_read | isolated_fixture
source_commit: <audited source commit>
deployed_image: <observed image or explicit unknown blocker>
retrieved_at_utc: <timestamp>
observation_started_monotonic: <value when available>
request: <method and redacted URL/query>
filters: <exact tuple>
initial_cursor: <value or null>
terminal_cursor: <value or null>
entity_ids: []
count: <integer>
data_change_observed: <description or none>
```

For continuously changing logs, history, audit data, or other append-only collections, bound the run with an explicit UTC time window and/or initial/terminal cursor. Record arrivals, removals, and updates observed during traversal. Never claim completion over unbounded future data. If the bounded set cannot be stabilized or reconciled, retain the expected cells as `BLOCKED`.

## 3. Expand the entire execution matrix

For each executable inventory item, expand:

$$
\text{interaction} \times \text{entity} \times \text{state/filter variant} \times \text{page} \times \text{poll cycle}
$$

A non-applicable dimension has cardinality 1 with a recorded reason. Keep every distinct entity, variant, cursor page, and required poll stream/cycle. Do not use one entity, one successful API response, one screenshot, one filter, or one page as a proxy for the rest.

Freeze expected execution keys before running. If the runtime denominator changes, either update the anchored denominator and regenerate affected keys or block those keys with the observed change; never silently drop them.

## 4. Risk and environment rules

Classify by actual source-audited behavior, not HTTP verb:

- `read`: authorized, bounded, low-concurrency observation with no state change;
- `reversible_write`: isolated fixture unless a separately approved target and rollback exist;
- `destructive_write`: isolated disposable fixture only;
- `unsafe_get`: a GET with refresh, write, upstream call, cost, rotation, or other side effect; execute under the corresponding write risk tier.

Never mutate shared production to complete a matrix. Separate production-read and isolated-fixture evidence in the ledger.

For a write comparison, do not click once and directly replay the same mutation against the already-changed state. Create two independent executions from the same known starting state:

1. snapshot or deterministically construct the fixture state;
2. run the browser-triggered mutation and capture the resulting transition;
3. restore or recreate the exact starting state and verify restoration;
4. run the direct request independently;
5. verify its transition and restore again;
6. record restoration evidence and confirm no fixture listener/process remains.

If equivalent restoration is impossible, direct replay is `not_applicable` or `BLOCKED` with a reason; it is never a copied timing.

## 5. Independent browser and direct-request measurement

For every matrix cell, perform the exact mount, click, input, blur, submit, scroll, confirmation, refresh, or poll trigger. One parent action that produces multiple requests creates separate child records.

The browser request and direct request must have different request IDs and independently observed timestamps. The direct request must reproduce the same atomic method, resolved URL, query, body semantics, relevant headers, auth scope, and anchored data conditions. It isolates transport/server work; it does not replace browser execution.

Record at least:

- UI action start;
- browser request start, TTFB, response end, transfer and decoded size when available;
- response status, row/turn cardinality, and semantic response hash;
- state-update completion;
- render completion, defined by the expected UI state plus a stable next animation frame;
- direct-request start, TTFB, completion, payload size, status, and semantic hash;
- UTC and monotonic timestamps for every independently measured interval.

Never copy browser timing into direct timing, derive one from the other, fill missing values with zero, or reuse another cell's measurement. Browser and direct responses must be compared for the same anchored semantics; expected differences must be documented.

## 6. Correlate the same atomic request through server, SQL, and pool

Use one execution key plus request/correlation IDs and a narrow timestamp window to connect:

```text
UI action -> browser request -> server handler -> storage/cache path
          -> SQL statement(s) -> pool acquisition/wait -> response -> render
```

For the **same atomic request**, record handler duration, each SQL count/duration, transaction boundaries where relevant, and connection-pool acquisition/wait. A standalone `EXPLAIN`, an unrelated log sample, aggregate database metrics, or a nearby request cannot fill per-request SQL/pool fields.

Observed correlation constraints for this codebase:

- The correlation chain is same-origin `Server-Timing` `rid` description → the UI `ResourceTiming` entry → the backend `request_id`. A RID is an identifier, not a duration; a cached or reused RID observed again is not a new server execution.
- `ResourceTiming` carries no method or body. Never infer the HTTP method or request payload from a ResourceTiming entry; take them from the recorded dispatch or the server-side log.
- A SQLx `elapsed` value covers the stream/driver lifetime of that query, not just execution. Keep total pool acquisition time and pure queue wait as separate fields; do not report one as the other.
- A hub log preview is not the complete log. Before observing bulk PostgreSQL or other high-volume logs, secure the complete private log file; a truncated preview cannot evidence per-request SQL/pool fields.
- In an authorized isolated run, test contextual filtering such as `RUST_LOG=info,[admin.request]=debug` before enabling broad SQL debug output. Confirm that each target request retains its query/acquire events, unrelated SQL debug is excluded, and normal INFO remains visible. This directive enables all DEBUG events inside the Admin span, not SQL alone; it is not an endpoint-wide privacy guarantee or authorization to change production logging. Preserve the comparison window and do not extrapolate startup log counts into production volume or latency.
- One native click that produces two GETs is two HTTP observations. Never collapse them into a single request record.

If existing telemetry cannot expose a required value, record `null` plus the exact missing instrumentation/access blocker. Do not infer SQL latency by subtracting browser timings. For source-proven cache/no-query paths, record `not_applicable` with the named cache/object and evidence.

## 7. Name and control cache states precisely

Never call the first request `cold` without controlling and observing what is cold. Label each relevant layer separately:

- browser HTTP cache and service worker state;
- application data/snapshot cache;
- request coalescing or single-flight state;
- database plan cache where relevant;
- database buffer cache;
- OS page cache;
- external/upstream cache.

Production observation may be labeled only with what was actually observed, such as `first_observed_request` or `application_cache_hit`; it is not a controlled cold-cache experiment.

Controlled cold/warm comparison belongs in an isolated environment. Keep build, database engine, dataset, concurrency, query, and browser conditions equal; document which layers were reset, how reset was verified, which layers could not be controlled, and repeat each condition independently. Never flush or restart production cache/database state for this QA.

## 8. Ledger every expected cell

Write exactly one row per frozen execution key:

```yaml
- execution_key: <interaction|entity|variant|page-or-cursor|poll-cycle>
  interaction_id: <parent or atomic ID>
  environment: production_read | isolated_fixture
  entity_type: principal | upstream | plugin | row | none
  entity_id: <actual ID or none>
  variant: <exact state/filter tuple>
  page_or_cursor: <value>
  poll_stream: <name or none>
  poll_cycle: <integer>
  browser_request_id: <ID or null>
  direct_request_id: <different ID or null>
  status: PASS | FAIL | BLOCKED
  ui_action_ms: <number or null>
  browser_ttfb_ms: <number or null>
  browser_download_ms: <number or null>
  state_update_ms: <number or null>
  render_complete_ms: <number or null>
  direct_ttfb_ms: <number or null>
  direct_total_ms: <number or null>
  handler_ms: <number or null>
  sql_count: <integer, not_applicable, or null>
  sql_ms: <number, not_applicable, or null>
  pool_wait_ms: <number, not_applicable, or null>
  cache_state: <qualified per-layer state>
  payload_bytes: <integer or null>
  response_semantic_hash: <hash or null>
  restoration_evidence: <write-cell evidence or not_applicable>
  evidence: <redacted request/UI/telemetry references or blocker>
```

Use `PASS` only when the exact cell ran and every required UI, response, correlation, risk, and restoration condition agrees. Use `FAIL` when it ran and a required condition failed. Use `BLOCKED` when it could not run or required evidence is unavailable; keep the cell in the denominator.

## 9. Completion and false-PASS gate

Reconcile exact sets after execution:

$$
\text{expected execution keys} = \text{unique ledger keys}
$$

$$
\text{unique ledger keys} = \text{PASS} + \text{FAIL} + \text{BLOCKED}
$$

Completion is forbidden by any unknown denominator, missing or duplicate key, unverified terminal cursor, unbounded time range, representative sampling, copied timing, missing required same-request correlation, ambiguous cache label, unsafe action in the wrong environment, or unverified mutation restoration.

A full QA PASS additionally requires `FAIL = 0`, `BLOCKED = 0`, and zero required evidence gaps. Otherwise report the run as failed, blocked, or `pending_full_reconciliation`; do not use `100%`, `zero omission`, or `exhaustive`.

Keep the functional-oracle pass and the full-measurement pass separate: a cell that proves the correct behavior happened is not evidence that its timing/correlation fields were measured. Applying anything to production requires separate approval and is never implied by a QA pass. While new UI↔DB or collector verification is still in progress, do not describe the run as an overall PASS.

## Execution report

Report:

1. source inventory version and unresolved static gaps;
2. runtime denominator snapshots, IDs, cursors, time anchors, and matrix size;
3. PASS/FAIL/BLOCKED counts and exact missing/duplicate-key reconciliation;
4. per-cell browser/direct/server/SQL/pool/cache measurements;
5. production-read versus isolated-fixture coverage;
6. mutation restoration results;
7. bottlenecks separated into UI scheduling/render, transfer, server, SQL, pool, cache, and external work;
8. missing instrumentation/access and the precise approval or capability needed.

Only report measurements actually observed in this run.