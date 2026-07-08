# response-transform-hooks - Work Plan

## TL;DR (For humans)

**What you'll get:** A documented and implemented plugin interface for rewriting upstream responses before they reach clients, covering both normal JSON responses and streaming SSE responses. The immediate proof case is safely mapping upstream PascalCase tool names back to lower-case tool names.

**Why this approach:** Response rewriting is split by actual upstream framing so the host keeps ownership of headers, compression, SSE framing, backpressure, and billing truth while plugins only rewrite decoded semantic payloads.

**What it will NOT do:** It will not change the existing request-shaping ABI, expose raw compressed bytes to plugins, or let plugin-mutated output become the billing/quota source of truth.

**Effort:** Large
**Risk:** High - this adds a public plugin ABI plus hot-path response/SSE behavior.
**Decisions to sanity-check:** Two new hooks, complete-SSE-event transform instead of raw stream transform, decoded plugin input, pre-transform accounting, and PR approval before creation.

Your next move: none; the user already approved writing the docs, committing them, then starting implementation. Full execution detail follows below.

---

> TL;DR (machine): Large/high-risk ABI + engine implementation; deliver ADR, reviewed plan, docs commit, Rust hooks, engine JSON/SSE transforms, tests, proxy QA, and approved PR flow.

## Scope

### Must have

- Add a publish-grade plugin response transform interface without changing existing filter/shape/observe schemas.
- Add a buffered response hook exported as `cc_lb_transform_response`.
- Add an SSE event hook exported as `cc_lb_transform_sse_event`.
- Dispatch by actual upstream response framing/content type, not the client request `stream` flag.
- Give plugins decoded semantic payloads only; host owns compressed payload decode and output header/encoding normalization.
- Keep usage, billing, quota, and cache accounting on pre-transform upstream truth.
- Pass trimmed stable response-hook context and do not retain/pass original request bodies by default.
- Preserve byte-equivalent passthrough when no transform slot is configured or when a plugin returns `Unchanged`.
- Cover JSON tool-use reverse mapping and SSE `content_block_start` reverse mapping in tests.
- Run proxy-path QA through the client-visible `/v1/messages` surface for buffered and streaming modes.
- After implementation, prepare a PR title/body and ask for explicit final approval before creating the PR.

### Must NOT have (guardrails, anti-slop, scope boundaries)

- Must not rename, extend, or field-modify `ShapeRequest` or `ShapeResponse`.
- Must not implement a raw TCP chunk transform or whole-stream-buffering API.
- Must not expose compressed upstream bytes to plugins.
- Must not let plugin output spoof hop-by-hop, signing, `x-cc-lb-*`, rate-limit, or proxy-owned limit headers.
- Must not bill or quota based on post-transform plugin-mutated bytes.
- Must not add opaque per-event SSE state blobs in V1.
- Must not pass or retain full original request bodies for response hooks by default.
- Must not create a PR before the title/body approval gate.
- Must not use `as any`, `@ts-ignore`, Rust `unwrap`/`expect` outside tests/main, or warning suppressions to force green checks.

## Verification strategy

> Zero human intervention - all verification is agent-executed.

- Test decision: TDD for Rust implementation. Record red-before-green evidence for each behavior slice; for compile/admission/conformance behavior, the first failing compile/test/admission result is the red evidence.
- Evidence: `.omo/evidence/response-transform-hooks/` for command output, proxy QA notes, and review receipts.
- Required local verification: `lsp_diagnostics` on changed Rust files, targeted `cargo test`/workspace checks for affected crates, and proxy-path JSON/SSE proof or an exact blocker.

## Execution strategy

### Parallel execution waves

- Wave 0: docs and review; no product code.
- Wave 1: wire/API/PDK/macro/runtime/conformance red tests and implementation.
- Wave 2: engine buffered JSON and SSE path red tests and implementation.
- Wave 3: proxy QA, full verification, PR approval flow.

### Dependency matrix

| Todo | Depends on | Blocks | Can parallelize with |
| --- | --- | --- | --- |
| 1 | none | 2,3,4 | none |
| 2 | 1 | 3,4 | none |
| 3 | 2 | 4 | none |
| 4 | 3 | 5,6,7 | none |
| 5 | 4 | 6,7 | limited with 7 test scaffolding |
| 6 | 5 | 7,8 | none |
| 7 | 5,6 | 8 | none |
| 8 | 7 | PR approval flow | none |

## Todos

> Implementation + Test = ONE todo. Never separate.

- [ ] 1. Document accepted response-transform interface decision
  What to do / Must NOT do: Write `docs/adr/0007-response-transform-hooks.md` and this `.omo` plan from the session decision ledger. Include both hooks, context, host-owned framing/headers/encoding/accounting, SSE failure/lifetime, backward compatibility, and PR approval rule. Must not start product-code implementation in this todo.
  Parallelization: Wave 0 | Blocked by: none | Blocks: 2,3,4
  References (executor has NO interview context - be exhaustive): `docs/adr/0001-plugin-runtime-vnext.md`, `docs/rfc/0001-plugin-runtime-vnext.md`, session decision ledger summarized in current conversation.
  Acceptance criteria (agent-executable): ADR and plan exist; both mention `cc_lb_transform_response`, `cc_lb_transform_sse_event`, no mutation of `ShapeResponse`, decoded plugin input, pre-transform accounting, and PR title/body approval before PR creation.
  QA scenarios (name the exact tool + invocation): `read docs/adr/0007-response-transform-hooks.md`; `read .omo/plans/response-transform-hooks.md`; Evidence `.omo/evidence/response-transform-hooks/task-1-docs.txt`.
  Commit: N | docs commit happens in todo 3.

- [ ] 2. Run independent document coverage review and patch omissions
  What to do / Must NOT do: Run at least two independent review agents against the ADR and plan: one for session-decision coverage, one for implementation-plan executability/proxy QA. Patch every material omission before continuing. Must not ignore Oracle/Momus-like blocking feedback.
  Parallelization: Wave 0 | Blocked by: 1 | Blocks: 3,4
  References (executor has NO interview context - be exhaustive): `docs/adr/0007-response-transform-hooks.md`, `.omo/plans/response-transform-hooks.md`, `.omo/drafts/response-transform-hooks.md`, session decisions in current context.
  Acceptance criteria (agent-executable): Reviewer outputs either pass or identify omissions; omissions are patched; final review receipts say no session decision is missing from both ADR and plan.
  QA scenarios (name the exact tool + invocation): `task(subagent_type="oracle", run_in_background=true, ...)` and `task(category="unspecified-high", load_skills=["programming","proxy-e2e-qa"], ...)`; Evidence `.omo/evidence/response-transform-hooks/task-2-review.txt`.
  Commit: N | docs commit happens in todo 3.

- [ ] 3. Commit reviewed ADR and plan only
  What to do / Must NOT do: Use git-master discipline: inspect `git status`, `git diff`, and recent log; stage only `docs/adr/0007-response-transform-hooks.md`, `.omo/drafts/response-transform-hooks.md`, and `.omo/plans/response-transform-hooks.md`; commit docs only. Must not stage unrelated work or add Sisyphus/co-author/automated attribution footers.
  Parallelization: Wave 0 | Blocked by: 2 | Blocks: 4
  References (executor has NO interview context - be exhaustive): repo AGENTS.md git/PR rules; git-master skill.
  Acceptance criteria (agent-executable): `git log -1 --oneline` shows a docs commit; `git diff --staged --stat` was inspected before commit; unrelated dirty files, if any, remain unstaged.
  QA scenarios (name the exact tool + invocation): `git status --short`; `git diff -- docs/adr/0007-response-transform-hooks.md .omo/drafts/response-transform-hooks.md .omo/plans/response-transform-hooks.md`; `git log --oneline -10`; Evidence `.omo/evidence/response-transform-hooks/task-3-git.txt`.
  Commit: Y | `docs(adr): add response transform hook decision`

- [ ] 4. Load Rust reference and map implementation surfaces
  What to do / Must NOT do: Read the Rust programming reference before any `.rs` edit. Re-map current code surfaces for wire schema, metadata, PDK, macros, runtime dispatch/admission, conformance, plugin API traits/types, admin slot kind, and engine response lifecycle. Must not speculate about unread code.
  Parallelization: Wave 1 | Blocked by: 3 | Blocks: 5,6,7
  References (executor has NO interview context - be exhaustive): `crates/cc-lb-plugin-wire/src/v1/mod.rs`, `schema.rs`, `metadata.rs`; `crates/cc-lb-pdk-wasmtime/src/lib.rs`; `crates/cc-lb-pdk-wasmtime-macros/src/plugin.rs`; `crates/cc-lb-runtime-wasmtime/src/*`; `crates/cc-lb-plugin-api/src/*`; `crates/cc-lb-engine/src/lifecycle.rs`; `crates/cc-lb-engine/src/usage_parser.rs`; `crates/cc-lb-engine/src/usage_decoder.rs`.
  Acceptance criteria (agent-executable): Rust references read; exact files/functions to edit are named; no Rust edit happened before reference read.
  QA scenarios (name the exact tool + invocation): `read /home/bhyoo/.cache/opencode/packages/oh-my-openagent@4.16.0/node_modules/oh-my-openagent/dist/skills/programming/references/rust/README.md`; targeted file reads; Evidence `.omo/evidence/response-transform-hooks/task-4-map.txt`.
  Commit: N.

- [ ] 5. Implement plugin wire/API/PDK/runtime surface with tests
  What to do / Must NOT do: Add `TransformResponse` and `TransformSseEvent` hook kinds, V1 support lists, schema sections/fingerprints, owned+Ref wire types, API traits/types, PDK `run_*` helpers, macro handler arms, runtime inspection/admission/dispatch support, and conformance fixtures. Write failing tests first for wire byte parity, schema recognition, PDK dispatch, macro signature checks where available, runtime admission, and conformance. Must not alter existing filter/shape/observe schemas. Must not add opaque per-event SSE state blobs in V1. Must not pass full original request bodies to response hooks by default; pass trimmed stable context only.
  Parallelization: Wave 1 | Blocked by: 4 | Blocks: 6,7
  References (executor has NO interview context - be exhaustive): current filter/shape/observe implementations in plugin wire/PDK/macro/runtime/conformance crates; ADR 0007 ABI/versioning section.
  Acceptance criteria (agent-executable): New hooks admit and dispatch in isolated tests; old hook tests still pass; no `ShapeResponse` fingerprint-affecting edits.
  QA scenarios (name the exact tool + invocation): Targeted `cargo test -p cc-lb-plugin-wire`; `cargo test -p cc-lb-pdk-wasmtime`; `cargo test -p cc-lb-runtime-wasmtime`; conformance tests; Evidence `.omo/evidence/response-transform-hooks/task-5-wire-runtime.txt`.
  Commit: N | implementation commit deferred until tested slice is complete unless a clean atomic split emerges.

- [ ] 6. Implement engine buffered JSON and SSE transform paths with tests
  What to do / Must NOT do: Wire transform dispatch into buffered response and SSE response paths. For buffered responses, decode before plugin input, keep accounting on upstream truth, apply sanitized status/header/body replacement, recompute/strip length/encoding headers, and fail open to the original upstream response on plugin trap/error. For SSE, parse complete events, invoke `transform_sse_event`, re-emit valid frames, preserve backpressure/cancellation, use a pinned per-stream guest instance with a hard concurrent stream instance cap/backpressure, and enforce no raw fallback after transformed output. Must not buffer whole SSE streams or transform raw chunks.
  Parallelization: Wave 2 | Blocked by: 5 | Blocks: 7,8
  References (executor has NO interview context - be exhaustive): `crates/cc-lb-engine/src/lifecycle.rs` buffered `finish_success_response` and streaming `relay_response`; `usage_parser.rs`; `usage_decoder.rs`; `hop_by_hop.rs`; `sse_error_frame.rs`; ADR 0007 host-owned safety rules.
  Acceptance criteria (agent-executable): Tests prove JSON reverse-map capability, SSE `content_block_start` reverse-map capability, no-transform passthrough, buffered fail-open, SSE fail-open only before any transformed bytes/events are emitted, SSE termination with classified transform error after transformed output, no raw passthrough after transformed output, accounting pre-transform truth, header spoof stripping, and length/encoding correctness.
  QA scenarios (name the exact tool + invocation): Targeted engine unit/integration tests including existing `byte_equivalent_passthrough`, `no_modification_of_success_body`, `unknown_event_preserved`, `utf8_boundary_handling`, `sse_usage_anthropic`, `upstream_mid_stream_error_emits_error_frame`; Evidence `.omo/evidence/response-transform-hooks/task-6-engine.txt`.
  Commit: N | implementation commit deferred until full verification.

- [ ] 7. Run full implementation verification and post-write review
  What to do / Must NOT do: Run diagnostics on changed Rust files, rustfmt/checks, targeted tests, and proportional workspace checks. Measure pure LOC for changed source files and apply the programming post-write review. Must not claim done with failing tests or uninspected diagnostics.
  Parallelization: Wave 3 | Blocked by: 5,6 | Blocks: 8
  References (executor has NO interview context - be exhaustive): programming skill post-write loop; changed-file list from git.
  Acceptance criteria (agent-executable): LSP diagnostics clean or documented pre-existing; relevant builds/tests pass or exact blocker documented; post-write review has no unresolved smell.
  QA scenarios (name the exact tool + invocation): `lsp_diagnostics` changed files; `cargo fmt --check`; targeted `cargo test`; any workspace check feasible; Evidence `.omo/evidence/response-transform-hooks/task-7-verification.txt`.
  Commit: Y | likely `feat(plugin): add response transform hooks` plus tests, plain body only if needed.

- [ ] 8. Proxy-path QA and PR approval flow
  What to do / Must NOT do: Prove client-visible proxy behavior for buffered and SSE response transform paths using deterministic local/test upstreams and a deterministic transform plugin when possible. Use the actual client-visible proxy URL, `x-api-key`, `anthropic-version`, and `/v1/messages` request shape. Non-streaming JSON must assert the client sees lower-case `tool_use.name`. Streaming SSE must assert `content_block_start.content_block.name` is lower-case, event framing/order is valid, and the stream reaches a valid terminal event. Capture selected-upstream/log evidence where available and clean up temporary credentials. Then prepare PR title/body and ask for explicit final approval before creating PR. After PR creation/push, monitor checks until mergeable; do not rerun failed CI to force green; fix root causes and push follow-up commits if needed.
  Parallelization: Wave 3 | Blocked by: 7 | Blocks: PR completion
  References (executor has NO interview context - be exhaustive): proxy-e2e-qa skill; repo AGENTS.md PR completion contract.
  Acceptance criteria (agent-executable): Proxy QA report is PASS or exact BLOCKED; PR title/body shown and approved before `gh pr create`; after PR creation checks are monitored to pass or blocker is reported.
  QA scenarios (name the exact tool + invocation): JSON `/v1/messages` request through proxy with `x-api-key`, `anthropic-version`, and `Content-Type: application/json`; SSE `curl -N` request through proxy with the same credential/header shape and `stream: true`; inspect response body/event names, event framing/order/terminal event, and selected-upstream/logs/metrics/traces where available; cleanup temp credentials if created; Evidence `.omo/evidence/response-transform-hooks/task-8-proxy-pr.txt`.
  Commit: N unless CI fixes require follow-up commits.

## Final verification wave

- [ ] F1. Plan compliance audit: Oracle or review-work style audit over ADR, plan, diff, and verification evidence.
- [ ] F2. Code quality review: checks type safety, rkyv ABI stability, no overbroad refactors, and existing hook compatibility.
- [ ] F3. Real manual QA: proxy-e2e-qa JSON and SSE client-visible requests through the proxy surface, not health checks.
- [ ] F4. Scope fidelity: confirm no raw stream API, no `ShapeResponse` mutation, no post-transform billing, and PR approval rule followed.

## Commit strategy

1. Docs-only commit after todo 2:
   - Stage only `docs/adr/0007-response-transform-hooks.md`, `.omo/drafts/response-transform-hooks.md`, and `.omo/plans/response-transform-hooks.md`.
   - Message: `docs(adr): add response transform hook decision`.
2. Implementation commit(s) after todo 7:
   - Keep wire/API/PDK/runtime and engine/tests together if they are not independently compileable.
   - Use plain commit bodies only; no Sisyphus attribution, no co-author footer.
3. PR after todo 8:
   - Show title/body first and wait for explicit final approval.
   - After PR creation, monitor checks until mergeable or report exact blocker.

## Success criteria

- ADR and plan capture every accepted session decision with no review omissions.
- Existing plugin ABI remains backward-compatible for filter/shape/observe.
- New buffered and SSE response hooks are schema-gated, documented, admitted, dispatched, and covered by conformance/runtime tests.
- Engine response paths transform only decoded semantic payloads and preserve host-owned framing/header/accounting invariants.
- JSON and SSE tool-name reverse-map use cases are demonstrably possible through tests.
- Proxy-path QA proves client-visible behavior or reports a precise blocker.
- PR is created only after title/body approval and is monitored until checks/mergeability are known.
