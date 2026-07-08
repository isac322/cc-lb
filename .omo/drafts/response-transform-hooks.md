---
slug: response-transform-hooks
status: approved-by-user-message
intent: clear
pending-action: commit docs then implement
approach: Write ADR + implementation plan from accepted session decisions, run independent coverage reviews, patch omissions, commit docs only, then implement and verify before PR approval flow.
---

# Draft: response-transform-hooks

## Components (topology ledger)

| id | outcome | status | evidence path |
| --- | --- | --- | --- |
| ADR | Record the publish-grade decision for two additive response transform hooks. | active | docs/adr/0007-response-transform-hooks.md |
| PLAN | Provide a decision-complete implementation plan for wire/PDK/runtime/engine/tests/proxy QA. | active | .omo/plans/response-transform-hooks.md |
| REVIEW | Run independent coverage reviews against the session decision ledger and patch omissions before code. | active | review agent outputs in current session |
| DOC_COMMIT | Commit only ADR/plan docs before implementation, preserving unrelated work. | active | git status/diff/log before commit |
| IMPLEMENTATION | Add `transform_response` and `transform_sse_event` hooks, engine wiring, tests, and verification. | active | crates/* + tests after doc commit |
| PR | After implementation, show PR title/body and ask final approval before creating PR. | active | repo rule from AGENTS.md |

## Open assumptions (announced defaults)

| assumption | adopted default | rationale | reversible? |
| --- | --- | --- | --- |
| Existing hook compatibility | Do not rename or extend `ShapeRequest`/`ShapeResponse`; add new hooks only. | rkyv schema fingerprints make existing type changes breaking. | No for V1 compatibility. |
| Hook split | Use `cc_lb_transform_response` for buffered bodies and `cc_lb_transform_sse_event` for complete SSE events. | The safe streaming abstraction is an SSE event, not a raw stream/chunk. | Hook names become public ABI; only via V2/new hook. |
| Dispatch discriminator | Dispatch by actual upstream `Content-Type`/framing, not request `stream`. | Upstream may return JSON error to a streaming request or vice versa. | Yes internally if tests prove another discriminator. |
| Plugin input encoding | Plugin sees decoded semantic payload, never compressed bytes. | Plugin authors should not implement gzip/br/zstd or transport decoding. | No for correctness; output strategy can evolve. |
| Buffered output encoding | V1 may normalize transformed output to identity and strip `Content-Encoding`. | Simpler safe behavior; future recompression is optimization. | Yes via later optimization. |
| Upstream error visibility | Transform upstream-produced 2xx/4xx/5xx, but not proxy-generated 502/503/bulkhead/timeout by default. | Plugins may normalize provider JSON errors; proxy errors are host semantics. | Yes via future opt-in/config. |
| SSE state | Pin a guest instance per in-flight SSE stream with a hard cap/backpressure. | Better author ergonomics and avoids opaque state blob rkyv overhead. | Yes in V2 if runtime pressure demands opaque state. |
| SSE failure | Fail open only before first transformed byte; after transformed output, terminate stream with classified transform error. | Raw-after-transformed would produce mixed tool-name semantics. | No for V1 safety. |
| Accounting | Usage/billing/quota parse pre-transform upstream truth. | Prevents plugins from altering billing source of truth. | No unless product policy changes. |
| Context | Pass trimmed stable context; do not pass original request body by default. | Avoids retaining up to 100 MiB request bodies through response lifecycle. | Yes via future opt-in capability. |
| Buffered failure | Buffered plugin trap/error fails open to the original upstream response. | A buffered body has not been emitted yet, so original passthrough is safe. | Yes via future policy config. |

## Findings (cited - path:lines)

- `docs/adr/0001-plugin-runtime-vnext.md` records the raw Wasmtime + rkyv plugin runtime decision and the invariant that hot-path plugins expose typed functions through schema-gated ABI.
- `docs/rfc/0001-plugin-runtime-vnext.md` records the PDK/macro/runtime shape used by current filter/shape/observe hooks and must be mirrored for new hooks.
- Session investigation established `ShapeResponse` is the upstream-bound shaped request output, not an HTTP response; it must not be renamed or field-modified.
- Session design review established response transforms require two interfaces: buffered response and complete SSE event, with host-owned framing, headers, decoding, accounting, and failure semantics.
- Proxy QA requirements come from `proxy-e2e-qa`: proxy behavior is not done until client-visible `/v1/messages` JSON and SSE paths prove the transformed response behavior.

## Decisions (with rationale)

1. Add additive hook kinds `TransformResponse` and `TransformSseEvent`; do not mutate existing hook schemas.
2. Export `cc_lb_transform_response` and `cc_lb_transform_sse_event`.
3. For buffered responses, pass decoded body plus request/upstream/response context and allow `Unchanged` or `Replace { status?, headers?, body? }`.
4. For SSE, pass one complete decoded `SseEvent { event, data }` and allow `Unchanged`, `Replace { events }`, or `Drop`.
5. The host owns `Content-Length`, `Content-Encoding`, hop-by-hop/proxy-owned header stripping, SSE frame parsing/re-emission, flushing, cancellation, and backpressure.
6. Usage/billing/quota use pre-transform upstream truth; transformed payloads are for client contract/log/debug only.
7. Existing passthrough invariants must remain when no transform slot is bound or when a plugin returns `Unchanged`.
8. V1 SSE state uses pinned per-stream guest instances and does not add opaque per-event state blobs.
9. Documentation must be committed before code implementation; implementation commit/PR comes later.

## Scope IN

- ADR capturing all response-transform interface decisions from the session.
- `.omo` implementation plan with exact phases, dependencies, acceptance criteria, and verification.
- Independent document coverage review and omissions patch before code.
- Docs-only commit of ADR/plan.
- Rust implementation after docs commit: wire/API/PDK/macro/runtime/conformance plus engine JSON/SSE paths and tests.
- After implementation and verification, prepare PR title/body, request explicit final approval, then create PR and monitor checks.

## Scope OUT (Must NOT have)

- No product-code implementation before ADR/plan review and docs commit.
- No mutation/rename of existing `ShapeRequest` or `ShapeResponse`.
- No raw TCP chunk transform or whole-stream buffering API.
- No exposing compressed payloads to plugins.
- No billing/quota based on plugin-mutated bytes.
- No PR creation before showing title/body and receiving explicit approval.

## Open questions

None blocking. Session decisions are treated as accepted defaults. Future V2 work can revisit compression recompression, request-body opt-in, non-SSE streaming transforms, and opaque state blobs.

## Approval gate

status: approved-by-user-message
