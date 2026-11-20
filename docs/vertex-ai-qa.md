# Vertex AI Gemini QA matrix and live-QA record

This matrix is the frozen Vertex specification's 96-item QA list. `PENDING` means run after implementation; `BLOCKED` is recorded only when the stated live prerequisite is unavailable. Auto rows name the owning test target; manual rows name the required exercise. Do not print or persist credential contents.

## Status rules

| Status | Meaning |
|---|---|
| `PENDING` | Not run. |
| `PASS` | All Then assertions passed; record evidence. |
| `FAIL` | One or more Then assertions failed; record output. |
| `BLOCKED` | Required credential, service, permission, or environment unavailable. |
| `RECORDED` | Observation/measurement without a pass/fail judgment. |

## Matrix

Each row preserves the frozen Given/When/Then contract and target.

## Shipped Vertex contract and operator caveats

The shipped implementation has two Vertex kinds. `vertex_api_key` supports Express and regional project-bound keys; `vertex_credential` supports regional service-account, authorized-user, and impersonated-service-account credentials. Both preserve Gemini-shaped ingress and are metered API-key fallback candidates, not subscription quota upstreams.

- Express and regional API-key targets ship only generate, stream, and countTokens. Google rejects API keys for cached-content creation, the publisher model catalog, and Live.
- Regional credential targets ship approved cached-content create/use, publisher model-catalog list/get, and Live WebSocket routes through Vertex. Live rewrites only the first setup model and retries at most one alternate eligible upstream on build/sign/mint failure.
- Model listing is a minimal publisher-catalog name/version projection and is not entitlement-aware; a listed model may still be unavailable to the configured project.
- Explicit cached-content billing remains provider-controlled; cc-lb does not promise cache discounts or provide billing receipts.
- Use least-privilege service accounts. Vertex is excluded from subscription quota, warmup, keepalive, embeddings, prediction, Files, and OpenAI-compatible routes.
- The admin DELETE endpoint soft-deletes rows. Before a downgrade, delete Vertex upstreams through the admin API, then use an approved database maintenance procedure to hard-delete corresponding soft-deleted rows from `upstream_spec_v1` after checking references, and verify no Vertex rows remain before starting the old binary.

The matrix below records the exact automated and manual checks for this contract.


1. **QA-KIND-1** — Given all eight kinds, when provider/classification/wire mappings run, then legacy mappings stay unchanged and Vertex maps to Gemini, ApiKey, and `vertex_*`. `[auto: domain/storage upstream mappings]`
2. **QA-KIND-2** — Given subscription and Vertex candidates, when preference/final-attempt failover runs, then Vertex is metered API-key fallback, never subscription. `[auto: engine lifecycle/subscription preference]`
3. **QA-ELIG-1** — Given the eligibility matrix, when candidates build, then Express and regional API-key targets are eligible only for generate/stream/countTokens, regional credentials are eligible for approved cache/catalog/Live rows, and API-key targets are excluded from cache/models/Live. `[auto: engine lifecycle filter tests]`
4. **QA-ELIG-2** — Given a Vertex-only pool, when embedding/prediction/OpenAI-compatible/Files requests arrive, then no signer runs and the existing no-eligible error returns. `[auto: engine lifecycle happy tests]`
5. **QA-STO-1** — Given SQLite at 0127, when 0128 runs, then rows, child rows, indexes, FKs, epochs, workspace, and warmup references survive. `[auto: storage-sqlite migration compatibility]`
6. **QA-STO-2** — Given Postgres at 0172, when 0173 runs, then schema/kind check apply with one-second lock timeout. `[auto: storage-postgres tests]`
7. **QA-STO-3** — Given invalid Vertex fields and combinations, when inserted, then every invalid case rejects and positive locations pass. `[auto: storage adapters/conformance]`
8. **QA-STO-4** — Given a Vertex record, when both adapters and memory store round-trip it, then configuration is byte-equivalent. `[auto: storage conformance]`
9. **QA-STO-5** — Given project/location/credential/key/base-url changes, when epochs are checked, then epochs increment and stale bindings fail; version-only does not. `[auto: storage affinity tests]`
10. **QA-STO-6** — Given terminal status, when stale/non-terminal/replacement writes occur, then terminal persists, stale conflicts, replacement clears. `[auto: storage conformance]`
11. **QA-STO-7** — Given view rebuilds, when status applies, then terminal markers are neither cleared nor overwritten. `[auto: server dynamic-view tests]`
12. **QA-STO-8** — Given peer terminal write, when reconciliation material is computed, then hash changes without revision bump. `[auto: server reconcile tests]`
13. **QA-CRD-1** — Given three valid credential fixtures, when parsed, then canonical forms and non-secret hints result. `[auto: cc-lb-vertex-auth]`
14. **QA-CRD-2** — Given forbidden/unknown credential forms, when parsed, then static rejection codes return without secret/value echo. `[auto: cc-lb-vertex-auth]`
15. **QA-CRD-3** — Given malformed, duplicate, BOM/CRLF, PKCS#1, and size-bound inputs, when parsed, then exact frozen behavior occurs including 16 KiB acceptance and +1 rejection. `[auto: cc-lb-vertex-auth]`
16. **QA-CRD-4** — Given Vertex ciphertext, when opened with wrong AAD or generic API-key resolution, then decryption fails closed. `[auto: control credential tests]`
17. **QA-JWT-1** — Given a test RSA key, when service JWT mints, then RS256, audience, scope, issuer, and sub-hour lifetime are correct. `[auto: cc-lb-vertex-auth]`
18. **QA-MNT-1** — Given fake transport/service account, when minting occurs, then exactly one fixed token-endpoint request occurs. `[auto: cc-lb-vertex-auth]`
19. **QA-MNT-2** — Given authorized user, when minting occurs, then grant has client credentials/refresh token and no scope; rotated token is ignored. `[auto: cc-lb-vertex-auth]`
20. **QA-MNT-3** — Given impersonation, when minting occurs, then source precedes IAM, URL/body/delegates/scopes/quota headers are exact. `[auto: auth and signer]`
21. **QA-MNT-4** — Given endpoint/IAM failures, when classified, then terminal/transient taxonomy matches the specification. `[auto: cc-lb-vertex-auth]`
22. **QA-MNT-5** — Given hostile transport instrumentation, when all credentials mint, then only fixed token or validated IAM URLs are contacted. `[auto: cc-lb-vertex-auth]`
23. **QA-MNT-6** — Given invalid-grant clock skew/strike sequences, when classified, then skew is transient and success resets strikes. `[auto: cc-lb-vertex-auth]`
24. **QA-TOK-1** — Given 100 cold callers, when bearer is requested, then exactly one detached mint occurs. `[auto: cc-lb-vertex-auth]`
25. **QA-TOK-2** — Given 120-second and sub-60-second tokens, when callers arrive, then soft refresh serves old token/background refresh and hard refresh awaits. `[auto: cc-lb-vertex-auth]`
26. **QA-TOK-3** — Given caller cancellation during mint, when another caller requests, then detached mint completes with no duplicate. `[auto: cc-lb-vertex-auth]`
27. **QA-TOK-4** — Given unchanged/replaced credentials across view rebuild, when requests arrive, then cache reuses only unchanged source. `[auto: server signer factory]`
28. **QA-TOK-5** — Given terminal status, when a view builds, then no token HTTP request occurs and build fails closed. `[auto: server signer factory]`
29. **QA-URL-1** — Given Express/global/multi-region/regional targets, when signed, then host/path/version exactly match the table. `[auto: vertex signer]`
30. **QA-URL-2** — Given stream `alt` variants, when signed, then only allowed alt survives and absent gets `alt=sse`. `[auto: vertex signer]`
31. **QA-URL-3** — Given forbidden queries, when routes sign, then route allow-lists alone survive. `[auto: vertex signer]`
32. **QA-URL-4** — Given hostile model IDs, when signed, then signing fails before credentials/dispatch. `[auto: vertex signer]`
33. **QA-URL-5** — Given hostile location/base URL, when built, then origin stays constrained and path is rebuilt. `[auto: domain and signer]`
34. **QA-URL-6** — Given scoped/unscoped Gemini ingress, when signed, then Vertex URL/body are identical. `[auto: engine prefix routes]`
35. **QA-BODY-1** — Given ordinary body, when signed, then bytes remain pointer-identical and no rewrite parse occurs. `[auto: vertex signer]`
36. **QA-BODY-2** — Given escaped/duplicate/value variants, when rewritten, then valid cases transform and ambiguous cases fail closed. `[auto: vertex signer]`
37. **QA-BODY-3** — Given count wrapper/mixed keys/nested cache, when signed, then only sole-wrapper flattening occurs and bytes remain faithful. `[auto: vertex signer]`
38. **QA-BODY-4** — Given multi-megabyte cache-create body, when rewritten, then only model changes and no DOM parse occurs. `[auto/bench: vertex signer]`
39. **QA-AFF-1** — Given numeric-project create response, when returned, then client cache/model names, expiry, and visible-name digest are correct. `[auto: engine affinity]`
40. **QA-AFF-2** — Given generated cache ID, when generate/count reuse it, then both pin creator and rewrite project/location. `[auto: engine affinity]`
41. **QA-AFF-3** — Given malformed/mismatched cache response, when processed, then 503 affinity-unavailable returns and no binding writes. `[auto: engine affinity]`
42. **QA-AFF-4** — Given wrong/no owner, when cache is used, then existing affinity error occurs without failover. `[auto: engine affinity]`
43. **QA-AFF-5** — Given token refresh, when affinity state is checked, then epoch/revision do not change. `[auto: server/storage conformance]`
44. **QA-RESP-1** — Given Vertex JSON/SSE success, when returned, then additive fields and usage remain correct. `[auto: engine response transforms]`
45. **QA-RESP-2** — Given response plugin, when cache/model response transforms, then plugin sees client form and replacement reaches client. `[auto: engine response transforms]`
46. **QA-RESP-3** — Given gzip create response, when decoded/rewritten, then encoding/length and client names are correct. `[auto: engine response transforms]`
47. **QA-RESP-4** — Given malformed catalog/unexpected cache names, when processed, then catalog/affinity errors return without binding. `[auto: engine response tests]`
48. **QA-MODEL-1** — Given a Vertex publisher model list, when `/v1beta/models` routes, then the minimal name/version projection and pagination result are returned without claiming entitlement-aware availability. `[auto: engine catalog]`
49. **QA-MODEL-2** — Given model get/pages/unsupported query/404, when routed, then filtering/projection/error passthrough match. `[auto: engine provider-native HTTP]`
50. **QA-LIVE-1** — Given credential Live request, when connected, then path/version/host/Bearer/setup model are correct. `[auto: server websocket]`
51. **QA-LIVE-2** — Given text/binary setup frames, when client model is present, then only setup model changes. `[auto: server websocket]`
52. **QA-LIVE-3** — Given Express-only pool, when Live requests, then no-eligible response occurs before upgrade. `[auto: server websocket]`
53. **QA-LIVE-4** — Given two candidates and first build/mint failure, when Live connects, then one alternate is attempted before 502. `[auto: server websocket]`
54. **QA-LIVE-5** — Given existing Gemini/OpenAI Live routes, when eligibility refactors, then behavior is unchanged. `[auto: server realtime lifecycle]`
55. **QA-ERR-1** — Given Vertex 400/403, when routed, then response relays and no failover occurs. `[auto: engine failover]`
56. **QA-ERR-2** — Given credential 401, when handled, then one refresh/retry occurs; API keys never refresh. `[auto: server signer factory]`
57. **QA-ERR-3** — Given hintless 429, when observed, then `RESOURCE_EXHAUSTED`, no retry delay, bounded failover. `[auto: usage/lifecycle]`
58. **QA-ERR-4** — Given preflight statuses, when mapped, then verdicts match the frozen mapping. `[auto: admin upstream tests]`
59. **QA-ADM-1** — Given five valid create types, when submitted, then metadata returns without secrets; real cases are pass or BLOCKED. `[auto: admin upstream tests]`
60. **QA-ADM-2** — Given invalid combinations, when submitted, then static validation rejects and nothing persists. `[auto: admin upstream tests]`
61. **QA-ADM-3** — Given counting AEAD, when list/get runs, then zero decryptions occur and metadata is present. `[auto: admin upstream tests]`
62. **QA-ADM-4** — Given project/location/base URL update without replacement, when submitted, then stored secret re-preflights, skip is honored, terminal persists. `[auto: admin upstream tests]`
63. **QA-ADM-5** — Given replacement/type switch, when submitted, then metadata updates, terminal clears, stale If-Match fails. `[auto: admin upstream tests]`
64. **QA-ADM-6** — Given recheck/workspace/access APIs, when called, then only allow-listed behavior is exposed. `[auto: admin upstream tests]`
65. **QA-POLL-1** — Given Vertex API-key/credential upstreams, when usage handling runs, then result is not-applicable and no poll candidate exists. `[auto: scheduler/storage]`
66. **QA-READY-1** — Given terminal-only/healthy pools, when `/readyz` runs, then readiness follows eligible health and does not decrypt per probe. `[auto: server readiness]`
67. **QA-OBS-1** — Given successful/terminal/transient mints, when telemetry captures, then metric labels are approved and secret-free. `[auto: observability/server]`
68. **QA-SEC-1** — Given client credential/quota/IAM/billing headers and forbidden queries, when signed, then only configured headers reach Vertex. `[auto: vertex signer]`
69. **QA-SEC-2** — Given logs containing token/private-key patterns, when redacted, then no secret remains. `[auto: observability redaction]`
70. **QA-SEC-3** — Given planted secret tracing in auth crate, when lint runs, then it fails; clean tree passes. `[auto: scripts/lint-audit-redaction.sh]`
71. **QA-SEC-4** — Given hostile credential URLs/IAM look-alikes, when uploaded/minted, then rejected and never contacted. `[auto: auth crate]`
72. **QA-SEC-5** — Given DB dumps after credential creation, when searched, then no plaintext secret/JSON appears. `[auto: storage backend tests]`
73. **QA-SEC-6** — Given Google resource references/tuned models, when forwarded, then current pass-through behavior is recorded with least-privilege warning. `[auto: vertex signer]`
74. **QA-WEB-1** — Given every kind, when web helpers run, then Vertex maps Gemini and capabilities are NONE. `[auto: admin web providers]`
75. **QA-WEB-2** — Given shared kind/host fixtures, when contract tests run, then Rust/web lists and derivation match. `[auto: admin web contracts]`
76. **QA-WEB-3** — Given legacy/API-key/credential payloads, when parsed, then compatibility and api_key_access remain. `[auto: admin web API tests]`
77. **QA-WEB-4** — Given Express/key/file flows, when completed, then flat fields are sent and browser storage stays clean. `[auto: admin web dialog tests]`
78. **QA-WEB-5** — Given Express detail, when rendered, then limited badge/nudge appears only there. `[auto: admin web]`
79. **QA-WEB-6** — Given credential status reasons, when rendered, then terminal replacement guidance and transient warning appear; OAuth reconnect never appears. `[auto: admin web]`
80. **QA-WEB-7** — Given mixed upstreams, when filtered/grouped, then Credential/API-key filters stay distinct and Vertex groups Gemini. `[auto: admin web]`
81. **QA-WEB-8** — Given upload flow at 390/768/1440px and both themes, when picker/paste/back/cancel/replace/detail run, then focus, scrolling, redaction, and conventions hold. `[manual: real browser; synthetic fixtures]`
82. **QA-REG-1** — Given existing Gemini/Anthropic/OpenAI flows, when suites run, then URLs, headers, OAuth, catalogs, affinity, failover remain unchanged. `[auto: existing suites]`
83. **QA-REG-2** — Given shape plugin and Vertex identity, when loaded, then fingerprints stay unchanged and no project/location leaks. `[auto: plugin-wire/runtime]`
84. **QA-COST-1** — Given equal cached usage for Gemini API and Vertex, when priced, then model pricing matches, explicit cached-content billing remains provider-controlled, and trafficType is not silently priced. `[auto: engine pricing]`
85. **QA-UPG-1** — Given previous-release DB, when new binary upgrades/restarts/reads, then legacy records stay unchanged and `vertex=None`. `[auto: SQLite/Postgres migration compatibility]`
86. **QA-UPG-2** — Given old binary plus Vertex row, when startup/view build runs, then documented mixed-version failure occurs and rollout guidance is present. `[manual: two-version deployment]`
87. **QA-DOC-1** — Given public surfaces, when parity runs, then eight kinds and supported Vertex wording pass while Bedrock remains unsupported. `[auto: node scripts/check-docs-provider-parity.mjs]`
88. **QA-DOC-2** — Given synced provider sources, when sync tests run, then Vertex is allowed only in approved sources and blocked in scheduler docs. `[auto: (cd site && bun test scripts/sync-docs.test.ts) — qa_doc_2_synced_provider_sources_allow_vertex_only_in_approved_sources]`
89. **QA-DOC-3** — Given metadata/capability surfaces, when checks run, then no false Vertex verification claim appears. `[auto: (cd site && bun test scripts/sync-docs.test.ts) — qa_doc_3_metadata_surfaces_reject_unsupported_vertex_verification_claims]`
90. **QA-E2E-1** — Given real service-account credential, when client runs generate/stream/count/cache/model list, then all work and contents stay secret. `[manual: real file; BLOCKED if unavailable]`
91. **QA-E2E-2** — Given real authorized-user ADC, when same flow runs, then quota headers and redaction are correct. `[manual: real file; BLOCKED if unavailable]`
92. **QA-E2E-3** — Given authorized user without quota project, when preflight/live run, then no quota header is sent and behavior is recorded. `[manual: fixture; BLOCKED if unavailable]`
93. **QA-E2E-4** — Given real Live in us-central1/global, when client connects, then text/audio/usage and setup behavior are recorded. `[manual: real credential/location; BLOCKED if unavailable]`
94. **QA-E2E-5** — Given a bound standard key, when Live runs against a regional target, then Google rejection is recorded; regional API-key cache/catalog/Live rejection was exercised. `[manual: real regional bound key; PASS — Google rejects API keys for these operations]`
95. **QA-E2E-6** — Given an Express-shaped target, when generate/stream/countTokens/cache/models/Live are considered, then generate/stream/countTokens are the API-key-supported operations and cache/models/Live are rejected before provider dispatch; express-only Google keys were unavailable for this account due to organization policy. `[manual: regional/projectless-shaped target exercised; PASS for observed regional API-key boundary; Express credential unavailable]`
96. **QA-E2E-7** — Given impersonated credential, when source/IAM/final call and quota headers run, then permissions and headers match. `[manual: Token Creator unavailable — BLOCKED]`

## Live QA: runbear-local

Run against an isolated `runbear-local` deployment with request/response capture and secret redaction. Repeat for **express API key**, **bound standard API key**, **service_account**, **authorized_user**, and **impersonated_service_account** where the operation is eligible. For each credential type record generate, stream, countTokens, cachedContents create/use (including client/Vertex name translation), model list/get, Live, 400/401/403/404/429/5xx behavior, preflight, admin recheck, terminal status, request events, `x-goog-request-id`, token-mint metrics, and redaction. Express must demonstrate only generate/stream/countTokens; standard and credentials must demonstrate their eligible cache/catalog/Live behavior. Mark unavailable credentials or permissions BLOCKED with the exact missing prerequisite, never with a fabricated PASS. Verify upgrade-before-create and delete-before-downgrade on the deployment before declaring rollout ready.
