# ADR 0014: Vertex AI Gemini upstreams

- Status: Accepted
- Date: 2026-11-20
- Amends: Gemini provider scope and runtime-management documentation

## Context

cc-lb already speaks the Gemini Developer API shape, but operators also need Gemini through Vertex AI. Vertex has two materially different authentication and endpoint families: Express API keys and regional project-bound API keys/Google credentials. Treating these as a flag on `gemini_api_key` would spread project, location, signer, eligibility, cache, and credential rules through every existing kind branch. Vertex rows must also remain unreadable by old binaries, and credentials must never be stored or routed through ambient ADC.

## Decision

Add two Gemini-provider upstream kinds: `vertex_api_key` (Express or regional project/location; API-key or service-account-bound key) and `vertex_credential` (regional only; uploaded canonical `service_account`, `authorized_user`, or `impersonated_service_account` JSON). Both classify as metered `ApiKey`, are API-key fallback candidates, and are excluded from subscription quota, warmup, keepalive, embeddings, prediction, Files, and OpenAI-compatible routes. Express and regional API-key targets ship only generate, stream, and countTokens. Regional credential targets additionally ship approved cached-content create/use, publisher model-catalog list/get, and Live WebSocket routes.

Persist typed Vertex target (`Express` or `Regional { project_id, location }`) and explicit `v1`/`v1beta1` (default `v1beta1`) columns. Validate project IDs, locations, model IDs, field combinations, and origin-only base URLs. Express is immutable. Project/location/base URL/key/credential changes increment `owner_epoch`; API-version-only changes and token refresh do not. Store credential metadata without decrypting on reads; encrypted credential blobs use `cc-lb/vertex-credential/v1\\0` plus upstream ID as AAD and are never accepted by generic API-key decryption.

Only the auth crate parses credentials and mints tokens. It accepts the three supported Google credential types and rejects external accounts, nested impersonation, credential sources, workforce fields, non-default universe domains, non-fixed token URIs, invalid URLs, PKCS#1 keys, and oversized input. It stores a minimal canonical form. Service accounts use a sub-hour RS256 JWT bearer grant; authorized users use refresh-token grant without scope; impersonation mints the source then calls the exact IAM Credentials URL with delegates and quota headers. No ambient ADC or metadata-server authentication is used.

The process-wide weak token registry is keyed by upstream ID and credential ciphertext hash. Strong views share detached single-flight sources, soft-refresh at 240 seconds, hard-refresh below 60 seconds, and never persist tokens. 401 invalidates only the attempted token and permits one refresh. Invalid-grant, invalid-client, IAM/Vertex errors, malformed responses, and rate limits follow the frozen transient/terminal taxonomy. Terminal status is asynchronous, CAS-protected by `expected_spec_revision`, and only credential replacement clears it. Mint telemetry is `cclb_vertex_token_mint_total{credential_type,outcome}` without upstream IDs.

The Vertex signer owns final URL and credentials after plugins shape the Gemini request. It maps Express/global/`us`/`eu`/regional hosts, persisted API versions, generate/stream/count, cached-content create, publisher model list/get, and Live WebSocket paths. It strips all client credentials, quota/billing headers, and disallowed queries; configured API keys or Bearer tokens are attached. Bodies remain byte-faithful except lazy decoded-key duplicate-safe rewrites for model and cached-content fields. Successful cached-content and model responses are translated back to Gemini client names before plugin transformation, affinity, usage parsing, and delivery. Client-visible `cachedContents/{id}` is hashed and pinned to its creator and owner epoch; no failover is allowed for pinned requests.

Vertex 400/403 responses pass through without failover; credential 401 refreshes once, 429 becomes `RESOURCE_EXHAUSTED` without a hintless retry delay, and existing bounded failover remains. Generic `FAILED_PRECONDITION` is not a billing stop. Model catalogs expose only the minimal publisher Gemini name/version projection and are not entitlement-aware. Explicit cached-content billing remains provider-controlled. Live supports regional credentials, never Express or API-key targets: Google rejects API keys for cached-content creation, the publisher model catalog, and Live. A service-account Live probe passed in `us-central1` with `gemini-live-2.5-flash-native-audio`; the global location had no Live model for this project. It rewrites only the first setup model and retries one alternate candidate on build/sign/mint failure.

Plugin wire schemas do not change: project/location stay signer-owned. Vertex is excluded from usage polling and keepalive. Admin APIs expose typed metadata, preflight, recheck limitations, terminal status, and no secrets. The web UI offers Express key, Cloud API key, and in-memory credential-file flows, with no quota/warmup/credits UI and Cloud Billing links. Regional credential deployments should use least-privilege service accounts.

## Upgrade and downgrade rule

Upgrade every replica before creating any Vertex row. Mixed-version operation is unsupported because old binaries cannot parse Vertex kinds. The admin DELETE endpoint soft-deletes upstream rows; before downgrading, delete every Vertex upstream through the admin API, then use an approved database maintenance procedure to hard-delete the corresponding soft-deleted rows from `upstream_spec_v1` (after checking references; configured foreign-key child rows cascade) and verify no Vertex rows remain. Do not leave Vertex rows for an old binary to encounter.

## Rejected alternatives and decision log

| Topic | Adopted | Rejected | Reason |
|---|---|---|---|
| Kind model | Two new kinds | Sub-mode on `gemini_api_key` | Keeps routing and storage exhaustive and typed. |
| Credential storage | Existing encrypted secret column with Vertex AAD | New table or raw-ID AAD | Minimal migration while preventing cross-kind decryption. |
| API version | Per-row, default `v1beta1` | Compile-time constant | Makes HTTP and Live upgrades explicit. |
| Token cache | Weak process registry and detached single-flight | Per-view cache/caller future | Prevents rebuild stampedes and cancellation races. |
| Body handling | Lazy decoded-key pass | DOM parse or literal byte gate | Preserves hot-path fidelity while preventing duplicate-key smuggling. |
| Error/resource references | Pass through unchanged | Rewrite/deny | Preserves provider fidelity; least privilege is the operator responsibility. |
| Live bound keys | Regional API-key Live is not supported | Provisional support | Real E2E: Google rejected API keys for cached-content creation, publisher model catalog, and Live; service-account Live passed in `us-central1`, while global had no Live model for this project. |
| Plugin wire | Unchanged | Add Vertex fields | Existing plugin fingerprints must remain compatible. |
| Changelog | No manual entry | Hand-edit generated changelog | Release-plz owns release notes. |

## Consequences and residual risks

Positive: Vertex routing is explicit and fail-closed; credentials are narrowly accepted, cached safely, and redacted; client-visible Gemini semantics remain stable; old Gemini, Anthropic, and OpenAI paths retain their contracts.

Negative and residual risks: each replica mints independently; regional Live remains environment-dependent and requires credentials; model catalogs intentionally expose only publisher name/version and are not entitlement-aware; provider error bodies may expose Vertex resource names; pass-through Google resource references create confused-deputy risk, so least-privilege service accounts are required; explicit cached-content billing is controlled by Vertex rather than cc-lb; API-version and typed-column migrations add schema surface; downgrade requires operational discipline.

## Verification scope

The complete 96-item matrix is in [`docs/vertex-ai-qa.md`](../vertex-ai-qa.md). Real service-account Live was exercised successfully in `us-central1`; API-key cache/catalog/Live rejection was exercised against regional targets. Express-only Google keys were unavailable for this account because of organization policy. Historical changelog entries and client-capability claims about client acceptance are unchanged.
