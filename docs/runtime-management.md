# Runtime Management

This document describes the architecture, API, and operations of the database-backed runtime management system in cc-lb.

## Overview

This system transitions cc-lb from static TOML configuration files to a dynamic, database-backed runtime management model. You no longer define upstreams, principals, or plugin chains in `cc-lb.toml`. Instead, all runtime configurations are stored in the database and managed dynamically. This change allows you to add, update, or delete resources on the fly without restarting the proxy. You can perform these operations through the web dashboard or directly via the admin v1 REST API.

Local HTTP limit admission checks whether a single request can fit within each configured cap before checking temporary exhaustion. Requests reserves one request; Input uses estimated input; Total uses the saturating sum of estimated input and requested maximum output; Cost uses the estimated cost reservation. Non-generation operations reserve zero output. For provider-specific generation requests, an effective `OutputTokens` limit requires an output bound recognized for that operation; without one, admission rejects the request before upstream dispatch with `400 invalid_request_error` and no retry hint. Requests without an `OutputTokens` limit remain unchanged, and unspecified output under existing Total or Cost estimates remains input-only for this change. A reservation that strictly exceeds a configured Requests, Input, Total, or Cost cap returns `429` with `x-should-retry: false`, no `Retry-After` header, and no `error.retry_after_seconds` body field. Equality alone does not trigger this rejection.

Temporary window exhaustion returns `429` with `x-should-retry: true`, `Retry-After: 60`, and `error.retry_after_seconds: 60`. Concurrency exhaustion returns `429` with `x-should-retry: true`, `Retry-After: 1`, and `error.retry_after_seconds: 1`. An output-cap violation remains `400 invalid_request_error` without `x-should-retry`, `Retry-After`, or a body delay. Per-request feasibility violations take precedence over temporary exhaustion.

A reduced cold allowance below a positive configured cap remains temporary and retryable if the request fits the configured cap. A configured Requests cap of zero cannot admit a request requiring one. These rules require no database migration.

## Architecture

The runtime architecture relies on a lock-free `DynamicView` snapshot managed via an `ArcSwap` primitive. When configuration changes occur in the database, the server builds a new immutable `DynamicView` snapshot and swaps it atomically. This design ensures that active request-handling threads never block on configuration updates.

In multi-replica deployments, replicas synchronize their local views using Postgres `LISTEN/NOTIFY` channels for sub-second updates. If a notification is missed, a background reconciler polls the database every 60 seconds as a fallback. The system uses a single rebind primitive to safely commit staged plugin slots and rebuild the routing table.


## Admin v1 REST API

Administrative operations use the providers configured in `admin.auth.providers`. A static-token provider accepts its environment-backed Bearer token in the `Authorization` header.

The default admin listener is `[::1]:9090`; use `http://[::1]:9090` for the examples below. Proxy traffic uses a separate listener on port 8080 (`http://localhost:8080/v1/messages`).

### Current-mode client acceptance

The proxy route tables below describe shipped operations and upstream credential eligibility. The agy 1.5.9 Gemini API-key mode has feature-specific client evidence through a clean-master cc-lb build for multi-turn coding tools, thought-signature continuity, conversation resume and native-part preservation. The A01 file-inspection result uses the corrected criterion applied to every run: any actual client file inspection counts, including `run_command(cat)`. The agy Code Assist/OAuth mode remains unverified because no healthy entitled subscription account was available. These observations do not establish full agy subscription compatibility.

In the agy API-key mode, the client calls Gemini `generateContent` and `streamGenerateContent`. `countTokens`, model discovery, and related `v1internal` Code Assist methods are outside that client's reachable API-key path. Gemini API-key `cachedContent` remains bound to its creating upstream; if that owner is unavailable, cc-lb returns `409 upstream_affinity_unavailable` and does not fail over.

The [client capability ledger](https://github.com/isac322/cc-lb/blob/master/docs/client-capabilities.json) records acceptance boundaries and evidence. The A14 clean-master run used source commit `9767566cf9f0be0e971a91657e99504f3c2556bc`, cc-lb binary SHA-256 `b7b35c678d15e6f0dca93c0fa90ecb48685999c3a96ab8b08d42877115b137c9`, and the agy 1.5.9 binary SHA-256 `fad3628ad91fe8613a72f074a1c5fde7cd816517788a5b9be96703f7d256741a`. Twelve observer request byte sizes matched cc-lb `request_body_bytes`, and all twelve requests retained `functionDeclarations=14`. The raw upstream request body was not captured. Source and descriptor coverage does not establish execution; acceptance requires the exact client, upstream class, required entitlement, and real feature execution.
The retained sanitized A14 execution record is [`a14-clean-master-record.json`](https://github.com/isac322/cc-lb/blob/master/tests/real-client/provider-e2e/agy159/a14-clean-master-record.json). It records the observed client-visible results and owner-affinity checks; the raw upstream request body was not captured.
| Endpoint family | Current-mode boundary |
|---|---|
| Vendor OAuth issuer flows | Authorization, device authorization, token exchange, refresh, and revocation retain the vendor issuer contract; a proxy principal key does not replace vendor identity. |
| Telemetry, Statsig, and feedback | Configured analytics-events can follow `chatgpt_base_url`; genuinely direct Statsig and feedback requests require separate vendor-service evidence. Configured client OpenTelemetry export is a separate destination and contract. |
| Updates, install, and pets | Distribution and asset retrieval are separate from generation-route acceptance. |
| GitHub archives | Archive retrieval retains the GitHub download and authorization contract. |
| Signed bytes | Fetch the signed destination under its own authorization contract; proxy or upstream credentials must not be forwarded automatically. |
| Cloud internal ThreadService and preview models | Codex internal HTTP/2 ThreadService and `/v2/models` preview access require their own service contracts and entitlements; REST Cloud tasks and generation results do not establish either. |
| Identity and JWKS | Default-off identity paths require explicit enablement and separate validation. |
| Configured remote environment, Noise, messageboard, and MCP | Require the configured remote service and real session execution; local or recorded traffic does not establish remote acceptance. |
| Authenticated plugins and remote-control | Not supported. Codex plugins are enabled by default, but authenticated plugin and remote-control traffic belongs to one ChatGPT account and is not proxied. |
| Workspace-switch-token | Not supported. Account switching belongs to one ChatGPT account and is separate from vendor issuer OAuth flows. |

cc-lb does not proxy account-bound subscription surfaces: Codex account and workspace management, Apps and hosted MCP, `/backend-api` files, Cloud tasks, remote control, and agy remote-agent, remote-control, and subscription voice services. Each of these belongs to one vendor account, while cc-lb pools upstream accounts, so they are outside its routing model.

Raw Code Assist traffic, remote sessions, and voice each require separate acceptance evidence. The existing Code Assist envelope used for supported Gemini OAuth generation, background admin operations, and quota polling do not establish an inbound raw Code Assist client contract. Shipped API-key Realtime and Live tunnels retain their contracts; they do not establish subscription voice acceptance. Local microphone access is a client/device boundary; an audio HTTP request or WebSocket upgrade does not establish a working microphone or voice session. Missing usage or quota evidence remains unknown, never an invented success or zero.

These boundaries preserve the supported operations and credential classes below. The [Antigravity terms warning](https://cc-lb.bhyoo.com/docs/reference/admin-api/#oauth-flow) still applies.

### Proxy capability boundary

Provider-prefixed routes select the provider explicitly: `/anthropic/v1/...`, `/openai/v1/...`, `/openai/backend-api/codex/...`, and `/gemini/v1beta/...` (plus Gemini `/gemini/v1/...` model catalogs). The tables below list supported operations and eligible credentials. Shared unprefixed routes are kept for backward compatibility and retain their existing operation set only: `POST /v1/messages` and `POST /v1/messages/count_tokens` (Anthropic Messages), `POST /v1/responses` (OpenAI Responses, plus the `GET /v1/responses` WebSocket upgrade), and Gemini `POST /v1beta/models/{model}:generateContent` / `:streamGenerateContent` / `:countTokens`. Every other unprefixed operation returns `404 operation_not_supported`; new operations require a matching provider prefix.

Provider-owned state referenced inside a supported HTTP generation request is forwarded unchanged and kept on its owning upstream through upstream affinity. cc-lb records a SHA-256 digest of each state id together with the upstream that created it, and routes later requests that reference the id only to that upstream:

- Anthropic Messages: web-search `encrypted_content`, file ids in `file` / `container_upload` blocks, and container ids.
- OpenAI HTTP Responses also records typed `reasoning`, `compaction`, and `compaction_summary` items when they carry `encrypted_content`; nested tool-schema annotations are request data, not provider-owned state. When a request contains unknown `reasoning.encrypted_content` (and no `previous_response_id` or `conversation`, and no `compaction`/`compaction_summary` encrypted items), `[upstream_affinity] unknown_openai_reasoning` defaults to `hybrid`: multiple eligible OpenAI upstreams strip unknown `reasoning` items upfront, while a single eligible upstream (or a resolved owner with additional unmatched `reasoning` items) tries verbatim first and retries once on the same upstream with only unowned `reasoning` items stripped on `400 invalid_encrypted_content` before any response frame is emitted; `compaction`/`compaction_summary` remain fail-closed (`409`), and `fail_closed` restores `409` for unknown reasoning items. Similarly, when a request's only OpenAI affinity state is `reasoning.encrypted_content` whose recorded owner upstream is disabled, quota-exhausted (`subscription_hard_blocked`), or fails pre-processing (`429`, `401` after refresh, signer/sign failure, or open circuit breaker) before any response starts—with `previous_response_id`, `conversation`, `prompt`, `compaction`, `compaction_summary`, and account-owned file/container/vector-store/item references absent—`[upstream_affinity] unavailable_openai_reasoning_owner` defaults to `strip_and_failover`, stripping `reasoning` items and failing over onto another healthy non-exhausted eligible OpenAI upstream when one exists (never downgrading API-key-only public `/v1/responses` requests onto OAuth, never masking `429` as `409` when all replacements are exhausted, and failing closed with `409` when the owner itself is disabled and no replacement remains); `fail_closed` rejects unroutable owners with `409 upstream_affinity_unavailable` and relays upstream `429` without failover. On Codex backend WebSocket chained `previous_response_id` turns (`store != true`, with `conversation`, `compaction`, and account-owned references absent), when the owner is quota-exhausted or hits a pre-`response.created` `429`/availability failure and a healthy replacement OAuth upstream exists, `strip_and_failover` evicts the owner's in-session response affinity and returns `400 previous_response_not_found` so Codex replays full history onto the replacement upstream; public `/v1/responses` never synthesizes `previous_response_not_found` on `429`. Affinity storage or persistence failures remain `503`.
- Gemini generateContent / countTokens: `cachedContent` references (including nested `generateContentRequest.cachedContent`). `thoughtSignature` is forwarded but is not treated as affinity state.

- Gemini cachedContents creation: the exact `POST /gemini/v1beta/cachedContents` operation is available only with Gemini API-key upstreams and explicit-caching Gemini 2.5 and Gemini 3 family models. Gemini 4+ explicit caching, other models, and non-create `cachedContents` API surfaces remain HTTP `404 operation_not_supported`. cc-lb forwards the request body unchanged and binds a successful canonical response `name` in the form `cachedContents/<id>` to the selected upstream before returning success; later `generateContent` and `countTokens` requests that reference that name remain pinned to its owner.

  Provider `4xx` and `5xx` responses retain their original status and create no owner binding. An unbound reference returns HTTP `409` with `api_error`; an unavailable owner returns HTTP `409 upstream_affinity_unavailable`, without cross-credential failover. Each cached-content binding is checked against its owner upstream's identity epoch. Replacing the API key or changing the base URL or project invalidates it; re-saving the same URL or project and refreshing access metadata preserve it. API-key replacement conservatively invalidates bindings even when the same key is entered again. Later reuse of an invalidated binding fails closed with HTTP `409` until a new resource is created, and a stale create response cannot rebind after an owner identity change. Affinity storage read/persistence failures (as well as malformed, non-JSON, or noncanonical create responses, and missing, invalid, or expired `expireTime`) return HTTP `503` without a success body.
- Prompt-cache analysis and cache-aware routing apply to Anthropic breakpoints and scoped older-model OpenAI Responses observations. Native pricing is exposed when catalog data exists. GPT-5.6+ accounting keys, Gemini implicit cache evidence, unknown scopes, and missing rates remain neutral; explicit Gemini `cachedContent` remains hard affinity.

Admin preflight is separate from client proxy routing: `GET /v1/models` and `POST /v1/messages/count_tokens` (Anthropic), `GET /v1/models` (OpenAI), and `GET /v1beta/models` (Gemini) run as direct upstream probes during key verification.

#### Anthropic endpoint reference

| Method | Client path | Credentials |
|---|---|---|
| POST | `/anthropic/v1/messages`, `/anthropic/v1/messages/count_tokens` | API key, OAuth |
| GET | `/anthropic/v1/models`, `/anthropic/v1/models/{id}` | API key, OAuth |
| POST | `/anthropic/v1/complete` (legacy text completions, including SSE) | API key |

Not yet supported: files, message batches, skills, agents, and sessions.

#### OpenAI endpoint reference

ChatGPT-subscription (`openai_oauth`) upstreams serve the public OpenAI Responses surface as well as the backend-shaped Codex surface. Codex-shaped requests pass through unchanged except when `[upstream_affinity]` recovery (`unknown_openai_reasoning` or `unavailable_openai_reasoning_owner`) strips unowned or unavailable-owner `reasoning.encrypted_content` items (both can be set to `"fail_closed"` to require strict passthrough). Public requests are translated or emulated only for `openai_oauth`; they are never translated across providers. The public `/v1/responses` route (including its WebSocket form) is supported, while unprefixed `/v1/chat/completions`, `/v1/models`, and `/v1/responses/input_tokens` remain unsupported.

The native Codex alias is `/openai/backend-api/codex/*`. Its supported Responses, models, alpha search, images (`generations` and `edits`), and realtime-call create routes are OAuth-only and forward raw request and response bytes (aside from ingress auth/proxy header scrubbing and optional `reasoning.encrypted_content` recovery stripping when enabled). Realtime hangup (`POST /openai/backend-api/codex/realtime/calls/{call_id}/hangup`) and realtime WebSocket (`GET /openai/backend-api/codex/realtime`, `/openai/backend-api/codex/live`, `/openai/backend-api/codex/live/{call_id}`, and `/openai/backend-api/codex`, including V1/V2/V3 sideband join) are also OAuth-only, use `openai_public_api.realtime_origin`, and stay pinned to the owning OAuth account without crossing to API-key upstreams. Downstream credential headers, including `x-openai-actor-authorization`, are stripped before upstream signing. Codex compaction uses the native `responses` operation; the alias does not expose a separate compact route. Zstd request bodies are decoded before forwarding. Restricted principals receive a filtered native catalog; unrestricted principals receive exact upstream bytes. The alias does not expose compact, memories, wham, usage, or file routes; unknown `/openai/backend-api/*` paths authenticate first, then return the OpenAI-shaped 404 without contacting an upstream.

HTTP compatibility includes `stream:false` aggregation, model-list/model retrieval conversion, Responses compaction emulation, image generation/edit conversion, and input-token counting through a WebSocket `generate:false` turn. Public `chat/completions` is translated to Responses, including tools, response formats, token limits, streaming, `n` fan-out on one subscription account, and `stop` cutting. Realtime calls are converted to the Codex call shape; hangup, sideband, and realtime WebSocket traffic uses `openai_public_api.realtime_origin` and stays pinned to the account that created the call or session. `audio/transcriptions` is disabled by default; when enabled it uses the subscription dictation endpoint, returns text/JSON only, and the upstream retains submitted audio for 30 days.

Operations without a subscription equivalent route to an eligible `openai_api_key` upstream: stored continuations and response operations, stored chat operations, `prompt`, hosted tools, background requests, account-owned resource families, and API-key-only families such as embeddings, moderation, speech, translations, and variations. Backend-shaped requests stay on their owning account and never fail over across ownership except under `[upstream_affinity] unavailable_openai_reasoning_owner = "strip_and_failover"` (default), which can strip `reasoning.encrypted_content` for a disabled, quota-exhausted, or pre-processing-failed (`429`/`401`/circuit/signer) owner when no `previous_response_id`, `conversation`, `compaction`, `compaction_summary`, or account-owned reference is present (or evict a Codex backend WebSocket `previous_response_id` owner on exhaustion/pre-`response.created` `429` with `400 previous_response_not_found` so Codex replays full history onto a healthy replacement).

The compatibility policy handles unsupported fields by coercing, dropping, emulating, or rejecting them. Defaults include `store:true` → `false`, dropping sampling/logprob fields and unsupported metadata fields (with metadata echoed), coercing `service_tier` `auto`/`flex` to `default`, converting `web_search_preview` to `web_search`, truncation retry, and dropping `prompt_cache_retention: in_memory`. `file_search` and `code_interpreter` route to API-key upstreams when available. `response.metadata` events are removed only from translated/public-only streams, and `{detail}` errors are converted to public error objects. cc-lb stores no request or response data; only bounded request-scoped buffers are used, with a WebSocket owner body retained until that connection closes.

WebSocket `stream_id` lanes are emulated with one upstream socket per lane, typically pinned to one subscription account. The lane id is restored on client events and is never sent upstream. Lanes are bounded by admission and close on completion or disconnect; once a response lifecycle frame is emitted, an active response never moves accounts. Before any response starts, bounded pre-response recovery applies when a healthy eligible replacement exists: unchained turns can retry after pre-dispatch preparation/handshake failures or an open-socket provider `429` (`store: true` turns admitted on an API-key upstream restrict replacements to API-key upstreams, and account-owned references never move across accounts), under `[upstream_affinity] unavailable_openai_reasoning_owner = "strip_and_failover"` (default) `reasoning.encrypted_content`-only turns can strip unavailable owner items and re-dispatch when slot capacity permits, and Codex backend `previous_response_id` turns (`store != true`, with `conversation`, `compaction`, and account-owned references absent) can evict an unavailable owner with `400 previous_response_not_found` so Codex replays full history; continuations stay pinned under `"fail_closed"`, once a response begins, or on public `/v1/responses`. Continuation works on the latest response in the same lane; older, cross-lane, or reconnect continuations are rejected with `previous_response_not_found`.

The converted model catalog request adds cc-lb's built-in `client_version` only for public clients; a client-supplied value is preserved for Codex-shaped requests. Restricted native catalogs are filtered by the principal's allowed models; unrestricted native catalogs remain byte-identical. `memories/trace_summarize` is not mapped.

Not supported: realtime client secrets and provider operations with no safe translation. The [configuration reference](../site/src/content/docs/reference/configuration.md) describes policy settings.

#### OpenAI endpoint reference (API-key and OAuth)

| Method | Client path | Credentials |
|---|---|---|
| POST / GET WebSocket | `/openai/backend-api/codex/responses` | OAuth only (native passthrough) |
| POST | `/openai/backend-api/codex/alpha/search`, `/openai/backend-api/codex/images/generations`, `/openai/backend-api/codex/images/edits`, `/openai/backend-api/codex/realtime/calls` | OAuth only (native passthrough) |
| POST / GET WebSocket | `/openai/backend-api/codex/realtime/calls/{call_id}/hangup`, `/openai/backend-api/codex/realtime`, `/openai/backend-api/codex/live`, `/openai/backend-api/codex/live/{call_id}`, `/openai/backend-api/codex` | OAuth only (pinned call / public API origin) |
| GET | `/openai/backend-api/codex/models` | OAuth only (native catalog; allowlists filter entries) |
| POST / GET WebSocket | `/openai/v1/responses` | API key, OAuth |
| POST | `/openai/v1/responses/input_tokens`, `/openai/v1/responses/compact` | API key, OAuth (emulated) |
| POST | `/openai/v1/chat/completions` | API key, OAuth (translated) |
| POST | `/openai/v1/completions` | API key |
| POST | `/openai/v1/alpha/search` | OAuth |
| GET | `/openai/v1/models`, `/openai/v1/models/{id}` | API key, OAuth |
| POST | `/openai/v1/images/generations`, `/openai/v1/images/edits` | API key, OAuth |
| POST | `/openai/v1/audio/transcriptions` | API key; OAuth when enabled |
| POST / GET WebSocket | `/openai/v1/realtime/calls`; hangup, sideband, `/openai/v1/realtime`, `/openai/v1/live`, `/openai/v1/live/{call_id}` | OAuth (calls converted; public API origin) |
| POST | `/openai/v1/embeddings`, `/openai/v1/moderations`, `/openai/v1/audio/speech`, `/openai/v1/audio/translations`, `/openai/v1/images/variations` | API key |
| Any | `/openai/v1/responses/{id}`, `/openai/v1/responses/{id}/cancel`, `/openai/v1/responses/{id}/input_items` | API key (recorded owner, else single owner) |
| Any | `/openai/v1/files`, `/uploads`, `/batches`, `/vector_stores`, `/conversations`, `/containers`, `/evals`, `/fine_tuning`, `/assistants`, `/threads`, `/skills`, `/chatkit`, `/videos`, `/audio/voices`, stored `/chat/completions` | API key (single owner) |

OAuth operations drop the canonical `/v1/` prefix and append the remaining path to the configured Codex base URL. The realtime operations use the separate public API origin. The `x-codex-turn-state` response header is forwarded unchanged.

Account-owned resources live in one API account. A response created through cc-lb keeps its recorded owner. Any other account-owned request routes only when exactly one eligible `openai_api_key` upstream exists; with none it returns `404 operation_not_supported`, and with more than one it returns `409 upstream_affinity_unavailable` instead of guessing. These requests never fail over. A realtime hangup for a call cc-lb did not create returns `404 realtime_call_not_found`.

#### Gemini endpoint reference

| Method | Client path | Credentials |
|---|---|---|
| POST | `/gemini/v1beta/models/{model}:generateContent`, `:streamGenerateContent`, `:countTokens` | API key, OAuth |
| POST | `/gemini/v1beta/cachedContents` (create only) | API key |
| POST | `/gemini/v1beta/models/{model}:embedContent`, `:batchEmbedContents`, `:predict` | API key |
| GET | `/gemini/v1beta/models`, `/gemini/v1/models` | API key, OAuth |
| GET | `/gemini/v1beta/models/{model}`, `/gemini/v1/models/{model}` | API key |
| POST | `/gemini/v1beta/openai/chat/completions`, `/gemini/v1beta/openai/embeddings`, `/gemini/v1beta/openai/images/generations` | API key |
| GET | `/gemini/v1beta/openai/models`, `/gemini/v1beta/openai/models/{id}` | API key |
| WebSocket | `/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent` | API key |

OpenAI-compatible Gemini requests retain their `/v1beta/openai/...` path and use the Gemini API key as `Authorization: Bearer`. Native Gemini API-key requests use `x-goog-api-key`; OAuth supports generation, token counting, and the model-list routes above.

OAuth model-list requests call FetchAvailableModels with the selected upstream's stored project. The native response exposes only `name` from each model-map key and observed string `displayName` and `description` fields. The existing scoped response hook receives the raw metadata before projection; this does not add a durable catalog cache. OAuth single-model lookup is unsupported, and ListModelConfigs consumer support remains unverified. These routes do not establish a new raw Code Assist gateway or actual-client acceptance.

For native `streamGenerateContent` requests with an explicit `alt=sse` query pair and a Gemini API-key upstream, a successful upstream `application/json` response produces the existing Gemini SSE `api_error` and records `upstream_response_framing_error`. cc-lb rejects this framing mismatch without reading or parsing the upstream JSON body. OAuth envelopes, native `alt=json` or absent-`alt` JSON arrays, and non-2xx provider errors retain their existing behavior.

For the JSON/SSE mismatch described above and incomplete Gemini generation SSE events detected by the final framing check after upstream reading, cc-lb finalizes the lifecycle error type and message before delivering the terminal error frame. For these paths, request-event diagnostics survive a client dropping the response body immediately after receiving that error. This ordering guarantee does not cover earlier SSE inspection-limit failures.

On proxy request timeout, Gemini HTTP routes return HTTP `504` with a Gemini `DEADLINE_EXCEEDED` JSON error. Other HTTP routes return plain `request timed out`.

Realtime and Live WebSockets authenticate the proxy client before upgrade and select one eligible API-key upstream for the connection lifetime; OAuth subscriptions are not eligible. A connection never reconnects or fails over to another credential. Text, binary, ping, and pong frames retain their payloads; client/provider close frames retain their explicit code and reason while propagating tunnel shutdown. A later connection failure closes the tunnel rather than returning a new HTTP response.
Realtime and Live WebSocket admission uses an upstream-scoped circuit breaker keyed by the stable upstream UUID, independent of HTTP's upstream-scoped breaker. Provider `5xx` responses, transport failures, and timeouts count toward that WebSocket breaker; `4xx` and other non-`5xx` handshake responses do not. Handshake errors use a generic `upstream_unavailable` error code while preserving the provider HTTP status. Before upgrade, that status and safe allowlisted `Retry-After`, `x-ratelimit-*`, `anthropic-ratelimit-*`, and `ratelimit-*` headers are returned as HTTP response status and headers; after upgrade, the status is preserved in the WebSocket error, with any non-empty allowlisted headers under a top-level JSON `headers` object. An open local breaker returns a generic `502 upstream_unavailable` error. Once a provider connection owns a WebSocket, failures close it without reconnecting or failing over.

Gemini Live rejects the handshake with HTTP `503` and a Gemini `UNAVAILABLE` JSON error when the principal has no eligible Gemini API-key upstream, before receiving a setup frame. This rejection records one `route_no_upstream_after_filter` request event. With an eligible key, model-specific routing still runs after the setup frame supplies the model.

Realtime and Live routing filters receive the parsed, bounded header session identity as `RequestContext.thread_id`, using the existing header precedence. No setup-frame metadata fallback or Claude ancestry fields are added. Principal-limit denials publish one rejected limit decision through the shared audit and metrics path before returning the existing protocol-appropriate error; rejected sessions do not contact the provider.

Successful provider upgrades publish a provider-neutral response-started milestone. Active sessions publish cumulative usage snapshots from the provider usage frames, and final settlement uses the terminal cumulative usage without adding earlier snapshots again. Gemini Live snapshots retain reported thinking and tool-use prompt counts and distinguish absent, zero, positive, and invalid cache evidence. These observations do not reinterpret WebSocket frames as Anthropic SSE events or buffer the full media session.

Terminal observations retain bounded, redacted native close code/reason and available relay I/O details, along with the provider handshake request ID from `x-request-id`, `request-id`, or `x-goog-request-id`. Close frames still reach the peer unchanged. Upgraded connections retain status `101`, including downstream cancellations; native diagnostics do not synthesize HTTP/2 fields or a new HTTP response. The connection emits one terminal completion.

With OpenTelemetry tracing enabled, Realtime and Live provider handshakes receive the current proxy child span's W3C trace context after signing, without replacing host-owned credentials or destination details.

OpenAI's `OpenAI-Beta` header is passed through. Gemini client-supplied `key=` query credentials are removed before the configured upstream key is applied. Usage accounting observes only provider usage frames: OpenAI `response.done` and Gemini frames containing `usageMetadata`.

New tunnel handshakes during server drain receive HTTP `503` without upgrading. Existing tunnels stop relaying and send best-effort close frames, without an explicit close code or reason, to both peers. Idle Gemini connections waiting for a setup frame also close on drain and record status `101` with `drain_rejected`. After an upgrade, drain cannot produce a new HTTP response.

Not yet supported: cachedContents operations other than create, file upload/download, batches, operations, predictLongRunning, fileSearchStores, interactions, agents, auth_tokens, and OpenAI-compatible files/batches. Code Assist account controls are not proxy operations: loadCodeAssist, onboardUser, retrieveUserQuota*, listExperiments, metrics, admin controls, and user settings are excluded.

#### Unsupported operations and body limits

An unsupported operation under a provider prefix returns HTTP **404**, with a provider-shaped `operation_not_supported` error and the message `<method> <canonical-path> is not supported by cc-lb yet`. It produces one terminal request event after authentication, without selecting an upstream. Scoped Gemini Live WebSocket handshakes follow the same rule. Unprefixed unsupported paths retain their previous rejection behavior.

Multipart audio transcription/translation and image edit/variation requests use `files_body_cap_bytes`; other supported HTTP operations use `messages_body_cap_bytes`. Bodies and provider-owned state are forwarded without rewriting. Model catalogs and model-less default moderation requests still pass through key/principal, rate, and concurrency admission rather than requiring a fabricated model id.

OpenAI Responses `POST` requests accept identity bodies and authenticated `Content-Encoding: zstd` bodies on `/v1/responses` and `/openai/v1/responses`. The compressed input is bounded by `messages_body_cap_bytes`; the decoded body is bounded by the same cap. Malformed, truncated, checksum-invalid, stacked, duplicate, or unsupported encodings return `400 invalid_request_error` without upstream dispatch. A decoded body over the cap returns `413`. Authentication runs before the proxy reads or decodes the body.

#### OpenAI Responses WebSocket continuation and errors

An unstored response id belongs to its live WebSocket connection. Reusable reasoning
`encrypted_content` remains pinned to its upstream across WebSocket and HTTP requests,
including `store:false` turns. cc-lb records reasoning from `response.output_item.done`
and later response snapshots before forwarding those events. Existing SHA-256 affinity
rows keep their storage format.

A live `previous_response_id` does not override another upstream's reasoning ownership:
conflicting owners return `400 upstream_affinity_unavailable`; unknown, unavailable, or
epoch-mismatched owners return `409` (while affinity storage or persistence failures return `503`),
except for `reasoning.encrypted_content`-only recovery when `previous_response_id`,
`conversation`, `compaction`, and `compaction_summary` are absent or empty:
- When `[upstream_affinity] unknown_openai_reasoning = "hybrid"` (the default), unknown
  `reasoning.encrypted_content` (or unmatched pending `reasoning` items on a resolved owner)
  strips upfront across multiple eligible OpenAI upstreams, or tries verbatim first on a
  single eligible upstream or resolved owner and retries once on the same upstream if
  `invalid_encrypted_content` occurs before any response frame is emitted.
- When `[upstream_affinity] unavailable_openai_reasoning_owner = "strip_and_failover"` (the default),
  if the recorded owner upstream is disabled, quota-exhausted (`subscription_hard_blocked`), or
  fails before a response starts (`429`, `401` after refresh, signer failure, or open circuit
  breaker), connection-free `reasoning.encrypted_content` items are stripped and routed to another
  healthy non-exhausted eligible OpenAI upstream when one exists (preserving the owner's real `429`
  response when no replacement exists, or `409` if the owner cannot route). For Codex backend
  WebSocket chained `previous_response_id` turns (`store != true`, with `conversation`,
  `compaction`, and account-owned references absent), exhaustion or a pre-`response.created` `429`/
  availability failure when a healthy OAuth replacement exists evicts the owner and returns `400
  previous_response_not_found` so Codex replays full history onto the replacement; public
  `/v1/responses` does not synthesize `previous_response_not_found` on `429`. When a later
  OpenAI Responses turn carries `reasoning.encrypted_content` items from both an earlier
  failed-over owner and the current surviving owner (with no `previous_response_id`,
  `conversation`, `compaction`, or account-owned references), cc-lb selects the surviving
  eligible owner, strips stale items from earlier owners, and keeps the surviving owner's
  reasoning verbatim.

Client close frames receive a matching close reply. A valid provider error that cannot
be assigned to a response is forwarded unchanged once, then its provider connection is
retired. The downstream connection remains available for new turns.

#### OpenAI Responses streaming failures

Before a streaming `POST /v1/responses` response starts, a proxy-generated failure returns the route's provider-shaped JSON error envelope with the matching error status (500, 502, or 503). After streaming has begun, the HTTP status remains `200`, and a proxy-generated failure is sent as a `response.failed` SSE event. Its payload includes `sequence_number` and a failed `response` with safe `error.code` and `error.message`, plus the observed response id when one is known.

If an OpenAI Responses SSE body ends cleanly before `response.completed`, `response.incomplete`, `response.failed`, or another terminal event, cc-lb records `upstream_response_incomplete` and appends one `response.failed` event. Provider-reported usage remains authoritative, and existing provider errors retain precedence.

An emitted provider failure keeps precedence over later upstream faults. Response transforms determine which terminal the client actually receives. SSE responses use streaming HTTP framing instead of forwarding an upstream `Content-Length`, so local failures or suppressed malformed suffixes cannot leave an incomplete fixed-length body. Passthrough payload bytes and their content encoding remain unchanged.

#### OpenAI Chat Completions stream endings

For `/openai/v1/chat/completions`, clean SSE EOF succeeds after a finished choice (`finish_reason`) or a fully delimited `data: [DONE]` event. EOF before either signal records `upstream_stream_error` with `upstream_response_incomplete`. When cc-lb emits the SSE bytes (identity passthrough or decoded transformation), it appends one native Chat-compatible `event: error` with an `api_error` payload, after committing the failed completion. Already-forwarded content and HTTP `200` remain unchanged. Compressed raw passthrough keeps its original encoding and bytes; the incomplete outcome is recorded without appending a plaintext frame.

A transport body fault after an emitted, fully delimited `[DONE]` can complete successfully with a body-end warning under the existing emitter, framing, and error-precedence checks. A finished choice alone does not permit this: an `include_usage` stream may still have a usage-only chunk before `[DONE]`. Provider-reported usage is preserved, and missing usage or cost remains unknown. Provider errors and existing decode, framing, transform, affinity, and cancellation rules retain precedence.

Final usage from a failed Chat stream uses the same bounded assembler overflow as completion events; progress snapshots remain best-effort. This preserves the existing nonblocking request path and overflow limits.

### Request events API

The authenticated request-event endpoints accept the same filters:

- `GET /admin/v1/events/recent`
- `GET /admin/v1/events/histogram`
- `GET /admin/v1/events/delta`
- `GET /admin/v1/events/stream`

`event_kind` selects one or more effective event kinds. The HTTP query accepts either a comma-separated value or repeated keys; both forms combine into one set:

```text
event_kind=messages,openai_responses
event_kind=messages&event_kind=openai_responses
```

The filter applies to retained rows, histogram buckets, SSE backfill, and live partial and final updates. A repeated or comma-separated value with any unknown or empty entry returns `400 {"error":"invalid_event_kind"}`. An omitted `event_kind` keeps the default that excludes cache-keepalive renewal events. Supplying `event_kind` makes renewal an explicit effective kind, while `source_kind` remains conjunctive when both filters are present.

### Responses WebSocket gateway

`GET /v1/responses` authenticates the proxy client before returning `101`.
The provider connection opens lazily after a supported `response.create` has
passed current client-key, principal, model, operation, owner-affinity, and quota
checks. A revoked key or disabled principal can therefore block a later turn on
an already-open client connection. Provider connection failures after `101`
arrive as WebSocket errors or connection closure, not a new HTTP response.
Responses WebSocket admission uses an upstream-scoped circuit breaker keyed by the stable upstream UUID, independent of HTTP's upstream-scoped breaker, with the same failure policy as HTTP requests. Provider `5xx` responses, transport failures, and timeouts count toward that WebSocket breaker; `4xx` and other non-`5xx` handshake responses do not. Handshake errors use a generic `upstream_unavailable` error code while preserving the provider HTTP status. Before upgrade, that status and safe allowlisted `Retry-After`, `x-ratelimit-*`, `anthropic-ratelimit-*`, and `ratelimit-*` headers are returned as HTTP response status and headers; after upgrade, the status is preserved in the WebSocket error, with any non-empty allowlisted headers under a top-level JSON `headers` object. An open local breaker returns a generic `502 upstream_unavailable` error. Once a provider connection owns a response (after `response.queued`, `response.created`, or `response.in_progress` is emitted), failures close it without reconnecting or failing over. Before a response begins on a healthy replacement path: pre-dispatch recovery (`fail_dispatch_with_recovery`) handles `429` (excluding local `limit_rejected`), `401` (excluding client `authentication_error`), signing failures (`upstream_signing_failed`), open circuit breakers, and connect/preparation failures — evicting Codex backend `previous_response_id` owners (`store != true`, with `conversation`, `compaction`, and account-owned references absent) with `400 previous_response_not_found` when a healthy OAuth replacement exists, or stripping unavailable `reasoning.encrypted_content` under `[upstream_affinity] unavailable_openai_reasoning_owner = "strip_and_failover"` (default) when slot capacity or a reusable/coalescable socket permits. Separately, on an already-open provider socket before `response.queued`, `response.created`, `response.in_progress`, or any forwarded provider frame, provider rate/usage-limit `error` frames (`429`) can retire an idle failed socket and re-dispatch eligible unchained turns (including `reasoning.encrypted_content`-only turns under `strip_and_failover` and ordinary unpinned turns without account-owned references) when slot capacity or a reusable/coalescable socket permits, or evict Codex backend `previous_response_id` owners with `400 previous_response_not_found` when a healthy OAuth replacement exists; otherwise the original provider `429` is relayed unchanged. Under `"fail_closed"`, when no slot or replacement exists, or on public `/v1/responses` continuations, failures remain pinned without failing over. If all provider connections, active turns, and live response owners on an OAuth session upstream close, subsequent independent full-history turns on the downstream socket can select another eligible upstream, while continuations referencing an unavailable `previous_response_id` continue to return `400 previous_response_not_found`.

Pre-dispatch Responses WebSocket candidate failover tries at most 3 distinct upstream attempts within a 30 s total connect budget (15 s per-attempt timeout covering setup, lazy OAuth refresh cancellation-safety waits, and the upstream handshake; OpenAI Realtime and Gemini Live keep their 30 s connect timeout). Candidates whose upstream-scoped local WebSocket circuit breaker is already open or whose unpinned fallback subscription quota is freshly exhausted are skipped immediately without consuming a connect attempt slot, and on public `/v1/responses` the final attempt is reserved for an eligible API-key candidate when one remains (while native Codex remains OAuth-only).

Ordinary `response.create` messages require an explicit, non-empty `model`.
cc-lb does not infer a missing model from a stored prompt or an account default.
Steering auto-successors inherit the parent turn's model. This is a bounded
generation gateway; support for the upgrade does not imply support for every
Responses operation or model-inheritance form.

Independent full-history turns can select another eligible upstream.
Active continuations and responses that have begun emitting events stay on the
exact provider connection that owns their state; stored `previous_response_id`
continuations (non-Codex or on Codex with `store: true`), `conversation`,
`compaction`, `compaction_summary`, non-reasoning account-owned references,
public `/v1/responses` continuations, and `"fail_closed"` requests never fail
over across accounts, while unchained turns with explicit `store: true`
admitted on an API-key upstream restrict pre-response failover replacements to
API-key upstreams rather than downgrading to OAuth, and eligible pre-response
recovery follows the bounded contract above. Stored response and conversation
references retain their durable upstream affinity. Work is FIFO within each lane,
while independent lanes can run in parallel. Existing request-body caps and the
WebSocket bulkhead bound buffered work and provider connections.

Response identity is bound before forwarding `response.queued`,
`response.created`, or `response.in_progress`. WebSocket response ids from
`store: false` turns remain connection-local instead of creating new durable
response-id bindings. Conversation ownership remains durable independently of
that flag. The HTTP response-id behavior described above is unchanged.

The gateway parses control messages once and forwards ordinary deltas unchanged,
without rewriting them as SSE or reserializing the client request body. Client
`response.interrupt` control frames are forwarded unchanged to the provider
WebSocket connection that owns the target `response_id` after verifying
principal and optional lane ownership, without creating a new turn or failing
over, and provider `response.interrupt.accepted` / `response.interrupt.failed`
acknowledgements are relayed back to the client. Closing or disconnecting
still cancels active turns; cc-lb does not add a `response.cancel` wire
operation. A terminal turn releases its concurrent slot immediately.
Measured usage, including a reported numeric zero, is reconciled once by the
background accounting worker even on an unsuccessful turn. Without measured
usage, the reservation is refunded once; `null` or an empty usage object alone
does not count as measured usage. Existing request and trace identifiers
correlate turn events without new database fields.

On `/v1/responses` WebSocket turns, a clean provider `response.incomplete` event
with an absent or `null` `response.error` completes the turn with
`outcome="completed"` and reconciles observed usage as complete. The assigned
upstream signer scrubs any non-empty `incomplete_details.reason` before redaction
and bounding; the credential-scrubbed, redacted, bounded reason is recorded under
`upstream_stream_warning_type="response.incomplete"` and
`upstream_stream_warning_message`. Explicit non-null `response.error` objects,
`response.failed` frames, and attributed `type: "error"` frames (reading top-level `error.code`, `error.type`, and `error.message` with credential scrubbing) remain upstream provider errors. When an upstream emits an attributed `type: "error"` frame followed by `response.failed` for the same `response_id`, cc-lb settles the turn once (capturing usage and running the original turn's response transform hook on `response.failed`, or settling after a bounded 2 s timeout / connection close) while preserving forwarding and keeping queued successor turns isolated; if an upstream emits a statusless in-response `type: "error"` and closes its WebSocket before sending `response.failed`, cc-lb emits a `502 upstream_connection_closed` error frame.
`cc_lb_stream_terminations_total` attributes turns to their assigned upstream
name, using `"unknown"` only when a dispatched turn fails before upstream
assignment.

### Generation usage accounting

Gemini GenerateContent's normalized output count is `candidatesTokenCount + thoughtsTokenCount`.
The reasoning count remains available as a subset of that normalized output;
cost and limit accounting must not add it again. Repeated cumulative GenerateContent
usage snapshots replace the previous counts instead of accumulating them. The
GenerateContent SSE accumulator retains the last component when a field is missing or `null`.

Native Gemini generation requests accept both `service_tier` and `serviceTier`
for routing and cost reservations; `service_tier` takes precedence when both
appear. A blank selected tier is absent from Gemini metadata; a non-blank
tier keeps its original bytes. The original request bytes are forwarded unchanged. Completed-request
costs use the observed `usageMetadata.serviceTier` or `service_tier`, including
wrapped `response.usageMetadata`; the requested tier never substitutes for a
missing response tier.

Request history recognizes both `generationConfig.thinkingConfig.thinkingLevel`
and the native SDK spelling `generationConfig.thinkingConfig.thinking_level` as
optional `thinking_level` metadata. It likewise recognizes `thinkingBudget` and
`thinking_budget` as optional `thinking_budget_tokens` metadata. CamelCase takes
precedence when both leaf aliases are present, including when that value is
malformed. Non-empty levels up to 64 bytes retain their original casing.
Thinking level does not affect routing or token accounting.

The existing cache-input subtraction is unchanged: reported cache-read tokens
are counted separately from uncached input. OpenAI and Anthropic output already
includes reasoning or thinking tokens, so their output counts are unchanged.

Completed request costs price uncached input, cached reads, and output as
disjoint token sets. OpenAI and Gemini cached-read rates come from their native
catalog entries, including published service-tier and context-threshold rates;
they do not fall back to Anthropic cache rates. Positive cached usage without
an applicable rate leaves actual request costs unknown. A published zero rate
remains a known zero, and a request with no cached usage does not require a
cached-read rate.

When a completed request's actual cost is unknown, managed-key limit accounting
finalizes its original admitted estimate as CostUsd liability. Request and token
usage still settle once, and concurrency is released. The request's actual-cost
fields remain empty. If no estimate was available and no CostUsd limit applied,
only measured request/token usage is added; the monetary accumulator is unchanged.
No unavailable cost is reported as a known zero, and finalized rows are not
re-priced later.

Keep-alive renewals retain their token usage, hit identity, and history when
actual cost is unavailable; their monetary PNL stays unknown. An automatic
storage upgrade records price availability separately from signed cost values
and preserves existing renewal costs as known.

Dashboard rollups preserve price completeness separately from cost totals.
The optional `pricing_completeness` field is one of `priced`, `known_zero`,
`partial`, `unpriced`, `unmetered`, or `unknown`. Nonempty local summaries and
buckets also include `priced_request_count`, `unpriced_request_count`, and
`unmetered_request_count`, with explicit zeros for unused categories.
Empty summaries and buckets omit all four fields. Estimated API-key month
totals carry these fields; provider-reported organization usage does not.

`known_zero` means a recorded price produced zero cost. `unmetered` requires
complete classification with no billable usage. `unpriced` means classified
billable usage lacks a price. An unclassified remainder makes the result
`partial` only when there is a priced request or positive recorded cost;
otherwise it is `unknown`, even with zero recorded tokens and cost or a mix
of only unpriced and unmetered requests. Known positive subtotals stay visible,
and a priced zero with an unclassified remainder is still partial.
The UI displays `unpriced` and `unknown` as a dash with `List price unavailable`
or `Price unknown`, and partial subtotals with `+`.
Indeterminate amounts do not contribute a fabricated zero to cost shares.

Storage upgrades add zero-default classification counters; ordinary rollup
ingestion updates them with the same request, token, cost and checkpoint
transaction. Existing rollup totals receive no historical pricing
classification, even when raw events remain. Old workers can add totals
without classification and leave new gaps. Updating all workers prevents new
gaps but never repairs existing ones. Old readers may still show unknown
amounts as zero, so readers also need the upgrade.

The independent historical category-cost backfill remains unchanged.
Dashboard validators include a semantics version, the ingestion checkpoint
and that category-cost cursor; old semantics invalidate cached responses
without new traffic. There is no pricing-classification recovery cursor.
Gemini model-identity merges preserve available counters and known totals;
any remaining classification gap stays subject to the same conservative rule.
Scheduled OpenAI Responses renewals record canonical uncached `input_tokens`
and `Option` native cache-write counters matching foreground requests.
Effective-write totals retain available evidence and native-write lower
bounds, without promising exact reconstruction for historical rollups.


Before dispatch, Anthropic Messages limit admission estimates text and metadata
string bytes at one token per four bytes, plus 4,784 tokens per recognized native
`image` block in message content, including images inside `tool_result.content`.
The allowance replaces only the image's base64 payload, URL, or file-ID reference
in the string-byte estimate; other metadata still counts. A single 4,000-token
floor applies to the combined request input, including images. This input estimate
is used for both input-token and total-token admission.

With resolved local prompt-cache breakpoints, cost reservations price disjoint
cache-prefix and uncached-tail regions measured on that same admission basis.
The request floor applies once; any deficit belongs to the uncached tail. The
tail uses the input rate. The prefix through the deepest requested 1-hour
breakpoint uses the highest of the input, 5-minute write, cache-read, and twice
the active input rate; the remaining local prefix uses the highest of the input,
5-minute write, and cache-read rates. Optional absent catalog rates are ignored,
but the derived 1-hour rate still applies. Rates follow the active service tier;
the price band uses the request's input counted once. No warm hit is assumed.
The existing opaque-cache worst-case allowance applies only when no local
breakpoint resolves, including the existing fallback for unresolved markers.
Resolved local breakpoints keep precedence even when an opaque marker is present.

Native OpenAI Responses `input_image`, Chat Completions `image_url`, and Gemini
image `inlineData`/`fileData` parts use provider-specific image quantities
independent of payload or reference length. Gemini OpenAI-compatible Chat uses
Gemini quantities. Recognition follows native content positions; arbitrary
tool arguments and non-image MIME parts keep their existing estimates. Only the
recognized image's payload/reference strings are excluded; sibling metadata
still counts.

For inline Base64 images, cc-lb uses bounded 4,096-byte header inspection to
read positive PNG, JPEG, GIF, or WebP dimensions. It does not decode pixels,
fetch URLs or files, or make a provider call. Unsupported formats, malformed or
truncated headers, and remote/file references use the unavailable-dimensions
policy below. An image remains an image even when its dimensions are unavailable.

OpenAI quantities follow the model-specific rules in
[Images and vision](https://developers.openai.com/api/docs/guides/images-vision#calculating-costs):

- Tile-based models use their own base and per-tile coefficients. `low` uses
  only the base; `high` and `auto` resize within 2048 pixels, then reduce a
  shortest side above 768 pixels before counting 512-pixel tiles. For `gpt-4.1`,
  `low` contributes 85 tokens and a 1024x1024 image at `high` contributes 765.
- Patch-based models use 32-pixel patches, the selected model/detail's
  dimension and patch budgets, and its multiplier, rounded up. `low`, `high`,
  `original`, and `auto` are model-specific; `low` need not be the cheapest,
  and `auto` can select `original` sizing.
- Only explicit known model and snapshot entries select these rules. Omitted
  `detail` means `auto`. A known model with unsupported or malformed `detail`
  uses the largest quantity among its supported details, or the largest
  supported-detail ceiling when dimensions are unavailable. With a supported
  detail but unavailable dimensions, its model/detail ceiling applies.
  Unknown OpenAI models receive 73,800 admission tokens per image.

Known Gemini 1.x and 2.x models use the rough crop rule from the
[image token guide](https://ai.google.dev/gemini-api/docs/image-understanding#token-calculation).
Images with both dimensions at most 384 pixels use 258 tokens. Otherwise, with
positive width `w` and height `h`, the crop unit is `floor(min(w, h) * 2 / 3)`;
the estimate is `ceil(w / crop) * ceil(h / crop) * 258`. A zero crop unit
counts as unavailable dimensions. Unknown Gemini models and known pre-3 models
without usable dimensions receive 65,536 admission tokens per image. This
allowance does not cap quantities calculated from known dimensions.

Known Gemini 3 and 4 models use the
[media-resolution table](https://ai.google.dev/gemini-api/docs/media-resolution#token-counts)
regardless of image dimensions:

| Image resolution | Admission tokens per image |
|---|---:|
| Omitted or `unspecified` | 1,120 |
| `low` | 280 |
| `medium` | 560 |
| `high` | 1,120 |
| `ultra_high` | 2,240 |
| Present but malformed or unsupported | 2,240 |

A per-part resolution overrides the request's generation configuration.
Resolution accepts a string or an object with `level`, using short names or
`MEDIA_RESOLUTION_*` values.

These are approximate pre-dispatch admission policies. The 73,800 and 65,536
fallbacks are explicit local allowances, not exact billing quantities or global
provider maxima. Image estimates can over-reserve, and provider-specific
overhead can exceed an allowance. A single 4,000-token floor applies to the
combined text, metadata, and image input for input-token and total-token
admission; it is not applied per image. Reservations that cannot fit still
reject before dispatch.

Anthropic's 4,784-token allowance and cache-region accounting above are
unchanged. Text, tools, documents, and other non-image content retain their
existing estimates. Provider-reported usage remains authoritative for
reconciliation and actual accounting. The existing conservative generic/image
rate maximum for image-rate CostUsd admission is unchanged, as are actual
image-rate pricing, routing, and forwarded request bytes.

See [Overview](#overview) for local HTTP limit rejection and retry behavior.

The [Admin consistency contract](consistency-contract.md) lists what each admin response guarantees about persistence, audit records, the routing-view rebuild, and convergence across replicas.

### Local proxy key files

If an operator keeps a local proxy client key file such as `~/.config/cc-lb/proxy-key`, treat it as a client-side cache of the plaintext key returned once by `POST /admin/v1/principals/{id}/keys`. Rotating a managed key is a two-step lifecycle: issue the replacement key, update the local key file atomically with mode `0600`, verify the proxy request path with the new key, then revoke the old key through the admin API. The server never rewrites operator key files automatically.

### Upstreams API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| POST | `/admin/v1/upstreams` | Bearer | UpstreamCreateBody | UpstreamResponse | `conflict`, `invalid_location`, `invalid_input`, `missing_api_key`, remediation codes |
| GET | `/admin/v1/upstreams` | Bearer | None | List of UpstreamResponse | None |
| GET | `/admin/v1/upstreams/{id}` | Bearer | None | UpstreamResponse | `upstream_not_found` |
| PUT | `/admin/v1/upstreams/{id}` | Bearer | UpstreamUpdateBody | UpstreamResponse | `stale_revision`, `upstream_not_found`, `invalid_input`, remediation codes |
| PATCH | `/admin/v1/upstreams/{id}` | Bearer | UpstreamPatchBody (`enabled`, `warmup_enabled`, `name`, ...) | UpstreamResponse | `stale_revision`, `upstream_not_found`, `invalid_input`, remediation codes |
| DELETE | `/admin/v1/upstreams/{id}` | Bearer | None | None | `referenced_by`, `stale_revision` |
| POST | `/admin/v1/upstreams/preflight` | Bearer | PreflightRequest | PreflightResponse | `missing_api_key`, `invalid_input` |
| POST | `/admin/v1/upstreams/{id}/preflight` | Bearer | None | PreflightResponse | `not_api_key_upstream`, `upstream_not_found`, `api_key_credential_decrypt_failed` |
| GET | `/admin/v1/upstreams/{id}/workspaces` | Bearer | None | `{"workspaces":[{"id","name"}]}` | `not_api_key_upstream`, `non_anthropic_base_url`, `workspace_list_denied`, `probe_unreachable`, `api_key_credential_decrypt_failed` |
| GET | `/admin/v1/upstreams/{id}/usage?range=&step=` | Bearer | None | UpstreamKeyUsageResponse | `not_api_key_upstream`, `upstream_not_found`, invalid `range`/`step` |

#### Subscription limit resets

`GET /admin/v1/upstreams/limit-resets` returns fleet-wide cached reset snapshots, grouped by upstream and provider. `GET /admin/v1/upstreams/{id}/limit-resets` reads one snapshot. Neither route contacts the provider. Missing, stale, credential-mismatched, or pending-claim observations do not establish availability.

Overview shows coupon inventory in a small ticket-count column (`Ticket × N`) in the **Upstreams** table only when at least one visible upstream supports limit resets (`anthropic_oauth` or `openai_oauth`), and omits the `Coupons` column and fleet inventory polling completely in Gemini-only or API-key-only views; it never appears on principal rows. In mixed tables, only supported rows render a coupon button. Each supported upstream displays its own cached provider inventory; rows that share a provider account are not deduplicated into a combined count. The count describes active remaining coupons, not immediate usability or guaranteed model routing. Missing or stale inventory remains unknown rather than zero.

Opening a known positive count shows the coupon confirmation, even when the provider currently blocks use. Zero, unknown, failed or disconnected inventory opens History instead. History remains available while inventory loads. If coupons become available while you read History, select View coupons to inspect them; polling does not switch the view automatically. An unresolved reset opens its pending-result view first.

Anthropic grants retain the provider's label, raw `type`, `gives_back_weekly`, `unavailable_reason`, remaining count, expiry, eligibility, and exact `clears` scope. Window names normalize `five_hour` to `5h`, `seven_day` to `7d`, and `seven_day_*` to `7d_*`; other names remain unchanged. The observed `type: "full"` with `gives_back_weekly: true` is presented as a “Fable usage reset,” not “All weekly.” `7d_overage_included` maps 1:1 to `7d_fable` (`Weekly (Fable)`, matching the unified rate-limit header `anthropic-ratelimit-unified-7d_oi-*`), so coupons that include `7d_overage_included` in `clears` (such as weekly resets and Fable usage resets) display `Weekly (Fable)` as resetting to `0% used`, while preserving the exact provider key in Details (and including `Weekly (Fable portion)` in Details `Scope:` for Fable grants). Non-cleared model-scoped weekly windows (such as `7d_sonnet` and `7d_opus`) and `overage` remain `Unchanged`, and an unobserved `5h` window is omitted on Fable grants. While `Weekly (Fable)` resets deterministically to `0% used` and Fable resets return a positive Fable portion when `Weekly (Fable) > 0%`, shared `Weekly` shows the qualitative outcome `Fable portion restored` instead of projecting a numeric percentage.

The confirmation dialog shows each relevant allowance's current usage, expected effect, and next-refresh behavior for the selected coupon. Missing or stale usage remains unknown. Fable giveback is qualitative: the UI never subtracts independent quota percentages to predict the remaining weekly usage. Undocumented provider scope fields remain in Details. Expiry uses relative time with the exact timestamp available in a tooltip. The browser's local pending marker suppresses resubmission after a pending or uncertain claim; this is separate from the server's in-flight claim fence.

Guidance above Use considers the selected coupon's current usage, natural resets, elapsed-period pace and expiry. For Fable grants, a fresh exhausted `Weekly` limit with positive `Weekly (Fable) > 0%` produces an informational `useful_now` notice (and triggers limit nudges and selection priority behind direct full resets); `Weekly = 100%` with `Weekly (Fable) = 0%` warns about `scope`; `Weekly < 100%` with `Weekly (Fable) = 0%` (and no 5h usage) yields `nothing_to_restore`; and sub-full positive Fable usage follows the standard `wait`, `save` (requiring both `Weekly` and `Weekly (Fable)` to satisfy the pace margin), and `expiry` rules while otherwise staying quiet without an uncertain warning. Only stale or missing runtime usage, reset, or expiry data produces an `unknown` warning. Cautions are warnings; a useful-now recommendation is informational. Guidance never disables Use or overrides provider restrictions.

OpenAI subscriptions expose banked reset-credit inventory and explicit credit consumption. These one-time resets are separate from usage-credit balances, automatic promotional resets, and paid instant-reset purchases. Preserve the provider's `available_count`, `applicable_available_count`, and each credit's `is_supported_by_plan` as distinct inventory and eligibility facts; the returned list may be capped. Unknown `reset_type` and status values remain provider values; titles and descriptions do not establish scope or eligibility. cc-lb does not offer a Gemini coupon claim without a verified provider contract.

The OpenAI confirmation dialog preserves `reset_type` and reports plan support as supported, not supported, or not reported. For `codex_rate_limits`, it describes the main Codex allowance; it does not map a credit title or support flag to specific plan windows. [OpenAI's reset guidance](https://help.openai.com/en/articles/20001498-how-resets-work-in-codex) explains full banked resets by plan: monthly for Go, five-hour and weekly for Plus and Business Standard, and weekly for Pro 100, Pro 200, and Pro 500. That guidance does not establish an individual credit's availability. A reset can change the affected allowance's next refresh time without changing the billing date; use refreshed usage instead of preserving the old countdown.

Manual OpenAI banked resets require at least one covered main allowance to have less than 80% remaining: more than 20% used, strictly. This is separate from the desktop assistant's automatic-use setting. cc-lb derives the manual condition from the account's native primary usage windows, excluding additional model limits and usage credits. The cached dialog observation controls the button; the server checks fresh provider usage before consumption. Unknown eligibility does not authorize redemption. Covered allowances with usage are restored; an allowance already at 100% remaining is not restarted. The new refresh time is confirmed from usage after the reset, not calculated from the click time.

[Claude's reset guidance](https://support.claude.com/en/articles/17007452-what-is-a-limit-reset) and [Fable launch offer](https://support.claude.com/en/articles/17506494-claude-fable-5-2-launch-offers) explain that a Fable reset returns only the Fable portion of shared weekly consumption; usage spent on other models remains. It also refills the whole five-hour window when the account has one and clears `7d_overage_included` (`7d_fable`). Weekly limits keep their usual reset day and time. These documented effects do not reveal the account's numeric rebate on the shared `7d` window.

`POST /admin/v1/upstreams/{id}/limit-resets/claim` verifies the current provider account and submits the explicitly selected grant or credit with one provider POST. Authentication and authorization precede provider calls. A durable claim fence prevents overlapping redemption while the claim is in flight. Settlement advances the epoch after every outcome, including an unknown outcome, so older snapshots cannot become usable again. The fence remains pending only while work is in flight or settlement fails. A timeout, lost reply, or uncertain outcome is never replayed automatically.

The response keeps the successful terminal `reset` or `already_used` result separate from `usage_refresh` and nullable `windows_reset`. An unknown reset-window count remains `null`, not zero. After either result, the handler awaits an immediate usage read, a fenced quota transaction, and publication to the serving replica's routing cache before returning `usage_refresh: "refreshed"`. For Anthropic OAuth upstreams, when `/api/oauth/usage` responds with HTTP `429`, cc-lb records `429` in `oauth_usage_poll_cursors` and falls back to a lightweight `/v1/messages` probe, publishing real `anthropic-ratelimit-unified-*` response headers under the same generation and reset-epoch fence only when at least one main quota window (`5h`, `7d`, `7d_fable`) is observed. `"failed"` preserves the reset result but reports that affected main quota freshness was not established, including when the refresh integration is unavailable or the fallback probe receives no main quota observations; it never triggers another redemption or replaces usage with optimistic zeroes. `"not_applicable"` means the upstream is no longer eligible for usage polling. Post-commit work retains the cancellation guarantee described in the [consistency contract](./consistency-contract.md#admin-mutations-and-audit-records).

For Anthropic and OpenAI polls, the quota transaction checks the captured OAuth generation and reset epoch before writing latest observations and checkpoints. Epoch settlement and quota publication serialize in storage, so a delayed pre-reset poll cannot commit stale quota after the reset. Cache publication then rejects older request sequences after a newer poll has published. Fenced observations are written directly rather than left in the asynchronous sample queue, and provider requests run outside cache synchronization. This guarantees the serving replica's publication boundary; peer replicas still converge asynchronously through their notification and hydration paths. No schema or migration is added.

Coupon inventory remains usable independently of routing-window support. OpenAI monthly main usage still supplies inventory and manual-redemption eligibility when ordinary usage credits are absent. Because monthly routing quota is not represented, a monthly-only observation does not report `usage_refresh: "refreshed"`. Publishing inventory, ordinary usage credits, or an additional model allowance alone is insufficient; the reset result remains successful while `usage_refresh` reports `"failed"`.

Valid monthly observations remain normal successful scheduled polls and record a `200` attempt cursor. The scheduler's provider-success classification is separate from the post-reset refresh guarantee: without supported main quota publication, `usage_refresh` remains `"failed"`.

`GET /admin/v1/upstreams/{id}/limit-resets/history?limit=100` requires the same sensitive-read permission as the audit endpoint. It returns `{items, has_more, limit}` for recent claim attempts recorded by cc-lb, filtered by the exact claim action and upstream UUID route before the database limit. Upstream renames do not change that identity. The limit defaults to 100 and accepts 1–1000; `has_more` marks a bounded recent view, not a complete ledger.

Each item reports cc-lb's audit-write time as `recorded_at_unix_millis`, the normalized provider result, and any recorded provider type, weekly-return flag, actor, request ID, cleared windows, reset-window count, and usage-refresh outcome. Missing metadata stays `null`. Errors without a known provider result and older HTTP-success rows without a normalized result remain `unknown`; neither proves that a reset was used or unused. Inventory, grant dates, and disappeared credits do not establish reset history or provider redemption time.


#### UpstreamCreateBody

```json
{
  "name": "upstream-name",
  "kind": "anthropic_api_key",
  "base_url": "https://api.anthropic.com",
  "api_key_value": "sk-ant-..."
}
```

Vertex upstreams use the same `UpstreamCreateBody` shape with these additional
fields:

```json
{
  "name": "vertex-regional",
  "kind": "vertex_api_key",
  "api_key_value": "<GOOGLE_API_KEY>",
  "project_id": "my-project",
  "location": "us-central1",
  "express": false,
  "api_version": "v1"
}
```

`location`, `express`, `google_credential`, `api_version`, and `project_id`
are optional on create. They are meaningful only for `vertex_api_key` and
`vertex_credential`. `google_credential` contains the JSON credential string
for a regional `vertex_credential` upstream and is never returned. `express`
selects the Vertex Express API-key target; regional targets require
`project_id` and `location`. `api_version` is `v1` or `v1beta1`.


`api_key_value` carries the literal API key. The API encrypts the credential for database storage and never returns the key; responses carry a masked `key_hint` instead. Keep literal credentials out of saved commands, shell history, and source control.

`anthropic_api_key` creates also accept:

- `workspace_id`: the `wrkspc_…` workspace an org-scoped key acts in. cc-lb sends it as `anthropic-workspace-id` on every proxied request for the upstream. It must be `wrkspc_` followed by 1–120 letters or digits. A key bound to one workspace rejects it with `400 invalid_input` (`field: "workspace_id"`).
- `skip_preflight`: `true` stores the key without contacting the provider. The upstream is saved unverified and reports `usage_never_checked` until it is re-checked.

Create normalizes the key, applies the local prefix checks, and runs preflight before anything is stored (see [API-key upstreams](#api-key-upstreams)). A rejected key answers with a nested remediation body, `{"error":{"code":"key_admin","message":"…"}}`, and stores nothing. A key that passes answers `201` with `api_key_access` and `key_hint`.

#### UpstreamUpdateBody

```json
{
  "base_url": "https://api.anthropic.com",
  "api_key_value": "sk-ant-...",
  "workspace_id": "wrkspc_...",
  "enabled": true
}
```

Every field is optional, and `PUT`/`PATCH` require `If-Match`. A missing or `null` `base_url` keeps the override and `clear_base_url: true` removes it; sending both is `400 conflicting_base_url`. `workspace_id` works the same way with `clear_workspace_id: true`, except that sending both is `400 invalid_input` with `field: "workspace_id"`. `skip_preflight` applies as on create.

On an API-key upstream, changing the key, workspace, or `base_url` re-runs preflight before the write. A `base_url`-only change never fails on the probe result; the stored access snapshot reflects the new host. The key does not have to be re-entered: an admin who can edit an upstream can direct its stored key to any host they set as `base_url`. The change is audited with the old and new hosts, never the key, and the key is re-probed at the new host. Successful create and update responses for an API-key upstream carry `Cache-Control: no-store`, like preflight.

Any upstream `base_url` must use `http` or `https` and must not contain userinfo; other values are rejected on write.

#### UpstreamResponse

```json
{
  "id": "<UPSTREAM_ID>",
  "name": "upstream-name",
  "kind": "anthropic_api_key",
  "enabled": true,
  "warmup_enabled": false,
  "warmup_dialect_plugin": null,
  "spec_revision": 1,
  "key_hint": "sk-ant-api03-...wxyz",
  "api_key_access": {
    "verified": true,
    "checked_at_unix_secs": 1760000000,
    "key_class": "org_scoped",
    "inference": "ok",
    "workspace": { "mode": "selected", "id": "wrkspc_..." },
    "workspace_list": "ok",
    "usage_report": "ok",
    "key_type": null,
    "last_error": null
  },
  "status": {
    "last_apply_error": null,
    "last_apply_at_unix_secs": null,
    "last_warmup_at_unix_secs": null
  }
}
```

For Vertex responses, `vertex` is present for `vertex_api_key` and
`vertex_credential` upstreams:

```json
"vertex": {
  "project_id": "my-project",
  "location": "us-central1",
  "api_version": "v1",
  "express": false,
  "credential_type": "service_account",
  "principal_hint": null,
  "quota_project_id": null
}
```

The response object contains exactly `project_id`, `location`, optional
`api_version`, `express`, `credential_type`, `principal_hint`, and
`quota_project_id`. Credential and principal values are metadata; the
credential secret is never returned.

`spec_revision` is the user-managed optimistic-lock counter used by `If-Match` and `ETag`. Background controller writes (status/lease/secret/token) never bump it. The nested `status` object holds system-managed operational state: only the apply daemon and warmup observer write here. `status.last_warmup_at_unix_secs` is the time any warm-up lane last succeeded. `warmup_enabled` defaults to `true` for `anthropic_oauth`, `openai_oauth`, and `gemini_oauth` upstreams created through `POST /admin/v1/upstreams` or the OAuth sign-in flow. For API-key upstreams, omitted or `false` remains disabled and explicit `true` is rejected; see [docs/upstream-warmup.md](./upstream-warmup.md).

`key_hint` and `api_key_access` appear only on `anthropic_api_key` upstreams that have been checked. `key_hint` is the key's `sk-ant-<family>-` prefix, plus `...` and the last four characters when the key is at least 20 characters long. `api_key_access` is the last stored capability snapshot. `verified` is `false` until a probe succeeds. `key_class` is `workspace_bound`, `org_scoped`, or `unknown`. `workspace.mode` is `bound` when a check found the key works without a workspace header (`id` is set when Anthropic named the workspace), `selected` when cc-lb sends a chosen workspace `id`, and `required` with no `id` otherwise: the key was never checked, was rejected locally or by the endpoint, was unreachable, or still needs a workspace. `workspace_list` and `usage_report` are `ok`, `denied`, `error`, or `not_applicable`. `key_type` is `personal`, `service_account`, `workspace`, or `null` when Anthropic did not say.

#### API-key upstreams

cc-lb serves standard Console API keys: workspace keys (`sk-ant-api03-…`) and identity-linked personal or service-account keys (`sk-ant-usr-…`), which act in every workspace and need a `workspace_id`. Before any network call, it trims outer whitespace and rejects a key with inner spaces, line breaks, or non-ASCII characters, or one longer than 512 bytes, with `400 invalid_input` (`field: "api_key_value"`). It then rejects three key families locally, because they cannot call models:

| Prefix | Class | Code |
|---|---|---|
| `sk-ant-admin…` | Admin key | `key_admin` |
| `sk-ant-api01…` | Enterprise management key | `key_enterprise_scoped` |
| `sk-ant-oat01…` | OAuth access token; use an `anthropic_oauth` upstream instead | `key_oat` |

Other prefixes, including unknown ones, go to the probes. Preflight always answers `200` with `Cache-Control: no-store`. The `verdict` is `ok`, `needs_workspace`, `rejected`, or `unreachable`. The response also carries `key_hint`, `classification`, `workspaces`, `capabilities`, `remediation[]`, and `checked_at`. The candidate route (`POST /admin/v1/upstreams/preflight`) stores nothing. The re-check route (`POST /admin/v1/upstreams/{id}/preflight`) probes the stored key and persists the new snapshot without bumping `spec_revision`. `GET /admin/v1/upstreams/preflight` returns `405`.

The inference probe sends `GET /v1/models`, then `POST /v1/messages/count_tokens`, to the same URLs proxied traffic uses: the scheme, host, and port of the configured `base_url`, with any path on `base_url` replaced by the request path. A `2xx` response that carries `anthropic-workspace-id` marks the key `workspace_bound`. A key whose account can act in several workspaces gets a `400` saying the request must include the `anthropic-workspace-id` header, so preflight reports `needs_workspace` with `workspace_required`. When a supplied workspace id is rejected by Anthropic (`400` "must be a valid workspace ID" or `404`), preflight reports `workspace_not_found`. A `2xx` with neither signal is classified `unknown` and accepted.

The org Admin API probes (workspace list, usage report, and per-key attribution) run only for an `org_scoped` key whose `base_url` is exactly `https://api.anthropic.com`: same scheme, host, and port, a root path, and no userinfo, query, or fragment. Any other host reports these capabilities as `not_applicable` with `non_anthropic_base_url`, and usage stays estimated. Probes never follow redirects, have bounded time and body size, and scrub the key from every stored or returned provider message.

The response's `remediation[].code`, the nested failure `error.code`, and `/usage` `fix.code` use one vocabulary:

| Code | Status | Meaning | Fix |
|---|---|---|---|
| `key_admin` | 400 | Admin key; it manages the org and can't call models | Use a standard Console API key |
| `key_enterprise_scoped` | 400 | Enterprise management key; no inference | Use a Console API key |
| `key_oat` | 400 | OAuth token; it expires and cc-lb cannot refresh it | Create an `anthropic_oauth` upstream |
| `key_invalid` | 400 | The configured endpoint answered `401` | Paste the key again or create a new one |
| `key_forbidden` | 400 | Valid key, but `count_tokens` answered `403` | Save it unverified with `skip_preflight` |
| `workspace_required` | 400 | The key acts in several workspaces | Send `workspace_id` |
| `workspace_not_found` | 400 | The key can't see that workspace | Check the id or the account's access |
| `workspace_id_malformed` | 400 | The id does not start with `wrkspc_` | Send a `wrkspc_…` id |
| `workspace_list_denied` | 403 | The key's account lacks the role to list workspaces | Copy the `wrkspc_…` id from Console |
| `non_anthropic_base_url` | 403 | `base_url` is not `https://api.anthropic.com`, so org probes do not run | Use the default endpoint for reported usage |
| `probe_unreachable` | 502 (504 on timeout) | The endpoint did not answer, or answered with something cc-lb can't classify: a `5xx`, a redirect (not followed), another `4xx`, or an oversized body | Try again, or save with `skip_preflight` |
| `key_workspace_scoped_usage_unavailable` | – | A bound key cannot read usage reports | Replace it with an All workspaces key |
| `role_insufficient` | – | The linked account's org role can't read usage reports | Raise the account's org role |
| `key_id_unresolved` | – | Usage can't be attributed to this key | Figures cover the whole workspace |
| `usage_never_checked` | – | Saved without a check, or created before checks existed | Re-check the key |
| `usage_poll_failed` | – | Usage reports stopped arriving | Fix the role or key; the poller retries |

Codes with a status are failure responses on create, update, and the workspace list. The rest appear only as data. Re-check, the workspace list, and any update that re-probes the stored key answer `500 {"error":"api_key_credential_decrypt_failed"}` when the stored credential cannot be decrypted; send a new `api_key_value` to replace it.

`GET /admin/v1/upstreams/{id}/usage` reads storage only and makes no outbound call. It accepts the dashboard `range` (`15m`, `1h`, `6h`, `24h`, `7d`) and `step` (`minute`, `hour`) parameters. Anthropic's reports are hourly, so a `real` response to `step=minute` returns hourly buckets and echoes `"step":"hour"`; an `estimated` response keeps the requested step. `source` is `real` when the series comes from Anthropic's usage report and `estimated` when it comes from cc-lb's own request log. `attribution` is `api_key`, or `workspace` when Anthropic can't attribute usage to the key; `attribution_workspace_id` names the workspace a `workspace` attribution sums and is `null` otherwise. `stale` is `true` while the last report is kept after polling failed. `last_reported_unix_secs` is when the poller last received a usable report, independent of `range`, or `null` when no poll has succeeded. `totals.cost_usd_micros` is always an estimate at list price (`cost_is_estimate: true`), because Anthropic reports tokens per key, not cost. `fix` carries the single remediation code that explains a degraded mode, or `null`.

Reported usage comes from the `api_key_usage_poll` scheduler job, which polls each eligible upstream every 300 seconds by default; see [docs/scheduler.md](./scheduler.md#api-key-usage-polling).

`month` covers the current UTC calendar month. Its `billing_stop` is classified from the upstream's latest finished proxied request. Anthropic uses `credits_exhausted` (400 credit balance), `spend_limit` (400 self-set usage limit), `tier_spend_cap` (429 tier monthly cap), or `billing_error` (402). OpenAI API-key upstreams use `openai_credit_balance_exhausted`, `openai_organization_spend_limit_exceeded`, `openai_project_spend_limit_exceeded`, or `openai_organization_usage_limit_exceeded` for documented 429 quota and spend-limit codes. Gemini API-key upstreams use `gemini_payment_required` for `payment_required` (402) and `gemini_failed_precondition` for `failed_precondition` (400). Any other outcome, including a later success, clears it. The admin usage card links each stop to the provider's remediation guidance; Anthropic billing remains in Console Billing.

The proxy strips any client-supplied `anthropic-workspace-id` header from every upstream request. API-key upstreams send their configured workspace instead.

### Principals API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| POST | `/admin/v1/principals` | Bearer | CreatePrincipalBody | PrincipalResponse | `conflict` |
| GET | `/admin/v1/principals` | Bearer | None | List of PrincipalResponse | None |
| GET | `/admin/v1/principals/{id}` | Bearer | None | PrincipalResponse | `unknown_principal` |
| PATCH | `/admin/v1/principals/{id}` | Bearer | UpdatePrincipalBody | PrincipalResponse | `stale_revision` |
| DELETE | `/admin/v1/principals/{id}` | Bearer | None | None | `stale_revision` |
| POST | `/admin/v1/principals/{id}/enable` | Bearer | None | PrincipalResponse | `stale_revision` |
| POST | `/admin/v1/principals/{id}/disable` | Bearer | None | PrincipalResponse | `stale_revision` |
| PUT | `/admin/v1/principals/{id}/allowed_models` | Bearer | AllowedModelsBody | PrincipalResponse | `stale_revision` |

#### CreatePrincipalBody

```json
{
  "name": "principal-name",
  "kind": "machine",
  "allowed_models": ["claude-sonnet-4-5-20250929"],
  "default_limits": [],
  "cache_keepalive": {
    "enabled": true,
    "refresh_lead_time_5m_secs": 10,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 4,
    "max_total_duration_secs": 14400,
    "snapshot_max_bytes": 8388608,
    "min_cache_tokens": 10000,
    "classifier": {
      "extra_wait_for_user_tools": [],
      "treat_end_turn_as_ambiguous": false
    }
  }
}
```

`cache_keepalive` is an optional object. Omit it (or pass `null`) to
disable the prompt-cache keep-alive feature for this principal. When
present, `enabled` is required. Full field semantics, defaults, and
provider-gating rules are documented in
[docs/cache-keepalive-operator.md](./cache-keepalive-operator.md).

For OpenAI Responses keep-alive with a selected `openai_api_key` upstream,
snapshots preserve client-supplied `OpenAI-Organization`, `OpenAI-Project`, and
`OpenAI-Processing-Region` selectors for renewals; absent selectors remain
absent. Credentials remain signer-owned and are never captured in snapshots.
OAuth behavior is unchanged. Historical snapshots lacking selector values need a
fresh eligible real request to capture them; an upgrade cannot recover values
that were not stored.

#### PrincipalResponse

```json
{
  "id": "<PRINCIPAL_ID>",
  "name": "principal-name",
  "kind": "machine",
  "enabled": true,
  "revision": 1,
  "allowed_models": ["claude-sonnet-4-5-20250929"],
  "default_limits": [],
  "cache_keepalive": null
}
```

The `cache_keepalive` field mirrors whatever was persisted on the
principal, or `null` when unset. `PATCH /admin/v1/principals/{id}` with
`"cache_keepalive": null` clears the config.

Each principal receives a built-in `subscription-preference` Router chain
entry at creation, with order `0`. Use the plugin-chain endpoints below to
remove the entry or change its position among other router plugins.

#### Subscription preference

Anthropic, OpenAI, and Gemini OAuth candidates use the same tier order:
`KnownBase` > `PartialBase` > `Overage` > `UnknownProbe`. Within the winning
tier, cost-first selection uses quota pressure and reset pace as tiebreaks;
base-window warnings do not affect ranking.

OpenAI uses `5h` and `7d`; Pro accounts that report an absent `5h` window are
weekly-only. Gemini models (`gemini-*`) use `gemini_5h`/`gemini_7d`. Claude
(`claude-*`) and GPT-OSS (`gpt-oss-*`) models routed through Antigravity use
`third_party_5h`/`third_party_7d`. An unknown Gemini model family treats both
families as unknown rather than using either family's exhaustion or headroom.
Anthropic's shared and model-scoped window rules are unchanged.

Fresh utilization at or above `1.0`, rejection, or a disabled reason blocks
base quota. A stale hard negative remains blocking until its reported reset;
without a reset it remains blocking. Stale positive data is unknown.
Confirmed-absent windows are skipped; unobserved windows remain unknown.
API keys are retained only when no usable OAuth subscription remains. An
already-pinned sole candidate retains the existing preservation behavior.


### Plugins API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| GET | `/admin/v1/plugins/registry` | Bearer | None | RegistryListResponse | None |
| GET | `/admin/v1/plugins/registry/{id}` | Bearer | None | RegistryEntryResponse | `unknown_registry_entry` |
| GET | `/admin/v1/plugins/registry/{id}/references` | Bearer | None | RegistryReferencesResponse | `unknown_registry_entry` |
| PATCH | `/admin/v1/plugins/registry/{id}` | Bearer | PatchRegistryBody | RegistryEntryResponse | `stale_revision` |
| DELETE | `/admin/v1/plugins/registry/{id}` | Bearer + `If-Match` | None | None | `referenced_by`, `stale_revision` |
| DELETE | `/admin/v1/plugins/registry/{id}?cascade=references` | Bearer + `If-Match` + `X-Reference-Fingerprint` | None | RegistryCascadeDeleteResponse | `reference_fingerprint_required`, `references_changed`, `stale_revision` |
| GET | `/admin/v1/principals/{principal_id}/plugin-chain` | Bearer | None | ChainListResponse | None |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain` | Bearer | InsertChainBody | PluginChainEntry | `conflict` |
| POST | `/admin/v1/principals/{principal_id}/plugin-chain/reorder` | Bearer | ReorderBody | None | `stale_revision` |
| GET | `/admin/v1/plugin-chain-entries/{id}` | Bearer | None | PluginChainEntry | `unknown_plugin_chain_entry` |
| DELETE | `/admin/v1/plugin-chain-entries/{id}` | Bearer | None | None | `stale_revision` |

#### InsertChainBody

```json
{
  "slot": "router",
  "wasm_registry_id": "<PLUGIN_ID>",
  "config": {},
  "position": null
}
```

`RegistryEntryResponse` includes the inspected plugin metadata name, nullable `version`, SHA-256, supported slots, revision, and a live `refcount`. The `refcount` is computed from both principal plugin-chain bindings and upstream warmup dialect plugin bindings.

`GET /admin/v1/plugins/registry/{id}/references` returns a `reference_fingerprint` plus the concrete chain and warmup resources that currently reference the plugin. Use that fingerprint when intentionally deleting a referenced plugin with `?cascade=references`; stale fingerprints are rejected with `409 references_changed` so operators see a fresh impact list before removing bindings.

### Status and Export API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| GET | `/admin/v1/status` | Bearer | None | StatusResponse | `storage_unavailable` |
| GET | `/admin/v1/export` | Bearer | None | ExportResponse | `storage_unavailable` |

The proxy listener's `/readyz` returns `200` once the server has started, at least one principal is active, and at least one upstream is usable: enabled, applied, and, for OAuth, holding decryptable credentials that do not need a reconnect. Otherwise it returns `503` with `reason` set to `draining`, `startup_not_ready`, `no_ready_principal`, or `no_ready_upstream`. A broken OAuth upstream does not take a ready instance out of rotation: routing skips it, and `/readyz` stays `200` with `reason: "oauth_credentials_not_decryptable"` while any enabled OAuth upstream has credentials that cannot be decrypted. API-key preflight results, capability snapshots, and usage-poll failures are not part of readiness.

### CLI/API OAuth bundle import

The authenticated Admin v1 upstream create and update endpoints also accept a
non-interactive OAuth token bundle for `openai_oauth` and `gemini_oauth`.
`oauth_tokens` is the canonical request field; `oauth_token_bundle` is accepted
as an alias. This path is additive: it does not change the Admin Web flow or
the Anthropic PKCE flow documented below.

The CLI sends the same request body through the Admin v1 API. It reads the
Bearer token from `CC_LB_ADMIN_TOKEN`, never from the JSON input:

```bash
# Supply CC_LB_ADMIN_TOKEN through a secret manager or process environment.
cc-lb admin --base-url http://[::1]:9090 \
  upstreams credential --input ./openai-oauth.json
```

Omit `--id` to create an upstream (`POST /admin/v1/upstreams`). To replace
credentials on an existing upstream, pass `--id <UPSTREAM_ID>` and
`--revision <SPEC_REVISION>`; the CLI sends `PUT
/admin/v1/upstreams/<UPSTREAM_ID>` with `If-Match`.

#### OpenAI OAuth

Use `kind: "openai_oauth"`:

```json
{
  "name": "openai-cli",
  "kind": "openai_oauth",
  "oauth_tokens": {
    "access_token": "<ACCESS_TOKEN>",
    "refresh_token": "<REFRESH_TOKEN>",
    "expires_at_unix_secs": 1760000000,
    "refresh_token_expires_at_unix_secs": 1770000000,
    "account_id": "<OPTIONAL_ACCOUNT_ID>",
    "scopes": ["openid"],
    "never_refresh": false
  }
}
```

In refreshing mode (`never_refresh: false`), `access_token` and `refresh_token`
must be non-empty. The access-token expiry may already have passed; the
refresh-token expiry, when supplied, must still be in the future.
`account_id` is optional. When it is absent, cc-lb attempts to derive it from the
OpenAI account claim in the access-token JWT. OpenAI imports must not include
`project_id`.

OpenAI OAuth Responses traffic uses
`https://chatgpt.com/backend-api/codex` as its default base and appends
`/responses`: HTTPS for HTTP requests and WSS for WebSocket connections. A custom
base retains its path prefix before `/responses`. The downstream query is
preserved on the signed request. Other provider URL construction is unchanged.

#### Gemini OAuth

Use `kind: "gemini_oauth"` for both Gemini CLI enterprise grants and Gemini subscriptions. New subscription sign-ins and reconnects use the Antigravity OAuth client. The operator MUST configure both `[oauth.antigravity]` `client_id` and `client_secret`; cc-lb does not embed or default those credentials. If either key is absent, sign-in and refresh return `oauth_client_not_configured` with the missing configuration path. Google retired Gemini CLI consumer login on June 18, 2026; see the [transition announcement](https://developers.googleblog.com/an-important-update-transitioning-gemini-cli-to-antigravity/).

The existing Gemini CLI profile behavior is unchanged: enterprise grants retain `gemini_cli`, and a missing `oauth_profile` defaults to `gemini_cli`. Antigravity credentials retain `antigravity`, and cc-lb never refreshes a grant with the other profile's OAuth client.

```json
{
  "name": "gemini-subscription",
  "kind": "gemini_oauth",
  "project_id": "my-gemini-project",
  "oauth_tokens": {
    "access_token": "<ACCESS_TOKEN>",
    "refresh_token": "<REFRESH_TOKEN>",
    "expires_at_unix_secs": 1760000000,
    "refresh_token_expires_at_unix_secs": 1770000000,
    "oauth_profile": "antigravity",
    "scopes": ["openid"],
    "never_refresh": false
  }
}
```

The same refreshing-mode token and expiry rules apply. A project-id mismatch is rejected, and a blank project id cannot be imported. Set `oauth_profile` to `gemini_cli` for an existing enterprise grant; omitting it preserves that legacy default.

For an interactive Gemini subscription sign-in, open the returned Google authorization URL in a browser. After approval, the browser lands on `http://localhost:51121/oauth-callback`; the page does not need to load. Copy the full URL from the address bar and paste it into the completion form. A bare authorization code also works. cc-lb validates the state token before exchanging the code.

Without an explicit upstream base override, Antigravity content requests use the configured Antigravity content origin, defaulting to `https://daily-cloudcode-pa.googleapis.com`. An explicit upstream base override takes precedence. For Antigravity generation requests, the signer qualifies bare tiered `/v1beta` model families on the wire from `generationConfig.thinkingConfig.thinkingLevel` (`low`, `medium`, `high`, `xhigh`), while routing and request events keep the downstream model and `thinking_level` separate. Usage polling calls `POST /v1internal:retrieveUserQuotaSummary` with `{"project":"<project-id>"}` and uses the same parser and fallback behavior as Gemini CLI sign-ins. Labeled `5h` and `weekly` summary buckets map to canonical windows: `gemini_5h` and `gemini_7d` for Gemini models, or `third_party_5h` and `third_party_7d` for Claude and GPT-OSS models.

`retrieveUserQuota` and unlabeled summary buckets retain period-unknown raw meters, exposed as `period_unknown_meters` on `GET /admin/v1/subscription-quotas/latest`. Polling keeps the provider's remaining fraction and reset time without inferring a `5h` or `7d` period from reset distance. Each poll makes at most one fallback fetch, preserves `429` throttling, and leaves prior observations intact when the fallback fails instead of writing false `Absent` records.

Gemini OAuth quota polls retain successful summary and fallback response bodies, including unknown provider fields, without deriving prices or quota periods from them. After credential-fenced publication, `GET /admin/v1/subscription-quotas/latest` exposes optional `upstreams[].raw_summary` and `upstreams[].raw_fallback` objects containing `observed_at_unix_millis` and `body`, subject to the current account/project binding, applicable credential generation, OAuth profile, and source/window filters; each side retains its own successful timestamp, and a failed side preserves its previous observation within the same binding and profile.

OpenAI OAuth credit balances are retained as raw decimal strings in quota
observations. `GET /admin/v1/subscription-quotas/credit-history` returns
per-upstream and pool balance history for the requested relative range. Pool
membership matches the enabled OpenAI OAuth quota pool. Historical points hold
recorded balance state; only the current tail uses the balance sidecar's own
freshness. Missing, invalid, unlimited, stale, and partial balances remain
distinguishable. The pool sum is exact before clients truncate it for display.

> [!WARNING]
> Google Antigravity Additional Terms [§6](https://antigravity.google/terms) prohibit using third-party software, tools, or services to access the Service with Antigravity OAuth. The terms state that this is a breach and may be grounds for suspension or termination of Antigravity and/or Gemini CLI accounts. Operators decide at their own risk whether to use this integration.

#### Subscription account metadata

`GET /admin/v1/upstreams/{id}/subscription-metadata` and OAuth status responses
include nullable `account_email`, `account_name`, `account_id`, `plan`,
`plan_name`, and `project_id` fields for OpenAI and Gemini subscriptions.
`plan` retains the provider's identifier; `plan_name` is a provider-supplied
display name when available.

OpenAI sign-in reads the account name and identity from display-only token
claims. Background usage polling updates the plan and email from the existing
Codex usage response only when its account ID matches the stored credential.
OpenAI organization memberships are not treated as ChatGPT workspace names.
Gemini sign-in reads the account name and email from Google userinfo and the
plan from `loadCodeAssist`, preferring the paid tier. The tier's name and ID
always come from the same tier. Background metadata collection fills older
records using the configured endpoints for the credential's OAuth profile.
Metadata failures preserve previously collected fields and do not invalidate
successful quota observations.
Project-only Gemini imports do not need an account email in the token bundle.
Their collected display identity is bound to the stored project and credential
generation. Replacing that credential invalidates the old identity until the
next successful collection. Background metadata writes atomically check both
the captured credential generation and the metadata read before collection,
so a delayed poll cannot overwrite account details saved by a newer reconnect.


The admin web uses the same plan labels in upstream lists, detail headers,
search, and OAuth account cards. Gemini's reported tier name takes precedence;
unknown plan identifiers remain readable. Claude Max 20x is shown as
[Claude Ultra](https://support.claude.com/en/articles/17728952-what-is-the-ultra-plan),
and Max 5x as
[Claude Max](https://support.claude.com/en/articles/11049741-what-is-the-max-plan).
Account identity appears in the detail header and remaining account facts,
while Connection contains the effective endpoint and the provider's Connect or
Reconnect action. Provider metadata collection adds no provider calls to the
proxy request path.

#### Access-only OAuth

For either `openai_oauth` or `gemini_oauth`, set `never_refresh: true` to import
an access token without granting cc-lb refresh access. Use the same `oauth_tokens`
body on create and update:

```json
{
  "access_token": "<ACCESS_TOKEN>",
  "refresh_token": "",
  "expires_at_unix_secs": 1760000000,
  "scopes": [],
  "never_refresh": true
}
```

`access_token` must be non-empty. `refresh_token`, `expires_at_unix_secs`, and
`scopes` remain required fields; `refresh_token` must be the empty string.
Omit `refresh_token_expires_at_unix_secs` or set it to `null`. Any non-empty
refresh token, including whitespace, or any non-null refresh-token expiry is
rejected before storage. Gemini still requires the project id described above;
OpenAI still rejects a project id and permits an optional account id.

Updating `oauth_tokens` replaces the complete stored bundle atomically,
including `never_refresh`; it does not retain a previous refresh token.
Import does not require an access-token expiry in the future. Before each new
HTTP signing operation or upstream WebSocket handshake, cc-lb rejects an
access-only token whose expiry is at or before the current clock, before adding
provider authentication headers or wrapping a Gemini body. An already-open
provider WebSocket is not reauthenticated or closed merely because that token
expires. Proxy client-key authorization is a separate check.

`never_refresh` prevents token refresh calls across the direct, scheduled, and
bootstrap refresh paths. It does not disable generation, quota checks, or other
provider network operations. Replace an expired access token through this API
using a token supplied by its external owner. Do not copy or refresh a shared
OAuth grant. Existing refreshing credentials and Anthropic's long-lived
credentials, including their retained but unused refresh token, are unchanged.

On successful create or update, cc-lb encrypts the bundle with AEAD storage
and seeds an `OAuthRefreshJob`. The response never contains the access token,
refresh token, or full bundle; inspect only the returned upstream metadata.
Imported credentials are supported only for OpenAI and Gemini OAuth. An
Anthropic OAuth upstream continues to use the Admin Web/PKCE connect and
reconnect flow below, and API-key upstreams continue to use API-key fields and
preflight.

Treat the JSON input as a secret-bearing file. Prefer stdin or a temporary
file with mode `0600`, keep it out of shell history and source control, do not
log its contents, and remove or securely expire the file after the import.
Never place access or refresh tokens in the upstream name, query string, or
diagnostic output. The server does not return imported token values.

## OAuth Subscription Flow

The OAuth subscription flow allows upstreams to authenticate dynamically using OAuth PKCE.

### Step-by-Step Walkthrough

1. Create an OAuth upstream:

```bash
curl -X POST http://[::1]:9090/admin/v1/upstreams \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "oauth-upstream",
    "kind": "anthropic_oauth"
  }'
```

2. Start the OAuth flow:

```bash
curl -X POST http://[::1]:9090/admin/v1/upstreams/<UPSTREAM_ID>/oauth/start \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{}'
```

The request body is an empty object; `POST /admin/v1/oauth/draft/start` takes the same body. Every start requests a 365-day long-lived credential — there is no mode to choose. The response returns the authorization URL and state token:

```json
{
  "authorize_url": "https://provider.com/oauth/authorize?client_id=<CLIENT_ID>&state=<STATE_TOKEN>...",
  "state_token": "<STATE_TOKEN>"
}
```

3. The user visits the `authorize_url` in their browser, completes the authorization, and gets redirected to the redirect URI with a `code` query parameter.

4. Complete the OAuth flow:

```bash
curl -X POST http://[::1]:9090/admin/v1/upstreams/<UPSTREAM_ID>/oauth/complete \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "state_token": "<STATE_TOKEN>",
    "code": "authorization-code"
  }'
```

Response:

```json
{
  "upstream_id": "<UPSTREAM_ID>",
  "expires_at_unix_secs": 1716934800,
  "access_token_fingerprint": "a1b2c3d4",
  "mode": "long_lived_365d",
  "long_lived_fallback": false,
  "fallback_reason": null,
  "granted_expires_in_secs": 31536000
}
```

`mode` reports the credential mode actually negotiated. `long_lived_fallback` is `true` when the long-lived request did not produce a long-lived credential and cc-lb stored a normal refreshing credential instead; `fallback_reason` reports why: `"rejected"` when the provider returned `400 invalid_request` for the custom expiry and cc-lb transparently retried the same authorization code without it, `"clamped"` when the provider accepted the exchange but granted a materially shorter lifetime than requested, or `"scope_rejected"` when the provider rejected the year-long grant because the operator configured a scope outside the inference-only set (`user:profile` and `user:inference`). `granted_expires_in_secs` reports the access-token lifetime the token endpoint actually granted. `POST /admin/v1/oauth/draft/complete` returns the same fields.
### Credential Modes

Every Anthropic OAuth connect requests a 365-day long-lived access token; there is no mode setting. cc-lb resolves the outcome itself and stores the credential in one of two modes:

- **`long_lived_365d`**: a long-lived access token that is never refreshed. The operator reauthorizes roughly once a year instead of roughly every 30 days. Quota polling, warm-up bootstrap and watchdog work, automatic subscription-metadata refresh, and authentication-failure reporting remain enabled exactly as for a refreshing credential.
- **`refreshing`**: the classic 8-hour access token kept alive by the background and lazy refresh paths described below. cc-lb stores this mode when the token endpoint refuses the year-long `expires_in`, accepts it but grants a lifetime that does not outlive the 30-day refresh-token window, or the configured scopes prevent a year-long grant.

The default `oauth.anthropic.scopes` is `["user:profile", "user:inference"]`. Anthropic only grants year-long access tokens for inference-only scopes. Operators who explicitly configure `org:create_api_key` keep that setting, but their connections fall back to `refreshing` mode, and cc-lb reports that reason. Other explicitly configured scopes can likewise prevent a year-long grant.

cc-lb always stores the access-token lifetime the token endpoint actually returned; the requested 365 days is never assumed or substituted. If Anthropic accepts the exchange but grants a materially shorter lifetime than requested — at or below the 30-day `LONG_LIVED_MIN_GRANT_SECS` threshold — cc-lb treats that as a refusal of the long-lived grant and keeps the credential in `refreshing` mode, so the stored refresh token remains the renewal path and the upstream cannot silently expire with no way to renew. The threshold exists because Anthropic anchors refresh tokens to a 30-day window: a grant that does not outlive that window buys nothing over the refreshing flow while giving up the ability to renew. A grant longer than 30 days but shorter than the request remains long-lived and is stored with the lifetime actually granted.

Long-lived credentials still store the refresh token returned with the grant, but cc-lb never uses it: redeeming it would make Anthropic revoke the 365-day access token and hand back a short-lived one. When Anthropic rejects a long-lived credential with `401`, on a proxied request or the periodic usage poll, cc-lb records the reconnect-required state, routing stops selecting the upstream, and the dashboard surfaces a reconnect prompt; cc-lb does not attempt a rotation as a recovery path. Re-running the connect/reconnect flow requests a long-lived credential again, so an upstream returns to `refreshing` mode only when the provider refuses, clamps, or cannot grant the requested scopes.
### Refresh Semantics

The system runs a background sweeper every 60 seconds to refresh tokens that expire within 300 seconds. To prevent multiple replicas from refreshing the same token, the replica must claim a 90-second database lease.

If the background refresh fails or a token expires before the sweeper runs, the proxy uses a lazy refresh fallback. When a request is signed, the signer checks whether the token is expired or within the 30-second skew window. If so, it triggers an on-demand refresh, updates the database, and retries the request once.

A terminal refresh result (`400 invalid_grant`, `401`, or a known expired refresh token), as well as an HTTP `401` that persists after a forced refresh succeeds on a live request or usage poll, records a reconnect-required state that scheduled, watchdog, and lazy refresh paths honor until the credential is replaced or the account is reconnected. Routing also stops selecting a reconnect-required OAuth upstream; the periodic reconciler picks up the state change on the next successful reconcile or rebuild on every replica. Other `400` responses, network failures, and `5xx` responses remain transient and retry.

Both refresh paths are disabled end to end for `long_lived_365d` credentials: the sweeper never schedules them and the signer never attempts a lazy refresh.

## Wasm Upload Workflow

You upload Wasm plugins via a multipart POST request to `/admin/v1/plugins/wasm`.

### Upload Process

1. Send a multipart request containing the Wasm binary:

```bash
curl -X POST http://[::1]:9090/admin/v1/plugins/wasm \
  -H "Authorization: Bearer <TOKEN>" \
  -F "original_filename=plugin.wasm" \
  -F "bytes=@path/to/plugin.wasm"
```

Supported slots are derived from the hooks the Wasm exports, and the registry name comes from the plugin metadata embedded in the Wasm. `original_filename` is optional and defaults to `<metadata-name>.wasm`.

Response:

```json
{
  "sha256_hex": "sha256-hex-string",
  "id": "<PLUGIN_ID>",
  "size_bytes": 1024,
  "original_filename": "plugin.wasm",
  "version": "1.0.0",
  "revision": 1,
  "idempotent": false,
  "action": "created"
}
```

Upload actions are:

- `created`: new plugin metadata name.
- `noop`: same metadata name and same SHA-256; returns `200` and preserves the registry row.
- `replaced`: same metadata name and different SHA-256; preserves the registry `id`, updates the version/SHA/metadata, and triggers dynamic rebind.

Higher semantic versions replace in place without a confirmation round trip. Same, lower, missing, or unparseable replacement versions require a `409 replacement_confirmation_required` response first:

```json
{
  "error": "replacement_confirmation_required",
  "name": "my-plugin",
  "replace_registry_id": "<PLUGIN_ID>",
  "expected_revision": 3,
  "current_version": "1.0.0",
  "incoming_version": "1.0.0",
  "current_sha256_hex": "...",
  "incoming_sha256_hex": "..."
}
```

Retry the same upload with `confirm_replacement=true`, `replace_registry_id`, and `expected_revision` to approve the replacement.

### Validation and Deduplication

- **Size Limit**: The upload is limited to 32 MiB per blob.
- **Magic Bytes**: The server performs a magic-bytes check to verify that the uploaded file starts with `\0asm`.
- **Wasmtime Validation**: The server inspects the plugin with wasmtime (`inspect_wasm`) to verify required exports (`cc_lb_alloc`, `cc_lb_free`, `memory`, and the per-slot hook), reject disallowed host imports, and confirm the embedded BLAKE3 schema hash matches the slot's wire contract.
- **SHA-256 Deduplication**: The server computes the SHA-256 hash of the bytes to deduplicate uploads. Same-name/same-SHA uploads are noops; same-name/different-SHA uploads follow the semantic-version replacement policy above.
- **Reference Counting**: The server computes plugin references from principal plugin-chain rows and upstream warmup dialect plugin rows. A normal delete of a referenced registry entry is blocked with `referenced_by`; an intentional cascade delete requires the latest reference fingerprint and removes all listed references atomically.

## Multi-Replica Operations

In multi-replica deployments, replicas coordinate configuration updates and background tasks.

### Coordination Mechanisms

- **LISTEN/NOTIFY**: Replicas subscribe to Postgres `LISTEN/NOTIFY` channels to receive real-time change events. When a replica receives a notification, it rebuilds its local `DynamicView` snapshot within sub-seconds.
- **Reconciliation Fallback**: If a replica misses a notification, a background reconciler polls the database every 60 seconds as a fallback.
- **Database Lease**: For background tasks like OAuth token refresh, replicas use a database-backed UUID lease to ensure only one replica performs the refresh.
- **Status Monitoring**: You can monitor the status of all replicas and check for partial-failure states via the `/admin/v1/status` endpoint.
- **Storage Backend**: Use Postgres for multi-replica deployments.

## Configuration Restarts and TLS Reloads

Every change to `cc-lb.toml` requires a process restart. cc-lb does not hot-reload the configuration file or any individual field.

`SIGHUP` reloads only the contents of the TLS certificate and key files at the paths already configured by `listener.tls.cert_path` and `listener.tls.key_path`. Keep both paths unchanged. Changing either path, or changing any other `cc-lb.toml` setting, requires a process restart.

Upstreams, principals, and plugin chains remain dynamic because they are stored in the database rather than `cc-lb.toml`.

## Audit and Redaction Guarantees

The system enforces strict audit and redaction guarantees to prevent sensitive credentials from leaking into logs or audit trails.

### Guarantees

- **Typed Audit Payload**: All administrative actions are logged using a typed `AuditPayload` enum, which ensures that only non-sensitive metadata is captured.
- **CI Grep Lint**: The codebase is protected by a CI grep lint that blocks any commit containing token literals or unredacted credentials.
- **AEAD Encryption**: Sensitive fields, such as OAuth tokens and API keys, are encrypted at rest using AEAD (Authenticated Encryption with Associated Data).
- **Associated Authenticated Data**: The system binds the encryption to the specific resource by using the upstream UUID as the Associated Authenticated Data (`aad = upstream_id.as_bytes()`). This prevents credentials from being copied or associated with a different upstream.

## Troubleshooting

This section lists common failures and their diagnosis steps.

### Server exits with "storage is required"

- **Symptom**: The server fails to start and logs "storage is required".
- **Diagnosis**: There is currently no Postgres-as-startup-storage path.
- **Workaround**: Use the sqlite backend for single-replica deployments until a follow-up implements Postgres startup storage.

### OAuth refresh fails persistently

- **Symptom**: OAuth tokens are not refreshed, and requests fail with expired token errors.
- **Diagnosis**: Check the `/metrics` endpoint for `cclb_oauth_refresh_total{outcome="..."}` to see the error categorization.
- **Workaround**: Verify that the network can reach the OAuth provider and that the client credentials in `cc-lb.toml` are correct. Transient network, `5xx`, and non-terminal `400` refresh failures retry. For `400 invalid_grant`, `401`, or a known expired refresh token (including an HTTP `401` that persists after a forced refresh succeeds on a live request or usage poll), rerun connect/reconnect to replace the credential; scheduled, watchdog, and lazy refresh paths do not retry while reconnect is required.

### Long-lived OAuth credential returns 401

- **Symptom**: An OAuth upstream stops receiving requests (they fail over to another upstream, or fail with expired token errors when no other upstream is usable), and no refresh is attempted.
- **Diagnosis**: Check `GET /admin/v1/upstreams/{id}/oauth/status`; the response includes `status`, `has_credentials`, `mode`, `refresh_token_present`, and `can_refresh`. A `long_lived_365d` credential reports `can_refresh: false`. A 401 on a long-lived credential is terminal — cc-lb never rotates it and records the upstream as reconnect-required until it is reauthorized.
- **Workaround**: Re-run the connect/reconnect flow to reauthorize. Storing the new credential clears the reconnect-required state, and routing resumes on the next successful reconcile or rebuild. The flow requests a long-lived credential again; the upstream ends up in `refreshing` mode only when the provider refuses or clamps the grant.

### Upstream reports `refreshing` instead of a long-lived credential

- **Symptom**: An OAuth upstream was connected expecting a `long_lived_365d` credential, but `GET /admin/v1/upstreams/{id}/oauth/status` and the completion response report `mode: "refreshing"`.
- **Diagnosis**: Anthropic accepted the exchange but granted a materially shorter lifetime than the requested 365 days — at or below the 30-day threshold — so cc-lb treated the grant as a refusal of long-lived mode and kept the credential refreshable. The completion response reports this as `long_lived_fallback: true` with `fallback_reason: "clamped"`, and a clamp is logged with a warning. This is the safe outcome: the stored refresh token remains the renewal path, so the upstream cannot silently expire with no way to renew. An outright `400 invalid_request` rejection produces the same `refreshing` result with `fallback_reason: "rejected"`.
- **Workaround**: None required — the credential renews normally. To attempt long-lived mode again, re-run the connect/reconnect flow; if Anthropic still clamps the grant, the provider does not currently honour the custom expiry for this account.

### Replica B doesn't see new upstream after 5s

- **Symptom**: Changes made on Replica A are not visible on Replica B within a few seconds.
- **Diagnosis**: This indicates a cross-replica visibility lag. Postgres `NOTIFY` should provide sub-second updates. If `NOTIFY` is blocked or misconfigured, the 60-second reconciliation fallback will eventually sync the state.
- **Workaround**: Check the database connection and verify that the database user has correct permissions for `LISTEN` and `NOTIFY` operations.

### Plugin upload returns 413

- **Symptom**: Uploading a Wasm plugin fails with a 413 Payload Too Large error.
- **Diagnosis**: The uploaded Wasm file exceeds the 32 MiB limit.
- **Workaround**: Optimize the Wasm binary size or split the plugin logic if possible. Ensure that the file size is strictly under 32 MiB.

### Vertex AI Gemini

The two supported Vertex kinds are `vertex_api_key` (Express or regional project-bound key) and `vertex_credential` (regional service-account, authorized-user, or impersonated-service-account JSON). Vertex preserves Gemini-shaped ingress. Express and regional API-key targets support generate, stream, and countTokens only: Google rejects API keys for cached-content creation, the publisher model catalog, and Live. Regional credential targets additionally support approved cached-content create/use, publisher model-catalog list/get, and Live WebSocket routes through Vertex. The catalog is a minimal publisher projection, not entitlement-aware; project access still controls whether a listed model works. Explicit cached-content billing remains Vertex-controlled, and cc-lb does not promise cache discounts or expose billing receipts. Use least-privilege service accounts. Bedrock, AWS SigV4, ambient ADC/metadata-server credentials, and external-account files remain unsupported. Vertex is excluded from subscription quota, warmup, and keepalive. Upgrade every replica before creating Vertex rows. The admin DELETE endpoint soft-deletes rows, so before downgrading use an approved database maintenance procedure to hard-delete every soft-deleted Vertex row from `upstream_spec_v1` (after checking references; foreign-key child rows cascade where configured), then verify no Vertex rows remain before starting the old binary. See [`vertex-ai-qa.md`](vertex-ai-qa.md).

Creating or updating a Vertex upstream runs a preflight that sends one free `countTokens` request with the stored key or a freshly minted token. It proves authentication, project access, and API enablement. It sends no billed generation request, so a billing problem can pass preflight and surface on the first generation request. Service-account JSON may carry its private key as PKCS#8 (`BEGIN PRIVATE KEY`, the form Google issues) or PKCS#1 (`BEGIN RSA PRIVATE KEY`).
