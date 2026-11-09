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

### Proxy capability boundary

Provider-prefixed routes select the provider explicitly: `/anthropic/v1/...`, `/openai/v1/...`, and `/gemini/v1beta/...` (plus Gemini `/gemini/v1/...` model catalogs). The tables below list supported operations and eligible credentials. Shared unprefixed routes are kept for backward compatibility and retain their existing operation set only: `POST /v1/messages` and `POST /v1/messages/count_tokens` (Anthropic Messages), `POST /v1/responses` (OpenAI Responses, plus the `GET /v1/responses` WebSocket upgrade), and Gemini `POST /v1beta/models/{model}:generateContent` / `:streamGenerateContent` / `:countTokens`. Every other unprefixed operation returns `404 operation_not_supported`; new operations require a matching provider prefix.

Provider-owned state referenced inside a supported HTTP generation request is forwarded unchanged and kept on its owning upstream through upstream affinity. cc-lb records a SHA-256 digest of each state id together with the upstream that created it, and routes later requests that reference the id only to that upstream:

- Anthropic Messages: web-search `encrypted_content`, file ids in `file` / `container_upload` blocks, and container ids.
- OpenAI HTTP Responses: response ids and stateless reasoning `encrypted_content` items observed from every successful response (including `store: false`, which stateless clients still chain through `previous_response_id`), and `conversation` ids. Streaming responses pin before `response.completed` is forwarded.
- Gemini generateContent / countTokens: `cachedContent` references (including nested `generateContentRequest.cachedContent`). `thoughtSignature` is forwarded but is not treated as affinity state.

- Gemini cachedContents creation: the exact `POST /gemini/v1beta/cachedContents` operation is available only with Gemini API-key upstreams and explicit-caching Gemini 2.5 and Gemini 3 family models. Gemini 4+ explicit caching, other models, and non-create `cachedContents` API surfaces remain HTTP `404 operation_not_supported`. cc-lb forwards the request body unchanged and binds a successful canonical response `name` in the form `cachedContents/<id>` to the selected upstream before returning success; later `generateContent` and `countTokens` requests that reference that name remain pinned to its owner.

  Provider `4xx` and `5xx` responses retain their original status and create no owner binding. An unbound reference returns HTTP `503` with `api_error`; an unavailable owner returns HTTP `503 upstream_affinity_unavailable`, without cross-credential failover. Each cached-content binding is checked against its owner upstream's identity epoch. Replacing the API key or changing the base URL or project invalidates it; re-saving the same URL or project and refreshing access metadata preserve it. API-key replacement conservatively invalidates bindings even when the same key is entered again. Later reuse of an invalidated binding fails closed with HTTP `503` until a new resource is created, and a stale create response cannot rebind after an owner identity change. Malformed, non-JSON, or noncanonical create responses, missing, invalid, or expired `expireTime`, and persistence failures return HTTP `503` without a success body.
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

| Method | Client path | Credentials |
|---|---|---|
| POST / GET WebSocket | `/openai/v1/responses` | API key, OAuth |
| POST | `/openai/v1/responses/input_tokens` | API key |
| POST | `/openai/v1/responses/compact` | API key, OAuth |
| POST | `/openai/v1/chat/completions`, `/openai/v1/completions` | API key |
| POST | `/openai/v1/embeddings`, `/openai/v1/moderations` | API key |
| POST | `/openai/v1/audio/speech`, `/openai/v1/audio/transcriptions`, `/openai/v1/audio/translations` | API key |
| POST | `/openai/v1/images/generations`, `/openai/v1/images/edits` | API key, OAuth |
| POST | `/openai/v1/images/variations` | API key |
| GET | `/openai/v1/models`, `/openai/v1/models/{id}` | API key, OAuth |
| POST | `/openai/v1/alpha/search`, `/openai/v1/memories/trace_summarize` | OAuth only |
| GET WebSocket | `/openai/v1/realtime?model={model}` | API key |

OAuth operations drop the canonical `/v1/` prefix and append the remaining path to the configured Codex base URL (for example, `{base}/responses/compact`). Model catalog queries preserve a client-supplied `client_version`; cc-lb does not invent one. The `x-codex-turn-state` response header is forwarded unchanged.

Not yet supported: files, uploads, batches, vector stores, conversations, containers, stored chat completion retrieve/update/delete, response retrieve/delete/cancel/input_items, Realtime calls control and client secrets, videos, and audio voices.

#### Gemini endpoint reference

| Method | Client path | Credentials |
|---|---|---|
| POST | `/gemini/v1beta/models/{model}:generateContent`, `:streamGenerateContent`, `:countTokens` | API key, OAuth |
| POST | `/gemini/v1beta/cachedContents` (create only) | API key |
| POST | `/gemini/v1beta/models/{model}:embedContent`, `:batchEmbedContents`, `:predict` | API key |
| GET | `/gemini/v1beta/models`, `/gemini/v1beta/models/{model}` (also `/gemini/v1/models` and `/gemini/v1/models/{model}`) | API key |
| POST | `/gemini/v1beta/openai/chat/completions`, `/gemini/v1beta/openai/embeddings`, `/gemini/v1beta/openai/images/generations` | API key |
| GET | `/gemini/v1beta/openai/models`, `/gemini/v1beta/openai/models/{id}` | API key |
| WebSocket | `/gemini/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent` | API key |

OpenAI-compatible Gemini requests retain their `/v1beta/openai/...` path and use the Gemini API key as `Authorization: Bearer`. Native Gemini requests use `x-goog-api-key`; OAuth remains limited to generation and token counting.

Realtime and Live WebSockets authenticate the proxy client before upgrade and select one eligible API-key upstream for the connection lifetime; OAuth subscriptions are not eligible. A connection never reconnects or fails over to another credential. Text, binary, ping, and pong frames retain their payloads; client/provider close frames retain their explicit code and reason while propagating tunnel shutdown. A later connection failure closes the tunnel rather than returning a new HTTP response.
Realtime and Live WebSocket admission uses an upstream-scoped circuit breaker keyed by the stable upstream UUID, independent of HTTP's host-keyed breaker. Provider `5xx` responses, transport failures, and timeouts count toward that WebSocket breaker; `4xx` and other non-`5xx` handshake responses do not. Handshake errors use a generic `upstream_unavailable` error code while preserving the provider HTTP status. Before upgrade, that status and safe allowlisted `Retry-After`, `x-ratelimit-*`, `anthropic-ratelimit-*`, and `ratelimit-*` headers are returned as HTTP response status and headers; after upgrade, the status is preserved in the WebSocket error, with any non-empty allowlisted headers under a top-level JSON `headers` object. An open local breaker returns a generic `502 upstream_unavailable` error. Once a provider connection owns a WebSocket, failures close it without reconnecting or failing over.

Gemini Live rejects the handshake with HTTP `503` when the principal has no eligible Gemini API-key upstream, before receiving a setup frame. With an eligible key, model-specific routing still runs after the setup frame supplies the model.

Realtime and Live routing filters receive the parsed, bounded header session identity as `RequestContext.thread_id`, using the existing header precedence. No setup-frame metadata fallback or Claude ancestry fields are added. Principal-limit denials publish one rejected limit decision through the shared audit and metrics path before returning the existing protocol-appropriate error; rejected sessions do not contact the provider.

Successful provider upgrades publish a provider-neutral response-started milestone. Active sessions publish cumulative usage snapshots from the provider usage frames, and final settlement uses the terminal cumulative usage without adding earlier snapshots again. Gemini Live snapshots retain reported thinking and tool-use prompt counts and distinguish absent, zero, positive, and invalid cache evidence. These observations do not reinterpret WebSocket frames as Anthropic SSE events or buffer the full media session.

Terminal observations retain bounded, redacted native close code/reason and available relay I/O details, along with the provider handshake request ID from `x-request-id`, `request-id`, or `x-goog-request-id`. Close frames still reach the peer unchanged. Upgraded connections retain status `101`, including downstream cancellations; native diagnostics do not synthesize HTTP/2 fields or a new HTTP response. The connection emits one terminal completion.

With OpenTelemetry tracing enabled, Realtime and Live provider handshakes receive the current proxy child span's W3C trace context after signing, without replacing host-owned credentials or destination details.

OpenAI's `OpenAI-Beta` header is passed through. Gemini client-supplied `key=` query credentials are removed before the configured upstream key is applied. Usage accounting observes only provider usage frames: OpenAI `response.done` and Gemini frames containing `usageMetadata`.

New tunnel handshakes during server drain receive HTTP `503` without upgrading. Existing tunnels stop relaying and send best-effort close frames, without an explicit close code or reason, to both peers. Idle Gemini connections waiting for a setup frame also close on drain. After an upgrade, drain cannot produce a new HTTP response.

Not yet supported: cachedContents operations other than create, file upload/download, batches, operations, predictLongRunning, fileSearchStores, interactions, agents, auth_tokens, and OpenAI-compatible files/batches. Code Assist account controls are not proxy operations: loadCodeAssist, onboardUser, retrieveUserQuota*, listExperiments, metrics, admin controls, and user settings are excluded.

#### Unsupported operations and body limits

An unsupported operation under a provider prefix returns HTTP **404**, with a provider-shaped `operation_not_supported` error and the message `<method> <canonical-path> is not supported by cc-lb yet`. It produces one terminal request event after authentication, without selecting an upstream. Unprefixed unsupported paths retain their previous rejection behavior.

Multipart audio transcription/translation and image edit/variation requests use `files_body_cap_bytes`; other supported HTTP operations use `messages_body_cap_bytes`. Bodies and provider-owned state are forwarded without rewriting. Model catalogs and model-less default moderation requests still pass through key/principal, rate, and concurrency admission rather than requiring a fabricated model id.

#### OpenAI Responses streaming failures

Before a streaming `POST /v1/responses` response starts, a proxy-generated failure returns the route's provider-shaped JSON error envelope with the matching error status (500, 502, or 503). After streaming has begun, the HTTP status remains `200`, and a proxy-generated failure is sent as a `response.failed` SSE event. Its payload includes `sequence_number` and a failed `response` with safe `error.code` and `error.message`, plus the observed response id when one is known.

An emitted provider failure keeps precedence over later upstream faults. Response transforms determine which terminal the client actually receives. SSE responses use streaming HTTP framing instead of forwarding an upstream `Content-Length`, so local failures or suppressed malformed suffixes cannot leave an incomplete fixed-length body. Passthrough payload bytes and their content encoding remain unchanged.

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
Responses WebSocket admission uses an upstream-scoped circuit breaker keyed by the stable upstream UUID, independent of HTTP's host-keyed breaker, with the same failure policy as HTTP requests. Provider `5xx` responses, transport failures, and timeouts count toward that WebSocket breaker; `4xx` and other non-`5xx` handshake responses do not. Handshake errors use a generic `upstream_unavailable` error code while preserving the provider HTTP status. Before upgrade, that status and safe allowlisted `Retry-After`, `x-ratelimit-*`, `anthropic-ratelimit-*`, and `ratelimit-*` headers are returned as HTTP response status and headers; after upgrade, the status is preserved in the WebSocket error, with any non-empty allowlisted headers under a top-level JSON `headers` object. An open local breaker returns a generic `502 upstream_unavailable` error. Once a provider connection owns a WebSocket, failures close it without reconnecting or failing over.

Ordinary `response.create` messages require an explicit, non-empty `model`.
cc-lb does not infer a missing model from a stored prompt or an account default.
Steering auto-successors inherit the parent turn's model. This is a bounded
generation gateway; support for the upgrade does not imply support for every
Responses operation or model-inheritance form.

Independent full-history turns can select another eligible upstream.
Continuations with connection-local response state stay on the exact provider
connection that owns that state; a failed connection is not replaced on another
account. Stored response and conversation references retain their durable
upstream affinity. Work is FIFO within each lane, while independent lanes can
run in parallel. Existing request-body caps and the WebSocket bulkhead bound
buffered work and provider connections.

Response identity is bound before forwarding `response.queued`,
`response.created`, or `response.in_progress`. WebSocket response ids from
`store: false` turns remain connection-local instead of creating new durable
response-id bindings. Conversation ownership remains durable independently of
that flag. The HTTP response-id behavior described above is unchanged.

The gateway parses control messages once and forwards ordinary deltas unchanged,
without rewriting them as SSE or reserializing the client request body. Closing
or disconnecting cancels active turns; cc-lb does not add a `response.cancel`
wire operation. A terminal turn releases its concurrent slot immediately.
Measured usage, including a reported numeric zero, is reconciled once by the
background accounting worker even on an unsuccessful turn. Without measured
usage, the reservation is refunded once; `null` or an empty usage object alone
does not count as measured usage. Existing request and trace identifiers
correlate turn events without new database fields.

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
| DELETE | `/admin/v1/upstreams/{id}` | Bearer | None | None | `referenced_by`, `stale_revision` |
| POST | `/admin/v1/upstreams/{id}/enable` | Bearer | None | UpstreamResponse | `stale_revision` |
| POST | `/admin/v1/upstreams/{id}/disable` | Bearer | None | UpstreamResponse | `stale_revision` |
| POST | `/admin/v1/upstreams/preflight` | Bearer | PreflightRequest | PreflightResponse | `missing_api_key`, `invalid_input` |
| POST | `/admin/v1/upstreams/{id}/preflight` | Bearer | None | PreflightResponse | `not_api_key_upstream`, `upstream_not_found`, `api_key_credential_decrypt_failed` |
| GET | `/admin/v1/upstreams/{id}/workspaces` | Bearer | None | `{"workspaces":[{"id","name"}]}` | `not_api_key_upstream`, `non_anthropic_base_url`, `workspace_list_denied`, `probe_unreachable`, `api_key_credential_decrypt_failed` |
| GET | `/admin/v1/upstreams/{id}/usage?range=&step=` | Bearer | None | UpstreamKeyUsageResponse | `not_api_key_upstream`, `upstream_not_found`, invalid `range`/`step` |

#### UpstreamCreateBody

```json
{
  "name": "upstream-name",
  "kind": "anthropic_api_key",
  "base_url": "https://api.anthropic.com",
  "api_key_value": "sk-ant-..."
}
```

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

`spec_revision` is the user-managed optimistic-lock counter used by `If-Match` and `ETag`. Background controller writes (status/lease/secret/token) never bump it. The nested `status` object holds system-managed operational state: only the apply daemon and warmup observer write here. `warmup_enabled` defaults to `true` for `anthropic_oauth` upstreams and `false` for other kinds; see [docs/upstream-warmup.md](./upstream-warmup.md).

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

`month` covers the current UTC calendar month. Its `billing_stop` is classified from the upstream's latest finished proxied request: `credits_exhausted` (400 credit balance), `spend_limit` (400 self-set usage limit), `tier_spend_cap` (429 tier monthly cap), or `billing_error` (402). Any other outcome, including a later success, clears it. Anthropic has no API for the credit balance, so cc-lb never shows one; check the Console Billing page.

The proxy strips any client-supplied `anthropic-workspace-id` header from every upstream request. API-key upstreams send their configured workspace instead.

### Principals API

| Method | Path | Auth | Request Body | Response Body | Error Codes |
|---|---|---|---|---|---|
| POST | `/admin/v1/principals` | Bearer | CreatePrincipalBody | PrincipalResponse | `conflict`, `unsupported_cache_keepalive_llm_judge` |
| GET | `/admin/v1/principals` | Bearer | None | List of PrincipalResponse | None |
| GET | `/admin/v1/principals/{id}` | Bearer | None | PrincipalResponse | `unknown_principal` |
| PUT | `/admin/v1/principals/{id}` | Bearer | UpdatePrincipalBody | PrincipalResponse | `stale_revision`, `unsupported_cache_keepalive_llm_judge` |
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
    "refresh_lead_time_5m_secs": 30,
    "refresh_lead_time_1h_secs": 300,
    "max_refreshes_per_session": 12,
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
`cache_keepalive.classifier.llm_judge` is reserved for a future release;
create/update requests that set it return `unsupported_cache_keepalive_llm_judge`.

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
| POST | `/admin/v1/principals/{principal_id}/plugin-chain/rebalance` | Bearer | None | None | None |
| PUT | `/admin/v1/plugin-chain-entries/{id}` | Bearer | PluginChainEntryUpdate | PluginChainEntry | `stale_revision` |
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

Antigravity content requests use `https://daily-cloudcode-pa.googleapis.com`. Usage polling calls `POST /v1internal:retrieveUserQuotaSummary` with `{"project":"<project-id>"}` and uses the same parser and fallback behavior as Gemini CLI sign-ins. It reports the same four windows: Gemini models have 5-hour (`gemini_5h`) and weekly (`gemini_7d`) windows; Claude and GPT-OSS models have 5-hour (`third_party_5h`) and weekly (`third_party_7d`) windows.

> [!WARNING]
> Google Antigravity Additional Terms [§6](https://antigravity.google/terms) prohibit using third-party software, tools, or services to access the Service with Antigravity OAuth. The terms state that this is a breach and may be grounds for suspension or termination of Antigravity and/or Gemini CLI accounts. Operators decide at their own risk whether to use this integration.

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

A terminal refresh result (`400 invalid_grant`, `401`, or a known expired refresh token) records a reconnect-required state that scheduled, watchdog, and lazy refresh paths honor until the credential is replaced or the account is reconnected. Routing also stops selecting a reconnect-required OAuth upstream; the periodic reconciler picks up the state change on the next successful reconcile or rebuild on every replica. Other `400` responses, network failures, and `5xx` responses remain transient and retry.

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
- **Workaround**: Verify that the network can reach the OAuth provider and that the client credentials in `cc-lb.toml` are correct. Transient network, `5xx`, and non-terminal `400` refresh failures retry. For `400 invalid_grant`, `401`, or a known expired refresh token, rerun connect/reconnect to replace the credential; scheduled, watchdog, and lazy refresh paths do not retry while reconnect is required.

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
