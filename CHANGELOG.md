# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.0.1] - 2026-10-02

### Removed

- The admin dashboard no longer shows the Unified account-restriction envelope: it has no quota row, chart series, legend entry or reset fact anywhere, and the web app no longer requests `unified` window data. This is presentation-only — the backend still parses the provider's unified rate-limit headers, stores the window, reports it in admin API payloads, and keeps using it for subscription-preference overage routing.

### Changed

- Shared non-Logs time controls persist the selected `1h`, `6h`, `24h` or `7d` preset in local storage, default to `7d`, and update other mounted consumers; Logs keeps its independent `All` default.
- Model filtering is case-insensitive and accepts an optional leading `claude-`: `sonnet` matches both `claude-sonnet-4-5` and model IDs such as `claude-3-5-sonnet-…`, while live, retained and historical rows use the same match.
- Warm-up status, last-run details, history counts and outcome filters now use explicit tones: success is green, transient failure is warning, permanent failure is danger, and skipped or idle states are neutral.
- Live indicators now use the shared semantic status colors: Admin connection and request-feed `Live` are green, while pending warm-up remains the accent `live` tone.
- Live tail status moved out of the shared request table and into each page's heading: Overview's Latest requests shows it at the start of the subtitle, the upstream and principal Recent requests sections show it beside their titles, and Logs keeps its single toolbar Live tail control.
- HTTP status codes in request details, the audit list and drawer, and warm-up history are colored by class: 2xx green, 4xx warning, 500 and above danger, and other or missing codes neutral. The request table's Status cell keeps coloring the request outcome, so a 200 that ended with an error code still reads as danger there.
- Request-table costs stay right-aligned without a latest-value shadow, plugin pages remove the redundant explanatory copy, and upstream renaming uses an inline borderless title editor.
- Shared request feeds bound history outside Logs: Overview and the entity-scoped feeds retain at most 500 rows, revealing them 50 at a time in a bounded scroll slot and fetching an older cursor page only when the retained rows run out, while live arrivals keep the newest rows and drop the oldest past the cap. Logs keeps its 50-row paged history with cursor Next for older pages.
- Top-principal cost bars share a common left edge and scale to the largest cost in the full result set, with fixed two-decimal values and exact details; cache-hit thresholds, token unit colors, and positive-only cost breakdowns match the request views.
- Quota severity is now pace-relative and gap-only: usage running 10+ points ahead of even pace warns and 30+ points turns danger, while windows with no pace reading stay neutral at any figure and the old absolute 80/90/95 thresholds are gone. Quota history charts no longer draw the 95% danger line or its legend entry, and the upstreams list's needs-attention quota boundary follows the same gap rule.

### Fixed

- Terminal OAuth refresh failures (`400 invalid_grant`, `401`, or an expired refresh token) now latch reconnect-required state across scheduled, watchdog, and lazy refresh paths until credential replacement or reconnect; transient network, `5xx`, and non-terminal `400` failures still retry.
- Disabled OAuth upstreams remain visible with reconnect notices, and quota rows hide extra-usage budgets that are switched off or have no positive monthly limit.
- Dashboard history regression tests isolate repeated row presentation from paging and live-row anchoring; real-table checks cover the Overview feed's 500-row scroll cap with live arrivals trimming the oldest rows, and Logs' 50-row pages with live-cursor movement.

## [1.0.0] - 2026-09-30

cc-lb 1.0.0 is the first stable release and marks the contract boundary: the versioned admin API (`/admin/v1/…`), the CLI, the configuration format, the Prometheus metrics and the `filter`/`shape` plugin slots are now the supported surface, and later breaking changes to them will come in a new major version. Getting there meant a final round of removals: the compatibility aliases, legacy data formats and dead schema left behind by earlier cutovers (#890), the Wasm observability plugin slot (#885), and the quota analysis endpoint (#883). This release also ships a redesigned admin dashboard with a new brand and colour scheme (#883).

Read the upgrade notes before upgrading: the migrations drop columns and tables, so every replica must be stopped first and a backup is the only way back.

### Upgrade notes

1. **Back up the database first.** The migrations drop columns and tables and cannot be reversed. Rolling back to 0.8.x is possible only by restoring a backup taken before the upgrade; without one there is no rollback.
2. **Stop every running replica before the first upgraded one starts.** Earlier builds still write the dropped columns and tables, so an old replica running next to a migrated database fails its request-event, audit and config-history writes. With the Helm chart, a PostgreSQL deployment defaults to `RollingUpdate`; set `updateStrategy.type: Recreate` for this upgrade or scale to zero first. (#890)
3. **On SQLite, plan for a slow first start.** Migration 0093 rebuilds `request_events_v1` once to drop its dead columns. With about a million request events (5 GB) the first start took about 2.5 minutes in a rehearsal and needed free disk for a second copy of the table plus its WAL; the file keeps that space until `VACUUM`. With the Helm chart, raise `probes.startup.failureThreshold` for this upgrade so the startup probe does not restart the pod mid-migration. PostgreSQL migrations 0129–0132 took under 3 seconds on the same volume. (#890)
4. **Check callers against the breaking changes below**, in particular scripts that use unversioned `/admin/…` paths, disabled API keys, observability-hook plugins, or the removed metrics.

### Breaking changes

- The admin endpoint `GET /admin/v1/subscription-quotas/analysis` (and its unversioned alias `/admin/subscription-quotas/analysis`) has been removed, along with its burn-rate, projected-burn, deficit, and ETA estimates. The admin dashboard no longer calls it. Scripts that queried it now receive 404; use `/admin/v1/subscription-quotas/series` for quota history and `/admin/v1/subscription-quotas/aggregate` for current pool state. (#883)
- The Wasm observability plugin slot has been removed. `slot_kind=observe` uploads and `ObservabilityHook` plugin-chain slots are now rejected as unknown, existing observability-hook chain entries are deleted on migration, and the unused `sse_per_event`, `batched_events_per_flush`, and `batched_flush_ms` plugin-chain fields are gone. Plugins can target only the `filter` and `shape` slots; the `cc_lb_observe` export, `ObserveEvent`, and `HookKind::Observe` no longer exist in the plugin wire, PDK, runtime, and conformance crates. (#885)
- Request events no longer carry `observability_post_ms`, which measured the removed observability-hook fan-out and has always been empty since that fan-out moved off the response path. The field is gone from the admin request-event APIs, and SQLite drops the `request_events_v1.list_observability_post_ms` column on migration. (#885)
- Unversioned admin API aliases have been removed. Use the `/admin/v1/…` path instead of `/admin/audit`, `/admin/usage`, `/admin/dashboard/summary`, `/admin/dashboard/usage`, `/admin/events/{recent,stream,delta,histogram}` and `/admin/subscription-quotas/{latest,series,aggregate,pool-history}`. The principal usage and limits endpoints (`/admin/principals/{id}/{usage,limits}` and their `/admin/v1` forms) are gone; nothing in the dashboard called them. The unversioned per-key routes under `/admin/principals/{id}/keys/{key_id}` (get, `revoke`, `disable`, `enable`, `usage`) are gone as well; list, issue and revoke keys through `/admin/v1/principals/{id}/keys`. (#890)
- Managed API keys can no longer be disabled; only active and revoked remain. The migration (SQLite 0095, PostgreSQL 0132) revokes any key still marked disabled, so such a key now gets 401 instead of 403, and `GET /admin/v1/principals/{id}/keys?status=disabled` returns 400. (#890)
- Admin API inputs no longer accept legacy spellings: Wasm uploads ignore the removed `slot_kind` and `name` form fields (slots come from the module's exports), plugin-chain `slot` values must be snake_case (`router`, `shape`), warmup-attempt lists filter by `status` only (the `outcome` alias is gone), and principal cache-keepalive cursors without a horizon tag are rejected with 400. (#890)
- `cc-lb config validate` no longer takes `--data-dir`, and config editor responses no longer include `restart_required`. (#890)
- Request events no longer carry `upstream`, `limit_reconcile_ms`, `lineage_would_have_predicted_read_tokens` or `lineage_would_have_picked_upstream_id`, and the subscription-preference stage of their routing trace no longer carries `previous_tier` or `incumbent_upstream_id`. The `upstream` query parameter on the admin events list, histogram, stream and delta endpoints is ignored instead of filtering, and request details no longer show the provider label ("Anthropic") next to the upstream name. Latency views show a single Finalize stage, and token views read `cache_creation_input_tokens_5m`/`_1h` directly. (#890, #894)
- `GET /admin/v1/audit` entries no longer include `kind`; read `admin_action`, which carried the same value. (#890, #894)
- These metrics are no longer registered: `cclb_price_catalog_refresh_failures_total`, `cclb_price_catalog_validation_failures_total`, `cclb_usage_writer_dropped_total`, `cclb_limit_state_writer_dropped_total`, `cclb_streaming_usage_missing_total`, `cc_lb_quota_active_principals_total`, `cc_lb_quota_rejected_total` and `cc_lb_cache_keepalive_llm_latency_seconds`. Keep-alive cancellations report only `user_turn_detected` and `snapshot_too_large`. (#890)
- Data written in pre-cutover formats is no longer read: API-key upstream secrets encrypted before #848, SSE frames that only parse line by line, OMP session markers without JSON metadata, and scheduler jobs queued under the `o_auth_*` names or the retired `quota_gc` cron (queued rows are purged on migration). Per-principal cost views count UUID principals and events without a principal (as `unknown`); request events whose `principal_id` is some other string, which current builds never write, are no longer normalized into those views. (#890)

### Removed

- Legacy compatibility code, fallbacks and dead code left behind by earlier cutovers: the unused router-plugin slot (`RouterPlugin`, `global_router`), the unused `cc-lb-dialect-anthropic` crate, the plugin blob repository, redb-era storage record formats and contract-version checks, the warmup-loop helpers, and the Bedrock/Vertex and Extism leftovers. (#890)
- Plugin SDK aliases and helpers the host no longer uses: the `SlotKind` alias (use `HookKind`), `#[derive(WireSchema)]` (hook wire types get their fingerprints from `cc-lb-plugin-wire`), `WasmtimeRuntime::admit_wasm(kind, bytes)`, and `ConformanceSuite::assert_static_admission`. The published plugin crates take a major version bump at the next release; see their changelogs. (#890)
- Schema that no current code reads or writes. PostgreSQL migration 0130 drops `killswitch_v1`, `quotas_by_principal_v1`, `principal_limit_states_v1`, `plugin_registry*` and 26 insert-only `request_events_v1` columns (their values stay in the payload JSON), plus stale columns, indexes and the cost-component compatibility trigger. SQLite migration 0093 does the same for its schema. Scheduler migration 0010 drops `anthropic_compat_etags.etag`. (#890)
- Schema follow-ups: SQLite migration 0094 adds `principals_v1_name_idx` so the name-ordered principal list uses an index instead of sorting the table, and PostgreSQL migration 0131 drops `upstream_spec_v1_name_idx`, which no query used. (#890)

### Added

- A brand asset kit now lives in `assets/brand/`: the let-gate mark as SVG originals (dark, light, mono and currentColor variants plus a 16px-snapped small-size version), app-icon and maskable tiles with PNG renders, wordmark and lockup compositions, a README hero, a social preview image, and the favicon set (`favicon.svg`, `favicon.ico`, `apple-touch-icon.png`) now wired into the admin dashboard. (#883)

### Changed

- **Admin dashboard redesign.** The brand mark is now the let-gate — two squared bracket "c" shapes forming a gate with one accent lane through it — replacing the old 240° dial in the sidebar, the top bar and the sign-in frame. (#883)
- The admin dashboard uses a new neutral graphite colour scheme in both Night and Day. Colour now comes from data (charts, meters, session and request-kind chips). Controls such as the primary button, the on switch and the focus ring are neutral silver (Night) or ink (Day), and session chips cycle through nine hues instead of seven. (#883)
- The Docs link at the foot of the dashboard sidebar has been removed until a dedicated documentation page exists. (#883)
- Real-client, soak, load, musl and TLS diagnostics now use `target/test-evidence/<suite>/` instead of `.omo/evidence/`. Playwright screenshots and attachments use per-test output paths, and Rust tests no longer write unused agent evidence files. CI artifact names no longer contain agent task numbers. Artifact retention is 7 days for final reports and Docker build records, and 1 day for intermediate coverage files. Existing stored artifacts require separate cleanup; GitHub may take time to recalculate the available storage.

### Fixed

- Dragging across the Logs histogram to pick a time range no longer leaves trailing afterimages or flickers. The selection, its handles and the time hint now move together with the pointer. (#883)
- Releasing a drag on the Logs histogram no longer blanks the bars for a moment, which left the selection alone on an empty strip. The histogram now keeps its data when a range is selected instead of refetching it. (#883)
- Latency breakdown colours are now easy to tell apart. The responsibility segments in the request log table, its hover breakdown and the request detail sheet were three near-identical blue-teals (Downstream, cc-lb and Upstream wait); they are now magenta, blue, lime and teal, differing in lightness as well as hue so they stay distinct under colour-vision deficiency. In the detail sheet's request timeline, the ten or so internal setup stages no longer share a barely changing shade of one blue: each takes a distinct lightness and hue step, so neighbouring segments stand apart. (#893)
- The PostgreSQL connection-pool gauges `cc_lb_sqlx_pool_size`, `cc_lb_sqlx_pool_idle` and `cc_lb_sqlx_pool_in_use` now report the live pool with `store="postgres"`, sampled at startup and once per second; previously only SQLite deployments recorded them. (#865)

## [0.8.1] - 2026-09-26

### Fixed

- Subscription routing no longer treats an approaching five-hour reset as a reason to pull traffic toward an account that is already at or ahead of its weekly pace. Previously the five-hour window was scored as an independent use-it-or-lose-it deadline, so near every five-hour reset such accounts took new requests from accounts that could still lose unused weekly quota, and prompt-cache affinity then kept those conversations there. Five-hour pressure now counts only while leaving the current window unused could actually forfeit weekly quota. If the five-hour or shared weekly quota data is stale or missing, routing behaves as before; a model-specific weekly window can keep the pressure but never removes it on its own. ([#878](https://github.com/isac322/cc-lb/pull/878), [ADR 0013](https://github.com/isac322/cc-lb/blob/master/docs/adr/0013-weekly-pace-gate-for-five-hour-pressure.md))

### Changed

- Routing traces now report the within-tier selection formula as `cost-first-v2` (previously `cost-first-v1`). Among candidates with equal effective urgency, the raw five-hour pressure is used as the next tiebreak, so refill-order spreading between otherwise equal accounts is preserved. Dashboards or alerts that match on the formula version string should be updated; the bundled `deploy/alerts/routing-anomaly.yml` already is.

### Upgrade notes

- Upgrading from 0.8.0 requires only a restart: no new configuration keys and no database migrations. Rolling back to 0.8.0 is safe.

## [0.8.0] - 2026-09-26

### Changed

- Adding an upstream and connecting a Claude account now share one step-by-step dialog in the admin dashboard. "New upstream" starts by choosing a Claude subscription or an Anthropic API key and continues in the same window. Connect and Reconnect on an OAuth upstream open that dialog at the sign-in step, from the detail page, the reconnect notice, the Overview summary, and `?action=` deep links. The previous separate create and reconnect windows are gone. ([#867](https://github.com/isac322/cc-lb/pull/867))
- The Claude sign-in button is now a plain link prepared in advance, so browsers no longer block it as a popup. The code field accepts the value Claude shows (`code#state`), the bare code, or the whole callback address. It flags a code copied from an older sign-in and submits as soon as a valid value is pasted. A countdown shows the 15-minute sign-in window, with a one-click new link once it expires, and the internal state token is no longer displayed.
- Reconnect shows which Claude account to sign in with, then reports whether the same account came back. If a different account was connected, the dialog shows the before and after accounts. The message after a fallback to a renewing credential now explains that the connection works and renews itself.
- Claude sign-in failures are shown in the dialog next to the code field instead of in a separate error toast.
- The compatibility store now tracks the Claude Code `stable` and `latest` release channels independently. Usage polling and its account-identity lookup identify as `latest`, falling back to `2.1.282` until the first refresh; other metadata requests and coupon claims keep using `stable`. ([#866](https://github.com/isac322/cc-lb/pull/866))
- The daily compatibility job refreshes both channels concurrently from the official Claude Code release endpoint, falling back to the matching npm dist-tag. A channel whose refresh fails keeps its last successful value without affecting the other.

### Fixed

- The Admin Logs `errors` filter now paginates over matching requests: HTTP statuses of 400 or higher and abnormally completed requests with an `error_code` are selected before the page limit, so older errors are no longer hidden behind pages of successful requests and the row range stays valid. The live tail still applies this filter in the browser, so a request whose upstream status is corrected by a retry (for example 401 followed by 200) leaves the list immediately. ([#862](https://github.com/isac322/cc-lb/issues/862), [#864](https://github.com/isac322/cc-lb/pull/864))

### Upgrade notes

- Upgrading from 0.7.0 requires only a restart: no new configuration keys and no database migrations. The `latest` channel value is stored in the existing compatibility table and appears after the next daily compatibility refresh; until then usage polling uses the `2.1.282` fallback.
- Rollback caution: compatibility-refresh jobs queued by 0.8.0 cover all channels in one payload, which 0.7.0 and older workers cannot decode. Jobs queued by earlier releases remain readable by 0.8.0.

## [0.7.0] - 2026-09-25

### Added

- Anthropic OAuth upstreams now support Anthropic's subscription limit-reset coupons. The periodic quota poll collects coupon status in the same provider request as quota windows and extra usage, and `GET /admin/v1/upstreams/{id}/limit-resets` serves that stored snapshot without contacting the provider. A coupon is reported only while the snapshot is fresh and bound to the current credential and account; before the first observation or after a credential change, the API reports no coupon rather than a possibly-consumed one. ([#857](https://github.com/isac322/cc-lb/pull/857), [#858](https://github.com/isac322/cc-lb/pull/858))
- The upstream detail page gains a "Reset quota" action, and the upstream list a coupon badge that highlights a coupon nearing expiry or a cleared window observed at its limit. The confirmation dialog lists each active coupon's remaining uses, the limits it clears, and its expiry, alongside provider blocking reasons and account cooldowns, and warns when the account is not at its limit; nothing is consumed until the operator confirms. Eligibility, cooldowns, and expiry are entirely provider-controlled — cc-lb caches the polled snapshot and invents no eligibility of its own.
- Confirming re-verifies the live OAuth account and organization, then dispatches exactly one reset request. Claims are serialized per upstream across replicas, and a successful reset queues a fresh usage poll so quota and coupon state reconcile.
- A claim is never retried, and an unknown outcome is never reported as failure: a timeout, cancellation, mid-flight transport failure, unreadable success body, or provider 5xx returns `claim_outcome_unknown` (504), because the provider may already have consumed the grant. A provider 4xx is a definite rejection and a connect failure means the request was never sent; only those are reported as definite outcomes. The dashboard keeps a "Reset result not confirmed" record until the operator dismisses it or the account changes, and each claim attempt is audited with its request ID. After an unknown outcome, check the account's usage (for example at claude.ai/settings/usage) before taking further action; nothing resubmits automatically.

### Changed

- Quota polling now identifies itself with the stored Claude Code client version on usage and profile requests, so the provider reports eligible reset coupons instead of rejecting the client surface. ([#858](https://github.com/isac322/cc-lb/pull/858))
- A claim interrupted before its outcome is recorded — cancelled, or its bookkeeping write lost — is reconciled by usage polling from a fresh provider observation; recovery never repeats the reset request.

### Upgrade notes

- Upgrading from 0.6.0 requires only a restart: no new configuration keys and no database migrations. Coupons appear after the next quota poll; until the first observation the dashboard reports no current coupon data.

## [0.6.0] - 2026-09-22

### Breaking changes

- `prompt_cache_shadow.refresh_debounce_secs` and `prompt_cache_shadow.max_live_entries_per_partition` have been removed; configuration files containing them fail to load. Prompt cache observations are now read directly from the shared observation store rather than a pod-local cache, so the debounce window and per-partition entry ceiling no longer exist. `prompt_cache_shadow.grace_margin_secs` is unchanged.
- For `anthropic_api_key` upstreams, a credential supplied by the downstream client is no longer forwarded to the upstream in any header. The upstream receives only the operator-configured key, mirroring how OAuth upstreams already replace `x-api-key` with their own bearer token. A deployment that relied on passing a client-supplied `Authorization` header through cc-lb to an authenticating gateway named in `base_url` must move that gateway in front of cc-lb instead.
- Request-log rows now carry new `ingress`, `storage`, and `invalid_input` diagnostic values. Rows written by earlier releases remain readable, but a binary from an earlier release may fail to decode rows written by this one.

### Added

- Connecting an Anthropic OAuth upstream now always requests a 365-day long-lived credential; there is no mode to choose. The request is not a guarantee: the stored expiry always reflects the lifetime Anthropic actually returned, never the requested value. If Anthropic refuses the year-long expiry outright, or accepts the exchange but grants a materially shorter lifetime that does not outlive the refresh-token window, the connect flow keeps the credential refreshable instead of marking it long-lived, so the upstream cannot silently expire with no way to renew; the response reports which fallback occurred. A long-lived credential is never refreshed: reauthorization is needed once the granted lifetime elapses — about once a year when Anthropic grants the full 365 days — instead of about every 30 days. Long-lived credentials remain fully managed otherwise: quota polling, warm-up seeding, automatic subscription-metadata refresh, and authentication-failure reporting apply exactly as they do for refreshing credentials. Existing upstreams keep their current behaviour.

### Changed

- The default `oauth.anthropic.scopes` is now `["user:profile", "user:inference"]`; `org:create_api_key` was removed because Anthropic only grants year-long access tokens for inference-only scopes. Operators who explicitly configured `org:create_api_key` keep that setting, but their connections fall back to refreshing mode with `fallback_reason: "scope_rejected"`, and cc-lb reports that reason.
- Request Log tables now default to `/v1/messages` requests. The Logs page filters by Messages, Token count, Models, Files, Other proxy requests, Renewals, or Unclassified, and a status filter can narrow the table to rows with a recorded error, including errors on delivered 2xx responses. Overview and principal/upstream previews show Messages only. Row contents and the set of recorded requests are unchanged.
- New request logs retain endpoint-category metadata for consistent history and live filtering. Existing logs without that metadata remain available under Unclassified on the Logs page; existing renewal logs remain under Renewals.
- Overview and Upstreams now surface OAuth reconnect guidance for known refresh-token expiry and failed renewal, with direct Connect/Reconnect actions. For a standard refreshing credential, warnings begin three days before a known deadline and routine access-token expiry does not trigger a warning while refresh remains available. A long-lived credential has no meaningful refresh deadline, so its guidance is driven by the access token instead and begins two weeks before it expires. Disabled upstreams are excluded from global attention counts.
- Upstreams shows which credential mode an OAuth upstream uses. For a long-lived credential the stored refresh token is listed as retained but unused, and no refresh-token expiry warning is shown.
- Authentication now runs on request headers before the body is buffered or parsed, so a request with a missing or invalid credential is rejected without buffering or parsing its body. A rejected credential produces exactly one request-log row; requests that never reach authentication — router 404/405 fallbacks, drain rejections, and pre-handler timeouts — produce none.
- Build toolchain, container base images, CI tooling, and Cargo dependencies were refreshed to current releases (Rust 1.98.1, wasmtime 48.0.2). No runtime behavior change is intended.

### Fixed

- Long-lived OAuth upstreams were excluded from the periodic OAuth usage poll, so their subscription quota was never refreshed. They are polled again.
- Newly connected long-lived OAuth upstreams with warm-up enabled did not receive their connect-time warm-up bootstrap task. They do again.
- A long-lived OAuth upstream whose credential was rejected by Anthropic kept showing as connected until the token expired, up to a year later. The periodic usage poll now records the rejection and the upstream surfaces a reconnect prompt.
- Request logs retain redacted internal failure reasons for authentication, ingress, routing, signing, transport, and storage failures. Failed connections retain measured DNS/connect timings and distinguish DNS, TCP, TLS, and timeout causes. Upstream HTTP error parsing accepts partial `error.type`/`error.code` and `error.message` fields without changing forwarded responses or SSE error semantics.
- Abnormal upstream stops — a refusal or a context-window rejection reported as a streaming stop reason — are now recorded as upstream errors on the request-log row even when the client still receives HTTP 200; the forwarded response bytes are unchanged.
- Graceful shutdown now holds each request's drain guard until its response body finishes relaying, and drains the request-log pipeline within a bounded grace window before exit. Row-bearing events that hit a full assembler channel are retried by bounded overflow tasks instead of being dropped.
- Prompt-cache routing reads a request-scoped shared-store snapshot, and out-of-order observation writes cannot shorten a committed expiry or replace its metadata with older data. Streaming observations are submitted when `message_start` usage arrives; response delivery never waits for their commit. Store lookup failures disable cache affinity for that decision without failing the request.
- `anthropic_api_key` upstreams now sign upstream requests with the credential configured as `api_key_value` or `api_key_env`, which was previously validated, encrypted, and stored but never used. Credentials written by earlier versions continue to work unchanged, in either historical storage form, with no operator action and no re-entry of keys. A credential that cannot be resolved fails the request with a gateway error and is reported through the upstream's apply status; it never falls back to a client-supplied key.

## [0.5.0] - 2026-09-20

### Breaking changes

- Runtime entities are now managed exclusively in the database through the Admin API and dashboard. Legacy bootstrap configuration and unauthenticated downstream mode have been removed, and unknown configuration fields now prevent startup.
- Update configuration before restarting: move `[tls]` to `[listener.tls]`, `[api_keys.price_catalog]` to `[price_catalog]`, `api_keys.usage_retention_days` to top-level `request_event_retention_days`, and `aead.oauth_aead_key_env` to `aead.key_env`. Remove obsolete `[api_keys]`, `[downstream_auth]`, `[dns]`, `[egress]`, and `lifecycle_*` sections, including empty tables left after moving their settings. Removed nested options include `listener.unix_socket`, `body.per_route_overrides`, `timeouts.request_header_secs`, `timeouts.request_body_chunk_secs`, `timeouts.idle_secs`, `scheduler.retry_classes`, `scheduler.idempotency`, `scheduler.staleness`, `scheduler.pgbouncer_transaction_mode`, `observability.prometheus_endpoint`, `cluster.token_env_optional`, `storage.storage_path`, `runtime.wasmtime.plugin_failure_policy`, `price_catalog.refresh_interval`, and `prompt_cache_shadow.enabled`; the Wasmtime `on_demand` allocation strategy is also no longer accepted.
- Admin access now requires an explicit provider in `[[admin.auth.providers]]` or `CC_LB_ADMIN_AUTH_PROVIDERS_JSON`. For an existing token, configure `kind = "static_token"`, an `id`, and `token_env = "CC_LB_ADMIN_TOKEN"` (or your existing variable). Remove the old `admin.token_env` and `admin.token` fields. Setting `CC_LB_ADMIN_TOKEN` alone no longer enables admin access.
- Process settings are fixed at startup. Settings saves write the configuration file and require a restart; SIGHUP only reloads TLS certificates and keys. The former configuration apply/diff endpoints have been removed.
- Database migrations permanently reduce stored configuration history to revision and application-time metadata, removing previous configuration payloads that could contain secrets. Preserve any configuration snapshots needed for recovery securely before upgrading.
- Legacy credential APIs and the Credentials page have been removed. Use Upstreams for provider credentials and Principals for managed client keys. Database migrations drop the obsolete credential tables; back up the database and migrate any remaining legacy credentials before upgrading.
- Obsolete admin status and killswitch endpoints have been removed. The retained `GET /admin/v1/status` response no longer includes `principals`, `plugin_chain_summary`, `killswitch`, `last_reload_status`, or `restart_required_changes`.

### Added

- Admin authentication supports provider-neutral identities with static-token and Cloudflare Access providers, with synchronous security auditing.
- Request Logs expose latency attribution across downstream I/O, cc-lb processing, upstream networking, upstream wait, and unattributed time.

### Fixed

- Cache Keepalive summary, list, and detail reads now use bounded storage queries and targeted detail lookup instead of materializing unrelated historical decisions, while preserving ordering, pricing, pagination, and selected-row error mappings.
- Cache Keepalive 24-hour and 7-day cursors now retain the first page's time-window anchor, so pagination remains valid while the server clock advances.
- PostgreSQL migrations no longer share version `0121` between the API-key concurrency-hold table and the audit principal read-order index, so a fresh database applies the full set instead of aborting on a duplicate `_sqlx_migrations` key.
- PostgreSQL Settings drafts now accept updates at the current revision, including draft cleanup after file saves and invalid-draft expiry, while still rejecting stale revisions.
- PostgreSQL replicas now share concurrent request holds, routing-cache invalidation, WASM upload limits, and OAuth PKCE handshakes.
- Audit queries apply the admin-only filter before limiting results and use read-order indexes.
- Admin APIs now accept empty managed-key labels, return invalid inputs as 400 Bad Request, allow clearing upstream base URL overrides, and include base URLs in OAuth draft responses.
- Admin Web prevents duplicate inline-name saves, keeps plugin deletion available during upload garbage collection, handles cascade-deletion query cleanup and dialog back-navigation, and refreshes terminal routing strategy after principal changes.

## [0.4.9] - 2026-09-11

### Changed

- Prompt-cache setup now sizes prefix boundaries by serialized JSON byte length instead of running full o200k BPE tokenization on the request path, restricting local BPE to ambiguous threshold bands inside a bounded blocking executor.
- Unused prompt-cache token drift subscriber and observation columns have been removed.

### Fixed

- Request latency tracking now accounts for complete end-to-end request durations and preserves lifecycle timing metrics across errored and aborted response streams.

## [0.4.8] - 2026-09-09

### Fixed

- Proxy relays now terminate after upstream body errors instead of continuing to stream a broken response.
- Completion observers no longer run on the response hot path, so they cannot stall or cancel a fully delivered stream.

## [0.4.7] - 2026-09-09

### Added

- Request Logs now expose eight disjoint setup timings across JSON parsing, prompt-cache analysis, tokenization, and signer preparation. New rows show the measured stages and residual setup separately while historical rows retain the existing aggregate fallback.
- Upstream-affinity retention is configurable with `[upstream_affinity] ttl_days = 90` or `CC_LB_UPSTREAM_AFFINITY__TTL_DAYS`. Positive values take effect after restart and also apply to existing mappings with no explicit expiry. Bindings expire after the configured time since their last successful bind, without writes on replay; expired or unknown history fails closed with 503. A recurring `upstream_affinity_purge` job removes expired mappings in bounded indexed batches outside the proxy path, every 10 minutes with up to 30 seconds of jitter by default.

### Fixed

- Subscription routing no longer demotes usable `KnownBase` or `PartialBase` quota solely because the provider reports `allowed_warning` or a surpassed threshold. Warning signals remain observable, hard-negative quota states still block routing, and the lower-priority Overage tier retains its warning penalty.
- Response-stream failures now retain bounded, redacted transport error chains, dedicated tracing spans, and exactly-once termination metrics through completion. Fully delivered Content-Length passthrough responses are no longer misclassified as cancelled when the client stops polling at the declared length.
- Anthropic native web-search history now keeps exact upstream affinity across cache misses and quota-driven routing changes. cc-lb persists only SHA-256 digests of opaque `encrypted_content`, fails closed when affinity is unknown, conflicting, unavailable, or cannot be persisted, and gates streaming search-result events until the mapping is durable.
- Upstream-affinity persistence now uses atomic bounded batches, with fixed-shape PostgreSQL array inserts and bounded SQLite statement caching. SSE observers share parsed events, affinity matching avoids quadratic key scans, and JSON traversal skips redundant work while preserving routing, replay, and fail-closed delivery semantics. Storage latency and batch-size metrics expose the remaining persistence cost.

## [0.4.6] - 2026-09-08

### Added

- Anthropic OAuth credentials now persist refresh-token expiry reported by the token endpoint, expose it through the admin status API, and show the remaining lifetime on upstream detail cards while keeping existing credentials compatible.

### Fixed

- Admin charts and request lists retain successful data during background refreshes instead of replacing it with skeletons. Rolling quota queries now keep stable cache keys and update their time bounds when responses arrive; range changes preserve the displayed chart until replacement data is ready.
- Histogram and detail queries no longer reuse another filter or entity's data. Query cancellation also preserves the existing 30-second request timeout.
- Admin editing sessions now retain their starting revision, isolate principal drafts, and preserve unsaved changes through external updates. Shared locale, timezone, and theme preferences update all mounted consumers.
- Live-event reconnection uses the current filters, keeps connections through short tab switches, and excludes intentional pauses from failure tracking. Query subscriptions and memoized request rows avoid unchanged-data rendering work.
- Command palette actions survive lazy route loading, plugin upload callbacks survive modal transitions, and Audit exposes only supported principal/time filters. Unsupported in-app admin-token rotation is replaced with accurate restart guidance.
- Request-detail timelines retain live data while final details load, session-detail errors retain mobile back navigation, and previously omitted state-management tests are collected.
- Soft-deleted upstream and principal names can be reused with new resource IDs while active-name uniqueness and historical records remain intact.
- PostgreSQL principal-cost aggregation now reads materialized cost columns from a covering index instead of decoding request payloads, while a compatibility trigger protects rolling deployments.
- PostgreSQL read-path migrations now allow 90 seconds for backfills and index builds, and can be replayed after a rollback rewinds their migration registry entries.
- Principal usage totals now use static filtered and unfiltered cost queries backed by covering indexes, skip dense empty buckets, and coalesce identical reads behind a two-second cache.
- Subscription quota series and provider-lot calculations now bound work to the requested range while preserving left-anchor state, and quota analysis filters usage rollups in storage with a stable 120-second query key.
- Subscription quota analysis now rejects caller-supplied ranges above 20,000 one-minute buckets before storage reads, while all supported 1h/6h/24h/7d ranges remain unchanged.
- Overview requests principal totals without dense time-series buckets and caps pooled-quota chart points while preserving bucket peaks and exact latest values. Pool-history defaults remain unchanged.
- Rollup-backed Overview polling now revalidates with weak ETags derived from the persisted rollup checkpoint before response construction; live principal-cost reads remain uncached.
- Warmup history and cache keepalive session lists now load only when their drawers open, eliminating hidden detail-page reads.
- Prompt-cache request analysis now reuses exact token-prefix counts, incrementally tokenizes nested breakpoints on a bounded blocking executor, and exposes low-cardinality Prometheus work metrics. Cache-control hashes, token estimates, routing, observations, and upstream request bytes remain unchanged; Request Log timelines exclude post-response observability work.

## [0.4.5] - 2026-08-24

### Fixed

- Scheduler housekeeping now reaps stale job locks before deleting workers, cleans the production `cache_keepalive` queue, and correctly decodes PostgreSQL attempt counters. This prevents stale worker references from blocking cleanup.
- Cache keep-alive snapshots now use a compact, compressed ciphertext format, keep the same payload across synthetic refresh generations, and discard ciphertext when a session terminates. Legacy JSON ciphertext remains readable and is rewritten once if its session renews.
- Streaming upstream body transport failures are now recorded as upstream stream failures instead of successful requests. Compressed SSE decode failures on the upstream-response leg are recorded as `upstream_response_decode_error`; this identifies the failing proxy leg, not whether malformed bytes originated at the provider, transport, or local decoder. When response transformation is active, cc-lb also emits an identity-encoded `api_error` frame; compressed passthrough responses remain byte-identical. Unterminated identity SSE and transform-active SSE responses emit the same client error and are recorded as upstream framing failures. Transform-hook and transform-local decode failures emit `response_transform_error` instead of replaying compressed bytes without `Content-Encoding`. Existing response-transform failures and HTTP error classifications remain authoritative.

## [0.4.4] - 2026-08-18

### Fixed

- Runtime logging now enforces `RUST_LOG` or `observability.tracing_level` across the dynamic tracing layer stack, so `DEBUG` and `TRACE` events no longer bypass an `info` filter.

## [0.4.3] - 2026-08-18

### Fixed

- Overview Top principals cost enrichment now uses principal-leading range indexes instead of normalizing or aggregating every request event in the selected period. Legacy non-UUID principal identifiers retain their normalized fallback path, and zero-cost recorded components remain visible.
- PostgreSQL request-event tail polling now seeks the highest sequence below the snapshot visibility horizon with a backward primary-key scan instead of aggregating the full event table twice per second.

## [0.4.2] - 2026-08-18

### Added

- Overview KPI cards now share synchronized hover/focus details with exact timestamps and values. Tokens also chart cache-miss ratio and show its period average, while Top principals show each principal's period-average cache-hit ratio.
- Top principals now break virtual cost into Input, Output, Cache create 5m, Cache create 1h, Cache read, and any unattributed remainder. Pointer hover and keyboard focus expose exact category and total values without fabricating splits for legacy or incomplete data.
- Hermes Agent requests now preserve the stable root conversation identity and classify main, subagent, session-title, and compaction traffic across Anthropic Messages and OpenAI-compatible chat-completions requests.

### Fixed

- Live Request Logs now re-render rows received over SSE and preserve correct pagination when a new live row displaces the historical page tail.
- Admin operations that wait on the server now use single-flight submission, visible progress labels, disabled conflicting controls, and dismissal protection for pending or one-time-secret dialogs. This prevents duplicate API key issuance and other same-tick duplicate writes.

## [0.4.1] - 2026-07-31

### Added

- Request Logs now provide a request-density histogram backed by `GET /admin/v1/events/histogram`. It replaces the fixed relative time presets with drag, resize, pan, zoom, absolute range controls, and row-centered shortcuts; shares the table's filters and error classification; and keeps live tailing only while the upper bound remains open.

## [0.4.0] - 2026-07-31

### Changed

- **BREAKING**: Removed the `event_bus.transport` config key. The event fanout now follows the storage backend: postgres always runs pg_notify fanout, sqlite always uses the in-memory bus. A config that still sets `event_bus.transport` fails to load. The postgres backend now requires `cluster.instance_url` and a cluster token; the Helm chart injects both and requires `secrets.clusterToken` unless `secrets.existingSecret` supplies the token.
- New principals receive the built-in `subscription-preference` Router chain entry at order `0`, and its presence is the enabled state. Existing principals and operator-configured chains remain unchanged on upgrade. Use the plugin-chain endpoints to insert, remove, or reorder the entry.
- Upstream warm-up is now opt-out for `anthropic_oauth`: `warmup_enabled` defaults to `true` for OAuth upstreams created through the admin API and through the OAuth flow. Other upstream kinds remain disabled by default. Creating an `anthropic_oauth` upstream no longer fails when warm-up is on without credentials; the warm-up scheduler already skips credential-less upstreams.
- Request logs show the request kind in its own `Request kind` column instead of a badge crowded into the session cell, and the request drawer lists it as a separate `Request kind` row with the full kind (`subagent`) rather than the table's abbreviation.

### Fixed

- Request-log model filtering now matches a case-insensitive prefix instead of the full model ID, so `claude-sonnet` finds `claude-sonnet-4-5-20250929`. Historical SQL, the live SSE tail and the dashboard row filter share the predicate, and the Model input commits once typing settles instead of re-querying and reconnecting the stream on every keystroke.
- Subscription routing and warmup now use only quota windows reported by each upstream, so accounts without a shared `7d` window continue through `5h` and model-scoped weekly quota while preserving shared-weekly precedence when both exist.
- The `db_unreachable_503` chaos test now calls the real `/admin/v1/principals/{id}/keys` route instead of a path that no longer exists, so it exercises its assertions again.

## [0.3.1] - 2026-07-28

### Changed

- Cache-affinity prefix hashing moves to schema 5 (`cc-lb-cache-v5:*` domain tags). Warm prompt-cache state recorded by earlier versions is dropped on hydration, so the first deploy after upgrading starts cold and re-warms over subsequent requests. No data migration is required.

### Fixed

- API key rolling limits now persist in durable usage buckets shared by every replica, so a restart or a second instance no longer resets or double-counts a key's rolling window.
- Prompt-cache simulation now models top-level `cache_control` (automatic caching). Such requests previously produced no breakpoints, leaving cache-affinity routing blind and recording no warm entry.
- Prompt-cache simulation now keeps `thinking`, `redacted_thinking` and `tool_reference` blocks in the prefix chain, and salts the documented request-level invalidators (`thinking`, `output_config.effort`, `speed`, `tool_choice`) at the tier each one affects. Requests differing only by replayed reasoning or by a changed invalidator no longer predict a hit the provider misses, and `tools`-tier prefixes stay byte-stable across those changes.
- Minimum cacheable prefix lengths now follow the provider table. Opus 4.7 is 2048 rather than 4096, which was discarding cacheable prefixes in that range; Opus 5 and Mythos 5 are 512, and Mythos Preview and Haiku 3.5 are 2048.
- Model pricing fallbacks no longer overcharge current Opus models by 3x. The cold-start catalog priced Opus 4.5 at $15/$75 instead of $5/$25, and the routing family fallback applied a blanket `claude-opus` rate correct only for Opus 4 and 4.1; both are now version-aware. Mythos, which previously returned unknown pricing and disabled cache scoring for that family, is now covered.
- One-hour cache writes now derive as 2.0x base input through a single shared helper, so the billing and routing paths no longer disagree whenever a catalog 5m rate is not exactly 1.25x base.
- Admin and dashboard token totals now include cache creation and cache read tokens. They previously summed only input plus output, understating a cached workload by an order of magnitude.

## [0.3.0] - 2026-07-25

### Added

- Admin request logs now preserve the observed OMP session ID and classify request kind, including main, advisor, subagent, recap, compaction, session-title, auto-thinking, and side requests.
- Admin request logs now preserve Claude Code agent lineage, parent-session context, client app mode, and session-ID source across SQLite/PostgreSQL and display them in request detail drawers.

### Changed

- Prompt-cache routing is now always enabled; prompt-cache observations are persisted and lifecycle drift observation starts with the server. Legacy prompt-cache disable switches are ignored with warnings.

### Fixed

- Subscription routing now fails locally when every OAuth upstream has exhausted a required quota window and no API-key fallback exists, instead of dispatching to a known-exhausted upstream.
- Subscription routing now activates Sonnet and Opus scoped weekly limits only when observed, preserves stale hard-negative base quota signals until reset, and requires fresh positive evidence before using overage quota.
- Claude Code main, compaction, side-question, notification, and header-less subagent requests now receive accurate request-kind labels in admin logs.

## [0.2.1] - 2026-07-24

### Fixed

- PostgreSQL-backed request history now reads from indexed materialized list columns, avoiding multi-second log-page loads caused by per-row payload decoding and sorting.

## [0.2.0] - 2026-07-23

### Changed

- **BREAKING**: Removed the `redb` storage backend entirely. SQLite is now the default storage backend for local development and CI. The `StorageConfig::Redb` and `BackendKind::Redb` configuration options are no longer supported.
- **BREAKING**: Upstreams, principals, and plugin chains are now managed via the database instead of TOML configuration sections. All administrative operations go through the dashboard or the `/admin/v1/*` REST API. See [docs/runtime-management.md](docs/runtime-management.md) for migration and API reference.

### Added

- **SQLite storage backend**: Added a new SQLite storage backend as the default database for local development and CI.
- **Per-principal plugin overrides**: Config authors can now configure custom plugins for individual principals.
- Validation, preflight checks, and hot-reload behavior now operate on an all-or-nothing basis.
- Admin `/status` JSON response now includes a `principals` map showing active overrides with redacted configuration hashes.
- Backward compatibility is fully preserved: zero-principal-plugin configurations remain unchanged, producing a byte-identical observe stream.

[Unreleased]: https://github.com/isac322/cc-lb/compare/cc-lb-v1.0.1...HEAD
[1.0.1]: https://github.com/isac322/cc-lb/compare/cc-lb-v1.0.0...cc-lb-v1.0.1
[1.0.0]: https://github.com/isac322/cc-lb/compare/cc-lb-v0.8.1...cc-lb-v1.0.0
[0.8.1]: https://github.com/isac322/cc-lb/compare/cc-lb-v0.8.0...cc-lb-v0.8.1
[0.8.0]: https://github.com/isac322/cc-lb/compare/cc-lb-v0.7.0...cc-lb-v0.8.0
[0.7.0]: https://github.com/isac322/cc-lb/compare/cc-lb-v0.6.0...cc-lb-v0.7.0
