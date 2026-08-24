# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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
