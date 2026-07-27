# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- **BREAKING**: Removed the `event_bus.transport` config key. The event fanout now follows the storage backend: postgres always runs pg_notify fanout, sqlite always uses the in-memory bus. A config that still sets `event_bus.transport` fails to load. The postgres backend now requires `cluster.instance_url` and a cluster token; the Helm chart injects both automatically and fails to render when `secrets.clusterToken` is unset.
- **BREAKING**: The `smart_routing_enabled` principal field is removed. The built-in `subscription-preference` filter is the Router chain entry: every new principal receives it at order `0`, and its presence is the enabled state. On upgrade, active principals with an empty Router chain receive that entry; configured chains remain untouched. Use the plugin-chain endpoints to insert, remove, or reorder the entry.
- Upstream warm-up is now opt-out: `warmup_enabled` defaults to `true` for upstreams created through the admin API and through the OAuth flow. Creating an `anthropic_oauth` upstream no longer fails when warm-up is on without credentials; the warm-up scheduler already skips credential-less upstreams.

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
