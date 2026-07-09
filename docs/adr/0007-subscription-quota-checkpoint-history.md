# ADR 0007: Subscription-quota checkpoint history

- Status: Proposed
- Date: 2026-07-08
- Scope: storage/schema, subscription-quota writer, admin quota APIs, and admin quota UI.
- Supersedes: ADR 0005's acceptance that `upstream_subscription_quota_observations_v1` can grow unbounded as lean raw observations.

## Context

After ADR 0005 and PR #369 removed `pool_subscription_quota_history_v1.contributors_json`, the production SQLite database shrank from ~383 MiB to ~114 MiB. The largest remaining consumer became `upstream_subscription_quota_observations_v1`: table plus indexes were ~66.6 MiB, about 58% of the post-VACUUM database.

The current observation model is raw-sample history. `SubscriptionQuotaObservationRecord` stores `(upstream_id, window, source, sample_kind, observed_at_unix_millis, sample_id)` plus quota payload fields such as `utilization`, `status`, `resets_at_unix_secs`, fallback state, overage state, `upgrade_paths`, `disabled_reason`, and extra-usage fields (`crates/cc-lb-storage-api/src/upstream_subscription_quota.rs:213`). The storage contract appends observations and separately upserts the latest sidecar row per `(upstream_id, window, source)` (`crates/cc-lb-storage-api/src/upstream_subscription_quota.rs:279`).

The writer has a 30 s duplicate-suppression window, not true change-point storage. A matching fingerprint is dropped only while `elapsed < dedup_elapsed_override_secs`; after that, the same payload is persisted again (`crates/cc-lb-engine/src/subscription_quota_events.rs:192`). The default dedup interval is 30 s (`crates/cc-lb-engine/src/subscription_quota_events.rs:68`), while the OAuth usage poll is periodic. Stable quota values therefore keep producing heartbeat rows. The current fingerprint also includes noisy fields such as `representative_claim` (`crates/cc-lb-engine/src/subscription_quota_events.rs:261`).

The read path buckets raw rows. The SQLite adapter reads raw observations, buckets them by `observed_at_unix_millis / bucket_secs`, and computes `sample_count`, utilization min/avg/max/last, status last, reset last, and sources seen (`crates/cc-lb-storage-sqlite/src/adapter/upstream_subscription_quota.rs:335`). The admin `/series`, `/analysis`, and `/aggregate` endpoints all consume `list_subscription_quota_series` (`crates/cc-lb-admin/src/subscription_quotas.rs:707`, `crates/cc-lb-admin/src/subscription_quotas.rs:775`, `crates/cc-lb-admin/src/subscription_quotas.rs:894`). The admin UI renders `/series` as a quota history chart using `bucket_start_unix_secs` and `utilization_last`, and already carries reset markers into `ReferenceLine`s (`crates/cc-lb-admin/web/src/routes/upstreams.tsx:673`, `crates/cc-lb-admin/web/src/routes/upstreams.tsx:1298`).

Live measurements showed that exact duplicate primary keys were not the issue. The dominant redundancy was repeated equivalent quota state over time: recent 48 h data had ~71% consecutive unchanged payload rows, while making rows unique by minute would remove only ~6%. Retention was explicitly rejected. The objective is both to reclaim the existing raw-history footprint and to reduce future growth without deleting old quota history.

## Decision

### 1. Store quota history as per-source checkpoints, not raw heartbeat samples

Create a v2 subscription-quota checkpoint history that records a row when the quota state changes. It keeps exact observation timestamps; it does not round storage to minutes. Minute or coarser charting is a query/rendering concern.

The physical checkpoint streams remain separated by `source` (`header` and `api`). A canonical or merged view is derived at query time by adapting the existing read-time source-merge behavior to checkpoint inputs. We do not collapse header/API into one stored stream, because doing so would permanently discard provenance and bake a write-time conflict rule into storage.

Checkpoint history is not monotonic. Utilization decreases are valid and must be persisted when the provider resets a quota window, Anthropic changes the account state, a plan/capacity/entitlement changes, or any other quota-semantic payload field changes. Strict monotonic filtering is rejected.

### 2. Split history from freshness/liveness

The checkpoint history log answers “when did the quota state change?” The latest sidecar and in-memory cache answer “was this upstream observed recently?”

Every accepted observation from either source must continue to update `upstream_subscription_quota_latest_v1` and the runtime cache, even when it does not create a new checkpoint row. Fresh/stale routing and admin latest state must not depend on the sparse checkpoint history. This preserves current liveness semantics while removing heartbeat rows from durable history.

### 3. Define checkpoint identity by quota-semantic fields only

The change key is a curated quota-semantic fingerprint, not the full raw row.

Included in the change key:

- `utilization` exactly as represented by the parsed/stored quota value. No tolerance, smoothing, or monotonic conflict rule is approved by this ADR.
- `status`.
- `resets_at_unix_secs`.
- `surpassed_threshold`.
- fallback fields: `fallback_percentage`, `fallback_available`.
- overage fields: `overage_in_use`, `overage_period_monthly_utilization`.
- `upgrade_paths`.
- `disabled_reason`.
- extra-usage fields: `extra_usage_enabled`, `extra_usage_monthly_limit`, `extra_usage_used_credits`.

Excluded from the change key:

- `sample_id`.
- `observed_at_unix_millis` and `ingested_at_unix_millis`.
- `representative_claim`.
- `sample_kind` and any future non-quota-semantic evidence or runtime bookkeeping fields.

Excluded fields may still be stored on the checkpoint row as evidence from the observation that created the checkpoint. They must not cause otherwise-identical quota state to emit new checkpoint rows.

### 4. Query checkpoints as step-series with a left anchor

Checkpoint storage is sparse, so range queries must return enough data for correct step rendering. For any requested range, the series query must include:

- the last checkpoint before `since`, when one exists, as the left anchor;
- all checkpoints inside `[since, until]`;
- source provenance; and
- reset/start markers compatible with the existing admin UI marker model.

Consumers render or compute by carrying the latest known checkpoint forward until the next checkpoint. If multiple checkpoints occur inside one display minute, storage preserves all exact timestamps; a 1-minute display bucket may choose the last checkpoint as its visible value and expose change count or max value only when the API/UI explicitly needs that metadata.

The API layer owns consistent interpretation for `/series`, `/analysis`, and `/aggregate`; the frontend may fill visual gaps only from API-provided checkpoints and anchors. No layer may invent zero values or synthetic unobserved quota samples for any unknown interval.

### 5. Backfill and reclaim the existing raw-history footprint

Because the chosen success criterion is actual DB-size recovery, implementation must include a one-time replay/backfill from `upstream_subscription_quota_observations_v1` into the checkpoint history, using the same change-key semantics as live ingestion. After validation, the old raw observation table and now-unused indexes are dropped, and SQLite is VACUUMed offline to reclaim file size.

The backfill is deterministic: replay raw rows in observed-time order with stable tie-breaking, preserve each source separately, and never synthesize unobserved quota values. If validation fails, the migration must fail closed rather than leave a partially migrated live service.

## Consequences

### Positive

- Repeated heartbeat rows stop growing the durable history.
- Existing raw-history bloat is reclaimed rather than only slowing future growth.
- Header/API provenance remains available because source streams stay physically separate.
- Latest/freshness behavior remains compatible with routing and admin status because latest/cache continue updating on every observation.
- Exact checkpoint timestamps preserve sub-minute changes while allowing minute-level rendering.

### Negative / risks to get right

- The meaning of historical series changes from “raw samples bucketed by time” to “sparse quota-state checkpoints rendered as a step function.” `/series`, `/analysis`, `/aggregate`, and the admin UI must be updated together.
- Compression can be defeated if noisy fields accidentally enter the change key. `representative_claim`, timestamps, and `sample_id` must remain excluded from checkpoint identity.
- Freshness can regress if any consumer reads sparse history instead of latest/cache for liveness.
- Backfill plus destructive cleanup is operationally sensitive. It needs a backup, validation report, old-table drop, and offline VACUUM.
- Raw sample counts are no longer a primary historical fact after compaction. If APIs expose counts, they must be named and interpreted as checkpoint/change counts or derived display-bucket counts, not raw poll counts.

### Neutral

- The latest sidecar remains; it is not a storage-size problem and is the correct freshness surface.
- `source = header|api` remains a storage dimension. `source = merged` remains a query/view concept.
- Retention remains rejected. The system still keeps history indefinitely; it is just checkpoint-compressed.

## Alternatives considered

- **Keep raw observations and reintroduce retention.** Rejected by stakeholder direction: no retention.
- **Make rows unique by minute.** Rejected after measurement: API polling is already roughly minute-cadenced, so minute uniqueness removes little (~6% in recent 48 h) and does not attack repeated unchanged state across minutes.
- **Persist a `minutes_v2` table as the primary history.** Rejected as the main storage model: it is safer for old bucket semantics but still stores one row per minute forever and does not materially reduce unchanged minute-to-minute repetition.
- **Strict monotonic checkpointing.** Rejected: provider resets, plan changes, entitlement changes, and account-state changes can legitimately decrease utilization within what looks like the same broad time window.
- **Single write-time canonical stream for header and API.** Rejected by user decision: store header/API separately and merge at read time to preserve provenance and future re-merge options.
- **Full-payload fingerprint.** Rejected by user decision: noisy evidence fields can defeat compaction. The fingerprint is quota-semantic, not raw-row-equivalence.
- **Utilization-only fingerprint.** Rejected by user decision: status, reset, fallback, overage, disabled, upgrade-path, and extra-usage changes are quota-semantic even when utilization is unchanged.

## Implementation Details

The offline compaction and cleanup process uses a two-phase approach to ensure database safety and integrity.

### 1. Compaction and Backfill Phase

The backfill process reads all raw observations from `upstream_subscription_quota_observations_v1` ordered by `upstream_id`, `window`, `source`, `observed_at_unix_millis`, and `sample_id`. It filters out duplicate semantic states using the canonical fingerprint helper. The remaining change-only checkpoints are inserted into `upstream_subscription_quota_checkpoints_v1`.

To ensure idempotency and prevent duplicate runs, the process writes a completion marker to the `meta_v1` table with the key `subscription_quota_checkpoint_backfill_v1_complete`. This marker stores a JSON report containing row counts and validation outcomes.

### 2. Destructive Cleanup Phase

The cleanup process runs only after the backfill phase completes and passes validation. It performs the following steps:

- Validates that the checkpoint table row count matches the expected count derived from raw observations.
- Drops the raw observations table `upstream_subscription_quota_observations_v1` and its indexes.
- Runs `PRAGMA wal_checkpoint(TRUNCATE)` to truncate the write-ahead log.
- Runs `VACUUM` to reclaim disk space and shrink the database file.
- Runs `PRAGMA integrity_check` to verify database integrity.
- Writes a completion marker to the `meta_v1` table with the key `subscription_quota_checkpoint_cleanup_v1_complete`.

If any validation check fails, the process aborts immediately and rolls back the transaction, leaving the raw observations table intact.

## Rollout and QA requirements

- Add SQLite and Postgres migrations for checkpoint history and destructive raw-table cleanup after validation. Current latest migrations are SQLite `0041` and Postgres `0071`; the next migrations are expected to be SQLite `0042` and Postgres `0072`.
- Add storage conformance tests proving: unchanged payloads create one checkpoint but still update latest; semantic changes create checkpoints; header/API remain separate; merged queries carry forward correctly; range queries include a left anchor; backfill is deterministic.
- Add admin API tests proving `/series`, `/analysis`, and `/aggregate` interpret checkpoint history consistently.
- Add frontend QA proving the quota chart renders step-series gaps from API-provided anchors and does not invent zeroes.
- Production deploy must back up the SQLite database, stop the service for cutover/backfill/drop/VACUUM if required, verify schema and row-count invariants, verify `PRAGMA integrity_check`, restart, and confirm real traffic succeeds.

## Update (2026-07-09): raw observations removed

The migration to checkpoint-only history is complete and the legacy raw
`upstream_subscription_quota_observations_v1` table has been retired:

- Live write paths no longer append raw observations; they only upsert the
  latest sidecar and insert change-only checkpoints.
- The one-time backfill/cleanup tooling (the
  `compact-subscription-quota-history --drop-raw-observations` CLI and its
  SQLite backfill/cleanup adapters) has been deleted.
- Final forward-only drop migrations retire the table on any database that
  still has it: SQLite `0045_drop_subscription_quota_observations.sql` and
  Postgres `0075_drop_subscription_quota_observations.sql`. Historical
  migrations are left immutable, so a fresh database briefly creates the table
  and then drops it.
- The observation-flavored API was renamed to sample terminology
  (`SubscriptionQuotaObservationRecord` -> `SubscriptionQuotaSample`,
  `put_subscription_quota{,_batch}` -> `record_subscription_quota_sample{,s}`).

The two-phase compaction/cleanup process described above is retained for
historical context; that tooling no longer ships.
