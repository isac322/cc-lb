# ADR 0005 — Pool subscription-quota: DB-managed historized plan-tier ratios, tier history, and unbounded observations

- Status: Proposed
- Date: 2026-07-07
- Scope: storage/schema + runtime design decision; not yet implemented.
- Note: this revises an earlier draft of ADR 0005 (which proposed "extend retention + store tier not ratio + keep the hardcoded multipliers"). The decision below supersedes that draft after a five-agent, three-round design debate reached consensus (Oracle-adjudicated).

## Context

`pool_subscription_quota_history_v1` had grown to 256 MB — 72% of a 357 MB SQLite database — because its write-only `contributors_json TEXT` column (246 MB, 69% of the whole DB) re-serialized a per-contributor JSON array every 60 s. No product code reads it; it was retained only to keep historical pool utilization **recomputable if the formula changes**.

The pool formula is `pool_util(window) = Σ(utilizationᵢ × ratioᵢ) / Σ(ratioᵢ)` over enabled AnthropicOauth upstreams whose latest observation is non-stale, where:

- `ratioᵢ = plan_capacity_ratio(organization_type, rate_limit_tier, seat_tier)` — a pure function of plan tier, **hardcoded** in `crates/cc-lb-engine/src/plan_capacity.rs:21` (literal multipliers `1.0 / 1.25 / 5.0 / 6.25 / 20.0`). The module comment states these are values *inferred* from plan-tier names, i.e. **expected to change over time**. `ratio → tier` is non-invertible (`6.25` maps to several tiers).
- `utilizationᵢ` comes from `upstream_subscription_quota_observations_v1` (per `upstream_id / window / source`, `observed_at_unix_millis`). Its 30 s writer dedup (`subscription_quota_events.rs`) drops only byte-identical repeats, so the stream faithfully preserves the utilization step-function.

The ratio is computed at **dynamic-view build time** (not per request) in `load_plan_info_by_upstream` (`crates/cc-lb-server/src/dynamic_view_builder.rs:351`), which joins `upstream_subscription_metadata` to `organization_metadata` and stores `PlanInfo.capacity_ratio` in the in-memory view for the subscription-preference router filter.

Two facts drive this decision:

1. **The multipliers are hardcoded guesses that change over time, and the stakeholder wants them DB-managed and historized** — so a multiplier can be corrected without a code deploy, and so historical pool numbers can be recomputed either "as they were" or "under the new multiplier".
2. **Plan tier exists nowhere as history.** `upstream_subscription_metadata_v1` is current-only and overwritten; because `ratio → tier` is non-invertible, nothing durably records which tier an upstream had at time `T`.

Out of scope (stakeholder-confirmed): penalizing enabled-but-silent upstreams, and bit-exact live-cache staleness replay.

## Decision

### 1. Three new SCD Type 2 (effective-dated) tables

- **`plan_tier_ratio_history_v1`** — the DB-managed, historized multiplier catalog: `(tier_key, pro_relative_ratio, effective_from_unix_millis, effective_to_unix_millis, provenance)`, one open row per `tier_key`. Seeded from the five current hardcoded ratios. Editing a multiplier **closes the open row and inserts a new one** (never in-place), so the ratio-vs-time history is preserved.
- **`metadata_tier_mapping_override_v1`** — an **exact-match** alias table `(organization_type, rate_limit_tier, seat_tier) → tier_key`, SCD2. No substring, no priority, no JSON predicates. Lets a brand-new Anthropic *alias* string for an existing tier be mapped by inserting a row, without a code deploy. A `NULL` field means "field absent" and participates in matching via a `COALESCE(..., sentinel)` normalized key (see Consequences → uniqueness trap).
- **`upstream_plan_tier_history_v1`** — per-upstream resolved-tier history: `(upstream_id, organization_uuid, raw organization_type/rate_limit_tier/seat_tier, resolved tier_key (nullable / "unknown"), resolution_source ∈ {override, builtin, unknown}, resolved_ratio_snapshot (audit only), observed_at_unix_millis, effective_from/to)`, one open row per upstream. A new interval is written only when the resolved tier / source / raw triple changes.

Migrations: SQLite `0038`, Postgres `0069` (verified current max: SQLite `0037`, Postgres `0068`). Precedent: the historized `config_history_v1` (SQLite `0005_price_config.sql`, Postgres `0012_config_history.sql`).

### 2. Classification becomes explicit, never silently "Pro"

Replace `plan_capacity_ratio(...) -> f64` with `classify_plan_tier(...) -> PlanTierClassification`, either `Known(TierKey)` (enum: `pro, team_standard, max_5x, team_premium, max_20x`) or `Unknown { organization_type, rate_limit_tier, seat_tier }`. The branchy matching stays in Rust (type-safe, tested). Resolution order at classification time: **exact DB override → Rust `classify_plan_tier` → `Unknown`**. `Unknown` is never silently treated as Pro; it is surfaced via log / metric / admin and persisted with its raw triple, so a human can add the enum case + seed the ratio deliberately. Operationally an `Unknown` upstream may use ratio `1.0` for routing (identical to today's Pro fallback — no routing regression), but it stays visible.

### 3. Dynamic-view build reads the catalog

`load_plan_info_by_upstream` stops computing ratios from raw metadata. It loads the current ratio catalog into `HashMap<TierKey, f64>`, resolves each upstream's tier (override → classifier → Unknown), and fills `PlanInfo.capacity_ratio`. A **known** tier missing a current ratio row **fails the rebind** (no hardcoded fallback after migration); an `Unknown` tier falls back to `1.0` but stays visible. Admin catalog/override edits reuse the existing `apply_dynamic_view_after_mutation` / `add_dynamic_rebind_headers` rebind seam (`crates/cc-lb-admin/src/v1/mod.rs:41,61`); the metadata-refresh path (`run_metadata_refresh`) must also trigger a rebind after it changes tier history, or the router keeps stale in-memory ratios.

### 4. Remove observation retention entirely

Subscription-quota observations are kept forever. Remove the `CronJob::QuotaGc` variant, its `quota_gc` singleton producer, the dispatch arm, the `SubscriptionQuotaGcStore` bridge, `delete_subscription_quota_before` from the trait/adapters, and the GC-only observation-time indexes. Do not delete the observation tables. Purge/no-op any queued serialized `quota_gc` jobs so the scheduler does not fail to deserialize an unknown job variant.

### 5. Recompute semantics

- **Faithful replay at `T`**: `upstream_plan_tier_history_v1` row covering `T` × `plan_tier_ratio_history_v1` row for that `tier_key` at `T`. Historical raw metadata is not reinterpreted through current overrides; `resolved_ratio_snapshot` is audit/debug only.
- **What-if under current ratios**: historical `tier@T` × the current open ratio row.

## Consequences

### Positive

- Multipliers are editable without a deploy and their change-over-time is tracked; new Anthropic alias strings for an existing tier are handled by an override row, not a deploy.
- Historical pool utilization is recomputable both "as it was" and "under new ratios". Plan-tier history (previously nonexistent) becomes durable.
- Type-safe core (`TierKey` enum, exhaustive match) — no priority-ordered rule engine to misconfigure. Unknown plan strings become loud instead of silently mis-weighted.
- Enables dropping the write-only `contributors_json` blob (~246 MB) once this durable state exists.

### Negative / risks to get right

- **Nullable exact-match uniqueness trap**: SQL `NULL != NULL`, so a plain partial-unique index over the nullable `(organization_type, rate_limit_tier, seat_tier)` triple will *not* prevent duplicate open overrides. Use a `COALESCE(field, sentinel)` generated/expression unique key plus a CHECK forbidding the sentinel as real input.
- **SCD2 writes must be transactional**: close the open row and insert the replacement in one transaction; idempotent no-op when the open row already matches; partial unique index guarantees one open row per logical key; treat a concurrent close/open as a conflict, not a silent duplicate.
- **Ratio validation**: reject non-finite / zero / negative ratios in Rust *and* via SQL CHECK (`isfinite(...)` on Postgres).
- **Rebind coverage**: the refresh path — not only the admin path — must rebind after tier-history changes.
- **Retention removal**: observations grow unbounded (accepted; lean rows). Queued `quota_gc` jobs are purged by a scheduler migration.

### Neutral

- Dual-DB parity: SQLite `INTEGER/TEXT`, Postgres `BIGINT/UUID/DOUBLE PRECISION`; keep the storage API typed and adapter SQL thin. Conformance must cover seed parity, exact-override precedence, SCD2 close/open, idempotent upsert, as-of lookups, one-open-row conflict, and null-field override uniqueness.
- Effort is large (multi-day) across `cc-lb-engine`, `cc-lb-storage-api`, both adapters, `cc-lb-server` dynamic view, `cc-lb-control` refresh, `cc-lb-scheduler`, `cc-lb-config`, and conformance.

## Alternatives considered

- **Full rules-in-DB catalog** (priority-ordered `match_kind` + substring + JSON predicates). Rejected by consensus: turns routing weights into unguarded operational state with no compile-time safety, for a ~5-upstream reality where new *semantic* tiers are rare. The exact-match override table captures the realistic "alias without deploy" need at far lower risk.
- **Numbers-only catalog with the old silent Pro fallback.** Rejected: a new Anthropic tier string would silently mis-weight routing as Pro until a deploy. The `Known | Unknown` split with surfaced Unknown fixes this.
- **Store the derived `capacity_ratio` in history instead of the tier.** Rejected: non-invertible and freezes history at the write-time multipliers, defeating recompute under changed multipliers.
- **Extend (not remove) observation retention; keep the hardcoded multipliers.** Superseded by stakeholder direction: remove retention entirely, and make the multipliers DB-managed + historized.
