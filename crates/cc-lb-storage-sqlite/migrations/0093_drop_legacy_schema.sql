-- Drop schema that no runtime reader or writer uses anymore.
--
-- Every statement below removes a table, column, index, or meta row whose
-- only remaining references were INSERT bindings, write-only bookkeeping, or
-- legacy fallbacks that the application no longer carries. Rebuilt tables
-- keep every row; the guards at the end abort the migration if a rebuild
-- lost data.

-- Legacy plugin marker and killswitch tables.
DROP TABLE IF EXISTS plugin_registry_marker_v1;
DROP TABLE IF EXISTS killswitch_v1;
DELETE FROM meta_v1 WHERE key IN ('killswitch_enabled', 'contract_version');

-- Plugin registry: schema_hash and parse_validated_at were only written.
ALTER TABLE wasm_registry_v2 DROP COLUMN schema_hash;
ALTER TABLE wasm_blobs_v2 DROP COLUMN parse_validated_at;

-- Hook metadata now always carries an explicit mode; stamp the default on
-- entries uploaded before the field existed.
UPDATE wasm_registry_v2
SET hook_metadata = (
    SELECT json_group_object(
        hook.key,
        CASE
            WHEN json_type(hook.value, '$.mode') IS NULL
                THEN json_set(hook.value, '$.mode', 'active')
            ELSE json(hook.value)
        END
    )
    FROM json_each(wasm_registry_v2.hook_metadata) AS hook
)
WHERE EXISTS (
    SELECT 1
    FROM json_each(wasm_registry_v2.hook_metadata) AS hook
    WHERE json_type(hook.value, '$.mode') IS NULL
);

-- Principals never recorded an apply status.
ALTER TABLE principals_v1 DROP COLUMN last_apply_error;
ALTER TABLE principals_v1 DROP COLUMN last_apply_at;

-- Upstream revision bookkeeping that nothing ever read.
ALTER TABLE upstream_status_v1 DROP COLUMN observed_spec_revision;
ALTER TABLE upstream_status_v1 DROP COLUMN observed_api_key_secret_revision;
ALTER TABLE upstream_status_v1 DROP COLUMN observed_oauth_token_revision;
ALTER TABLE upstream_api_key_secret_v1 DROP COLUMN secret_revision;
ALTER TABLE upstream_oauth_token_v1 DROP COLUMN token_revision;
ALTER TABLE upstream_oauth_token_v1 DROP COLUMN refreshed_at;

-- request_events_v1: indexes no query uses, then the insert-only columns.
-- The payload JSON remains the source for every runtime read.
DROP INDEX IF EXISTS request_events_v1_cache_state_ts;
DROP INDEX IF EXISTS request_events_v1_v3_cache_key_ts;
DROP INDEX IF EXISTS request_events_v1_upstream_id_idx;

ALTER TABLE request_events_v1 DROP COLUMN event_type;
ALTER TABLE request_events_v1 DROP COLUMN cache_state;
ALTER TABLE request_events_v1 DROP COLUMN message_id;
ALTER TABLE request_events_v1 DROP COLUMN message_index;
ALTER TABLE request_events_v1 DROP COLUMN message_count;
ALTER TABLE request_events_v1 DROP COLUMN cache_control_block_count;
ALTER TABLE request_events_v1 DROP COLUMN cache_breakpoints;
ALTER TABLE request_events_v1 DROP COLUMN cache_prefix_hash;
ALTER TABLE request_events_v1 DROP COLUMN web_search_requests;
ALTER TABLE request_events_v1 DROP COLUMN web_fetch_requests;
ALTER TABLE request_events_v1 DROP COLUMN inference_geo;
ALTER TABLE request_events_v1 DROP COLUMN matched_v3_cache_key;
ALTER TABLE request_events_v1 DROP COLUMN breakpoint_content_block_index;
ALTER TABLE request_events_v1 DROP COLUMN matched_content_block_index;
ALTER TABLE request_events_v1 DROP COLUMN lookback_distance;
ALTER TABLE request_events_v1 DROP COLUMN predicted_cache_read_tokens;
ALTER TABLE request_events_v1 DROP COLUMN predicted_cache_creation_tokens_5m;
ALTER TABLE request_events_v1 DROP COLUMN predicted_cache_creation_tokens_1h;
ALTER TABLE request_events_v1 DROP COLUMN cache_value_micros;
ALTER TABLE request_events_v1 DROP COLUMN formula_winner_upstream_id;
ALTER TABLE request_events_v1 DROP COLUMN kept_upstream_id;
ALTER TABLE request_events_v1 DROP COLUMN lineage_would_have_predicted_read_tokens;
ALTER TABLE request_events_v1 DROP COLUMN lineage_would_have_picked_upstream_id;
ALTER TABLE request_events_v1 DROP COLUMN quota_urgency_5h;
ALTER TABLE request_events_v1 DROP COLUMN quota_urgency_7d;
ALTER TABLE request_events_v1 DROP COLUMN quota_urgency_combined;
ALTER TABLE request_events_v1 DROP COLUMN quota_warning_multiplier;
ALTER TABLE request_events_v1 DROP COLUMN list_upstream;
ALTER TABLE request_events_v1 DROP COLUMN list_limit_reconcile_ms;

-- Unused secondary indexes.
DROP INDEX IF EXISTS prompt_cache_observations_upstream_model_expires_idx;
DROP INDEX IF EXISTS idx_warmup_attempts_v1_upstream_trigger_scheduled;
DROP INDEX IF EXISTS idx_warmup_attempts_v1_outcome_attempted;
DROP INDEX IF EXISTS idx_audit_log_principal_seq;
DROP INDEX IF EXISTS idx_cache_keepalive_sessions_principal_updated;
DROP INDEX IF EXISTS idx_cache_keepalive_sessions_principal_message;

-- audit_log_v1.kind duplicated admin_action. Fold it in, rebuild the admin
-- partial indexes on admin_action alone, then drop the column.
UPDATE audit_log_v1
SET admin_action = kind
WHERE admin_action IS NULL AND kind IS NOT NULL;

DROP INDEX IF EXISTS idx_audit_log_admin_ts_id;
DROP INDEX IF EXISTS idx_audit_log_admin_principal_ts_id;
DROP INDEX IF EXISTS idx_audit_log_admin_actor_ts_id;

ALTER TABLE audit_log_v1 DROP COLUMN kind;

CREATE INDEX idx_audit_log_admin_ts_id
    ON audit_log_v1 (ts DESC, id DESC)
    WHERE admin_action IS NOT NULL;

CREATE INDEX idx_audit_log_admin_principal_ts_id
    ON audit_log_v1 (principal_id, ts DESC, id DESC)
    WHERE admin_action IS NOT NULL;

CREATE INDEX idx_audit_log_admin_actor_ts_id
    ON audit_log_v1 (actor_authority, actor_subject, ts DESC, id DESC)
    WHERE admin_action IS NOT NULL;

-- Row counts captured before the table rebuilds below.
CREATE TEMP TABLE legacy_schema_rebuild_before AS
SELECT
    (SELECT COUNT(*) FROM managed_keys_v1) AS managed_keys,
    (SELECT COUNT(*) FROM cache_keepalive_decisions) AS keepalive_decisions,
    (SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1) AS quota_checkpoints,
    (SELECT COUNT(*) FROM upstream_plan_tier_history_v1) AS plan_tier_history,
    (SELECT COUNT(*) FROM upstream_subscription_quota_latest_v1) AS quota_latest;

-- managed_keys_v1: the synthetic id and name columns duplicated
-- (principal_id, key_id), which becomes the primary key.
DROP TABLE IF EXISTS managed_keys_v1_new;

CREATE TABLE managed_keys_v1_new (
    principal_id TEXT NOT NULL,
    key_id TEXT NOT NULL,
    secret_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    status TEXT NOT NULL,
    label TEXT NOT NULL,
    revoked_at INTEGER,
    verify_hash BLOB NOT NULL,
    secret_salt BLOB NOT NULL,
    limit_overrides TEXT NOT NULL,
    last_4 TEXT NOT NULL,
    description TEXT,
    index_hash BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (principal_id, key_id)
);

INSERT INTO managed_keys_v1_new (
    principal_id,
    key_id,
    secret_hash,
    created_at,
    expires_at,
    status,
    label,
    revoked_at,
    verify_hash,
    secret_salt,
    limit_overrides,
    last_4,
    description,
    index_hash,
    updated_at
)
SELECT
    principal_id,
    key_id,
    secret_hash,
    created_at,
    expires_at,
    status,
    label,
    revoked_at,
    verify_hash,
    secret_salt,
    limit_overrides,
    last_4,
    description,
    index_hash,
    updated_at
FROM managed_keys_v1;

DROP TABLE managed_keys_v1;
ALTER TABLE managed_keys_v1_new RENAME TO managed_keys_v1;

CREATE UNIQUE INDEX managed_keys_v1_active_index_hash
    ON managed_keys_v1 (index_hash)
    WHERE status != 'revoked';

-- cache_keepalive_decisions: every writer records last_message_at_ms. Backfill
-- the rows written before the column existed and make it required so reads
-- no longer need the ts * 1000 fallback.
DROP TABLE IF EXISTS cache_keepalive_decisions_new;

CREATE TABLE cache_keepalive_decisions_new (
    source_ref_id TEXT PRIMARY KEY,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    generation INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    principal_id TEXT,
    session_key_hash TEXT,
    upstream_id TEXT,
    error TEXT,
    ttl TEXT,
    config_snapshot TEXT,
    last_message_at_ms INTEGER NOT NULL
);

INSERT INTO cache_keepalive_decisions_new (
    source_ref_id,
    decision,
    reason,
    generation,
    ts,
    principal_id,
    session_key_hash,
    upstream_id,
    error,
    ttl,
    config_snapshot,
    last_message_at_ms
)
SELECT
    source_ref_id,
    decision,
    reason,
    generation,
    ts,
    principal_id,
    session_key_hash,
    upstream_id,
    error,
    ttl,
    config_snapshot,
    COALESCE(last_message_at_ms, ts * 1000)
FROM cache_keepalive_decisions;

DROP TABLE cache_keepalive_decisions;
ALTER TABLE cache_keepalive_decisions_new RENAME TO cache_keepalive_decisions;

CREATE INDEX idx_cache_keepalive_decisions_principal_message_entry
    ON cache_keepalive_decisions (
        principal_id,
        last_message_at_ms DESC,
        ('decision:' || source_ref_id) ASC
    );

-- Subscription quota sample kinds: nothing ever wrote 'process_start'.
-- Checkpoints only hold real samples; the latest sidecar also holds 'absent'.
DROP TABLE IF EXISTS upstream_subscription_quota_checkpoints_v1_new;

CREATE TABLE upstream_subscription_quota_checkpoints_v1_new (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    changed_at_unix_millis INTEGER NOT NULL CHECK (changed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    semantic_fingerprint BLOB NOT NULL CHECK (length(semantic_fingerprint) = 32),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample')),
    representative_claim TEXT,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold REAL CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0)),
    fallback_percentage REAL CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0)),
    fallback_available INTEGER CHECK (fallback_available IS NULL OR fallback_available IN (0, 1)),
    overage_in_use INTEGER CHECK (overage_in_use IS NULL OR overage_in_use IN (0, 1)),
    overage_period_monthly_utilization REAL CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    upgrade_paths TEXT,
    disabled_reason TEXT,
    extra_usage_enabled INTEGER CHECK (extra_usage_enabled IS NULL OR extra_usage_enabled IN (0, 1)),
    extra_usage_monthly_limit REAL,
    extra_usage_used_credits REAL,
    ingested_at_unix_millis INTEGER NOT NULL CHECK (ingested_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, window, source, changed_at_unix_millis, sample_id)
);

INSERT INTO upstream_subscription_quota_checkpoints_v1_new (
    upstream_id,
    window,
    source,
    changed_at_unix_millis,
    sample_id,
    semantic_fingerprint,
    sample_kind,
    representative_claim,
    utilization,
    status,
    resets_at_unix_secs,
    surpassed_threshold,
    fallback_percentage,
    fallback_available,
    overage_in_use,
    overage_period_monthly_utilization,
    upgrade_paths,
    disabled_reason,
    extra_usage_enabled,
    extra_usage_monthly_limit,
    extra_usage_used_credits,
    ingested_at_unix_millis
)
SELECT
    upstream_id,
    window,
    source,
    changed_at_unix_millis,
    sample_id,
    semantic_fingerprint,
    sample_kind,
    representative_claim,
    utilization,
    status,
    resets_at_unix_secs,
    surpassed_threshold,
    fallback_percentage,
    fallback_available,
    overage_in_use,
    overage_period_monthly_utilization,
    upgrade_paths,
    disabled_reason,
    extra_usage_enabled,
    extra_usage_monthly_limit,
    extra_usage_used_credits,
    ingested_at_unix_millis
FROM upstream_subscription_quota_checkpoints_v1;

DROP TABLE upstream_subscription_quota_checkpoints_v1;
ALTER TABLE upstream_subscription_quota_checkpoints_v1_new
    RENAME TO upstream_subscription_quota_checkpoints_v1;

CREATE INDEX upstream_subscription_quota_checkpoints_range_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, window, source, changed_at_unix_millis ASC, sample_id ASC);

CREATE INDEX upstream_subscription_quota_checkpoints_anchor_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, window, source, changed_at_unix_millis DESC, sample_id DESC);

CREATE INDEX upstream_subscription_quota_checkpoints_slim_idx
    ON upstream_subscription_quota_checkpoints_v1
    (upstream_id, window, source, changed_at_unix_millis ASC, sample_id ASC,
     utilization, status, resets_at_unix_secs);

DROP TABLE IF EXISTS upstream_subscription_quota_latest_v1_new;

CREATE TABLE upstream_subscription_quota_latest_v1_new (
    upstream_id TEXT NOT NULL,
    window TEXT NOT NULL CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified')),
    source TEXT NOT NULL CHECK (source IN ('header','api')),
    sample_kind TEXT NOT NULL DEFAULT 'sample' CHECK (sample_kind IN ('sample','absent')),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    sample_id TEXT NOT NULL,
    utilization REAL CHECK (utilization IS NULL OR (utilization >= 0.0 AND utilization <= 1.0)),
    status TEXT CHECK (status IS NULL OR status IN ('allowed','allowed_warning','rejected')),
    resets_at_unix_secs INTEGER CHECK (resets_at_unix_secs IS NULL OR resets_at_unix_secs >= 0),
    surpassed_threshold REAL CHECK (surpassed_threshold IS NULL OR (surpassed_threshold >= 0.0 AND surpassed_threshold <= 1.0)),
    representative_claim TEXT,
    fallback_percentage REAL CHECK (fallback_percentage IS NULL OR (fallback_percentage >= 0.0 AND fallback_percentage <= 1.0)),
    fallback_available INTEGER CHECK (fallback_available IS NULL OR fallback_available IN (0, 1)),
    overage_in_use INTEGER CHECK (overage_in_use IS NULL OR overage_in_use IN (0, 1)),
    overage_period_monthly_utilization REAL CHECK (overage_period_monthly_utilization IS NULL OR (overage_period_monthly_utilization >= 0.0 AND overage_period_monthly_utilization <= 1.0)),
    upgrade_paths TEXT,
    disabled_reason TEXT,
    extra_usage_enabled INTEGER CHECK (extra_usage_enabled IS NULL OR extra_usage_enabled IN (0, 1)),
    extra_usage_monthly_limit REAL,
    extra_usage_used_credits REAL,
    ingested_at_unix_millis INTEGER NOT NULL CHECK (ingested_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, window, source)
);

INSERT INTO upstream_subscription_quota_latest_v1_new (
    upstream_id,
    window,
    source,
    sample_kind,
    observed_at_unix_millis,
    sample_id,
    utilization,
    status,
    resets_at_unix_secs,
    surpassed_threshold,
    representative_claim,
    fallback_percentage,
    fallback_available,
    overage_in_use,
    overage_period_monthly_utilization,
    upgrade_paths,
    disabled_reason,
    extra_usage_enabled,
    extra_usage_monthly_limit,
    extra_usage_used_credits,
    ingested_at_unix_millis
)
SELECT
    upstream_id,
    window,
    source,
    sample_kind,
    observed_at_unix_millis,
    sample_id,
    utilization,
    status,
    resets_at_unix_secs,
    surpassed_threshold,
    representative_claim,
    fallback_percentage,
    fallback_available,
    overage_in_use,
    overage_period_monthly_utilization,
    upgrade_paths,
    disabled_reason,
    extra_usage_enabled,
    extra_usage_monthly_limit,
    extra_usage_used_credits,
    ingested_at_unix_millis
FROM upstream_subscription_quota_latest_v1;

DROP TABLE upstream_subscription_quota_latest_v1;
ALTER TABLE upstream_subscription_quota_latest_v1_new
    RENAME TO upstream_subscription_quota_latest_v1;

CREATE INDEX upstream_subscription_quota_latest_lookup_idx
    ON upstream_subscription_quota_latest_v1
    (upstream_id, window, observed_at_unix_millis DESC);

-- Plan-tier history: the one-shot backfill source is gone. Rewrite its rows
-- to 'builtin' and rebuild the table with the tightened resolution_source
-- CHECK.
DROP TABLE IF EXISTS upstream_plan_tier_history_v1_new;

CREATE TABLE upstream_plan_tier_history_v1_new (
    upstream_id TEXT NOT NULL,
    organization_uuid TEXT,
    organization_type TEXT,
    rate_limit_tier TEXT,
    seat_tier TEXT,
    tier_key TEXT CHECK (tier_key IS NULL OR tier_key IN ('pro','team_standard','max_5x','team_premium','max_20x')),
    resolution_source TEXT NOT NULL CHECK (resolution_source IN ('override','builtin','unknown')),
    resolved_ratio_snapshot REAL CHECK (resolved_ratio_snapshot IS NULL OR (resolved_ratio_snapshot > 0.0 AND resolved_ratio_snapshot < 1.0e308)),
    observed_at_unix_millis INTEGER NOT NULL CHECK (observed_at_unix_millis >= 0),
    effective_from_unix_millis INTEGER NOT NULL CHECK (effective_from_unix_millis >= 0),
    effective_to_unix_millis INTEGER CHECK (effective_to_unix_millis IS NULL OR effective_to_unix_millis > effective_from_unix_millis),
    provenance TEXT NOT NULL CHECK (length(trim(provenance)) > 0),
    created_at_unix_millis INTEGER NOT NULL CHECK (created_at_unix_millis >= 0),
    PRIMARY KEY (upstream_id, effective_from_unix_millis),
    CHECK ((tier_key IS NULL) = (resolution_source = 'unknown')),
    CHECK (resolution_source <> 'unknown' OR resolved_ratio_snapshot IS NULL)
);

INSERT INTO upstream_plan_tier_history_v1_new (
    upstream_id,
    organization_uuid,
    organization_type,
    rate_limit_tier,
    seat_tier,
    tier_key,
    resolution_source,
    resolved_ratio_snapshot,
    observed_at_unix_millis,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
)
SELECT
    upstream_id,
    organization_uuid,
    organization_type,
    rate_limit_tier,
    seat_tier,
    tier_key,
    CASE WHEN resolution_source = 'backfill' THEN 'builtin' ELSE resolution_source END,
    resolved_ratio_snapshot,
    observed_at_unix_millis,
    effective_from_unix_millis,
    effective_to_unix_millis,
    provenance,
    created_at_unix_millis
FROM upstream_plan_tier_history_v1;

DROP TABLE upstream_plan_tier_history_v1;
ALTER TABLE upstream_plan_tier_history_v1_new RENAME TO upstream_plan_tier_history_v1;

CREATE UNIQUE INDEX upstream_plan_tier_history_v1_one_open_idx
    ON upstream_plan_tier_history_v1 (upstream_id)
    WHERE effective_to_unix_millis IS NULL;

CREATE INDEX upstream_plan_tier_history_v1_asof_idx
    ON upstream_plan_tier_history_v1 (upstream_id, effective_from_unix_millis DESC, effective_to_unix_millis);

CREATE INDEX upstream_plan_tier_history_v1_tier_time_idx
    ON upstream_plan_tier_history_v1 (tier_key, effective_from_unix_millis DESC);

CREATE TEMP TABLE legacy_schema_rebuild_guard (
    ok INTEGER NOT NULL CHECK (ok = 1)
);

INSERT INTO legacy_schema_rebuild_guard (ok)
SELECT CASE
    WHEN (SELECT COUNT(*) FROM managed_keys_v1) = b.managed_keys
     AND (SELECT COUNT(*) FROM cache_keepalive_decisions) = b.keepalive_decisions
     AND (SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1) = b.quota_checkpoints
     AND (SELECT COUNT(*) FROM upstream_plan_tier_history_v1) = b.plan_tier_history
     AND (SELECT COUNT(*) FROM upstream_subscription_quota_latest_v1) = b.quota_latest
    THEN 1
    ELSE 0
END
FROM legacy_schema_rebuild_before b;

DROP TABLE temp.legacy_schema_rebuild_guard;
DROP TABLE temp.legacy_schema_rebuild_before;
