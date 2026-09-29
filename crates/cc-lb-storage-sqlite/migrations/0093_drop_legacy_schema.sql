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

-- Row counts captured before the table rebuilds below. request_events_seq
-- keeps the AUTOINCREMENT high-water mark so the rebuilt table never reissues
-- an id that was handed out and later deleted.
CREATE TEMP TABLE legacy_schema_rebuild_before AS
SELECT
    (SELECT COUNT(*) FROM request_events_v1) AS request_events,
    (SELECT seq FROM sqlite_sequence WHERE name = 'request_events_v1') AS request_events_seq,
    (SELECT COUNT(*) FROM managed_keys_v1) AS managed_keys,
    (SELECT COUNT(*) FROM cache_keepalive_decisions) AS keepalive_decisions,
    (SELECT COUNT(*) FROM upstream_subscription_quota_checkpoints_v1) AS quota_checkpoints,
    (SELECT COUNT(*) FROM upstream_plan_tier_history_v1) AS plan_tier_history,
    (SELECT COUNT(*) FROM upstream_subscription_quota_latest_v1) AS quota_latest;

-- request_events_v1: drop the insert-only columns (the payload JSON remains
-- the source for every runtime read) and the indexes no query uses. One
-- rebuild copies the table once; a separate ALTER TABLE ... DROP COLUMN per
-- column would rewrite the whole table for each column. Not recreated:
-- request_events_v1_cache_state_ts and request_events_v1_v3_cache_key_ts
-- (their columns are gone), request_events_v1_upstream_id_idx (covered by
-- request_events_v1_upstream_list_order_idx), and
-- request_events_v1_non_uuid_principal_cost_idx (principal cost reads cover
-- only UUID and NULL principals, both served by
-- request_events_v1_principal_list_order_idx).
DROP TABLE IF EXISTS request_events_v1_new;

CREATE TABLE request_events_v1_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    ts INTEGER NOT NULL,
    upstream_id TEXT,
    payload TEXT NOT NULL,
    principal_id BLOB,
    created_at INTEGER,
    key_id TEXT,
    model TEXT,
    upstream_name TEXT,
    thread_id TEXT,
    input_tokens INTEGER CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens INTEGER CHECK (output_tokens IS NULL OR output_tokens >= 0),
    cache_creation_input_tokens INTEGER CHECK (cache_creation_input_tokens IS NULL OR cache_creation_input_tokens >= 0),
    cache_read_input_tokens INTEGER CHECK (cache_read_input_tokens IS NULL OR cache_read_input_tokens >= 0),
    event_id TEXT NOT NULL UNIQUE,
    error_code TEXT NULL,
    upstream_error_type TEXT NULL,
    upstream_error_message TEXT NULL,
    thinking_tokens INTEGER NULL,
    service_tier TEXT NULL,
    cache_creation_input_tokens_5m INTEGER NULL,
    cache_creation_input_tokens_1h INTEGER NULL,
    token_estimate_source TEXT NULL,
    thinking_budget_tokens INTEGER NULL,
    reasoning_effort TEXT NULL,
    list_ts_ms INTEGER NOT NULL DEFAULT 0,
    list_event_key TEXT NOT NULL DEFAULT '',
    list_status INTEGER NULL,
    list_duration_ms INTEGER NULL,
    list_auth_ms INTEGER NULL,
    list_route_ms INTEGER NULL,
    list_limit_reserve_ms INTEGER NULL,
    list_bulkhead_wait_ms INTEGER NULL,
    list_dns_ms INTEGER NULL,
    list_connect_ms INTEGER NULL,
    list_connection_reused INTEGER NULL,
    list_proxy_setup_ms INTEGER NULL,
    list_shape_ms INTEGER NULL,
    list_sign_ms INTEGER NULL,
    list_upstream_ttfb_ms INTEGER NULL,
    list_upstream_body_ms INTEGER NULL,
    list_stream_first_content_delta_ms INTEGER NULL,
    list_stream_last_content_delta_ms INTEGER NULL,
    list_inter_token_avg_ms INTEGER NULL,
    list_cost_usd_micros INTEGER NULL,
    list_cost_input_micros INTEGER NULL,
    list_cost_output_micros INTEGER NULL,
    list_cost_cache_creation_5m_micros INTEGER NULL,
    list_cost_cache_creation_1h_micros INTEGER NULL,
    list_cost_cache_read_micros INTEGER NULL,
    source_kind TEXT NULL,
    source_ref_id TEXT NULL,
    observed_session_id TEXT,
    request_kind TEXT,
    claude_agent_id TEXT,
    claude_parent_agent_id TEXT,
    parent_session_id TEXT,
    client_app TEXT,
    session_id_source TEXT,
    list_json_parse_ms REAL NULL,
    list_cache_structure_ms REAL NULL,
    list_cache_token_key_ms REAL NULL,
    list_cache_count_lookup_ms REAL NULL,
    list_cache_tokenizer_queue_ms REAL NULL,
    list_cache_serialize_ms REAL NULL,
    list_cache_tokenize_ms REAL NULL,
    list_prepare_signer_ms REAL NULL,
    list_request_body_read_ms INTEGER NULL,
    list_request_body_bytes INTEGER NULL,
    list_finalize_ms INTEGER NULL,
    list_request_body_first_chunk_ms REAL NULL,
    list_request_body_receive_ms REAL NULL,
    list_request_body_wait_ms REAL NULL,
    list_request_body_process_ms REAL NULL,
    list_request_body_chunk_count INTEGER NULL,
    list_response_body_wait_ms REAL NULL,
    list_response_body_process_ms REAL NULL,
    list_response_body_downstream_poll_gap_ms REAL NULL,
    list_retry_overhead_ms REAL NULL,
    event_kind TEXT NULL
);

INSERT INTO request_events_v1_new (
    id, request_id, ts, upstream_id, payload, principal_id, created_at, key_id, model,
    upstream_name, thread_id, input_tokens, output_tokens, cache_creation_input_tokens,
    cache_read_input_tokens, event_id, error_code, upstream_error_type, upstream_error_message,
    thinking_tokens, service_tier, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h,
    token_estimate_source, thinking_budget_tokens, reasoning_effort, list_ts_ms, list_event_key,
    list_status, list_duration_ms, list_auth_ms, list_route_ms, list_limit_reserve_ms,
    list_bulkhead_wait_ms, list_dns_ms, list_connect_ms, list_connection_reused,
    list_proxy_setup_ms, list_shape_ms, list_sign_ms, list_upstream_ttfb_ms,
    list_upstream_body_ms, list_stream_first_content_delta_ms, list_stream_last_content_delta_ms,
    list_inter_token_avg_ms, list_cost_usd_micros, list_cost_input_micros,
    list_cost_output_micros, list_cost_cache_creation_5m_micros,
    list_cost_cache_creation_1h_micros, list_cost_cache_read_micros, source_kind, source_ref_id,
    observed_session_id, request_kind, claude_agent_id, claude_parent_agent_id,
    parent_session_id, client_app, session_id_source, list_json_parse_ms,
    list_cache_structure_ms, list_cache_token_key_ms, list_cache_count_lookup_ms,
    list_cache_tokenizer_queue_ms, list_cache_serialize_ms, list_cache_tokenize_ms,
    list_prepare_signer_ms, list_request_body_read_ms, list_request_body_bytes,
    list_finalize_ms, list_request_body_first_chunk_ms, list_request_body_receive_ms,
    list_request_body_wait_ms, list_request_body_process_ms, list_request_body_chunk_count,
    list_response_body_wait_ms, list_response_body_process_ms,
    list_response_body_downstream_poll_gap_ms, list_retry_overhead_ms, event_kind
)
SELECT
    id, request_id, ts, upstream_id, payload, principal_id, created_at, key_id, model,
    upstream_name, thread_id, input_tokens, output_tokens, cache_creation_input_tokens,
    cache_read_input_tokens, event_id, error_code, upstream_error_type, upstream_error_message,
    thinking_tokens, service_tier, cache_creation_input_tokens_5m, cache_creation_input_tokens_1h,
    token_estimate_source, thinking_budget_tokens, reasoning_effort, list_ts_ms, list_event_key,
    list_status, list_duration_ms, list_auth_ms, list_route_ms, list_limit_reserve_ms,
    list_bulkhead_wait_ms, list_dns_ms, list_connect_ms, list_connection_reused,
    list_proxy_setup_ms, list_shape_ms, list_sign_ms, list_upstream_ttfb_ms,
    list_upstream_body_ms, list_stream_first_content_delta_ms, list_stream_last_content_delta_ms,
    list_inter_token_avg_ms, list_cost_usd_micros, list_cost_input_micros,
    list_cost_output_micros, list_cost_cache_creation_5m_micros,
    list_cost_cache_creation_1h_micros, list_cost_cache_read_micros, source_kind, source_ref_id,
    observed_session_id, request_kind, claude_agent_id, claude_parent_agent_id,
    parent_session_id, client_app, session_id_source, list_json_parse_ms,
    list_cache_structure_ms, list_cache_token_key_ms, list_cache_count_lookup_ms,
    list_cache_tokenizer_queue_ms, list_cache_serialize_ms, list_cache_tokenize_ms,
    list_prepare_signer_ms, list_request_body_read_ms, list_request_body_bytes,
    list_finalize_ms, list_request_body_first_chunk_ms, list_request_body_receive_ms,
    list_request_body_wait_ms, list_request_body_process_ms, list_request_body_chunk_count,
    list_response_body_wait_ms, list_response_body_process_ms,
    list_response_body_downstream_poll_gap_ms, list_retry_overhead_ms, event_kind
FROM request_events_v1
ORDER BY id;

DROP TABLE request_events_v1;
ALTER TABLE request_events_v1_new RENAME TO request_events_v1;

UPDATE sqlite_sequence
SET seq = (SELECT request_events_seq FROM legacy_schema_rebuild_before)
WHERE name = 'request_events_v1'
  AND seq < (SELECT request_events_seq FROM legacy_schema_rebuild_before);

CREATE INDEX request_events_v1_thread_ts
    ON request_events_v1 (thread_id, ts);
CREATE INDEX request_events_v1_list_order_idx
    ON request_events_v1 (list_ts_ms DESC, list_event_key DESC, id DESC);
CREATE INDEX request_events_v1_principal_list_order_idx
    ON request_events_v1 (principal_id, list_ts_ms DESC, list_event_key DESC, id DESC);
CREATE INDEX request_events_v1_model_list_order_idx
    ON request_events_v1 (model, list_ts_ms DESC, list_event_key DESC, id DESC);
CREATE INDEX request_events_v1_upstream_list_order_idx
    ON request_events_v1 (upstream_id, list_ts_ms DESC, list_event_key DESC, id DESC);
CREATE INDEX request_events_v1_principal_key_ts_idx
    ON request_events_v1 (principal_id, key_id, ts);
CREATE INDEX request_events_v1_principal_key_usage_idx
    ON request_events_v1 (principal_id, key_id, list_ts_ms);
CREATE INDEX request_events_v1_thread_list_order_idx
    ON request_events_v1 (thread_id, list_ts_ms DESC, list_event_key DESC, id DESC);
CREATE INDEX request_events_v1_source_kind_list_order_idx
    ON request_events_v1 (source_kind, list_ts_ms DESC, list_event_key DESC, id DESC);
CREATE INDEX request_events_v1_event_kind_list_order_idx
    ON request_events_v1 (
        (CASE WHEN source_kind = 'renewal' THEN 'renewal'
              ELSE COALESCE(event_kind, 'unclassified') END),
        list_ts_ms DESC,
        list_event_key DESC,
        id DESC
    );

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
    WHEN (SELECT COUNT(*) FROM request_events_v1) = b.request_events
     AND (SELECT COUNT(*) FROM managed_keys_v1) = b.managed_keys
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
