-- Drop schema that no current code path reads or writes.
--
-- Data rewrites below touch only legacy rows: audit rows written before
-- admin_action existed, keepalive decisions recorded before
-- last_message_at_ms existed, plan-tier rows tagged by the removed one-shot
-- backfill, and the 50-row config history. Each UPDATE is guarded by a
-- predicate that matches nothing once applied.
--
-- Plain CREATE INDEX, not CONCURRENTLY: see
-- 0126_audit_log_principal_ts_seq_index.sql for the advisory-lock rationale.
-- Every index created below has a name that no earlier migration used, so on
-- a large table an operator MAY pre-create it with CREATE INDEX CONCURRENTLY
-- using the identical name and definition; IF NOT EXISTS then makes the
-- statement a no-op. The superseded indexes are dropped only after their
-- replacements exist.
SET LOCAL lock_timeout = '1s';

-- Tables with no reader or writer.
DROP TABLE IF EXISTS killswitch_v1;
DROP TABLE IF EXISTS quotas_by_principal_v1;
DROP TABLE IF EXISTS principal_limit_states_v1;
DROP TABLE IF EXISTS plugin_registry;
DROP TABLE IF EXISTS plugin_registry_marker;
DROP TABLE IF EXISTS plugin_registry_blobs;

DELETE FROM meta WHERE key IN ('killswitch_enabled', 'contract_version');

-- Plugin registry columns superseded by per-hook fingerprints and the
-- wasm_registry_v2 upload timestamp.
ALTER TABLE wasm_registry_v2 DROP COLUMN IF EXISTS schema_hash;
ALTER TABLE wasm_blobs_v2 DROP COLUMN IF EXISTS parse_validated_at;

-- Principal apply status was never set by any runtime path.
ALTER TABLE principals_v1
    DROP COLUMN IF EXISTS last_apply_error,
    DROP COLUMN IF EXISTS last_apply_at;

DROP INDEX IF EXISTS principals_v1_enabled_idx;
DROP INDEX IF EXISTS principals_v1_deleted_at_idx;
DROP INDEX IF EXISTS upstream_spec_v1_enabled_idx;

-- Upstream spec-split revision counters were written but never consumed.
ALTER TABLE upstream_api_key_secret_v1 DROP COLUMN IF EXISTS secret_revision;
ALTER TABLE upstream_oauth_token_v1
    DROP COLUMN IF EXISTS token_revision,
    DROP COLUMN IF EXISTS refreshed_at;
ALTER TABLE upstream_status_v1
    DROP COLUMN IF EXISTS observed_spec_revision,
    DROP COLUMN IF EXISTS observed_api_key_secret_revision,
    DROP COLUMN IF EXISTS observed_oauth_token_revision;

-- Plan-tier history: the one-shot backfill source is gone.
UPDATE upstream_plan_tier_history_v1
SET resolution_source = 'builtin'
WHERE resolution_source = 'backfill';

ALTER TABLE upstream_plan_tier_history_v1
    DROP CONSTRAINT IF EXISTS upstream_plan_tier_history_v1_resolution_source_check;
ALTER TABLE upstream_plan_tier_history_v1
    ADD CONSTRAINT upstream_plan_tier_history_v1_resolution_source_check
    CHECK (resolution_source IN ('override','builtin','unknown'));

-- Config history keeps only revision and application time.
ALTER TABLE config_history_v1 ADD COLUMN IF NOT EXISTS applied_at_unix_secs BIGINT;
UPDATE config_history_v1
SET applied_at_unix_secs = COALESCE(
    (config ->> 'applied_at_unix_secs')::BIGINT,
    EXTRACT(EPOCH FROM created_at)::BIGINT
)
WHERE applied_at_unix_secs IS NULL;
ALTER TABLE config_history_v1 ALTER COLUMN applied_at_unix_secs SET NOT NULL;
ALTER TABLE config_history_v1 DROP COLUMN IF EXISTS config;
DROP INDEX IF EXISTS config_history_v1_created_at;

-- Audit log: admin_action is the only admin marker. Copy the legacy kind into
-- admin_action for rows that predate the column, build the admin partial
-- indexes on admin_action alone under new names, then drop the kind-based
-- indexes and the kind column.
UPDATE audit_log_v1
SET admin_action = kind
WHERE admin_action IS NULL AND kind IS NOT NULL;

CREATE INDEX IF NOT EXISTS audit_log_v1_admin_action_ts_seq
    ON audit_log_v1 (ts DESC, seq DESC)
    WHERE admin_action IS NOT NULL;
CREATE INDEX IF NOT EXISTS audit_log_v1_admin_action_principal_ts_seq
    ON audit_log_v1 (principal_id, ts DESC, seq DESC)
    WHERE admin_action IS NOT NULL;
CREATE INDEX IF NOT EXISTS audit_log_v1_admin_action_actor_ts_seq
    ON audit_log_v1 (actor_authority, actor_subject, ts DESC, seq DESC)
    WHERE admin_action IS NOT NULL;

DROP INDEX IF EXISTS audit_log_v1_admin_ts_seq;
DROP INDEX IF EXISTS audit_log_v1_admin_principal_ts_seq;
DROP INDEX IF EXISTS audit_log_v1_admin_actor_ts_seq;
DROP INDEX IF EXISTS audit_log_v1_principal_seq;
ALTER TABLE audit_log_v1 DROP COLUMN IF EXISTS kind;

-- Keepalive decisions: every writer records last_message_at_ms.
UPDATE cache_keepalive_decisions
SET last_message_at_ms = ts * 1000
WHERE last_message_at_ms IS NULL;
ALTER TABLE cache_keepalive_decisions ALTER COLUMN last_message_at_ms SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cache_keepalive_decisions_principal_last_message
    ON cache_keepalive_decisions (
        principal_id,
        last_message_at_ms DESC,
        (('decision:' || source_ref_id)) ASC
    );

DROP INDEX IF EXISTS idx_cache_keepalive_decisions_principal_effective_message;
DROP INDEX IF EXISTS idx_cache_keepalive_decisions_principal_message;
DROP INDEX IF EXISTS idx_cache_keepalive_sessions_principal_updated;
DROP INDEX IF EXISTS idx_cache_keepalive_sessions_principal_message;

-- Unused indexes.
DROP INDEX IF EXISTS prompt_cache_observations_upstream_model_expires_idx;
DROP INDEX IF EXISTS idx_warmup_attempts_v1_upstream_trigger_scheduled;
DROP INDEX IF EXISTS idx_warmup_attempts_v1_outcome_attempted;
DROP INDEX IF EXISTS managed_api_keys_v1_active_idx;
DROP INDEX IF EXISTS managed_api_keys_v1_principal_id;

-- Request events: principal cost reads cover only UUID and NULL principals,
-- both served by request_events_v1_principal_list_order_idx and
-- request_events_v1_upstream_id_idx.
DROP INDEX IF EXISTS request_events_v1_normalized_non_uuid_principal_cost_idx;

-- Cost-component compatibility trigger for writers that did not send the
-- materialization marker; every writer now binds the cost columns directly.
DROP TRIGGER IF EXISTS request_events_v1_materialize_cost_components ON request_events_v1;
DROP FUNCTION IF EXISTS request_events_v1_materialize_cost_components();

-- Request-event columns that only the INSERT ever touched. The payload JSON
-- still carries these values.
DROP INDEX IF EXISTS request_events_v1_cache_state_ts;
DROP INDEX IF EXISTS request_events_v1_v3_cache_key_ts;
ALTER TABLE request_events_v1
    DROP COLUMN IF EXISTS list_cost_components_materialized,
    DROP COLUMN IF EXISTS list_upstream,
    DROP COLUMN IF EXISTS cache_state,
    DROP COLUMN IF EXISTS message_id,
    DROP COLUMN IF EXISTS message_index,
    DROP COLUMN IF EXISTS message_count,
    DROP COLUMN IF EXISTS cache_control_block_count,
    DROP COLUMN IF EXISTS cache_breakpoints,
    DROP COLUMN IF EXISTS cache_prefix_hash,
    DROP COLUMN IF EXISTS web_search_requests,
    DROP COLUMN IF EXISTS web_fetch_requests,
    DROP COLUMN IF EXISTS inference_geo,
    DROP COLUMN IF EXISTS matched_v3_cache_key,
    DROP COLUMN IF EXISTS breakpoint_content_block_index,
    DROP COLUMN IF EXISTS matched_content_block_index,
    DROP COLUMN IF EXISTS lookback_distance,
    DROP COLUMN IF EXISTS predicted_cache_read_tokens,
    DROP COLUMN IF EXISTS predicted_cache_creation_tokens_5m,
    DROP COLUMN IF EXISTS predicted_cache_creation_tokens_1h,
    DROP COLUMN IF EXISTS cache_value_micros,
    DROP COLUMN IF EXISTS formula_winner_upstream_id,
    DROP COLUMN IF EXISTS kept_upstream_id,
    DROP COLUMN IF EXISTS lineage_would_have_predicted_read_tokens,
    DROP COLUMN IF EXISTS lineage_would_have_picked_upstream_id,
    DROP COLUMN IF EXISTS quota_urgency_5h,
    DROP COLUMN IF EXISTS quota_urgency_7d,
    DROP COLUMN IF EXISTS quota_urgency_combined,
    DROP COLUMN IF EXISTS quota_warning_multiplier;
