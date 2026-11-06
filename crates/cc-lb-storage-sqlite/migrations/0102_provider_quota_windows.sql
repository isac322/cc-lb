-- SQLite cannot alter a CHECK constraint with ALTER TABLE, but relaxing a CHECK
-- without changing the on-disk row format is safe through sqlite_schema. This
-- migration updates only schema text: it is O(1) in row count, remains inside
-- sqlx's migration transaction, and bumps schema_version to force connection
-- caches to reparse; the compatibility test runs integrity_check after startup.
-- Existing rows remain in place; the quota pruner still bounds future history
-- to retention_days.min(30) days and preserves its per-series anchor rows.

PRAGMA writable_schema = ON;

UPDATE sqlite_schema
SET sql = replace(
    sql,
    "CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified'))",
    "CHECK (window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','gemini_5h','gemini_7d','third_party_5h','third_party_7d','overage','unified'))"
)
WHERE type = 'table'
  AND name IN (
      'upstream_subscription_quota_checkpoints_v1',
      'upstream_subscription_quota_latest_v1'
  );

UPDATE sqlite_schema
SET sql = replace(
    sql,
    "CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','overage','unified'))",
    "CHECK (quota_window IN ('5h','7d','7d_sonnet','7d_opus','7d_fable','gemini_5h','gemini_7d','third_party_5h','third_party_7d','overage','unified'))"
)
WHERE type = 'table'
  AND name = 'pool_subscription_quota_history_v1';

PRAGMA writable_schema = OFF;

-- Force every existing connection to discard prepared statements that were
-- compiled against the pre-migration schema. The value is deliberately far
-- above the current migration count and leaves room for future migrations.
PRAGMA schema_version = 1000000;
