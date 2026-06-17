ALTER TABLE audit_log_v1
    ADD COLUMN IF NOT EXISTS api_key_id TEXT,
    ADD COLUMN IF NOT EXISTS cost_usd_micros BIGINT CHECK (cost_usd_micros IS NULL OR cost_usd_micros >= 0),
    ADD COLUMN IF NOT EXISTS limit_violation TEXT,
    ADD COLUMN IF NOT EXISTS admin_action TEXT,
    ADD COLUMN IF NOT EXISTS actor TEXT;
