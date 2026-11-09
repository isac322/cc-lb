-- Defer historical Gemini model identity reconciliation to the off-request
-- rollup worker. Startup must only install durable, resumable state.
CREATE TABLE IF NOT EXISTS usage_rollup_reconciliations_v1 (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('pending', 'rewriting', 'rewriting_nested', 'complete')),
    cursor BIGINT NOT NULL DEFAULT 0 CHECK (cursor >= 0),
    cursor_xid xid8 NOT NULL DEFAULT '0'::xid8,
    upper_bound BIGINT NOT NULL DEFAULT 0 CHECK (upper_bound >= 0),
    upper_bound_xid xid8 NOT NULL DEFAULT '0'::xid8
);

INSERT INTO usage_rollup_reconciliations_v1 (id, status, cursor, upper_bound)
VALUES ('canonicalize_gemini_model_identity', 'pending', 0, 0)
ON CONFLICT (id) DO NOTHING;
