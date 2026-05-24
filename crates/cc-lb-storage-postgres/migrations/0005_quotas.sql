CREATE TABLE IF NOT EXISTS quotas_by_principal_v1 (
    principal_id TEXT NOT NULL,
    window_start BIGINT NOT NULL,
    kind TEXT NOT NULL,
    value BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (principal_id, window_start, kind)
);
