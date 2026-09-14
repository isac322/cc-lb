ALTER TABLE audit_log_v1 ADD COLUMN IF NOT EXISTS actor_authority TEXT;
ALTER TABLE audit_log_v1 ADD COLUMN IF NOT EXISTS actor_subject TEXT;
ALTER TABLE audit_log_v1 ADD COLUMN IF NOT EXISTS actor_kind TEXT;
ALTER TABLE audit_log_v1 ADD COLUMN IF NOT EXISTS actor_email TEXT;

CREATE INDEX IF NOT EXISTS audit_log_v1_actor_subject
    ON audit_log_v1 (actor_authority, actor_subject, seq);
