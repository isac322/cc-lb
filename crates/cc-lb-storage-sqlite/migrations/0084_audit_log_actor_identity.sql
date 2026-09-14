ALTER TABLE audit_log_v1 ADD COLUMN actor_authority TEXT;
ALTER TABLE audit_log_v1 ADD COLUMN actor_subject TEXT;
ALTER TABLE audit_log_v1 ADD COLUMN actor_kind TEXT;
ALTER TABLE audit_log_v1 ADD COLUMN actor_email TEXT;

CREATE INDEX IF NOT EXISTS idx_audit_log_actor_subject
    ON audit_log_v1 (actor_authority, actor_subject, id);
