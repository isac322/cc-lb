-- Principal list reads filter with `WHERE (? OR deleted_at IS NULL)` and
-- order by name. The partial unique index principals_v1_name_active_uniq
-- cannot serve the include-deleted variant, so the list fell back to a full
-- scan plus a TEMP B-TREE sort. A plain name index serves the ordering for
-- both variants and matches the PostgreSQL principals_v1_name_idx.
CREATE INDEX IF NOT EXISTS principals_v1_name_idx ON principals_v1 (name);
