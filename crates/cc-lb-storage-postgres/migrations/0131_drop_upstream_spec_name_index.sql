-- upstream_spec_v1_name_idx was copied from the dropped upstreams_v1 table in
-- 0041. Name lookups filter `name = $1 AND deleted_at IS NULL` and use the
-- partial unique index upstream_spec_v1_name_active_uniq (0107); upstream
-- lists order by id. No query needs a plain name index, and SQLite never had
-- one.
SET LOCAL lock_timeout = '1s';

DROP INDEX IF EXISTS upstream_spec_v1_name_idx;
