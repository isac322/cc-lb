-- Legacy redb-to-sqlite migration tooling left api_keys_v1 rows whose principals
-- had already been deleted; purge them so legacy readers cannot authenticate orphans.
DELETE FROM api_keys_v1
WHERE principal_id NOT IN (SELECT id FROM principals_v1);
