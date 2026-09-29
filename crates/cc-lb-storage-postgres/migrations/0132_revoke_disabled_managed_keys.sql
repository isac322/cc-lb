-- The managed-key 'disabled' status is gone: the only writers were the removed
-- unversioned disable/enable admin routes, so a disabled key could never be
-- re-enabled. Fold any remaining disabled row into the terminal revoked state
-- exactly as revoke_zero_secrets does: drop its lookup-index row, stamp
-- revoked_at when missing, and zero the lookup/verify material. Each statement
-- is guarded by a predicate that matches nothing once applied; the table holds
-- one row per issued key, far below production-scale rewrite size.
SET LOCAL lock_timeout = '1s';

DELETE FROM managed_api_key_index_v1 AS i
USING managed_api_keys_v1 AS k
WHERE k.principal_id = i.principal_id
  AND k.key_id = i.key_id
  AND k.status = 'disabled';

UPDATE managed_api_keys_v1
SET status = 'revoked',
    revoked_at_unix_secs = COALESCE(revoked_at_unix_secs, EXTRACT(EPOCH FROM NOW())::BIGINT),
    index_hash = decode(repeat('00', 32), 'hex'),
    verify_hash = decode(repeat('00', 32), 'hex'),
    secret_salt = decode(repeat('00', 16), 'hex'),
    updated_at = NOW()
WHERE status = 'disabled';

ALTER TABLE managed_api_keys_v1
    DROP CONSTRAINT IF EXISTS managed_api_keys_v1_status_check;
ALTER TABLE managed_api_keys_v1
    ADD CONSTRAINT managed_api_keys_v1_status_check
    CHECK (status IN ('active', 'revoked'));
