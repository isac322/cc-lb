-- The managed-key 'disabled' status is gone: the only writers were the removed
-- unversioned disable/enable admin routes, so a disabled key could never be
-- re-enabled. Fold any remaining disabled row into the terminal revoked state
-- exactly as revoke_zero_secrets does: stamp revoked_at when missing and zero
-- the lookup/verify material so the key stays rejected and leaves the
-- active-index-hash unique index. The predicate matches nothing once applied.
UPDATE managed_keys_v1
SET status = 'revoked',
    revoked_at = COALESCE(revoked_at, CAST(strftime('%s', 'now') AS INTEGER)),
    index_hash = zeroblob(32),
    verify_hash = zeroblob(32),
    secret_salt = zeroblob(16),
    updated_at = CAST(strftime('%s', 'now') AS INTEGER)
WHERE status = 'disabled';
