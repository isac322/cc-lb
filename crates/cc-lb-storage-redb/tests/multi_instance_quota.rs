/// redb does not support multi-process safe use due to file locking.
/// Multi-instance scenarios require a Postgres backend.
#[test]
#[ignore = "single-process only; redb file locking prevents multi-process safe use"]
fn multi_instance_quota_not_supported_for_redb() {
    // This test is intentionally ignored.
    // Use Postgres backend for multi-instance scenarios (T34).
}
