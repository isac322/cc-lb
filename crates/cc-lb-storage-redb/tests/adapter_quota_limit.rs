use cc_lb_storage_redb::RedbStorage;

#[test]
fn adapter_quota_limit_opens_current_redb_storage() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter_quota_limit.redb");
    let storage = RedbStorage::open(&path, [0; 32])?;

    assert_eq!(
        storage.schema_version()?,
        cc_lb_storage_redb::CURRENT_SCHEMA_VERSION
    );
    assert!(!storage.killswitch_enabled()?);
    Ok(())
}
