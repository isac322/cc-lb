use cc_lb_storage_redb::RedbStorage;

#[test]
fn adapter_oauth_apikey_opens_current_redb_storage() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter_oauth_apikey.redb");
    let storage = RedbStorage::open(&path, [0; 32])?;

    assert_eq!(storage.schema_version()?, cc_lb_storage_redb::CURRENT_SCHEMA_VERSION);
    assert!(!storage.killswitch_enabled()?);
    Ok(())
}
