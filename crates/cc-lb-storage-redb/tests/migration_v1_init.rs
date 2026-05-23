use cc_lb_storage_redb::{
    API_KEYS_V1, AUDIT_LOG_V1, CURRENT_SCHEMA_VERSION, KILLSWITCH_V1, OAUTH_CREDENTIALS_V1,
    PRINCIPAL_LIMIT_STATES_V1, QUOTAS_BY_PRINCIPAL_V1, REQUEST_EVENTS_V1, SCHEMA_VERSION_V1,
    Storage, StorageError, USAGE_ROLLUP_CHECKPOINTS_V1, USAGE_ROLLUPS_V1,
};
use redb::TableHandle;

#[test]
fn opening_empty_database_initializes_schema_v1() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");

    let storage = Storage::open(&path, [17; 32])?;
    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
    assert!(!storage.killswitch_enabled()?);
    storage.set_killswitch_enabled(true)?;
    assert!(storage.killswitch_enabled()?);
    drop(storage);

    let storage = Storage::open(&path, [17; 32])?;
    assert!(storage.killswitch_enabled()?);
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table_names = read_txn
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect::<Vec<_>>();

    assert!(table_names.contains(&OAUTH_CREDENTIALS_V1.name().to_owned()));
    assert!(table_names.contains(&API_KEYS_V1.name().to_owned()));
    assert!(table_names.contains(&QUOTAS_BY_PRINCIPAL_V1.name().to_owned()));
    assert!(table_names.contains(&AUDIT_LOG_V1.name().to_owned()));
    assert!(table_names.contains(&PRINCIPAL_LIMIT_STATES_V1.name().to_owned()));
    assert!(table_names.contains(&REQUEST_EVENTS_V1.name().to_owned()));
    assert!(table_names.contains(&USAGE_ROLLUPS_V1.name().to_owned()));
    assert!(table_names.contains(&USAGE_ROLLUP_CHECKPOINTS_V1.name().to_owned()));
    assert!(table_names.contains(&SCHEMA_VERSION_V1.name().to_owned()));
    assert!(table_names.contains(&KILLSWITCH_V1.name().to_owned()));

    Ok(())
}

#[test]
fn future_schema_version_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let future_version = CURRENT_SCHEMA_VERSION + 1;

    {
        let db = redb::Database::create(&path)?;
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(SCHEMA_VERSION_V1)?;
            table.insert("version", &future_version)?;
        }
        write_txn.commit()?;
    }

    match Storage::open(&path, [17; 32]) {
        Err(StorageError::UnsupportedSchemaVersion { found, current }) => {
            assert_eq!(found, future_version);
            assert_eq!(current, CURRENT_SCHEMA_VERSION);
        }
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("future schema version must be rejected"),
    }

    Ok(())
}
