use cc_lb_storage_redb::{
    AUDIT_LOG_V1, CURRENT_SCHEMA_VERSION, KILLSWITCH_V1, OAUTH_CREDENTIALS_V1,
    PROMPT_CACHE_OBSERVATIONS, REQUEST_EVENTS_V1, SCHEMA_VERSION_V1, Storage, StorageError,
    UPSTREAM_SPEC_V1, UPSTREAMS_V2,
};
use redb::{ReadableDatabase, TableHandle};
use serde_json::json;
use uuid::Uuid;

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
    assert!(table_names.contains(&AUDIT_LOG_V1.name().to_owned()));
    assert!(table_names.contains(&REQUEST_EVENTS_V1.name().to_owned()));
    assert!(table_names.contains(&PROMPT_CACHE_OBSERVATIONS.name().to_owned()));
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

#[test]
fn v3_to_v4_rewrites_legacy_upstreams_without_shape_plugin()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let id = Uuid::new_v4();

    seed_legacy_upstream(&path, id, "anthropic_api_key")?;

    let storage = Storage::open(&path, [17; 32])?;
    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(UPSTREAM_SPEC_V1)?;
    let stored = table
        .get(id.as_bytes().as_slice())?
        .expect("upstream survives migration")
        .value()
        .to_vec();
    let value: serde_json::Value = serde_json::from_slice(&stored)?;
    assert!(value.get("shape_plugin").is_none());
    assert_eq!(
        value.get("id").and_then(|v| v.as_str()),
        Some(id.to_string().as_str())
    );
    assert_eq!(value.get("name").and_then(|v| v.as_str()), Some("legacy-api-key"));
    assert_eq!(
        value.get("kind").and_then(|v| v.as_str()),
        Some("anthropic_api_key")
    );

    Ok(())
}

#[test]
fn v3_to_v4_converts_active_custom_upstream_to_anthropic_api_key()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let id = Uuid::new_v4();

    seed_legacy_upstream(&path, id, "custom")?;

    let storage = Storage::open(&path, [17; 32])?;
    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(UPSTREAM_SPEC_V1)?;
    let stored = table
        .get(id.as_bytes().as_slice())?
        .expect("upstream survives migration")
        .value()
        .to_vec();
    let value: serde_json::Value = serde_json::from_slice(&stored)?;
    assert_eq!(
        value.get("id").and_then(|v| v.as_str()),
        Some(id.to_string().as_str())
    );
    assert_eq!(value.get("name").and_then(|v| v.as_str()), Some("legacy-api-key"));
    assert_eq!(
        value.get("kind").and_then(|v| v.as_str()),
        Some("anthropic_api_key")
    );

    Ok(())
}

#[test]
fn v3_to_v4_drops_soft_deleted_custom_upstream() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("storage.redb");
    let id = Uuid::new_v4();

    seed_legacy_upstream_with_deleted(&path, id, "custom", Some(1234567890))?;

    let storage = Storage::open(&path, [17; 32])?;
    assert_eq!(storage.schema_version()?, CURRENT_SCHEMA_VERSION);
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(UPSTREAM_SPEC_V1)?;
    let stored = table.get(id.as_bytes().as_slice())?;
    assert!(
        stored.is_none(),
        "soft-deleted custom upstream should be dropped"
    );

    Ok(())
}

fn seed_legacy_upstream(
    path: &std::path::Path,
    id: Uuid,
    kind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    seed_legacy_upstream_with_deleted(path, id, kind, None)
}

fn seed_legacy_upstream_with_deleted(
    path: &std::path::Path,
    id: Uuid,
    kind: &str,
    deleted_at_unix_secs: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
        schema.insert("version", &3)?;
    }
    {
        let record = json!({
            "id": id,
            "name": "legacy-api-key",
            "kind": kind,
            "base_url": "https://api.anthropic.com",
            "enabled": true,
            "oauth_credentials": null,
            "api_key_ciphertext": [1, 2, 3, 4],
            "refresh_lease_holder": null,
            "refresh_lease_until_unix_secs": null,
            "last_apply_error": null,
            "last_apply_at_unix_secs": null,
            "deleted_at_unix_secs": deleted_at_unix_secs,
            "shape_plugin": {
                "registry_id": Uuid::new_v4(),
                "config": {"legacy": true}
            },
            "revision": 7,
            "created_at_unix_secs": 11,
            "updated_at_unix_secs": 13
        });
        let mut table = write_txn.open_table(UPSTREAMS_V2)?;
        table.insert(
            id.as_bytes().as_slice(),
            serde_json::to_vec(&record)?.as_slice(),
        )?;
    }
    write_txn.commit()?;
    Ok(())
}
