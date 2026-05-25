use cc_lb_storage_api::{
    BackendKind, CURRENT_CONTRACT_VERSION, ConfigDraftState, ConfigStore, HistorySummary,
    MetaStore, StorageError,
};
use cc_lb_storage_redb::{META_BACKEND_KIND_V1, RedbStorage, SCHEMA_VERSION_V1};
use redb::ReadableDatabase;
use serde_json::json;

#[tokio::test]
async fn config_store_trait_path_handles_draft_validation_and_history()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-config.redb");
    let storage = RedbStorage::open(&path)?;

    let initial = ConfigStore::get_config_draft(&storage).await?;
    assert_eq!(initial, ConfigDraftState::default());

    let revision = ConfigStore::put_config_draft(
        &storage,
        ConfigDraftState {
            draft: Some(json!({"upstreams": {"primary": {"url": "https://example.test"}}})),
            saved_at_unix_secs: Some(1_800_000_000),
            last_validated_revision: Some(99),
            last_validation_error: Some("old".to_owned()),
            ..ConfigDraftState::default()
        },
        0,
    )
    .await?;
    assert_eq!(revision, 1);

    let draft = ConfigStore::get_config_draft(&storage).await?;
    assert_eq!(draft.revision, 1);
    assert_eq!(draft.last_validated_revision, None);
    assert_eq!(draft.last_validation_error, None);

    ConfigStore::set_last_validated_revision(&storage, 1, None).await?;
    let draft = ConfigStore::get_config_draft(&storage).await?;
    assert_eq!(draft.last_validated_revision, Some(1));

    let stale = ConfigStore::put_config_draft(&storage, ConfigDraftState::default(), 0)
        .await
        .expect_err("stale revision should conflict");
    assert!(matches!(stale, StorageError::Conflict { .. }));

    let summary = HistorySummary {
        upstreams: 2,
        principals: 3,
        plugin_count: 4,
        tls_enabled: true,
    };
    ConfigStore::append_config_history(
        &storage,
        1,
        "[listener]".to_owned(),
        1_800_000_001,
        summary.clone(),
    )
    .await?;

    let listed = ConfigStore::list_config_history(&storage, 10).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].revision, 1);
    assert_eq!(listed[0].summary, summary);
    assert_eq!(
        ConfigStore::get_config_history(&storage, 1).await?,
        Some(listed[0].clone())
    );

    Ok(())
}

#[tokio::test]
async fn meta_store_trait_path_initializes_backend_contract_and_killswitch()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("adapter-meta.redb");
    let storage = RedbStorage::open(&path)?;

    MetaStore::initialize(&storage, BackendKind::Redb).await?;
    assert_eq!(
        MetaStore::contract_version(&storage).await?,
        CURRENT_CONTRACT_VERSION
    );
    assert_eq!(MetaStore::backend_kind(&storage).await?, BackendKind::Redb);
    assert!(!MetaStore::killswitch_enabled(&storage).await?);

    MetaStore::set_killswitch_enabled(&storage, true).await?;
    assert!(MetaStore::killswitch_enabled(&storage).await?);

    let mismatch = MetaStore::initialize(&storage, BackendKind::Postgres)
        .await
        .expect_err("redb storage must reject postgres request");
    assert!(matches!(
        mismatch,
        StorageError::BackendKindMismatch {
            stored: BackendKind::Redb,
            configured: BackendKind::Postgres
        }
    ));

    Ok(())
}

#[test]
fn legacy_database_without_backend_kind_autostamps_redb() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("legacy-meta.redb");

    {
        let db = redb::Database::create(&path)?;
        let write_txn = db.begin_write()?;
        {
            let mut schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
            schema.insert("version", &1)?;
        }
        {
            write_txn.open_table(META_BACKEND_KIND_V1)?;
        }
        write_txn.commit()?;
    }

    let storage = RedbStorage::open(&path)?;
    assert_eq!(storage.backend_kind()?, BackendKind::Redb);
    drop(storage);

    let db = redb::Database::create(&path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(META_BACKEND_KIND_V1)?;
    assert_eq!(
        table
            .get("backend_kind")?
            .map(|stored| stored.value().to_owned()),
        Some("redb".to_owned())
    );

    Ok(())
}
