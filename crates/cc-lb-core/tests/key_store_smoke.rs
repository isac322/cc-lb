use std::sync::Arc;

use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore, KeyStoreError};
use cc_lb_core::api_keys::secret;
use cc_lb_storage_api::types::{
    ApiKeyMutation, KeyStatus, Limit, LimitKind, PrincipalKindLite, UpstreamKind,
};
use cc_lb_storage_redb::{RedbManagedKeyStore, Storage};

#[tokio::test]
async fn create_lists_principal() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store()?;

    store
        .create("principal-1", create_params("managed key"))
        .await?;

    let listed = store.list_by_principal("principal-1").await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].label, "managed key");

    Ok(())
}

#[tokio::test]
async fn lookup_by_index_hash_returns_matching_key() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store()?;
    let (record, secret) = store
        .create("principal-1", create_params("managed key"))
        .await?;
    let (key_id, _) = secret::parse(secret.expose())?;

    let lookup = store
        .lookup_by_index_hash(&record.index_hash)
        .await?
        .expect("index lookup returns record");

    assert_eq!(lookup.0, "principal-1");
    assert_eq!(lookup.1, key_id);
    assert_eq!(lookup.2, record);

    Ok(())
}

#[tokio::test]
async fn revoke_removes_index() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store()?;
    let (record, secret) = store
        .create("principal-1", create_params("managed key"))
        .await?;
    let (key_id, _) = secret::parse(secret.expose())?;
    let index_hash = record.index_hash;

    store.revoke("principal-1", &key_id).await?;

    assert!(store.lookup_by_index_hash(&index_hash).await?.is_none());
    let listed = store.list_by_principal("principal-1").await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, KeyStatus::Revoked);
    assert!(listed[0].revoked_at_unix_secs.is_some());
    assert_eq!(listed[0].index_hash, [0; 32]);
    assert_eq!(listed[0].verify_hash, [0; 32]);
    assert_eq!(listed[0].secret_salt, [0; 16]);
    assert_eq!(listed[0].last_4, "");

    Ok(())
}

#[tokio::test]
async fn disable_keeps_index() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store()?;
    let (record, secret) = store
        .create("principal-1", create_params("managed key"))
        .await?;
    let (key_id, _) = secret::parse(secret.expose())?;

    store.disable("principal-1", &key_id).await?;

    let lookup = store
        .lookup_by_index_hash(&record.index_hash)
        .await?
        .expect("disabled key remains indexed");
    assert_eq!(lookup.2.status, KeyStatus::Disabled);

    Ok(())
}

#[tokio::test]
async fn patch_label_change_reflected_in_list() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store()?;
    let (_record, secret) = store
        .create("principal-1", create_params("managed key"))
        .await?;
    let (key_id, _) = secret::parse(secret.expose())?;

    store
        .patch(
            "principal-1",
            &key_id,
            ApiKeyMutation {
                label: Some("renamed key".to_owned()),
                ..Default::default()
            },
        )
        .await?;

    let listed = store.list_by_principal("principal-1").await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].label, "renamed key");

    Ok(())
}

#[tokio::test]
async fn enable_on_revoked_returns_err() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store()?;
    let (_record, secret) = store
        .create("principal-1", create_params("managed key"))
        .await?;
    let (key_id, _) = secret::parse(secret.expose())?;

    store.revoke("principal-1", &key_id).await?;
    let error = store
        .enable("principal-1", &key_id)
        .await
        .expect_err("revoked key cannot be enabled");

    assert!(matches!(error, KeyStoreError::KeyAlreadyRevoked { .. }));

    Ok(())
}

fn new_store() -> Result<(tempfile::TempDir, KeyStore), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("key_store.redb");
    let storage = Storage::open(&path, [21; 32])?;
    let managed_store = RedbManagedKeyStore::new(Arc::new(storage));
    Ok((dir, KeyStore::new(Arc::new(managed_store))))
}

fn create_params(label: &str) -> CreateParams {
    CreateParams {
        upstream_kind: UpstreamKind::AnthropicKey,
        upstream_credential_ref: "anthropic-prod".to_owned(),
        label: label.to_owned(),
        description: Some("test key".to_owned()),
        expires_at_unix_secs: Some(1_800_000_000),
        limit_overrides: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
        principal_kind: PrincipalKindLite::Machine,
    }
}
