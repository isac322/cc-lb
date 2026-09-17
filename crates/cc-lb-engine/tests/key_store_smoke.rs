use std::sync::Arc;

use cc_lb_engine::api_keys::key_store::{CreateParams, KeyStore, KeyStoreError};
use cc_lb_engine::api_keys::secret;
use cc_lb_storage_api::types::{ApiKeyMutation, IssueParams, KeyStatus, Limit, LimitKind};
use cc_lb_storage_api::{BackendKind, ManagedKeyStore, MetaStore};

#[path = "../../../tests/fixtures/managed_key_seed.rs"]
mod managed_key_seed;

#[tokio::test]
async fn existing_key_lists_principal() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store().await?;

    store
        .seed_existing("principal-1", create_params("managed key"))
        .await?;

    let listed = store.list_by_principal("principal-1").await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].label, "managed key");

    Ok(())
}

#[tokio::test]
async fn lookup_by_index_hash_returns_matching_key() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store().await?;
    let (record, secret) = store
        .seed_existing("principal-1", create_params("managed key"))
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
    let (_dir, store) = new_store().await?;
    let (record, secret) = store
        .seed_existing("principal-1", create_params("managed key"))
        .await?;
    let (key_id, _) = secret::parse(secret.expose())?;
    let index_hash = record.index_hash;
    let last_4 = record.last_4.clone();

    store.revoke("principal-1", &key_id).await?;

    assert!(store.lookup_by_index_hash(&index_hash).await?.is_none());
    let listed = store.list_by_principal("principal-1").await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, KeyStatus::Revoked);
    assert!(listed[0].revoked_at_unix_secs.is_some());
    assert_eq!(listed[0].index_hash, [0; 32]);
    assert_eq!(listed[0].verify_hash, [0; 32]);
    assert_eq!(listed[0].secret_salt, [0; 16]);
    assert_eq!(listed[0].last_4, last_4);

    Ok(())
}

#[tokio::test]
async fn disable_keeps_index() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, store) = new_store().await?;
    let (record, secret) = store
        .seed_existing("principal-1", create_params("managed key"))
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
    let (_dir, store) = new_store().await?;
    let (_record, secret) = store
        .seed_existing("principal-1", create_params("managed key"))
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
    let (_dir, store) = new_store().await?;
    let (_record, secret) = store
        .seed_existing("principal-1", create_params("managed key"))
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

async fn new_store() -> Result<(tempfile::TempDir, ExistingKeyFixture), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let database_url = format!("sqlite://{}", dir.path().join("key_store.sqlite").display());
    let storage =
        cc_lb_storage_sqlite::open_sqlite(&database_url, Arc::new(cc_lb_engine::SystemClock))
            .await?;
    storage.initialize(BackendKind::Sqlite).await?;
    let storage = Arc::new(storage);
    Ok((
        dir,
        ExistingKeyFixture {
            store: KeyStore::new(storage.clone()),
            storage,
        },
    ))
}

fn create_params(label: &str) -> CreateParams {
    CreateParams {
        label: label.to_owned(),
        description: Some("test key".to_owned()),
        expires_at_unix_secs: Some(1_800_000_000),
        limit_overrides: vec![Limit {
            kind: LimitKind::Requests,
            window_secs: 60,
            cap_micros: 100,
        }],
    }
}

struct ExistingKeyFixture {
    store: KeyStore,
    storage: Arc<cc_lb_storage_sqlite::SqliteStorage>,
}

impl std::ops::Deref for ExistingKeyFixture {
    type Target = KeyStore;

    fn deref(&self) -> &Self::Target {
        &self.store
    }
}

impl ExistingKeyFixture {
    async fn seed_existing(
        &self,
        principal_id: &str,
        params: CreateParams,
    ) -> Result<
        (
            cc_lb_storage_api::StoredApiKeyRecord,
            secret::RedactedSecret,
        ),
        Box<dyn std::error::Error>,
    > {
        let generated = secret::generate_new();
        let issue = IssueParams {
            label: params.label,
            description: params.description,
            expires_at_unix_secs: params.expires_at_unix_secs,
            limit_overrides: params.limit_overrides,
            secret_salt: generated.secret_salt,
            verify_hash: generated.verify_hash,
            last_4: generated.last_4,
            index_hash: generated.index_hash,
        };
        managed_key_seed::seed_sqlite(
            self.storage.pool(),
            principal_id,
            &generated.key_id,
            &issue,
            1_700_000_000,
        )
        .await?;
        let record = self
            .storage
            .get(principal_id, &generated.key_id)
            .await?
            .ok_or("seeded key is missing")?;
        Ok((record, generated.plaintext))
    }
}

#[tokio::test]
async fn issuance_is_paused_without_creating_a_key() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, fixture) = new_store().await?;
    let error = fixture
        .store
        .create("principal-1", create_params("paused"))
        .await
        .expect_err("bridge must refuse issuance");
    assert!(matches!(error, KeyStoreError::IssuancePaused));
    assert!(fixture.store.list_all().await?.is_empty());
    Ok(())
}
