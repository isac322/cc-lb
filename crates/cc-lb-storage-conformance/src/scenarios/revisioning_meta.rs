//! Revisioning and metadata conformance scenarios.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, CURRENT_CONTRACT_VERSION, ConfigDraftState, ConfigStore, HistoryEntry, MetaStore,
    StorageError, StorageResult,
};

use crate::harness::{ConformanceBackend, ConformanceFixture};

#[async_trait]
pub trait RevisioningMetaBackend: ConformanceBackend {
    async fn initialize_with_stamped_backend_kind(
        &self,
        stored: BackendKind,
        configured: BackendKind,
    ) -> StorageResult<()>;

    async fn create_legacy_without_backend_kind(&self) -> Result<Self::Fixture>;

    async fn stored_backend_kind(&self, fixture: &Self::Fixture) -> Result<Option<BackendKind>>;
}

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: RevisioningMetaBackend,
{
    config_draft_optimistic_revision(Arc::clone(&backend)).await?;
    config_history_cap_50(Arc::clone(&backend)).await?;
    config_last_validated_revision(Arc::clone(&backend)).await?;
    meta_contract_version(Arc::clone(&backend)).await?;
    meta_backend_kind_stamp(Arc::clone(&backend)).await?;
    meta_backend_kind_mismatch(Arc::clone(&backend)).await?;

    Ok(())
}

pub async fn config_draft_optimistic_revision<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        let initial = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(initial, ConfigDraftState::default());

        let revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_000),
                last_validated_revision: Some(99),
                last_validation_error: Some("stale validation".to_owned()),
                ..ConfigDraftState::default()
            },
            0,
        )
        .await?;
        assert_eq!(revision, 1);

        let stored = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(stored.revision, 1);
        assert_eq!(stored.saved_at_unix_secs, Some(1_800_000_000));
        assert_eq!(stored.last_validated_revision, None);
        assert_eq!(stored.last_validation_error, None);

        let stale = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_001),
                ..ConfigDraftState::default()
            },
            0,
        )
        .await
        .expect_err("stale draft revision must not be accepted");
        assert!(matches!(stale, StorageError::Conflict { .. }));

        let revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_002),
                ..ConfigDraftState::default()
            },
            1,
        )
        .await?;
        assert_eq!(revision, 2);

        let stored = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(stored.revision, 2);
        assert_eq!(stored.saved_at_unix_secs, Some(1_800_000_002));

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn config_history_cap_50<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        for revision in 1..=60 {
            ConfigStore::append_config_history(
                storage.as_ref(),
                revision,
                1_800_000_000 + revision,
            )
            .await?;
        }

        let entries = ConfigStore::list_config_history(storage.as_ref(), 100).await?;
        let expected = (11..=60)
            .rev()
            .map(|revision| HistoryEntry {
                revision,
                applied_at_unix_secs: 1_800_000_000 + revision,
            })
            .collect::<Vec<_>>();
        assert_eq!(entries, expected);

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn config_last_validated_revision<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        let revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_010),
                ..ConfigDraftState::default()
            },
            0,
        )
        .await?;
        assert_eq!(revision, 1);

        ConfigStore::set_last_validated_revision(
            storage.as_ref(),
            revision,
            Some("missing upstream".to_owned()),
        )
        .await?;
        let draft = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(draft.revision, revision);
        assert_eq!(draft.last_validated_revision, None);
        assert_eq!(
            draft.last_validation_error,
            Some("missing upstream".to_owned())
        );

        ConfigStore::set_last_validated_revision(storage.as_ref(), revision, None).await?;
        let draft = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(draft.last_validated_revision, Some(revision));
        assert_eq!(draft.last_validation_error, None);

        let next_revision = ConfigStore::put_config_draft(
            storage.as_ref(),
            ConfigDraftState {
                saved_at_unix_secs: Some(1_800_000_011),
                ..ConfigDraftState::default()
            },
            revision,
        )
        .await?;
        assert_eq!(next_revision, 2);
        let draft = ConfigStore::get_config_draft(storage.as_ref()).await?;
        assert_eq!(draft.last_validated_revision, None);
        assert_eq!(draft.last_validation_error, None);

        let stale = ConfigStore::set_last_validated_revision(storage.as_ref(), revision, None)
            .await
            .expect_err("validation must reject stale draft revisions");
        assert!(matches!(stale, StorageError::Conflict { .. }));

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn meta_contract_version<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();

        MetaStore::initialize(storage.as_ref(), fixture.backend_kind()).await?;
        assert_eq!(
            MetaStore::contract_version(storage.as_ref()).await?,
            CURRENT_CONTRACT_VERSION
        );

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn meta_backend_kind_stamp<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let expected = fixture.backend_kind();

        MetaStore::initialize(storage.as_ref(), expected).await?;
        assert_eq!(MetaStore::backend_kind(storage.as_ref()).await?, expected);

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn meta_backend_kind_mismatch<B>(backend: Arc<B>) -> Result<()>
where
    B: RevisioningMetaBackend,
{
    let mismatch = backend
        .initialize_with_stamped_backend_kind(BackendKind::Postgres, BackendKind::Sqlite)
        .await
        .expect_err("backend kind mismatch must be rejected");
    assert!(matches!(
        mismatch,
        StorageError::BackendKindMismatch {
            stored: BackendKind::Postgres,
            configured: BackendKind::Sqlite
        }
    ));

    Ok(())
}
