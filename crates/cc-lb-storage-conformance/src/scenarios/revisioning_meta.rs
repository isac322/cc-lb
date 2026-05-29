//! Revisioning and metadata conformance scenarios.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use cc_lb_storage_api::{
    BackendKind, CURRENT_CONTRACT_VERSION, ConfigDraftState, ConfigStore, HistoryEntry,
    HistorySummary, MetaStore, StorageError, StorageResult,
};

use crate::harness::{ConformanceBackend, ConformanceFixture, scenario_applies_to_backend};

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
    config_get_history_by_revision(Arc::clone(&backend)).await?;
    config_last_validated_revision(Arc::clone(&backend)).await?;
    meta_contract_version(Arc::clone(&backend)).await?;
    meta_backend_kind_stamp(Arc::clone(&backend)).await?;
    meta_backend_kind_mismatch(Arc::clone(&backend)).await?;
    meta_killswitch_persistence(Arc::clone(&backend)).await?;

    if scenario_applies_to_backend(backend.kind(), &[BackendKind::Redb]) {
        meta_legacy_redb_autostamp(backend).await?;
    }

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
                format!("revision = {revision}"),
                1_800_000_000 + revision,
                history_summary(revision),
            )
            .await?;
        }

        let entries = ConfigStore::list_config_history(storage.as_ref(), 100).await?;
        let revisions = revisions(&entries);
        let expected = (11..=60).rev().collect::<Vec<_>>();
        assert_eq!(entries.len(), 50);
        assert_eq!(revisions, expected);
        assert!(
            ConfigStore::get_config_history(storage.as_ref(), 10)
                .await?
                .is_none()
        );
        assert_eq!(
            ConfigStore::get_config_history(storage.as_ref(), 11)
                .await?
                .map(|entry| entry.revision),
            Some(11)
        );

        Ok(())
    }
    .await;
    let teardown = fixture.teardown().await;
    result?;
    teardown
}

pub async fn config_get_history_by_revision<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let mut fixture = ConformanceFixture::new(backend).await?;
    let result: Result<()> = async {
        let storage = fixture.storage();
        let summary = history_summary(7);

        ConfigStore::append_config_history(
            storage.as_ref(),
            7,
            "[legacy-upstreams.primary]".to_owned(),
            1_800_000_007,
            summary.clone(),
        )
        .await?;
        ConfigStore::append_config_history(
            storage.as_ref(),
            3,
            "[legacy-principals.local]".to_owned(),
            1_800_000_003,
            history_summary(3),
        )
        .await?;

        let entry = ConfigStore::get_config_history(storage.as_ref(), 7)
            .await?
            .expect("revision 7 should be present");
        assert_eq!(entry.revision, 7);
        assert_eq!(entry.config_toml, "[legacy-upstreams.primary]");
        assert_eq!(entry.applied_at_unix_secs, 1_800_000_007);
        assert_eq!(entry.summary, summary);
        assert!(
            ConfigStore::get_config_history(storage.as_ref(), 4)
                .await?
                .is_none()
        );

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
        .initialize_with_stamped_backend_kind(BackendKind::Postgres, BackendKind::Redb)
        .await
        .expect_err("backend kind mismatch must be rejected");
    assert!(matches!(
        mismatch,
        StorageError::BackendKindMismatch {
            stored: BackendKind::Postgres,
            configured: BackendKind::Redb
        }
    ));

    Ok(())
}

pub async fn meta_killswitch_persistence<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
{
    let fixture = backend.create_fixture().await?;

    let storage = backend.open(&fixture).await?;
    assert!(!MetaStore::killswitch_enabled(&storage).await?);
    MetaStore::set_killswitch_enabled(&storage, true).await?;
    drop(storage);

    let reopened = backend.open(&fixture).await?;
    assert!(MetaStore::killswitch_enabled(&reopened).await?);
    MetaStore::set_killswitch_enabled(&reopened, false).await?;
    drop(reopened);

    let reopened = backend.open(&fixture).await?;
    assert!(!MetaStore::killswitch_enabled(&reopened).await?);
    drop(reopened);

    backend.teardown(fixture).await
}

pub async fn meta_legacy_redb_autostamp<B>(backend: Arc<B>) -> Result<()>
where
    B: RevisioningMetaBackend,
{
    if !scenario_applies_to_backend(backend.kind(), &[BackendKind::Redb]) {
        return Ok(());
    }

    let fixture = backend.create_legacy_without_backend_kind().await?;
    assert_eq!(backend.stored_backend_kind(&fixture).await?, None);

    let storage = backend.open(&fixture).await?;
    assert_eq!(MetaStore::backend_kind(&storage).await?, BackendKind::Redb);
    drop(storage);
    assert_eq!(
        backend.stored_backend_kind(&fixture).await?,
        Some(BackendKind::Redb)
    );

    let reopened = backend.open(&fixture).await?;
    MetaStore::initialize(&reopened, BackendKind::Redb).await?;
    assert_eq!(MetaStore::backend_kind(&reopened).await?, BackendKind::Redb);
    drop(reopened);
    assert_eq!(
        backend.stored_backend_kind(&fixture).await?,
        Some(BackendKind::Redb)
    );

    backend.teardown(fixture).await
}

fn history_summary(seed: u64) -> HistorySummary {
    HistorySummary {
        upstreams: seed as usize,
        principals: (seed + 1) as usize,
        plugin_count: (seed + 2) as usize,
        tls_enabled: seed.is_multiple_of(2),
    }
}

fn revisions(entries: &[HistoryEntry]) -> Vec<u64> {
    entries.iter().map(|entry| entry.revision).collect()
}
