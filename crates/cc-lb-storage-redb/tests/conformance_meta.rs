use std::{path::Path, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use cc_lb_storage_api::{BackendKind, MetaStore, StorageError, StorageResult};
use cc_lb_storage_conformance::{
    ConformanceBackend,
    scenarios::revisioning_meta::{self, RevisioningMetaBackend},
};
use cc_lb_storage_redb::{
    CURRENT_SCHEMA_VERSION, META_BACKEND_KIND_V1, RedbStorage, SCHEMA_VERSION_V1,
};
use redb::ReadableDatabase;

struct RedbConformanceBackend;

struct RedbFixture {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

#[async_trait]
impl ConformanceBackend for RedbConformanceBackend {
    type Storage = RedbStorage;
    type Fixture = RedbFixture;

    async fn create_fixture(&self) -> anyhow::Result<Self::Fixture> {
        create_fixture(None, false)
    }

    async fn open(&self, fixture: &Self::Fixture) -> anyhow::Result<Self::Storage> {
        Ok(RedbStorage::open(&fixture.path)?)
    }

    async fn teardown(&self, _fixture: Self::Fixture) -> anyhow::Result<()> {
        Ok(())
    }

    fn kind(&self) -> BackendKind {
        BackendKind::Redb
    }
}

#[async_trait]
impl RevisioningMetaBackend for RedbConformanceBackend {
    async fn initialize_with_stamped_backend_kind(
        &self,
        stored: BackendKind,
        configured: BackendKind,
    ) -> StorageResult<()> {
        let fixture = create_fixture(Some(stored), true).map_err(api_fatal)?;

        match RedbStorage::open(&fixture.path) {
            Ok(storage) => MetaStore::initialize(&storage, configured).await,
            Err(cc_lb_storage_redb::StorageError::BackendKindMismatch { stored, configured }) => {
                Err(StorageError::BackendKindMismatch { stored, configured })
            }
            Err(error) => Err(api_fatal(error)),
        }
    }

    async fn create_legacy_without_backend_kind(&self) -> anyhow::Result<Self::Fixture> {
        create_fixture(None, true)
    }

    async fn stored_backend_kind(
        &self,
        fixture: &Self::Fixture,
    ) -> anyhow::Result<Option<BackendKind>> {
        read_backend_kind(&fixture.path)
    }
}

#[tokio::test]
async fn conformance_meta_run_all() -> anyhow::Result<()> {
    revisioning_meta::run_all(backend()).await
}

#[tokio::test]
async fn config_draft_optimistic_revision() -> anyhow::Result<()> {
    revisioning_meta::config_draft_optimistic_revision(backend()).await
}

#[tokio::test]
async fn config_history_cap_50() -> anyhow::Result<()> {
    revisioning_meta::config_history_cap_50(backend()).await
}

#[tokio::test]
async fn config_get_history_by_revision() -> anyhow::Result<()> {
    revisioning_meta::config_get_history_by_revision(backend()).await
}

#[tokio::test]
async fn config_last_validated_revision() -> anyhow::Result<()> {
    revisioning_meta::config_last_validated_revision(backend()).await
}

#[tokio::test]
async fn meta_contract_version() -> anyhow::Result<()> {
    revisioning_meta::meta_contract_version(backend()).await
}

#[tokio::test]
async fn meta_backend_kind_stamp() -> anyhow::Result<()> {
    revisioning_meta::meta_backend_kind_stamp(backend()).await
}

#[tokio::test]
async fn meta_backend_kind_mismatch() -> anyhow::Result<()> {
    revisioning_meta::meta_backend_kind_mismatch(backend()).await
}

#[tokio::test]
async fn meta_killswitch_persistence() -> anyhow::Result<()> {
    revisioning_meta::meta_killswitch_persistence(backend()).await
}

#[tokio::test]
async fn meta_legacy_redb_autostamp() -> anyhow::Result<()> {
    revisioning_meta::meta_legacy_redb_autostamp(backend()).await
}

fn backend() -> Arc<RedbConformanceBackend> {
    Arc::new(RedbConformanceBackend)
}

fn create_fixture(
    backend_kind: Option<BackendKind>,
    create_backend_table: bool,
) -> anyhow::Result<RedbFixture> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("conformance-meta.redb");

    if backend_kind.is_some() || create_backend_table {
        seed_redb_file(&path, backend_kind, create_backend_table)?;
    }

    Ok(RedbFixture { _dir: dir, path })
}

fn seed_redb_file(
    path: &Path,
    backend_kind: Option<BackendKind>,
    create_backend_table: bool,
) -> anyhow::Result<()> {
    let db = redb::Database::create(path)?;
    let write_txn = db.begin_write()?;
    {
        let mut schema = write_txn.open_table(SCHEMA_VERSION_V1)?;
        schema.insert("version", &CURRENT_SCHEMA_VERSION)?;
    }
    if create_backend_table || backend_kind.is_some() {
        let mut backend = write_txn.open_table(META_BACKEND_KIND_V1)?;
        if let Some(kind) = backend_kind {
            backend.insert("backend_kind", kind.as_str())?;
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn read_backend_kind(path: &Path) -> anyhow::Result<Option<BackendKind>> {
    let db = redb::Database::create(path)?;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(META_BACKEND_KIND_V1)?;
    let stored = table
        .get("backend_kind")?
        .map(|stored| stored.value().to_owned());

    match stored.as_deref() {
        Some("redb") => Ok(Some(BackendKind::Redb)),
        Some("postgres") => Ok(Some(BackendKind::Postgres)),
        Some(other) => Err(anyhow::anyhow!("unexpected backend kind {other}")),
        None => Ok(None),
    }
}

fn api_fatal(error: impl std::fmt::Display) -> StorageError {
    StorageError::Fatal {
        message: format!("redb conformance fixture failed: {error}"),
    }
}
