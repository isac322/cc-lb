//! Storage backend bootstrap helpers used by `BddCtx`.
//!
//! - SQLite: tempfile-per-scenario, dropped on teardown via `TempDir`.
//! - Postgres: schema-per-scenario, owned pool, dropped on teardown via
//!   `DROP SCHEMA ... CASCADE`. Postgres path is gated behind the
//!   `postgres` cargo feature and the `CI_POSTGRES_URL` env var.

use std::sync::Arc;

use anyhow::Result;
use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};

/// Anything that can act as the BDD harness storage. A trait object
/// allows the macro to emit one closure body that drives either sqlite
/// or postgres without per-backend monomorphisation.
pub type StorageHandle = Arc<SqliteStorage>;

/// Opaque ownership of the on-disk fixture for a single scenario. Held
/// by `BddCtx` so the underlying tempdir / schema lives for the entire
/// scenario and is dropped on scope exit.
pub struct StorageFixture {
    #[allow(dead_code)]
    inner: FixtureInner,
}

enum FixtureInner {
    Sqlite { _dir: tempfile::TempDir },
}

impl StorageFixture {
    pub(crate) fn new_sqlite(dir: tempfile::TempDir) -> Self {
        Self {
            inner: FixtureInner::Sqlite { _dir: dir },
        }
    }
}

/// Build a fresh, isolated SQLite-backed storage for a single scenario.
/// The returned `StorageFixture` owns the tempdir and must outlive the
/// returned storage handle.
pub async fn bootstrap_sqlite() -> Result<(StorageHandle, StorageFixture)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("bdd_scenario.sqlite");
    let url = format!("sqlite://{}", path.display());
    let storage = open_sqlite(&url).await?;
    storage.initialize(BackendKind::Sqlite).await?;
    Ok((Arc::new(storage), StorageFixture::new_sqlite(dir)))
}
