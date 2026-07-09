use std::io::Write;
use std::path::PathBuf;

use cc_lb_storage_api::{BackendKind, MetaStore};
use cc_lb_storage_sqlite::{
    SubscriptionQuotaCheckpointBackfillError, SubscriptionQuotaCheckpointBackfillReport,
    SubscriptionQuotaCheckpointCleanupError, SubscriptionQuotaCheckpointCleanupReport,
};
use thiserror::Error;

use crate::local_storage_path::storage_path_from_env;

#[derive(Debug, Error)]
pub enum SubscriptionQuotaBackfillCliError {
    #[error(
        "SQLite storage path does not exist for subscription quota checkpoint backfill: {path}; refusing to create a new database"
    )]
    MissingSqliteStoragePath { path: PathBuf },
    #[error(
        "failed to check SQLite storage path for subscription quota checkpoint backfill at {path}: {source}"
    )]
    SqliteStoragePathCheck {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("--storage-path is required when --drop-raw-observations is set")]
    MissingExplicitCleanupStoragePath,
    #[error("failed to open SQLite storage: {0}")]
    StorageOpen(#[source] cc_lb_storage_api::StorageError),
    #[error("failed to initialize SQLite storage: {0}")]
    StorageInit(#[source] cc_lb_storage_api::StorageError),
    #[error(transparent)]
    Backfill(#[from] SubscriptionQuotaCheckpointBackfillError),
    #[error(transparent)]
    Cleanup(#[from] SubscriptionQuotaCheckpointCleanupError),
    #[error("failed to write JSON report: {0}")]
    WriteJson(#[source] serde_json::Error),
    #[error("failed to write stdout: {0}")]
    Stdout(#[source] std::io::Error),
}

pub async fn run(
    clock: cc_lb_engine::ClockHandle,
    storage_path: Option<PathBuf>,
    drop_raw_observations: bool,
) -> Result<(), SubscriptionQuotaBackfillCliError> {
    let path = match (drop_raw_observations, storage_path) {
        (true, Some(path)) => require_existing_sqlite_storage_path(path)?,
        (true, None) => {
            return Err(SubscriptionQuotaBackfillCliError::MissingExplicitCleanupStoragePath);
        }
        (false, Some(path)) => require_existing_sqlite_storage_path(path)?,
        (false, None) => require_existing_sqlite_storage_path(storage_path_from_env())?,
    };
    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .map_err(SubscriptionQuotaBackfillCliError::StorageOpen)?;
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .map_err(SubscriptionQuotaBackfillCliError::StorageInit)?;
    if drop_raw_observations {
        let report = storage
            .cleanup_compacted_subscription_quota_history(&path)
            .await?;
        return print_cleanup_report(&report);
    }
    let report = storage.compact_subscription_quota_history().await?;
    print_report(&report)
}

fn require_existing_sqlite_storage_path(
    path: PathBuf,
) -> Result<PathBuf, SubscriptionQuotaBackfillCliError> {
    match path.try_exists() {
        Ok(true) => Ok(path),
        Ok(false) => Err(SubscriptionQuotaBackfillCliError::MissingSqliteStoragePath { path }),
        Err(source) => {
            Err(SubscriptionQuotaBackfillCliError::SqliteStoragePathCheck { path, source })
        }
    }
}

fn print_report(
    report: &SubscriptionQuotaCheckpointBackfillReport,
) -> Result<(), SubscriptionQuotaBackfillCliError> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, report)
        .map_err(SubscriptionQuotaBackfillCliError::WriteJson)?;
    writeln!(stdout).map_err(SubscriptionQuotaBackfillCliError::Stdout)
}

fn print_cleanup_report(
    report: &SubscriptionQuotaCheckpointCleanupReport,
) -> Result<(), SubscriptionQuotaBackfillCliError> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, report)
        .map_err(SubscriptionQuotaBackfillCliError::WriteJson)?;
    writeln!(stdout).map_err(SubscriptionQuotaBackfillCliError::Stdout)
}
