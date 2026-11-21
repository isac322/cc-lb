#![allow(dead_code)]

use std::path::Path;
use std::sync::{Arc, OnceLock};

use cc_lb_clock::{ClockHandle, SystemClock};
use cc_lb_storage_api::{MetaStore as _, StorageError, StorageResult};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite, open_sqlite_existing};
use sqlx::Connection as _;

/// Clones a blank initialized SQLite template into `path` and opens it with `clock`.
pub async fn templated_sqlite_storage(
    path: &Path,
    clock: ClockHandle,
) -> StorageResult<SqliteStorage> {
    static TEMPLATE_BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = TEMPLATE_BYTES.get_or_init(|| {
        if let Some(shared) = std::env::var_os("CC_LB_SQLITE_TEMPLATE_PATH")
            && !shared.is_empty()
            && let Ok(cached) = std::fs::read(&shared)
        {
            return cached;
        }
        let generated = std::thread::spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build template sqlite runtime");
            rt.block_on(async {
                let dir = tempfile::tempdir().expect("template sqlite dir");
                let template_path = dir.path().join("template.sqlite");
                let url = format!("sqlite://{}", template_path.display());
                let storage = open_sqlite(&url, Arc::new(SystemClock))
                    .await
                    .expect("open template sqlite");
                storage
                    .initialize()
                    .await
                    .expect("initialize template sqlite");
                storage.pool().close().await;
                let mut conn = sqlx::SqliteConnection::connect(&url)
                    .await
                    .expect("connect template sqlite for checkpoint");
                sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                    .execute(&mut conn)
                    .await
                    .expect("checkpoint template sqlite");
                sqlx::query("PRAGMA journal_mode = DELETE")
                    .execute(&mut conn)
                    .await
                    .expect("switch template sqlite journal mode");
                conn.close().await.expect("close template sqlite");
                std::fs::read(&template_path).expect("read template sqlite bytes")
            })
        })
        .join()
        .expect("template sqlite thread");
        if let Some(shared) = std::env::var_os("CC_LB_SQLITE_TEMPLATE_PATH")
            && !shared.is_empty()
        {
            let shared_path = std::path::PathBuf::from(shared);
            if let Some(parent) = shared_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let tmp_path =
                shared_path.with_file_name(format!("template.sqlite.tmp.{}", std::process::id()));
            if std::fs::write(&tmp_path, &generated).is_ok() {
                let _ = std::fs::rename(&tmp_path, &shared_path);
                let _ = std::fs::remove_file(&tmp_path);
            }
        }
        generated
    });
    std::fs::write(path, bytes).map_err(|error| StorageError::Fatal {
        message: format!("write templated sqlite {}: {error}", path.display()),
    })?;
    let url = format!("sqlite://{}", path.display());
    open_sqlite_existing(&url, clock).await
}
