use std::sync::Arc;

pub async fn templated_sqlite_storage(
    path: &std::path::Path,
    clock: cc_lb_clock::ClockHandle,
) -> cc_lb_storage_api::StorageResult<cc_lb_storage_sqlite::SqliteStorage> {
    static TEMPLATE_BYTES: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
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
                use cc_lb_storage_api::MetaStore as _;
                let storage =
                    cc_lb_storage_sqlite::open_sqlite(&url, Arc::new(cc_lb_clock::SystemClock))
                        .await
                        .expect("open template sqlite");
                storage
                    .initialize()
                    .await
                    .expect("initialize template sqlite");
                storage.pool().close().await;
                use sqlx::Connection as _;
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
    std::fs::write(path, bytes).map_err(|error| cc_lb_storage_api::StorageError::Fatal {
        message: format!("write templated sqlite {}: {error}", path.display()),
    })?;
    let url = format!("sqlite://{}", path.display());
    cc_lb_storage_sqlite::open_sqlite_existing(&url, clock).await
}
