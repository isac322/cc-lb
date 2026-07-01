use std::{error::Error, future::Future, path::PathBuf, sync::Arc};

use cc_lb_storage_api::{BackendKind, ConfigStore, EffectiveConfig, MetaStore};
use cc_lb_storage_sqlite::{SqliteStorage, open_sqlite};
use serde_json::json;
use tokio::runtime::Builder;
use uuid::Uuid;

#[test]
fn get_effective_config_returns_none_when_unset() {
    run_test(|storage| async move {
        assert_eq!(storage.get_effective_config().await?, None);
        Ok(())
    });
}

#[test]
fn put_then_get_round_trips() {
    run_test(|storage| async move {
        let config = json!({
            "principals": [{"id": "local", "kind": "machine"}],
            "upstreams": {"primary": {"kind": "anthropic_direct"}}
        });

        storage
            .put_effective_config(7, config.clone(), 1_800_400_010)
            .await?;

        assert_eq!(
            storage.get_effective_config().await?,
            Some(EffectiveConfig {
                revision: 7,
                config,
                applied_at_unix_secs: 1_800_400_010,
            })
        );
        Ok(())
    });
}

#[test]
fn put_updates_existing_row() {
    run_test(|storage| async move {
        storage
            .put_effective_config(7, json!({"version": "old"}), 1_800_400_010)
            .await?;

        let config = json!({"version": "new", "flags": ["db-config"]});
        storage
            .put_effective_config(8, config.clone(), 1_800_400_020)
            .await?;

        assert_eq!(
            storage.get_effective_config().await?,
            Some(EffectiveConfig {
                revision: 8,
                config,
                applied_at_unix_secs: 1_800_400_020,
            })
        );
        Ok(())
    });
}

fn run_test<F, Fut>(test: F)
where
    F: FnOnce(SqliteStorage) -> Fut,
    Fut: Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
{
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move {
            let path = database_path();
            let database_url = format!("sqlite://{}", path.display());
            let storage = open_sqlite(&database_url, Arc::new(cc_lb_clock::SystemClock)).await?;
            storage.initialize(BackendKind::Sqlite).await?;
            let result = test(storage).await;
            cleanup_database(&path);
            result?;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        })
        .expect("effective config sqlite test");
}

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "cc_lb_effective_config_{}.sqlite",
        Uuid::new_v4().simple()
    ))
}

fn cleanup_database(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
}
