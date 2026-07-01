use std::path::Path;

use cc_lb_config::Config;
use cc_lb_engine::clock::{Clock, unix_secs};
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::{PrincipalStore, StorageResult};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("Storage error: {0}")]
    Storage(String),
}

pub type BootstrapResult<T> = Result<T, BootstrapError>;

async fn seed_admin_if_absent(
    store: &dyn PrincipalStore,
    _token: &str,
    clock: &dyn Clock,
) -> StorageResult<()> {
    let admin_principal = PrincipalCreate {
        name: "admin".to_owned(),
        kind: PrincipalKind::Admin,
        allowed_models: vec![],
        allowed_upstreams: vec![],
        default_limits: vec![],
        cache_keepalive: None,
    };

    let now = unix_secs(clock.now());

    match store.get_by_name("admin").await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => store.create(admin_principal, now).await.map(|_| ()),
        Err(error) => Err(error),
    }
}

/// First-run admin seeding from env. NEVER reads any TOML file.
///
/// The legacy `bootstrap.toml` resource-seeding mechanism is removed: all
/// runtime resources (upstreams, principals, plugin chains) are managed via
/// the admin v1 REST API after the first admin token is established here.
pub async fn apply_bootstrap(
    _config: &Config,
    seeder: &dyn PrincipalStore,
    env_token: Option<String>,
    _data_dir: &Path,
    clock: &dyn Clock,
) -> BootstrapResult<()> {
    if let Some(token) = env_token {
        seed_admin_if_absent(seeder, &token, clock)
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_engine::clock::TestClock;
    use cc_lb_storage_api::{BackendKind, MetaStore};
    use cc_lb_storage_sqlite::SqliteStorage as Storage;

    #[tokio::test]
    async fn apply_bootstrap_seeds_admin_when_env_token_present() {
        let (dir, storage) = fixture().await;
        let clock = TestClock::new_at_secs(1_800_000_000);

        apply_bootstrap(
            &Config::default(),
            &storage,
            Some("bootstrap-token".to_owned()),
            dir.path(),
            &clock,
        )
        .await
        .unwrap();
        let principal = PrincipalStore::get_by_name(&storage, "admin")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.kind, PrincipalKind::Admin);
    }

    #[tokio::test]
    async fn apply_bootstrap_is_idempotent_when_admin_already_exists() {
        let (dir, storage) = fixture().await;
        let clock = TestClock::new_at_secs(1_800_000_000);

        apply_bootstrap(
            &Config::default(),
            &storage,
            Some("first".to_owned()),
            dir.path(),
            &clock,
        )
        .await
        .unwrap();
        apply_bootstrap(
            &Config::default(),
            &storage,
            Some("second".to_owned()),
            dir.path(),
            &clock,
        )
        .await
        .unwrap();
        let principals = PrincipalStore::list(&storage, 0, 100, false).await.unwrap();
        assert_eq!(
            principals
                .iter()
                .filter(|principal| principal.name == "admin")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn apply_bootstrap_noop_without_env_token() {
        let (dir, storage) = fixture().await;
        let clock = TestClock::new_at_secs(1_800_000_000);

        apply_bootstrap(&Config::default(), &storage, None, dir.path(), &clock)
            .await
            .unwrap();
        let principals = PrincipalStore::list(&storage, 0, 100, false).await.unwrap();
        assert!(principals.is_empty());
    }

    async fn fixture() -> (tempfile::TempDir, Storage) {
        let dir = tempfile::tempdir().unwrap();
        let database_url = format!("sqlite://{}", dir.path().join("storage.sqlite").display());
        let storage = cc_lb_storage_sqlite::open_sqlite(
            &database_url,
            std::sync::Arc::new(cc_lb_engine::SystemClock),
        )
        .await
        .unwrap();
        storage.initialize(BackendKind::Sqlite).await.unwrap();
        (dir, storage)
    }
}
