use std::sync::Arc;

use anyhow::{Result, ensure};
use cc_lb_storage_api::{ConfigStore as _, EffectiveConfig};
use serde_json::json;

use crate::harness::{ConformanceBackend, with_conformance_fixture};

pub async fn run_all<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    get_effective_config_returns_none_when_unset(Arc::clone(&backend)).await?;
    put_then_get_round_trips(Arc::clone(&backend)).await?;
    put_updates_existing_row(backend).await
}

pub async fn get_effective_config_returns_none_when_unset<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        ensure!(
            storage.get_effective_config().await?.is_none(),
            "unset effective config should return none"
        );
        Ok(())
    })
    .await
}

pub async fn put_then_get_round_trips<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        let config = json!({
            "principals": [{"id": "local", "label": "Local"}],
            "upstreams": {"primary": {"kind": "anthropic_direct"}}
        });

        storage
            .put_effective_config(7, config.clone(), 1_800_400_010)
            .await?;

        ensure!(
            storage.get_effective_config().await?
                == Some(EffectiveConfig {
                    revision: 7,
                    config,
                    applied_at_unix_secs: 1_800_400_010,
                }),
            "effective config should round-trip"
        );
        Ok(())
    })
    .await
}

pub async fn put_updates_existing_row<B>(backend: Arc<B>) -> Result<()>
where
    B: ConformanceBackend,
    B::Storage: cc_lb_storage_api::Storage,
{
    with_conformance_fixture(backend, |storage| async move {
        storage
            .put_effective_config(7, json!({"version": "old"}), 1_800_400_010)
            .await?;

        let config = json!({"version": "new", "features": ["db-config"]});
        storage
            .put_effective_config(8, config.clone(), 1_800_400_020)
            .await?;

        ensure!(
            storage.get_effective_config().await?
                == Some(EffectiveConfig {
                    revision: 8,
                    config,
                    applied_at_unix_secs: 1_800_400_020,
                }),
            "effective config upsert should replace the singleton row"
        );
        Ok(())
    })
    .await
}
