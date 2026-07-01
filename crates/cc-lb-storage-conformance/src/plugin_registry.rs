use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use cc_lb_storage_api::{
    AugmentedMetadata, PluginBlobRepo, PluginRegistryRecord, PluginRegistryRepo,
    PluginRegistryStatus,
};
use serde_json::json;

const BASE_TS: i64 = 1_900_000_000;
const CC_LB_PLUGIN_MAGIC: [u8; 8] = [0xCC, 0x1B, 0x70, 0x10, 0x00, 0x01, 0x00, 0x00];

pub async fn plugin_registry_roundtrip(
    registry: &impl PluginRegistryRepo,
    blobs: &impl PluginBlobRepo,
) -> Result<()> {
    roundtrip_basic(registry, blobs).await?;
    idempotent_upsert(registry, blobs).await?;
    list_active_excludes_disabled(registry, blobs).await?;
    delete_then_get_returns_none(registry, blobs).await?;
    count_accurate(registry, blobs).await?;
    shutdown_marker_lifecycle(registry, blobs).await?;
    Ok(())
}

pub async fn roundtrip_basic(
    registry: &impl PluginRegistryRepo,
    _blobs: &impl PluginBlobRepo,
) -> Result<()> {
    let record = record(11, "roundtrip-basic", PluginRegistryStatus::Active)?;
    registry.upsert_record(&record).await?;

    let fetched = registry.get_by_sha256(&record.sha256).await?;
    ensure!(
        fetched == Some(record.clone()),
        "record round-trips unchanged"
    );

    registry.delete_by_sha256(&record.sha256).await?;
    Ok(())
}

pub async fn idempotent_upsert(
    registry: &impl PluginRegistryRepo,
    _blobs: &impl PluginBlobRepo,
) -> Result<()> {
    let first = record(21, "idempotent-first", PluginRegistryStatus::Active)?;
    let mut second = record(22, "idempotent-second", PluginRegistryStatus::Disabled)?;
    second.sha256 = first.sha256;

    registry.upsert_record(&first).await?;
    registry.upsert_record(&second).await?;

    let fetched = registry.get_by_sha256(&first.sha256).await?;
    ensure!(
        fetched == Some(second.clone()),
        "second upsert wins by sha256"
    );

    registry.delete_by_sha256(&first.sha256).await?;
    Ok(())
}

pub async fn list_active_excludes_disabled(
    registry: &impl PluginRegistryRepo,
    _blobs: &impl PluginBlobRepo,
) -> Result<()> {
    let active_a = record(31, "active-a", PluginRegistryStatus::Active)?;
    let active_b = record(32, "active-b", PluginRegistryStatus::Active)?;
    let disabled = record(33, "disabled-c", PluginRegistryStatus::Active)?;

    registry.upsert_record(&active_a).await?;
    registry.upsert_record(&active_b).await?;
    registry.upsert_record(&disabled).await?;
    registry
        .set_status(&disabled.sha256, PluginRegistryStatus::Disabled)
        .await?;

    let active = registry.list_active().await?;
    ensure!(active.len() == 2, "list_active returns two active records");
    let active_shas = shas(&active);
    ensure!(
        active_shas.contains(&active_a.sha256),
        "first active listed"
    );
    ensure!(
        active_shas.contains(&active_b.sha256),
        "second active listed"
    );
    ensure!(
        !active_shas.contains(&disabled.sha256),
        "disabled record excluded"
    );
    ensure!(
        active_shas
            .iter()
            .filter(|sha| [active_a.sha256, active_b.sha256, disabled.sha256].contains(sha))
            .count()
            == 2,
        "two records from this scenario are active"
    );

    delete_records(
        registry,
        &[active_a.sha256, active_b.sha256, disabled.sha256],
    )
    .await
}

pub async fn delete_then_get_returns_none(
    registry: &impl PluginRegistryRepo,
    _blobs: &impl PluginBlobRepo,
) -> Result<()> {
    let record = record(41, "delete-roundtrip", PluginRegistryStatus::Active)?;
    registry.upsert_record(&record).await?;
    registry.delete_by_sha256(&record.sha256).await?;

    ensure!(
        registry.get_by_sha256(&record.sha256).await?.is_none(),
        "deleted record is absent"
    );
    Ok(())
}

pub async fn count_accurate(
    registry: &impl PluginRegistryRepo,
    _blobs: &impl PluginBlobRepo,
) -> Result<()> {
    let before = registry.count().await?;
    let records = [
        record(51, "count-a", PluginRegistryStatus::Active)?,
        record(52, "count-b", PluginRegistryStatus::Disabled)?,
        record(53, "count-c", PluginRegistryStatus::Active)?,
    ];

    for record in &records {
        registry.upsert_record(record).await?;
    }
    ensure!(
        registry.count().await? == before + 3,
        "count includes inserts"
    );

    registry.delete_by_sha256(&records[0].sha256).await?;
    ensure!(
        registry.count().await? == before + 2,
        "count reflects delete"
    );

    delete_records(registry, &[records[1].sha256, records[2].sha256]).await
}

pub async fn shutdown_marker_lifecycle(
    registry: &impl PluginRegistryRepo,
    _blobs: &impl PluginBlobRepo,
) -> Result<()> {
    registry.clear_shutdown_marker().await?;
    ensure!(
        registry.get_shutdown_marker().await?.is_none(),
        "shutdown marker starts empty"
    );

    registry.set_shutdown_marker(100).await?;
    ensure!(
        registry.get_shutdown_marker().await? == Some(100),
        "shutdown marker persists"
    );

    registry.clear_shutdown_marker().await?;
    ensure!(
        registry.get_shutdown_marker().await?.is_none(),
        "shutdown marker clears"
    );
    Ok(())
}

fn record(
    seed: u8,
    plugin_name: &str,
    status: PluginRegistryStatus,
) -> Result<PluginRegistryRecord> {
    let plugin_version = "1.0.0";
    let abi_envelope = 1;
    let metadata = metadata_from_parts(
        plugin_name,
        plugin_version,
        abi_envelope,
        functions(&[("route", 1)]),
        capabilities(&["core"]),
        BASE_TS + seed as i64,
        true,
        BASE_TS + seed as i64 + 1,
        BASE_TS + seed as i64 + 600,
    )?;
    Ok(record_with_metadata(
        seed,
        plugin_name,
        plugin_version,
        abi_envelope,
        metadata,
        status,
    ))
}

fn record_with_metadata(
    seed: u8,
    plugin_name: &str,
    plugin_version: &str,
    abi_envelope: u32,
    augmented_metadata: AugmentedMetadata,
    status: PluginRegistryStatus,
) -> PluginRegistryRecord {
    PluginRegistryRecord {
        sha256: [seed; 32],
        plugin_name: plugin_name.to_owned(),
        plugin_version: plugin_version.to_owned(),
        abi_envelope,
        augmented_metadata,
        host_offer_hash: [seed.wrapping_add(1); 32],
        handshake_schema_version: 1,
        last_handshake_at: BASE_TS + seed as i64,
        status,
    }
}

#[allow(clippy::too_many_arguments)]
fn metadata_from_parts(
    plugin_name: &str,
    plugin_version: &str,
    abi_envelope: u32,
    negotiated_functions: BTreeMap<String, u32>,
    negotiated_capabilities: BTreeSet<String>,
    handshake_completed_at: i64,
    self_check_passed: bool,
    self_check_completed_at: i64,
    expires_at: i64,
) -> Result<AugmentedMetadata> {
    serde_json::from_value(json!({
        "identity": {
            "magic": CC_LB_PLUGIN_MAGIC,
            "abi_envelope": abi_envelope,
            "plugin_name": plugin_name,
            "plugin_version": plugin_version,
        },
        "negotiated_functions": negotiated_functions,
        "negotiated_capabilities": negotiated_capabilities,
        "handshake_completed_at": handshake_completed_at,
        "self_check_passed": self_check_passed,
        "self_check_completed_at": self_check_completed_at,
        "expires_at": expires_at,
    }))
    .context("build augmented metadata")
}

fn functions(entries: &[(&str, u32)]) -> BTreeMap<String, u32> {
    entries
        .iter()
        .map(|(name, version)| ((*name).to_owned(), *version))
        .collect()
}

fn capabilities(entries: &[&str]) -> BTreeSet<String> {
    entries
        .iter()
        .map(|capability| (*capability).to_owned())
        .collect()
}

fn shas(records: &[PluginRegistryRecord]) -> Vec<[u8; 32]> {
    records.iter().map(|record| record.sha256).collect()
}

async fn delete_records(registry: &impl PluginRegistryRepo, shas: &[[u8; 32]]) -> Result<()> {
    for sha256 in shas {
        registry.delete_by_sha256(sha256).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use cc_lb_storage_api::RepoError;

    use super::*;

    #[derive(Default)]
    struct MemoryPluginRepos {
        records: Mutex<BTreeMap<[u8; 32], PluginRegistryRecord>>,
        blobs: Mutex<BTreeMap<[u8; 32], Vec<u8>>>,
        shutdown_marker: Mutex<Option<i64>>,
    }

    #[async_trait]
    impl PluginRegistryRepo for MemoryPluginRepos {
        async fn upsert_record(&self, record: &PluginRegistryRecord) -> Result<(), RepoError> {
            self.records
                .lock()
                .unwrap()
                .insert(record.sha256, record.clone());
            Ok(())
        }

        async fn get_by_sha256(
            &self,
            sha256: &[u8; 32],
        ) -> Result<Option<PluginRegistryRecord>, RepoError> {
            Ok(self.records.lock().unwrap().get(sha256).cloned())
        }

        async fn list_active(&self) -> Result<Vec<PluginRegistryRecord>, RepoError> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .values()
                .filter(|record| record.status == PluginRegistryStatus::Active)
                .cloned()
                .collect())
        }

        async fn set_status(
            &self,
            sha256: &[u8; 32],
            status: PluginRegistryStatus,
        ) -> Result<(), RepoError> {
            if let Some(record) = self.records.lock().unwrap().get_mut(sha256) {
                record.status = status;
            }
            Ok(())
        }

        async fn delete_by_sha256(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
            self.records.lock().unwrap().remove(sha256);
            Ok(())
        }

        async fn count(&self) -> Result<usize, RepoError> {
            Ok(self.records.lock().unwrap().len())
        }

        async fn get_shutdown_marker(&self) -> Result<Option<i64>, RepoError> {
            Ok(*self.shutdown_marker.lock().unwrap())
        }

        async fn set_shutdown_marker(&self, unix_secs: i64) -> Result<(), RepoError> {
            *self.shutdown_marker.lock().unwrap() = Some(unix_secs);
            Ok(())
        }

        async fn clear_shutdown_marker(&self) -> Result<(), RepoError> {
            *self.shutdown_marker.lock().unwrap() = None;
            Ok(())
        }
    }

    #[async_trait]
    impl PluginBlobRepo for MemoryPluginRepos {
        async fn put_blob(&self, sha256: &[u8; 32], bytes: &[u8]) -> Result<(), RepoError> {
            self.blobs.lock().unwrap().insert(*sha256, bytes.to_vec());
            Ok(())
        }

        async fn get_blob(&self, sha256: &[u8; 32]) -> Result<Option<Vec<u8>>, RepoError> {
            Ok(self.blobs.lock().unwrap().get(sha256).cloned())
        }

        async fn delete_blob(&self, sha256: &[u8; 32]) -> Result<(), RepoError> {
            self.blobs.lock().unwrap().remove(sha256);
            Ok(())
        }

        async fn list_blob_keys(&self) -> Result<Vec<[u8; 32]>, RepoError> {
            Ok(self.blobs.lock().unwrap().keys().copied().collect())
        }
    }

    #[tokio::test]
    async fn plugin_registry_roundtrip_runs_against_in_memory_repo() -> Result<()> {
        let repo = MemoryPluginRepos::default();
        plugin_registry_roundtrip(&repo, &repo).await
    }
}
