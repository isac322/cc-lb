use std::io::Write;

use cc_lb_storage_api::{
    BackendKind, MetaStore, PluginRegistryStore, PluginSlotKind, PrincipalStore, Storage,
};
use serde::Serialize;
use thiserror::Error;

use crate::local_storage_path::storage_path_from_env;

#[derive(Debug, Error)]
pub enum DoctorError {
    #[error("storage not found: {path}")]
    StorageNotFound { path: String },
    #[error("failed to open storage: {0}")]
    StorageOpen(#[source] cc_lb_storage_api::StorageError),
    #[error("storage query failed: {0}")]
    StorageQuery(#[source] cc_lb_storage_api::StorageError),
    #[error("failed to write JSON report: {0}")]
    WriteJson(#[source] serde_json::Error),
    #[error("failed to write stdout: {0}")]
    Stdout(#[source] std::io::Error),
}

#[derive(Debug, Serialize)]
struct AbandonedChainEntriesReport {
    abandoned_chain_entries: Vec<AbandonedChainEntryReport>,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Serialize)]
struct AbandonedChainEntryReport {
    principal_id: String,
    wasm_registry_id: String,
    wasm_registry_name: String,
    slot: String,
    order: i64,
}

pub async fn run_list_abandoned_chain_entries(
    clock: cc_lb_engine::ClockHandle,
) -> Result<(), DoctorError> {
    let path = storage_path_from_env();
    if !path.exists() {
        return Err(DoctorError::StorageNotFound {
            path: path.display().to_string(),
        });
    }

    let database_url = format!("sqlite://{}", path.display());
    let storage = cc_lb_storage_sqlite::open_sqlite(&database_url, clock)
        .await
        .map_err(DoctorError::StorageOpen)?;
    storage
        .initialize(BackendKind::Sqlite)
        .await
        .map_err(DoctorError::StorageOpen)?;
    let report = build_abandoned_chain_entries_report(&storage).await?;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &report).map_err(DoctorError::WriteJson)?;
    writeln!(stdout).map_err(DoctorError::Stdout)
}

async fn build_abandoned_chain_entries_report(
    storage: &dyn Storage,
) -> Result<AbandonedChainEntriesReport, DoctorError> {
    let mut abandoned_chain_entries = Vec::new();
    let principals = PrincipalStore::list(storage, 0, usize::MAX, true)
        .await
        .map_err(DoctorError::StorageQuery)?;

    for principal in principals {
        for slot in [
            PluginSlotKind::Router,
            PluginSlotKind::ObservabilityHook,
            PluginSlotKind::Shape,
        ] {
            let chain_entries =
                PluginRegistryStore::list_chain_for_principal(storage, principal.id, slot)
                    .await
                    .map_err(DoctorError::StorageQuery)?;
            for chain_entry in chain_entries {
                let Some(registry_entry) = PluginRegistryStore::get_registry_entry_by_id(
                    storage,
                    chain_entry.wasm_registry_id,
                )
                .await
                .map_err(DoctorError::StorageQuery)?
                else {
                    continue;
                };
                if registry_entry.supported_slots.is_empty() && !registry_entry.is_builtin {
                    abandoned_chain_entries.push(AbandonedChainEntryReport {
                        principal_id: chain_entry.principal_id.to_string(),
                        wasm_registry_id: registry_entry.id.to_string(),
                        wasm_registry_name: registry_entry.name,
                        slot: chain_entry.slot.as_str().to_owned(),
                        order: chain_entry.order,
                    });
                }
            }
        }
    }

    abandoned_chain_entries.sort();
    Ok(AbandonedChainEntriesReport {
        abandoned_chain_entries,
    })
}

#[cfg(test)]
#[allow(non_snake_case)]
mod t2__tests {
    use super::*;
    use cc_lb_storage_api::{
        PluginChainEntryInput, WasmBlob, WasmRegistryEntry, WasmRegistryEntryInput,
        principal::{PrincipalCreate, PrincipalKind},
    };
    use cc_lb_testkit::{InMemoryStorage, fixed_clock};
    use serde_json::json;
    use uuid::Uuid;

    const NOW_UNIX_SECS: u64 = 1_800_000_000;
    const TEST_ADMIN_ID: Uuid = Uuid::from_u128(1);

    #[tokio::test]
    async fn report_lists_empty_supported_slots_in_sorted_slot_and_order() {
        let storage = InMemoryStorage::with_clock(fixed_clock(NOW_UNIX_SECS));
        let principal_id = create_principal(&storage, "doctor-abandoned-principal").await;
        let registry_entry =
            upload_plugin(&storage, "doctor-abandoned-plugin", [11; 32], Vec::new()).await;
        insert_chain(
            &storage,
            principal_id,
            registry_entry.id,
            PluginSlotKind::ObservabilityHook,
            500,
        )
        .await;
        insert_chain(
            &storage,
            principal_id,
            registry_entry.id,
            PluginSlotKind::Shape,
            700,
        )
        .await;
        insert_chain(
            &storage,
            principal_id,
            registry_entry.id,
            PluginSlotKind::Router,
            300,
        )
        .await;
        insert_chain(
            &storage,
            principal_id,
            registry_entry.id,
            PluginSlotKind::ObservabilityHook,
            100,
        )
        .await;

        let report = build_abandoned_chain_entries_report(&storage)
            .await
            .unwrap();

        assert_eq!(
            serde_json::to_string(&report).unwrap(),
            format!(
                concat!(
                    "{{\"abandoned_chain_entries\":[",
                    "{{\"principal_id\":\"{principal_id}\",",
                    "\"wasm_registry_id\":\"{registry_id}\",",
                    "\"wasm_registry_name\":\"doctor-abandoned-plugin\",",
                    "\"slot\":\"observability_hook\",\"order\":100}},",
                    "{{\"principal_id\":\"{principal_id}\",",
                    "\"wasm_registry_id\":\"{registry_id}\",",
                    "\"wasm_registry_name\":\"doctor-abandoned-plugin\",",
                    "\"slot\":\"observability_hook\",\"order\":500}},",
                    "{{\"principal_id\":\"{principal_id}\",",
                    "\"wasm_registry_id\":\"{registry_id}\",",
                    "\"wasm_registry_name\":\"doctor-abandoned-plugin\",",
                    "\"slot\":\"router\",\"order\":300}},",
                    "{{\"principal_id\":\"{principal_id}\",",
                    "\"wasm_registry_id\":\"{registry_id}\",",
                    "\"wasm_registry_name\":\"doctor-abandoned-plugin\",",
                    "\"slot\":\"shape\",\"order\":700}}",
                    "]}}"
                ),
                principal_id = principal_id,
                registry_id = registry_entry.id,
            )
        );
    }

    #[tokio::test]
    async fn report_omits_healthy_chain_entries() {
        let storage = InMemoryStorage::with_clock(fixed_clock(NOW_UNIX_SECS));
        let principal_id = create_principal(&storage, "doctor-mixed-principal").await;
        let abandoned =
            upload_plugin(&storage, "doctor-abandoned-plugin", [12; 32], Vec::new()).await;
        let healthy = upload_plugin(
            &storage,
            "doctor-healthy-plugin",
            [13; 32],
            vec![PluginSlotKind::Router],
        )
        .await;
        insert_chain(
            &storage,
            principal_id,
            healthy.id,
            PluginSlotKind::Router,
            100,
        )
        .await;
        insert_chain(
            &storage,
            principal_id,
            abandoned.id,
            PluginSlotKind::Router,
            200,
        )
        .await;

        let report = build_abandoned_chain_entries_report(&storage)
            .await
            .unwrap();

        assert_eq!(
            serde_json::to_value(report).unwrap(),
            json!({
                "abandoned_chain_entries": [{
                    "principal_id": principal_id.to_string(),
                    "wasm_registry_id": abandoned.id.to_string(),
                    "wasm_registry_name": abandoned.name,
                    "slot": "router",
                    "order": 200,
                }]
            })
        );
    }

    async fn create_principal(storage: &InMemoryStorage, name: &str) -> Uuid {
        PrincipalStore::create(
            storage,
            PrincipalCreate {
                name: name.to_owned(),
                kind: PrincipalKind::Human,
                allowed_models: Vec::new(),
                allowed_upstreams: Vec::new(),
                default_limits: Vec::new(),
                cache_keepalive: None,
            },
            NOW_UNIX_SECS,
        )
        .await
        .unwrap()
        .id
    }

    async fn upload_plugin(
        storage: &InMemoryStorage,
        name: &str,
        sha256: [u8; 32],
        supported_slots: Vec<PluginSlotKind>,
    ) -> WasmRegistryEntry {
        let bytes = format!("{name}-bytes").into_bytes();
        let (entry, existed) = PluginRegistryStore::persist_wasm_upload(
            storage,
            WasmBlob {
                sha256,
                size_bytes: bytes.len() as u64,
                bytes,
                parse_validated_at_unix_secs: NOW_UNIX_SECS,
            },
            WasmRegistryEntryInput {
                schema_hash: None,
                name: name.to_owned(),
                version: None,
                original_filename: format!("{name}.wasm"),
                label: None,
                uploaded_at_unix_secs: NOW_UNIX_SECS,
                uploaded_by_admin_id: TEST_ADMIN_ID,
                description: format!("{name} description"),
                usage: "test fixture".to_owned(),
                hook_metadata: Default::default(),
                supported_slots,
            },
        )
        .await
        .unwrap();
        assert!(!existed);
        entry
    }

    async fn insert_chain(
        storage: &InMemoryStorage,
        principal_id: Uuid,
        wasm_registry_id: Uuid,
        slot: PluginSlotKind,
        order: i64,
    ) {
        PluginRegistryStore::insert_chain_entry(
            storage,
            PluginChainEntryInput {
                principal_id,
                slot,
                order,
                wasm_registry_id,
                config: json!({}),
                sse_per_event: false,
                batched_events_per_flush: 100,
                batched_flush_ms: 1_000,
            },
        )
        .await
        .unwrap();
    }
}
