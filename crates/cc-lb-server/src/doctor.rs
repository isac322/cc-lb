use std::io::Write;

use cc_lb_storage_api::{BackendKind, MetaStore, PluginRegistryStore, PluginSlot, PrincipalStore};
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
    let mut abandoned_chain_entries = Vec::new();
    let principals = storage
        .list(0, usize::MAX, true)
        .await
        .map_err(DoctorError::StorageQuery)?;

    for principal in principals {
        for slot in [
            PluginSlot::Router,
            PluginSlot::ObservabilityHook,
            PluginSlot::Shape,
        ] {
            let chain_entries = storage
                .list_chain_for_principal(principal.id, slot)
                .await
                .map_err(DoctorError::StorageQuery)?;
            for chain_entry in chain_entries {
                let Some(registry_entry) = storage
                    .get_registry_entry_by_id(chain_entry.wasm_registry_id)
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
    let report = AbandonedChainEntriesReport {
        abandoned_chain_entries,
    };
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &report).map_err(DoctorError::WriteJson)?;
    writeln!(stdout).map_err(DoctorError::Stdout)
}
