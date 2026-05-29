use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_config::Config;
use cc_lb_storage_api::plugin_registry::{PluginChainEntryInput, PluginSlot, WasmRegistryEntry};
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::sparse_order;
use cc_lb_storage_api::upstream::UpstreamCreate;
use cc_lb_storage_api::{PluginRegistryStore, PrincipalStore, StorageResult, UpstreamStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Invalid bootstrap spec: {0}")]
    InvalidSpec(String),
}

pub type BootstrapResult<T> = Result<T, BootstrapError>;

async fn seed_admin_if_absent(store: &dyn PrincipalStore, _token: &str) -> StorageResult<()> {
    let admin_principal = PrincipalCreate {
        name: "admin".to_owned(),
        kind: PrincipalKind::Admin,
        allowed_models: vec![],
        default_limits: vec![],
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    match store.get_by_name("admin").await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => store.create(admin_principal, now).await.map(|_| ()),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct BootstrapSpec {
    #[serde(default)]
    pub upstreams: Vec<BootstrapUpstream>,
    #[serde(default)]
    pub principals: Vec<BootstrapPrincipal>,
    #[serde(default)]
    pub plugin_chains: Vec<BootstrapPluginChain>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapUpstream {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub api_key_env: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapPrincipal {
    pub name: String,
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapPluginChain {
    pub principal_id: String,
    #[serde(default)]
    pub slot: Option<String>,
    #[serde(default)]
    pub plugins: Vec<BootstrapPluginRef>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum BootstrapPluginRef {
    Name(String),
    Entry(BootstrapPluginEntry),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapPluginEntry {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub wasm_registry_id: Option<Uuid>,
    #[serde(default)]
    pub slot: Option<String>,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub sse_per_event: Option<bool>,
    #[serde(default)]
    pub batched_events_per_flush: Option<u32>,
    #[serde(default)]
    pub batched_flush_ms: Option<u64>,
}

pub async fn apply_bootstrap(
    _config: &Config,
    seeder: &dyn PrincipalStore,
    upstream_store: &dyn UpstreamStore,
    plugin_store: &dyn PluginRegistryStore,
    env_token: Option<String>,
    data_dir: &Path,
) -> BootstrapResult<()> {
    if let Some(token) = env_token {
        seed_admin_if_absent(seeder, &token)
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()))?;
    }

    let bootstrap_path = data_dir.join("bootstrap.toml");
    if bootstrap_path.exists() {
        let content = fs::read_to_string(&bootstrap_path)?;

        let spec: BootstrapSpec = if content.trim().is_empty() {
            BootstrapSpec::default()
        } else {
            toml::from_str::<BootstrapSpec>(&content).unwrap_or_default()
        };

        for principal in spec.principals {
            apply_principal(seeder, principal).await?;
        }

        for upstream in spec.upstreams {
            if upstream_store
                .get_by_name(&upstream.name)
                .await
                .map_err(|e| BootstrapError::Storage(e.to_string()))?
                .is_some()
            {
                continue;
            }

            let _api_key = upstream
                .api_key_env
                .as_ref()
                .and_then(|env_var| std::env::var(env_var).ok());

            let create_input = UpstreamCreate {
                name: upstream.name,
                kind: parse_upstream_kind(&upstream.kind),
                base_url: None,
                api_key_ciphertext: None,
            };

            upstream_store
                .create(create_input)
                .await
                .map_err(|e| BootstrapError::Storage(e.to_string()))?;
        }

        for chain in spec.plugin_chains {
            apply_plugin_chain(seeder, plugin_store, chain).await?;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let consumed_name = format!("bootstrap.toml.consumed-{}", timestamp);
        let consumed_path = data_dir.join(&consumed_name);
        fs::rename(&bootstrap_path, &consumed_path)?;
    }

    Ok(())
}

async fn apply_principal(
    principal_store: &dyn PrincipalStore,
    principal: BootstrapPrincipal,
) -> BootstrapResult<()> {
    if principal_store
        .get_by_name(&principal.name)
        .await
        .map_err(|e| BootstrapError::Storage(e.to_string()))?
        .is_some()
    {
        return Ok(());
    }

    let now = unix_now_secs();
    let input = PrincipalCreate {
        name: principal.name,
        kind: parse_principal_kind(principal.kind.as_deref()),
        allowed_models: Vec::new(),
        default_limits: Vec::new(),
    };
    principal_store
        .create(input, now)
        .await
        .map(|_| ())
        .map_err(|e| BootstrapError::Storage(e.to_string()))
}

async fn apply_plugin_chain(
    principal_store: &dyn PrincipalStore,
    plugin_store: &dyn PluginRegistryStore,
    chain: BootstrapPluginChain,
) -> BootstrapResult<()> {
    let Some(principal) = resolve_principal(principal_store, &chain.principal_id).await? else {
        tracing::warn!(
            principal_id = chain.principal_id.as_str(),
            "bootstrap plugin chain principal is missing; skipping chain"
        );
        return Ok(());
    };

    let mut resolved = Vec::with_capacity(chain.plugins.len());
    for plugin in chain.plugins {
        let slot_name = plugin.slot(chain.slot.as_deref());
        let Some(slot) = parse_plugin_slot(&slot_name) else {
            tracing::warn!(
                principal_id = %principal.id,
                slot = slot_name.as_str(),
                "bootstrap plugin chain slot is invalid; skipping chain"
            );
            return Ok(());
        };
        let Some(registry_entry) = resolve_wasm_registry_entry(plugin_store, &plugin).await? else {
            tracing::warn!(
                principal_id = %principal.id,
                plugin_ref = plugin.reference().as_str(),
                "bootstrap plugin chain wasm registry entry is missing; skipping chain"
            );
            return Ok(());
        };
        resolved.push((plugin, slot, registry_entry));
    }

    for (plugin, slot, registry_entry) in resolved {
        let existing = plugin_store
            .list_chain_for_principal(principal.id, slot)
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()))?;
        if existing
            .iter()
            .any(|entry| entry.wasm_registry_id == registry_entry.id)
        {
            continue;
        }

        let order =
            sparse_order::next_after(&existing.iter().map(|entry| entry.order).collect::<Vec<_>>());
        plugin_store
            .insert_chain_entry(PluginChainEntryInput {
                principal_id: principal.id,
                slot,
                order,
                wasm_registry_id: registry_entry.id,
                config: plugin
                    .config()
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Default::default())),
                sse_per_event: plugin.sse_per_event().unwrap_or(false),
                batched_events_per_flush: plugin.batched_events_per_flush().unwrap_or(1),
                batched_flush_ms: plugin.batched_flush_ms().unwrap_or(100),
            })
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()))?;
    }

    Ok(())
}

async fn resolve_principal(
    principal_store: &dyn PrincipalStore,
    principal_id: &str,
) -> BootstrapResult<Option<cc_lb_storage_api::principal::PrincipalRecord>> {
    if let Some(principal) = principal_store
        .get_by_name(principal_id)
        .await
        .map_err(|e| BootstrapError::Storage(e.to_string()))?
    {
        return Ok(Some(principal));
    }

    let Ok(id) = principal_id.parse::<Uuid>() else {
        return Ok(None);
    };
    principal_store
        .get_by_id(id)
        .await
        .map_err(|e| BootstrapError::Storage(e.to_string()))
}

async fn resolve_wasm_registry_entry(
    plugin_store: &dyn PluginRegistryStore,
    plugin: &BootstrapPluginRef,
) -> BootstrapResult<Option<WasmRegistryEntry>> {
    if let Some(id) = plugin.wasm_registry_id() {
        return plugin_store
            .get_registry_entry_by_id(id)
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()));
    }

    let Some(name) = plugin.name() else {
        return Ok(None);
    };
    let mut after = None;
    loop {
        let entries = plugin_store
            .list_registry(after, 100)
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()))?;
        if entries.is_empty() {
            return Ok(None);
        }
        if let Some(entry) = entries.iter().find(|entry| entry.name == name) {
            return Ok(Some(entry.clone()));
        }
        after = entries.last().map(|entry| entry.id);
    }
}

impl BootstrapPluginRef {
    fn name(&self) -> Option<&str> {
        match self {
            Self::Name(name) => Some(name),
            Self::Entry(entry) => entry.name.as_deref(),
        }
    }

    fn wasm_registry_id(&self) -> Option<Uuid> {
        match self {
            Self::Name(value) => value.parse().ok(),
            Self::Entry(entry) => entry.wasm_registry_id,
        }
    }

    fn slot(&self, chain_slot: Option<&str>) -> String {
        match self {
            Self::Name(_) => chain_slot.map(ToOwned::to_owned),
            Self::Entry(entry) => entry.slot.as_deref().or(chain_slot).map(ToOwned::to_owned),
        }
        .unwrap_or_else(|| "observability_hook".to_owned())
    }

    fn config(&self) -> Option<&Value> {
        match self {
            Self::Name(_) => None,
            Self::Entry(entry) => entry.config.as_ref(),
        }
    }

    fn sse_per_event(&self) -> Option<bool> {
        match self {
            Self::Name(_) => None,
            Self::Entry(entry) => entry.sse_per_event,
        }
    }

    fn batched_events_per_flush(&self) -> Option<u32> {
        match self {
            Self::Name(_) => None,
            Self::Entry(entry) => entry.batched_events_per_flush,
        }
    }

    fn batched_flush_ms(&self) -> Option<u64> {
        match self {
            Self::Name(_) => None,
            Self::Entry(entry) => entry.batched_flush_ms,
        }
    }

    fn reference(&self) -> String {
        self.wasm_registry_id()
            .map(|id| id.to_string())
            .or_else(|| self.name().map(ToOwned::to_owned))
            .unwrap_or_else(|| "<missing>".to_owned())
    }
}

fn parse_principal_kind(kind: Option<&str>) -> PrincipalKind {
    match kind {
        Some("admin") => PrincipalKind::Admin,
        Some("human") => PrincipalKind::Human,
        _ => PrincipalKind::Machine,
    }
}

fn parse_plugin_slot(slot: &str) -> Option<PluginSlot> {
    match slot {
        "Router" | "router" => Some(PluginSlot::Router),
        "ObservabilityHook" | "observability_hook" => Some(PluginSlot::ObservabilityHook),
        _ => None,
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn parse_upstream_kind(kind_str: &str) -> cc_lb_storage_api::upstream::UpstreamKind {
    match kind_str {
        "anthropic_api_key" => cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey,
        "anthropic_oauth" => cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth,
        _ => cc_lb_storage_api::upstream::UpstreamKind::Custom,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_storage_api::{
        PluginRegistryStore, PrincipalStore, WasmBlob, WasmRegistryEntryInput,
    };
    use cc_lb_storage_redb::Storage;

    #[tokio::test]
    async fn bootstrap_seeds_principals_from_toml() {
        let (dir, storage) = fixture();
        fs::write(
            dir.path().join("bootstrap.toml"),
            format!("{}\nname = \"alice\"\nkind = \"human\"\n", ["[[", "principals", "]]"].concat()),
        )
        .unwrap();

        apply_bootstrap(
            &Config::default(),
            &storage,
            &storage,
            &storage,
            None,
            dir.path(),
        )
        .await
        .unwrap();

        let principal = PrincipalStore::get_by_name(&storage, "alice").await.unwrap().unwrap();
        assert_eq!(principal.kind, PrincipalKind::Human);

        fs::write(
            dir.path().join("bootstrap.toml"),
            format!("{}\nname = \"alice\"\nkind = \"human\"\n", ["[[", "principals", "]]"].concat()),
        )
        .unwrap();
        apply_bootstrap(
            &Config::default(),
            &storage,
            &storage,
            &storage,
            None,
            dir.path(),
        )
        .await
        .unwrap();

        let principals = PrincipalStore::list(&storage, 0, 100, false).await.unwrap();
        assert_eq!(
            principals
                .iter()
                .filter(|principal| principal.name == "alice")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn bootstrap_seeds_plugin_chain_when_wasm_exists() {
        let (dir, storage) = fixture();
        seed_principal(&storage, "plugin-principal").await;
        seed_registry(&storage, "audit").await;
        fs::write(
            dir.path().join("bootstrap.toml"),
            r#"
[[plugin_chains]]
principal_id = "plugin-principal"
slot = "router"
plugins = ["audit"]
"#,
        )
        .unwrap();

        apply_bootstrap(
            &Config::default(),
            &storage,
            &storage,
            &storage,
            None,
            dir.path(),
        )
        .await
        .unwrap();

        let principal = PrincipalStore::get_by_name(&storage, "plugin-principal")
            .await
            .unwrap()
            .unwrap();
        let entries = storage
            .list_chain_for_principal(principal.id, PluginSlot::Router)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);

        fs::write(
            dir.path().join("bootstrap.toml"),
            r#"
[[plugin_chains]]
principal_id = "plugin-principal"
slot = "router"
plugins = ["audit"]
"#,
        )
        .unwrap();
        apply_bootstrap(
            &Config::default(),
            &storage,
            &storage,
            &storage,
            None,
            dir.path(),
        )
        .await
        .unwrap();
        let entries = storage
            .list_chain_for_principal(principal.id, PluginSlot::Router)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn bootstrap_skips_chain_when_wasm_missing_with_warning() {
        let (dir, storage) = fixture();
        let principal = seed_principal(&storage, "plugin-principal").await;
        fs::write(
            dir.path().join("bootstrap.toml"),
            r#"
[[plugin_chains]]
principal_id = "plugin-principal"
slot = "router"
plugins = ["missing-plugin"]
"#,
        )
        .unwrap();

        apply_bootstrap(
            &Config::default(),
            &storage,
            &storage,
            &storage,
            None,
            dir.path(),
        )
        .await
        .unwrap();

        let entries = storage
            .list_chain_for_principal(principal.id, PluginSlot::Router)
            .await
            .unwrap();
        assert!(entries.is_empty());
    }

    fn fixture() -> (tempfile::TempDir, Storage) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(&dir.path().join("storage.redb"), [7; 32]).unwrap();
        (dir, storage)
    }

    async fn seed_principal(
        storage: &Storage,
        name: &str,
    ) -> cc_lb_storage_api::principal::PrincipalRecord {
        PrincipalStore::create(
            storage,
            PrincipalCreate {
                    name: name.to_owned(),
                    kind: PrincipalKind::Machine,
                    allowed_models: Vec::new(),
                    default_limits: Vec::new(),
            },
            1_800_000_000,
        )
            .await
            .unwrap()
    }

    async fn seed_registry(storage: &Storage, name: &str) -> WasmRegistryEntry {
        storage
            .persist_wasm_upload(
                WasmBlob {
                    sha256: [3; 32],
                    bytes: vec![3; 4],
                    size_bytes: 4,
                    parse_validated_at_unix_secs: 1_800_000_000,
                },
                WasmRegistryEntryInput {
                    name: name.to_owned(),
                    original_filename: format!("{name}.wasm"),
                    label: None,
                    uploaded_at_unix_secs: 1_800_000_000,
                    uploaded_by_admin_id: Uuid::new_v4(),
                },
            )
            .await
            .unwrap()
    }
}
