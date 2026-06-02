#![forbid(unsafe_code)]

pub mod handshake;
mod host_functions;
pub mod identity;
mod plugin_wrap;
pub mod self_check;
mod sse_batch;

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use cc_lb_plugin_api::{
    ObservabilityHook, PluginManifest, PluginRuntime, RouterPlugin, RuntimeError, SignerFactory,
    UpstreamDialect,
};
use extism::{Manifest, Plugin, PluginBuilder, Wasm};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::host_functions::{HostFunctionContext, HostState};
use crate::plugin_wrap::{ExtismDialectPlugin, ExtismRouterPlugin, ExtismSignerFactory};
use crate::sse_batch::ExtismObservabilityHook;

const DEFAULT_MEMORY_MAX_PAGES: u32 = 32;
const DEFAULT_FUEL_MAX: u64 = 1_000_000_000;
const DEFAULT_MAX_CALL_DURATION_MS: u64 = 5_000;
const DEFAULT_STORAGE_QUOTA_BYTES: usize = 1024 * 1024;
const DEFAULT_OBSERVE_BATCH_COUNT: usize = 32;
const DEFAULT_OBSERVE_FLUSH_MS: u64 = 100;
const GLOBAL_PRINCIPAL: &str = "__global__";

// Guardrail exemption: per-principal plugin overrides require runtime slots to be
// keyed by both principal and plugin so same-name plugins do not collide.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SlotKey {
    principal: String,
    plugin: String,
}

impl SlotKey {
    fn new(principal: impl Into<String>, plugin: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            plugin: plugin.into(),
        }
    }

    fn global(plugin: impl Into<String>) -> Self {
        Self::new(GLOBAL_PRINCIPAL, plugin)
    }
}

#[derive(Clone, Debug)]
pub struct ExtismRuntimeConfig {
    pub memory_max_pages: u32,
    pub fuel_max: u64,
    pub max_call_duration: Duration,
    pub storage_quota_bytes: usize,
    pub observe_batch_count: usize,
    pub observe_flush_interval: Duration,
}

impl Default for ExtismRuntimeConfig {
    fn default() -> Self {
        Self {
            memory_max_pages: DEFAULT_MEMORY_MAX_PAGES,
            fuel_max: DEFAULT_FUEL_MAX,
            max_call_duration: Duration::from_millis(DEFAULT_MAX_CALL_DURATION_MS),
            storage_quota_bytes: DEFAULT_STORAGE_QUOTA_BYTES,
            observe_batch_count: DEFAULT_OBSERVE_BATCH_COUNT,
            observe_flush_interval: Duration::from_millis(DEFAULT_OBSERVE_FLUSH_MS),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResourceLimits {
    pub(crate) memory_max_pages: u32,
    pub(crate) fuel_max: u64,
    pub(crate) max_call_duration: Duration,
    pub(crate) storage_quota_bytes: usize,
    pub(crate) observe_batch_count: usize,
    pub(crate) observe_flush_interval: Duration,
}

impl ResourceLimits {
    fn from_manifest(
        manifest: &PluginManifest,
        defaults: &ExtismRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        let metadata = &manifest.metadata;
        Ok(Self {
            memory_max_pages: metadata_u32(metadata, "memory_max_pages")?
                .unwrap_or(defaults.memory_max_pages),
            fuel_max: metadata_u64(metadata, "fuel_max")?.unwrap_or(defaults.fuel_max),
            max_call_duration: Duration::from_millis(
                metadata_u64(metadata, "max_call_duration_ms")?.unwrap_or_else(|| {
                    defaults
                        .max_call_duration
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX)
                }),
            ),
            storage_quota_bytes: metadata_usize(metadata, "storage_quota_bytes")?
                .unwrap_or(defaults.storage_quota_bytes),
            observe_batch_count: metadata_usize(metadata, "observe_batch_count")?
                .unwrap_or(defaults.observe_batch_count)
                .max(1),
            observe_flush_interval: Duration::from_millis(
                metadata_u64(metadata, "observe_flush_ms")?.unwrap_or_else(|| {
                    defaults
                        .observe_flush_interval
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX)
                }),
            ),
        })
    }
}

pub struct ExtismRuntime {
    config: ExtismRuntimeConfig,
    manifests: RwLock<HashMap<SlotKey, PluginEntry>>,
    instances: RwLock<HashMap<SlotKey, Arc<PluginSlot>>>,
    host_state: Arc<HostState>,
}

impl ExtismRuntime {
    pub fn new() -> Self {
        Self::with_config(ExtismRuntimeConfig::default())
    }

    pub fn with_config(config: ExtismRuntimeConfig) -> Self {
        Self {
            config,
            manifests: RwLock::new(HashMap::new()),
            instances: RwLock::new(HashMap::new()),
            host_state: Arc::new(HostState::new()),
        }
    }

    pub fn with_manifests(
        manifests: impl IntoIterator<Item = PluginManifest>,
        config: ExtismRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        let runtime = Self::with_config(config);
        for manifest in manifests {
            runtime.register_slot(&manifest)?;
        }
        Ok(runtime)
    }

    pub fn register_manifest(&self, manifest: &PluginManifest) -> Result<(), RuntimeError> {
        self.register_slot(manifest).map(|_| ())
    }

    fn register_slot(&self, manifest: &PluginManifest) -> Result<Arc<PluginSlot>, RuntimeError> {
        self.register_slot_for_key(SlotKey::global(manifest.name.clone()), manifest)
    }

    fn register_slot_for_key(
        &self,
        key: SlotKey,
        manifest: &PluginManifest,
    ) -> Result<Arc<PluginSlot>, RuntimeError> {
        let entry = PluginEntry::from_manifest(manifest, &self.config)?;
        let cell = build_plugin_cell(&entry, self.host_state.clone())?;

        if let Some(slot) = self
            .instances
            .read()
            .map_err(|_| runtime_error("instance registry lock poisoned"))?
            .get(&key)
            .cloned()
        {
            slot.replace(entry.clone(), cell)?;
            self.manifests
                .write()
                .map_err(|_| runtime_error("manifest registry lock poisoned"))?
                .insert(key, entry);
            return Ok(slot);
        }

        let slot = Arc::new(PluginSlot::new(entry.clone(), cell));
        self.manifests
            .write()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?
            .insert(key.clone(), entry);
        self.instances
            .write()
            .map_err(|_| runtime_error("instance registry lock poisoned"))?
            .insert(key, slot.clone());
        Ok(slot)
    }

    pub fn reload(&self, manifest_ref: &str) -> Result<(), RuntimeError> {
        let key = SlotKey::global(manifest_ref.to_owned());
        let entry = self
            .manifests
            .read()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?
            .get(&key)
            .cloned()
            .ok_or_else(|| RuntimeError::InvalidManifest {
                reason: format!("unknown plugin manifest: {manifest_ref}"),
            })?;
        let refreshed = PluginEntry::from_manifest(&entry.original, &self.config)?;
        let cell = build_plugin_cell(&refreshed, self.host_state.clone())?;
        let slot = self
            .instances
            .read()
            .map_err(|_| runtime_error("instance registry lock poisoned"))?
            .get(&key)
            .cloned()
            .ok_or_else(|| RuntimeError::InstantiateFailed {
                reason: format!("plugin instance not registered: {manifest_ref}"),
            })?;
        slot.replace(refreshed.clone(), cell)?;
        self.manifests
            .write()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?
            .insert(key, refreshed);
        Ok(())
    }

    pub fn reload_manifest(&self, manifest: &PluginManifest) -> Result<(), RuntimeError> {
        self.register_slot(manifest).map(|_| ())
    }

    fn instantiate_slot(
        &self,
        manifest: &PluginManifest,
        hook: &str,
    ) -> Result<Arc<PluginSlot>, RuntimeError> {
        let slot = self.register_slot(manifest)?;
        if !slot.function_exists(hook)? {
            return Err(RuntimeError::InstantiateFailed {
                reason: format!("plugin {} does not export {hook}", manifest.name),
            });
        }
        Ok(slot)
    }

    fn stage_slot(
        &self,
        principal_id: &str,
        plugin_name: &str,
        manifest: &PluginManifest,
        hook: &'static str,
    ) -> Result<(Arc<PluginSlot>, StagedSlot), RuntimeError> {
        let key = SlotKey::new(principal_id, plugin_name);
        let entry = PluginEntry::from_manifest(manifest, &self.config)?;
        let cell = build_plugin_cell(&entry, self.host_state.clone())?;
        let slot = Arc::new(PluginSlot::new(entry.clone(), cell));
        if !slot.function_exists(hook)? {
            return Err(RuntimeError::InstantiateFailed {
                reason: format!("plugin {} does not export {hook}", manifest.name),
            });
        }
        Ok((slot.clone(), StagedSlot { key, entry, slot }))
    }

    pub fn instantiate_router_for(
        &self,
        principal_id: &str,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn RouterPlugin>, StagedSlot), RuntimeError> {
        let (slot, staged) = self.stage_slot(principal_id, plugin_name, manifest, "route")?;
        Ok((Arc::new(ExtismRouterPlugin::new(slot)), staged))
    }

    pub fn instantiate_observability_for(
        &self,
        principal_id: &str,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn ObservabilityHook>, StagedSlot), RuntimeError> {
        let (slot, staged) = self.stage_slot(principal_id, plugin_name, manifest, "observe")?;
        let limits = slot.limits()?;
        Ok((Arc::new(ExtismObservabilityHook::new(slot, limits)), staged))
    }

    pub fn instantiate_router_global(
        &self,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn RouterPlugin>, StagedSlot), RuntimeError> {
        self.instantiate_router_for(GLOBAL_PRINCIPAL, plugin_name, manifest)
    }

    pub fn instantiate_observability_global(
        &self,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn ObservabilityHook>, StagedSlot), RuntimeError> {
        self.instantiate_observability_for(GLOBAL_PRINCIPAL, plugin_name, manifest)
    }

    pub fn instantiate_dialect_for_upstream(
        &self,
        upstream_id: &str,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn UpstreamDialect>, StagedSlot), RuntimeError> {
        let scope = format!("upstream:{upstream_id}");
        let (slot, staged) = self.stage_slot(&scope, plugin_name, manifest, "shape")?;
        Ok((Arc::new(ExtismDialectPlugin::new(slot)), staged))
    }

    pub fn instantiate_dialect_for_principal(
        &self,
        principal_id: &str,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn UpstreamDialect>, StagedSlot), RuntimeError> {
        let scope = format!("principal:{principal_id}");
        let (slot, staged) = self.stage_slot(&scope, plugin_name, manifest, "shape")?;
        Ok((Arc::new(ExtismDialectPlugin::new(slot)), staged))
    }

    pub fn commit_staged(&self, staged: Vec<StagedSlot>) -> Result<(), RuntimeError> {
        let mut instances = self
            .instances
            .write()
            .map_err(|_| runtime_error("instance registry lock poisoned"))?;
        let mut manifests = self
            .manifests
            .write()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?;
        for StagedSlot { key, entry, slot } in staged {
            manifests.insert(key.clone(), entry);
            instances.insert(key, slot);
        }
        Ok(())
    }

    pub fn evict_slot(&self, principal_id: &str, plugin_name: &str) {
        let key = SlotKey::new(principal_id, plugin_name);
        if let Ok(mut manifests) = self.manifests.write() {
            manifests.remove(&key);
        }
        if let Ok(mut instances) = self.instances.write() {
            instances.remove(&key);
        }
    }

    pub fn registered_slot_keys(&self) -> Vec<(String, String)> {
        let Ok(instances) = self.instances.read() else {
            return Vec::new();
        };
        instances
            .keys()
            .map(|k| (k.principal.clone(), k.plugin.clone()))
            .collect()
    }
}

pub struct StagedSlot {
    key: SlotKey,
    entry: PluginEntry,
    slot: Arc<PluginSlot>,
}

impl Default for ExtismRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl PluginRuntime for ExtismRuntime {
    fn instantiate_router(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn RouterPlugin>, RuntimeError> {
        let slot = self.instantiate_slot(manifest, "route")?;
        Ok(Arc::new(ExtismRouterPlugin::new(slot)))
    }

    fn instantiate_dialect(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn UpstreamDialect>, RuntimeError> {
        let slot = self.instantiate_slot(manifest, "shape")?;
        Ok(Arc::new(ExtismDialectPlugin::new(slot)))
    }

    fn instantiate_signer_factory(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn SignerFactory>, RuntimeError> {
        let slot = self.instantiate_slot(manifest, "build_signer")?;
        Ok(Arc::new(ExtismSignerFactory::new(slot, Value::Null)))
    }

    fn instantiate_observability(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn ObservabilityHook>, RuntimeError> {
        let slot = self.instantiate_slot(manifest, "observe")?;
        let limits = slot.limits()?;
        Ok(Arc::new(ExtismObservabilityHook::new(slot, limits)))
    }
}

#[derive(Clone)]
pub(crate) struct PluginEntry {
    name: String,
    original: PluginManifest,
    extism_manifest: Manifest,
    limits: ResourceLimits,
}

impl PluginEntry {
    fn from_manifest(
        manifest: &PluginManifest,
        defaults: &ExtismRuntimeConfig,
    ) -> Result<Self, RuntimeError> {
        if manifest.name.trim().is_empty() {
            return Err(RuntimeError::InvalidManifest {
                reason: "plugin manifest name is empty".to_owned(),
            });
        }
        let limits = ResourceLimits::from_manifest(manifest, defaults)?;
        let extism_manifest = extism_manifest_from_plugin_manifest(manifest, &limits)?;
        Ok(Self {
            name: manifest.name.clone(),
            original: manifest.clone(),
            extism_manifest,
            limits,
        })
    }
}

pub(crate) struct PluginSlot {
    name: String,
    entry: RwLock<PluginEntry>,
    current: ArcSwap<PluginCell>,
}

impl PluginSlot {
    fn new(entry: PluginEntry, cell: PluginCell) -> Self {
        Self {
            name: entry.name.clone(),
            entry: RwLock::new(entry),
            current: ArcSwap::from_pointee(cell),
        }
    }

    fn replace(&self, entry: PluginEntry, cell: PluginCell) -> Result<(), RuntimeError> {
        self.current.store(Arc::new(cell));
        *self
            .entry
            .write()
            .map_err(|_| runtime_error("plugin entry lock poisoned"))? = entry;
        Ok(())
    }

    pub(crate) fn limits(&self) -> Result<ResourceLimits, RuntimeError> {
        Ok(self
            .entry
            .read()
            .map_err(|_| runtime_error("plugin entry lock poisoned"))?
            .limits
            .clone())
    }

    fn function_exists(&self, hook: &str) -> Result<bool, RuntimeError> {
        let cell = self.current.load_full();
        let plugin = cell
            .plugin
            .lock()
            .map_err(|_| RuntimeError::InstantiateFailed {
                reason: "plugin lock poisoned".to_owned(),
            })?;
        Ok(plugin.function_exists(hook))
    }

    pub(crate) async fn call_value_async(
        &self,
        hook: &'static str,
        input: Value,
    ) -> Result<Value, PluginCallError> {
        let input = serde_json::to_string(&input).map_err(|source| PluginCallError::Serialize {
            reason: source.to_string(),
        })?;
        let cell = self.current.load_full();
        let limits = self
            .entry
            .read()
            .map_err(|_| PluginCallError::Runtime {
                reason: "plugin entry lock poisoned".to_owned(),
            })?
            .limits
            .clone();
        let started = Instant::now();
        let result = call_plugin_with_timeout(cell, hook, input, limits.max_call_duration).await;
        let elapsed = started.elapsed().as_secs_f64();
        metrics::histogram!(
            "cc_lb_extism_call_duration_seconds",
            "plugin" => self.name.clone(),
            "hook" => hook.to_owned(),
        )
        .record(elapsed);
        let output = result?;
        serde_json::from_str(&output).map_err(|source| PluginCallError::Deserialize {
            reason: source.to_string(),
        })
    }

    pub(crate) fn call_value_sync(
        self: &Arc<Self>,
        hook: &'static str,
        input: Value,
    ) -> Result<Value, PluginCallError> {
        let slot = self.clone();
        let thread = std::thread::Builder::new()
            .name(format!("cc-lb-extism-{hook}"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                    .map_err(|source| PluginCallError::Runtime {
                        reason: source.to_string(),
                    })?;
                runtime.block_on(slot.call_value_async(hook, input))
            })
            .map_err(|source| PluginCallError::Runtime {
                reason: source.to_string(),
            })?;
        thread.join().map_err(|_| PluginCallError::Panic { hook })?
    }
}

pub(crate) struct PluginCell {
    plugin: Mutex<Plugin>,
}

#[derive(Debug)]
pub(crate) enum PluginCallError {
    Serialize { reason: String },
    Deserialize { reason: String },
    Runtime { reason: String },
    Plugin { reason: String },
    Timeout { hook: &'static str },
    Panic { hook: &'static str },
}

impl fmt::Display for PluginCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize { reason } => {
                write!(f, "plugin request serialization failed: {reason}")
            }
            Self::Deserialize { reason } => {
                write!(f, "plugin response deserialization failed: {reason}")
            }
            Self::Runtime { reason } => write!(f, "plugin runtime failed: {reason}"),
            Self::Plugin { reason } => write!(f, "plugin call failed: {reason}"),
            Self::Timeout { hook } => write!(f, "plugin hook timed out: {hook}"),
            Self::Panic { hook } => write!(f, "plugin hook panicked: {hook}"),
        }
    }
}

impl std::error::Error for PluginCallError {}

async fn call_plugin_with_timeout(
    cell: Arc<PluginCell>,
    hook: &'static str,
    input: String,
    timeout_duration: Duration,
) -> Result<String, PluginCallError> {
    let (cancel_tx, cancel_rx) = oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        let mut plugin = cell.plugin.lock().map_err(|_| PluginCallError::Runtime {
            reason: "plugin lock poisoned".to_owned(),
        })?;
        let _ = cancel_tx.send(plugin.cancel_handle());
        let call = panic::catch_unwind(AssertUnwindSafe(|| {
            plugin.call::<&str, String>(hook, input.as_str())
        }));
        match call {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(source)) => Err(PluginCallError::Plugin {
                reason: source.to_string(),
            }),
            Err(_) => Err(PluginCallError::Panic { hook }),
        }
    });

    let cancel_handle = tokio::time::timeout(Duration::from_millis(25), cancel_rx)
        .await
        .ok()
        .and_then(Result::ok);

    match tokio::time::timeout(timeout_duration, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(source)) => Err(PluginCallError::Runtime {
            reason: source.to_string(),
        }),
        Err(_) => {
            if let Some(cancel_handle) = cancel_handle {
                let _ = cancel_handle.cancel();
            }
            Err(PluginCallError::Timeout { hook })
        }
    }
}

fn build_plugin_cell(
    entry: &PluginEntry,
    host_state: Arc<HostState>,
) -> Result<PluginCell, RuntimeError> {
    let context = HostFunctionContext::new(
        entry.name.clone(),
        entry.limits.storage_quota_bytes,
        host_state,
    );
    let mut builder = PluginBuilder::new(&entry.extism_manifest)
        .with_functions(host_functions::functions(context))
        .with_wasi(true)
        .with_cache_disabled();
    if entry.limits.fuel_max > 0 {
        builder = builder.with_fuel_limit(entry.limits.fuel_max);
    }
    let plugin = builder
        .build()
        .map_err(|source| RuntimeError::InstantiateFailed {
            reason: source.to_string(),
        })?;
    Ok(PluginCell {
        plugin: Mutex::new(plugin),
    })
}

fn extism_manifest_from_plugin_manifest(
    manifest: &PluginManifest,
    limits: &ResourceLimits,
) -> Result<Manifest, RuntimeError> {
    let artifact_path = Path::new(&manifest.artifact);
    let bytes = fs::read(artifact_path).map_err(|source| RuntimeError::LoadFailed {
        reason: format!("{}: {source}", artifact_path.display()),
    })?;
    let mut extism_manifest = Manifest::new([Wasm::data(bytes)])
        .with_memory_max(limits.memory_max_pages)
        .with_timeout(limits.max_call_duration)
        .disallow_all_hosts();

    if let Some(config) = manifest.config.as_object() {
        for (key, value) in config {
            let value = value
                .as_str()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| value.to_string());
            extism_manifest = extism_manifest.with_config_key(key.clone(), value);
        }
    }
    Ok(extism_manifest)
}

fn metadata_u64(
    metadata: &std::collections::BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<u64>, RuntimeError> {
    metadata
        .get(key)
        .map(|value| {
            value.as_u64().ok_or_else(|| RuntimeError::InvalidManifest {
                reason: format!("metadata {key} must be a non-negative integer"),
            })
        })
        .transpose()
}

fn metadata_u32(
    metadata: &std::collections::BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<u32>, RuntimeError> {
    metadata_u64(metadata, key)?
        .map(|value| {
            u32::try_from(value).map_err(|_| RuntimeError::InvalidManifest {
                reason: format!("metadata {key} exceeds u32"),
            })
        })
        .transpose()
}

fn metadata_usize(
    metadata: &std::collections::BTreeMap<String, Value>,
    key: &str,
) -> Result<Option<usize>, RuntimeError> {
    metadata_u64(metadata, key)?
        .map(|value| {
            usize::try_from(value).map_err(|_| RuntimeError::InvalidManifest {
                reason: format!("metadata {key} exceeds usize"),
            })
        })
        .transpose()
}

fn runtime_error(reason: impl Into<String>) -> RuntimeError {
    RuntimeError::InstantiateFailed {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use cc_lb_plugin_api::PluginManifest;
    use serde_json::json;

    use super::*;

    #[test]
    fn slots_with_same_plugin_name_and_different_principals_coexist() {
        let fixture = wasm_manifest("router", router_module());
        let runtime = ExtismRuntime::new();
        let alice_key = SlotKey::new("alice", "router");
        let bob_key = SlotKey::new("bob", "router");

        let alice_slot = runtime
            .register_slot_for_key(alice_key.clone(), &fixture.manifest)
            .expect("alice router slot registers");
        let bob_slot = runtime
            .register_slot_for_key(bob_key.clone(), &fixture.manifest)
            .expect("bob router slot registers");

        assert!(!Arc::ptr_eq(&alice_slot, &bob_slot));
        let instances = runtime
            .instances
            .read()
            .expect("instances lock is readable");
        assert_eq!(instances.len(), 2);
        assert!(instances.contains_key(&alice_key));
        assert!(instances.contains_key(&bob_key));
    }

    #[test]
    fn instantiate_router_for_stages_without_touching_live_maps() {
        let fixture = wasm_manifest("router", router_module());
        let runtime = ExtismRuntime::new();

        let (_handle, staged) = runtime
            .instantiate_router_for("alice", "alice-router", &fixture.manifest)
            .expect("staging succeeds");

        assert!(
            runtime.registered_slot_keys().is_empty(),
            "staging must NOT touch live instances map until commit_staged"
        );

        runtime
            .commit_staged(vec![staged])
            .expect("commit succeeds");

        let keys = runtime.registered_slot_keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0],
            ("alice".to_owned(), "alice-router".to_owned()),
            "committed key uses (principal_id, plugin_name) pair"
        );
    }

    #[test]
    fn commit_staged_registers_both_principal_and_global_in_one_batch() {
        let fixture = wasm_manifest("router", router_module());
        let runtime = ExtismRuntime::new();

        let (_global_handle, global_staged) = runtime
            .instantiate_router_global("shared", &fixture.manifest)
            .expect("global staging");
        let (_alice_handle, alice_staged) = runtime
            .instantiate_router_for("alice", "shared", &fixture.manifest)
            .expect("alice staging");

        assert!(runtime.registered_slot_keys().is_empty());

        runtime
            .commit_staged(vec![global_staged, alice_staged])
            .expect("commit");

        let mut keys = runtime.registered_slot_keys();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                ("__global__".to_owned(), "shared".to_owned()),
                ("alice".to_owned(), "shared".to_owned()),
            ],
            "global and per-principal slots with same plugin name commit as distinct keys"
        );
    }

    #[test]
    fn instantiate_router_for_with_missing_export_returns_err_without_staging() {
        let fixture = wasm_manifest("noroute", noroute_module());
        let runtime = ExtismRuntime::new();

        let err = runtime
            .instantiate_router_for("alice", "noroute", &fixture.manifest)
            .map(|_| ())
            .expect_err("missing 'route' export must fail");
        let message = format!("{err:?}");
        assert!(
            message.contains("does not export route"),
            "error must surface missing-export cause, got: {message}"
        );
        assert!(
            runtime.registered_slot_keys().is_empty(),
            "failed staging must NOT leave anything in instances map"
        );
    }

    #[test]
    fn evict_slot_removes_committed_entry() {
        let fixture = wasm_manifest("router", router_module());
        let runtime = ExtismRuntime::new();
        let (_handle, staged) = runtime
            .instantiate_router_for("alice", "alice-router", &fixture.manifest)
            .expect("staging");
        runtime.commit_staged(vec![staged]).expect("commit");
        assert_eq!(runtime.registered_slot_keys().len(), 1);

        runtime.evict_slot("alice", "alice-router");

        assert!(
            runtime.registered_slot_keys().is_empty(),
            "evict_slot must remove the entry from both instances and manifests"
        );
    }

    #[test]
    fn instantiate_observability_for_stages_observe_export_check() {
        let fixture = wasm_manifest("hook", observe_module());
        let runtime = ExtismRuntime::new();

        let (_handle, staged) = runtime
            .instantiate_observability_for("alice", "alice-hook", &fixture.manifest)
            .expect("observability staging");
        runtime.commit_staged(vec![staged]).expect("commit");

        assert_eq!(
            runtime.registered_slot_keys(),
            vec![("alice".to_owned(), "alice-hook".to_owned())]
        );
    }

    fn noroute_module() -> &'static str {
        r#"(module (func (export "shape") (result i32) (i32.const 0)))"#
    }

    fn observe_module() -> &'static str {
        r#"(module (func (export "observe") (result i32) (i32.const 0)))"#
    }

    struct WasmManifestFixture {
        _dir: tempfile::TempDir,
        manifest: PluginManifest,
    }

    fn wasm_manifest(name: &str, wat: &str) -> WasmManifestFixture {
        let dir = tempfile::tempdir().expect("tempdir is created");
        let wasm = wat::parse_str(wat).expect("wat parses");
        let artifact = dir.path().join(format!("{name}.wasm"));
        fs::write(&artifact, wasm).expect("wasm fixture is written");
        WasmManifestFixture {
            _dir: dir,
            manifest: PluginManifest {
                name: name.to_owned(),
                artifact: artifact.to_string_lossy().into_owned(),
                config: json!({}),
                metadata: BTreeMap::new(),
            },
        }
    }

    fn router_module() -> &'static str {
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (func $out (result i64)
    (local $ptr i64)
    (local.set $ptr (call $alloc (i64.const 2)))
    (call $store_u8 (local.get $ptr) (i32.const 123))
    (call $store_u8 (i64.add (local.get $ptr) (i64.const 1)) (i32.const 125))
    (local.get $ptr))
  (func (export "route") (result i32)
    (call $output_set (call $out) (i64.const 2))
    (i32.const 0)))
"#
    }
}
