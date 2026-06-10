#![forbid(unsafe_code)]

pub mod dispatch;
pub mod handshake;
mod host_functions;
pub mod identity;
mod plugin_wrap;
pub mod registry;
pub mod self_check;
mod sse_batch;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use cc_lb_plugin_api::{
    FilterPlugin, ObservabilityHook, PluginManifest, PluginRuntime, RouterPlugin, RuntimeError,
    SignerFactory, UpstreamDialect,
};
use extism::{Manifest, Plugin, PluginBuilder, Wasm};
use serde_json::Value;
use uuid::Uuid;

use crate::host_functions::{HostFunctionContext, HostState};
use crate::plugin_wrap::{ExtismDialectPlugin, ExtismFilterPlugin, ExtismSignerFactory};
use crate::sse_batch::ExtismObservabilityHook;

const DEFAULT_MEMORY_MAX_PAGES: u32 = 32;
const DEFAULT_FUEL_MAX: u64 = 1_000_000_000;
const DEFAULT_MAX_CALL_DURATION_MS: u64 = 5_000;
const DEFAULT_STORAGE_QUOTA_BYTES: usize = 1024 * 1024;
const DEFAULT_OBSERVE_BATCH_COUNT: usize = 32;
const DEFAULT_OBSERVE_FLUSH_MS: u64 = 100;
const GLOBAL_PRINCIPAL: &str = "__global__";
const WIRE_VERSION_V1: u8 = 1;
const WIRE_VERSION_V2: u8 = 2;
const WIRE_VERSION_V3: u8 = 3;

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
        _principal_id: &str,
        _plugin_name: &str,
        _manifest: &PluginManifest,
    ) -> Result<(Arc<dyn RouterPlugin>, StagedSlot), RuntimeError> {
        Err(router_wire_removed_error())
    }

    pub fn instantiate_filter_for(
        &self,
        principal_id: &str,
        plugin_id: Uuid,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn FilterPlugin>, StagedSlot), RuntimeError> {
        let (slot, staged) = self.stage_slot(principal_id, plugin_name, manifest, "filter")?;
        Ok((Arc::new(ExtismFilterPlugin::new(slot, plugin_id)), staged))
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

    pub fn instantiate_filter_global(
        &self,
        plugin_id: Uuid,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn FilterPlugin>, StagedSlot), RuntimeError> {
        self.instantiate_filter_for(GLOBAL_PRINCIPAL, plugin_id, plugin_name, manifest)
    }

    pub fn instantiate_observability_global(
        &self,
        plugin_name: &str,
        manifest: &PluginManifest,
    ) -> Result<(Arc<dyn ObservabilityHook>, StagedSlot), RuntimeError> {
        self.instantiate_observability_for(GLOBAL_PRINCIPAL, plugin_name, manifest)
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

    pub fn instantiate_filter(
        &self,
        manifest: &PluginManifest,
    ) -> Result<Arc<dyn FilterPlugin>, RuntimeError> {
        let slot = self.instantiate_slot(manifest, "filter")?;
        Ok(Arc::new(ExtismFilterPlugin::new(
            slot,
            plugin_id_from_manifest(manifest),
        )))
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

fn router_wire_removed_error() -> RuntimeError {
    RuntimeError::InstantiateFailed {
        reason: "router wire v1/v2 plugins are no longer supported; use wire v3 filter plugins"
            .to_owned(),
    }
}

impl PluginRuntime for ExtismRuntime {
    fn instantiate_router(
        &self,
        _manifest: &PluginManifest,
    ) -> Result<Arc<dyn RouterPlugin>, RuntimeError> {
        Err(router_wire_removed_error())
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
    negotiated_wire_version: u8,
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
        let negotiated_wire_version = negotiate_wire_version(manifest);
        let extism_manifest = extism_manifest_from_plugin_manifest(manifest, &limits)?;
        Ok(Self {
            name: manifest.name.clone(),
            original: manifest.clone(),
            extism_manifest,
            limits,
            negotiated_wire_version,
        })
    }
}

fn negotiate_wire_version(manifest: &PluginManifest) -> u8 {
    match manifest.wire_version {
        Some(version @ (WIRE_VERSION_V1 | WIRE_VERSION_V2 | WIRE_VERSION_V3)) => version,
        None => WIRE_VERSION_V1,
        Some(version) => {
            tracing::warn!(
                plugin = %manifest.name,
                declared_wire_version = version,
                fallback_wire_version = WIRE_VERSION_V1,
                "plugin {} declared unsupported wire_version {}, falling back to v1",
                manifest.name,
                version,
            );
            WIRE_VERSION_V1
        }
    }
}

fn plugin_id_from_manifest(manifest: &PluginManifest) -> Uuid {
    manifest
        .metadata
        .get("plugin_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::nil)
}

pub(crate) struct PluginSlot {
    name: String,
    entry: RwLock<PluginEntry>,
    current: ArcSwap<PluginCell>,
}

impl PluginSlot {
    fn new(entry: PluginEntry, cell: PluginCell) -> Self {
        let slot = Self {
            name: entry.name.clone(),
            entry: RwLock::new(entry),
            current: ArcSwap::from_pointee(cell),
        };
        slot.log_negotiated_wire_version();
        slot
    }

    fn replace(&self, entry: PluginEntry, cell: PluginCell) -> Result<(), RuntimeError> {
        self.current.store(Arc::new(cell));
        *self
            .entry
            .write()
            .map_err(|_| runtime_error("plugin entry lock poisoned"))? = entry;
        self.log_negotiated_wire_version();
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

    pub(crate) fn negotiated_wire_version(&self) -> Result<u8, RuntimeError> {
        Ok(self
            .entry
            .read()
            .map_err(|_| runtime_error("plugin entry lock poisoned"))?
            .negotiated_wire_version)
    }

    fn log_negotiated_wire_version(&self) {
        if let Ok(wire_version) = self.negotiated_wire_version() {
            tracing::debug!(
                plugin = %self.name,
                wire_version,
                "plugin wire version negotiated"
            );
        }
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
}

pub(crate) struct PluginCell {
    plugin: Mutex<Plugin>,
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
        .with_wasi(true);
    if let Some(cache_config) =
        std::env::var_os("EXTISM_CACHE_CONFIG").filter(|value| !value.is_empty())
    {
        builder = builder.with_cache_config(std::path::PathBuf::from(cache_config));
    } else {
        builder = builder.with_cache_disabled();
    }
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

    mod wire_version_negotiation {
        use super::*;

        #[test]
        fn wire_version_v1_default_for_unmarked_plugin() {
            let fixture = wasm_manifest("wire-version-default", noroute_module());
            let runtime = ExtismRuntime::new();

            let slot = runtime
                .register_slot(&fixture.manifest)
                .expect("slot registers with default wire version");

            assert_eq!(
                slot.negotiated_wire_version()
                    .expect("wire version is readable"),
                WIRE_VERSION_V1
            );
        }

        #[test]
        fn wire_version_v2_for_marked_plugin() {
            let mut fixture = wasm_manifest("wire-version-v2", noroute_module());
            fixture.manifest.wire_version = Some(WIRE_VERSION_V2);
            let runtime = ExtismRuntime::new();

            let slot = runtime
                .register_slot(&fixture.manifest)
                .expect("slot registers with v2 wire version");

            assert_eq!(
                slot.negotiated_wire_version()
                    .expect("wire version is readable"),
                WIRE_VERSION_V2
            );
        }

        #[test]
        fn wire_version_v3_for_marked_plugin() {
            let mut fixture = wasm_manifest("wire-version-v3", noroute_module());
            fixture.manifest.wire_version = Some(WIRE_VERSION_V3);
            let runtime = ExtismRuntime::new();

            let slot = runtime
                .register_slot(&fixture.manifest)
                .expect("slot registers with v3 wire version");

            assert_eq!(
                slot.negotiated_wire_version()
                    .expect("wire version is readable"),
                WIRE_VERSION_V3
            );
        }

        #[test]
        fn wire_version_unknown_falls_back_to_v1() {
            let mut fixture = wasm_manifest("wire-version-unknown", noroute_module());
            fixture.manifest.wire_version = Some(99);
            let runtime = ExtismRuntime::new();

            let slot = runtime
                .register_slot(&fixture.manifest)
                .expect("slot registers with fallback wire version");

            assert_eq!(
                slot.negotiated_wire_version()
                    .expect("wire version is readable"),
                WIRE_VERSION_V1
            );
        }
    }

    #[test]
    fn slots_with_same_plugin_name_and_different_principals_coexist() {
        let fixture = wasm_manifest("shape", noroute_module());
        let runtime = ExtismRuntime::new();
        let alice_key = SlotKey::new("alice", "shape");
        let bob_key = SlotKey::new("bob", "shape");

        let alice_slot = runtime
            .register_slot_for_key(alice_key.clone(), &fixture.manifest)
            .expect("alice shape slot registers");
        let bob_slot = runtime
            .register_slot_for_key(bob_key.clone(), &fixture.manifest)
            .expect("bob shape slot registers");

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
    fn instantiate_filter_for_stages_without_touching_live_maps() {
        let fixture = wasm_manifest("filter", filter_module());
        let runtime = ExtismRuntime::new();

        let (_handle, staged) = runtime
            .instantiate_filter_for(
                "alice",
                Uuid::from_u128(0x11111111111111111111111111111111),
                "alice-filter",
                &fixture.manifest,
            )
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
            ("alice".to_owned(), "alice-filter".to_owned()),
            "committed key uses (principal_id, plugin_name) pair"
        );
    }

    #[test]
    fn commit_staged_registers_both_principal_and_global_in_one_batch() {
        let fixture = wasm_manifest("filter", filter_module());
        let runtime = ExtismRuntime::new();

        let (_global_handle, global_staged) = runtime
            .instantiate_filter_global(
                Uuid::from_u128(0x22222222222222222222222222222222),
                "shared",
                &fixture.manifest,
            )
            .expect("global staging");
        let (_alice_handle, alice_staged) = runtime
            .instantiate_filter_for(
                "alice",
                Uuid::from_u128(0x33333333333333333333333333333333),
                "shared",
                &fixture.manifest,
            )
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
    fn instantiate_router_for_returns_removed_error_without_staging() {
        let fixture = wasm_manifest("router", router_module());
        let runtime = ExtismRuntime::new();

        let err = match runtime.instantiate_router_for("alice", "router", &fixture.manifest) {
            Ok(_) => panic!("router wire v1/v2 unexpectedly staged"),
            Err(error) => error,
        };
        let message = format!("{err:?}");
        assert!(
            message.contains("router wire v1/v2 plugins are no longer supported"),
            "error must surface router removal cause, got: {message}"
        );
        assert!(
            runtime.registered_slot_keys().is_empty(),
            "failed staging must NOT leave anything in instances map"
        );
    }

    #[test]
    fn evict_slot_removes_committed_entry() {
        let fixture = wasm_manifest("filter", filter_module());
        let runtime = ExtismRuntime::new();
        let (_handle, staged) = runtime
            .instantiate_filter_for(
                "alice",
                Uuid::from_u128(0x44444444444444444444444444444444),
                "alice-filter",
                &fixture.manifest,
            )
            .expect("staging");
        runtime.commit_staged(vec![staged]).expect("commit");
        assert_eq!(runtime.registered_slot_keys().len(), 1);

        runtime.evict_slot("alice", "alice-filter");

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

    fn filter_module() -> &'static str {
        r#"(module (func (export "filter") (result i32) (i32.const 0)))"#
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
                wire_version: None,
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
