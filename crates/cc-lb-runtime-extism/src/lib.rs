#![forbid(unsafe_code)]

mod host_functions;
mod plugin_wrap;
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
    ObservabilityHook, PluginManifest, PluginRuntime, Principal, RouterPlugin, RuntimeError,
    SignerFactory, UpstreamDialect,
};
use extism::{Manifest, Plugin, PluginBuilder, Wasm};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::host_functions::{HostFunctionContext, HostState};
use crate::plugin_wrap::{ExtismDialectPlugin, ExtismRouterPlugin, ExtismSignerFactory};
use crate::sse_batch::ExtismObservabilityHook;

const DEFAULT_MEMORY_MAX_PAGES: u32 = 32;
const DEFAULT_FUEL_MAX: u64 = 10_000_000;
const DEFAULT_MAX_CALL_DURATION_MS: u64 = 1_000;
const DEFAULT_STORAGE_QUOTA_BYTES: usize = 1024 * 1024;
const DEFAULT_OBSERVE_BATCH_COUNT: usize = 32;
const DEFAULT_OBSERVE_FLUSH_MS: u64 = 100;

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

pub type SignerFactoryResolver =
    Arc<dyn Fn(&str, &Principal, &Value) -> Option<Arc<dyn SignerFactory>> + Send + Sync>;

pub struct ExtismRuntime {
    config: ExtismRuntimeConfig,
    manifests: RwLock<HashMap<String, PluginEntry>>,
    instances: RwLock<HashMap<String, Arc<PluginSlot>>>,
    host_state: Arc<HostState>,
    signer_factory_resolver: Option<SignerFactoryResolver>,
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
            signer_factory_resolver: None,
        }
    }

    pub fn with_signer_factory_resolver(resolver: SignerFactoryResolver) -> Self {
        Self {
            signer_factory_resolver: Some(resolver),
            ..Self::new()
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
        let entry = PluginEntry::from_manifest(manifest, &self.config)?;
        let cell = build_plugin_cell(&entry, self.host_state.clone())?;
        let name = entry.name.clone();

        if let Some(slot) = self
            .instances
            .read()
            .map_err(|_| runtime_error("instance registry lock poisoned"))?
            .get(&name)
            .cloned()
        {
            slot.replace(entry.clone(), cell)?;
            self.manifests
                .write()
                .map_err(|_| runtime_error("manifest registry lock poisoned"))?
                .insert(name, entry);
            return Ok(slot);
        }

        let slot = Arc::new(PluginSlot::new(entry.clone(), cell));
        self.manifests
            .write()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?
            .insert(name.clone(), entry);
        self.instances
            .write()
            .map_err(|_| runtime_error("instance registry lock poisoned"))?
            .insert(name, slot.clone());
        Ok(slot)
    }

    pub fn reload(&self, manifest_ref: &str) -> Result<(), RuntimeError> {
        let entry = self
            .manifests
            .read()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?
            .get(manifest_ref)
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
            .get(manifest_ref)
            .cloned()
            .ok_or_else(|| RuntimeError::InstantiateFailed {
                reason: format!("plugin instance not registered: {manifest_ref}"),
            })?;
        slot.replace(refreshed.clone(), cell)?;
        self.manifests
            .write()
            .map_err(|_| runtime_error("manifest registry lock poisoned"))?
            .insert(manifest_ref.to_owned(), refreshed);
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
