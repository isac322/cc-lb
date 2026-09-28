use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use cc_lb_clock::ClockHandle;
use cc_lb_config::{Config, DEFAULT_SQLITE_PATH, StorageConfig, TlsConfig};
use cc_lb_storage_api::{
    PluginChainEntry, PluginSlotKind, PrincipalRecord, StorageError as ApiStorageError,
    UpstreamRecord, WasmRegistryEntry,
};
use thiserror::Error;
use tokio::net::TcpListener;

use crate::dynamic_view_builder::Stores;
use crate::storage_factory;
use crate::tls;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PreflightReport {
    pub upstream_count: usize,
    pub upstream_warnings: usize,
    pub principal_count: usize,
    pub principal_disabled_count: usize,
    pub plugin_chain_entry_count: usize,
    pub plugin_blob_missing_count: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PreflightOptions {
    pub skip_bind: bool,
}

#[derive(Debug, Error)]
pub enum PreflightError {
    #[error("storage master key env {0} is missing")]
    MasterKeyMissing(String),
    #[error("cluster token env {0} is missing or empty")]
    ClusterTokenMissing(String),

    #[error("storage master key must decode to 32 bytes; got {actual}")]
    MasterKeyBadLength { actual: usize },
    #[error("storage master key env {0} must be hex")]
    MasterKeyBadHex(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("failed to bind {addr}: {message}")]
    ListenerBind { addr: SocketAddr, message: String },
    #[error("missing TLS certificate or key file: {0}")]
    TlsCertMissing(PathBuf),
    #[error("failed to parse TLS files: {0}")]
    TlsParse(String),
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
}

pub async fn run_preflight(
    stores: &Stores,
    data_dir: &Path,
) -> Result<PreflightReport, PreflightError> {
    let mut report = PreflightReport::default();

    let upstreams = list_upstreams(stores).await?;
    report.upstream_count = upstreams.len();

    let principals = list_principals(stores).await?;
    report.principal_count = principals.len();
    for principal in &principals {
        if !principal.enabled {
            report.principal_disabled_count += 1;
        }
        tracing::info!(
            principal_id = %principal.id,
            principal_name = principal.name.as_str(),
            allowed_models_count = principal.allowed_models.len(),
            enabled = principal.enabled,
            "preflight principal summary"
        );
    }

    let chain_entries = list_plugin_chain_entries(stores, &principals).await?;
    report.plugin_chain_entry_count = chain_entries.len();
    for entry in &chain_entries {
        check_plugin_chain_entry(stores, data_dir, entry, &mut report).await?;
    }

    Ok(report)
}

pub async fn run(
    cfg: &Config,
    options: PreflightOptions,
    clock: ClockHandle,
) -> Result<PreflightReport, PreflightError> {
    run_inner(cfg, options, true, clock).await
}

pub async fn run_offline(
    cfg: &Config,
    options: PreflightOptions,
    clock: ClockHandle,
) -> Result<PreflightReport, PreflightError> {
    run_inner(cfg, options, false, clock).await
}

async fn run_inner(
    cfg: &Config,
    options: PreflightOptions,
    probe_storage: bool,
    clock: ClockHandle,
) -> Result<PreflightReport, PreflightError> {
    let report = PreflightReport::default();
    if matches!(cfg.storage, StorageConfig::Postgres { .. })
        && env::var(&cfg.cluster.token_env)
            .ok()
            .is_none_or(|token| token.trim().is_empty())
    {
        return Err(PreflightError::ClusterTokenMissing(
            cfg.cluster.token_env.clone(),
        ));
    }

    if probe_storage {
        let key_name = &cfg.aead.key_env;
        let key_hex =
            env::var(key_name).map_err(|_| PreflightError::MasterKeyMissing(key_name.clone()))?;
        decode_master_key(key_name, &key_hex)?;
        match &cfg.storage {
            StorageConfig::Postgres { url, .. } => storage_factory::probe_postgres_connection(url)
                .await
                .map_err(|error| PreflightError::Storage(error.to_string()))?,
            StorageConfig::Sqlite { path } => validate_sqlite_path(path)?,
        }
        storage_factory::open_storage(&cfg.storage, clock)
            .await
            .map_err(|error| PreflightError::Storage(error.to_string()))?;
    }
    if !options.skip_bind {
        bind_addr(cfg.listener.proxy_addr).await?;
        bind_addr(cfg.listener.admin_addr).await?;
        bind_addr(cfg.listener.metrics_addr).await?;
    }
    if let Some(tls_config) = active_tls_config(cfg) {
        verify_tls(tls_config)?;
    }
    Ok(report)
}

async fn list_upstreams(stores: &Stores) -> Result<Vec<UpstreamRecord>, PreflightError> {
    let mut upstreams = Vec::new();
    let mut after = None;
    loop {
        let page = stores
            .upstreams
            .list(after, 100)
            .await
            .map_err(storage_error)?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        upstreams.extend(
            page.into_iter()
                .filter(|record| record.deleted_at_unix_secs.is_none()),
        );
    }
    Ok(upstreams)
}

async fn list_principals(stores: &Stores) -> Result<Vec<PrincipalRecord>, PreflightError> {
    let mut principals = Vec::new();
    let mut offset = 0;
    loop {
        let page = stores
            .principals
            .list(offset, 100, false)
            .await
            .map_err(storage_error)?;
        if page.is_empty() {
            break;
        }
        offset += page.len();
        principals.extend(page);
    }
    Ok(principals)
}

async fn list_plugin_chain_entries(
    stores: &Stores,
    principals: &[PrincipalRecord],
) -> Result<Vec<PluginChainEntry>, PreflightError> {
    let mut entries = Vec::new();
    for principal in principals {
        for slot in [PluginSlotKind::Router, PluginSlotKind::Shape] {
            entries.extend(
                stores
                    .plugin_registry
                    .list_chain_for_principal(principal.id, slot)
                    .await
                    .map_err(storage_error)?,
            );
        }
    }
    Ok(entries)
}

async fn check_plugin_chain_entry(
    stores: &Stores,
    data_dir: &Path,
    entry: &PluginChainEntry,
    report: &mut PreflightReport,
) -> Result<(), PreflightError> {
    let Some(registry_entry) = stores
        .plugin_registry
        .get_registry_entry_by_id(entry.wasm_registry_id)
        .await
        .map_err(storage_error)?
    else {
        push_plugin_warning(
            report,
            format!(
                "plugin chain entry {}: missing wasm registry row {}",
                entry.id, entry.wasm_registry_id
            ),
        );
        return Ok(());
    };

    if registry_entry_unsupported_slot(&registry_entry, entry.slot) {
        report.warnings.push(format!(
            "plugin chain entry {}: registry entry {} ({}) unsupported slot {}; supported slots: {}",
            entry.id,
            registry_entry.id,
            registry_entry.name.as_str(),
            entry.slot.as_str(),
            supported_slot_names(&registry_entry).join(", ")
        ));
    }

    if stores
        .plugin_registry
        .get_blob(registry_entry.sha256)
        .await
        .map_err(storage_error)?
        .is_none()
    {
        push_plugin_warning(
            report,
            format!(
                "plugin chain entry {}: missing wasm blob {}",
                entry.id,
                hex_sha256(registry_entry.sha256)
            ),
        );
        return Ok(());
    }

    let cache_path = wasm_cache_path(data_dir, registry_entry.sha256);
    match tokio::fs::try_exists(&cache_path).await {
        Ok(true) => tracing::info!(
            plugin_chain_entry_id = %entry.id,
            wasm_sha256 = hex_sha256(registry_entry.sha256).as_str(),
            cache_path = %cache_path.display(),
            "preflight plugin wasm cache present"
        ),
        Ok(false) => tracing::info!(
            plugin_chain_entry_id = %entry.id,
            wasm_sha256 = hex_sha256(registry_entry.sha256).as_str(),
            cache_path = %cache_path.display(),
            "preflight plugin wasm cache will be materialized at first use"
        ),
        Err(error) => tracing::info!(
            plugin_chain_entry_id = %entry.id,
            wasm_sha256 = hex_sha256(registry_entry.sha256).as_str(),
            cache_path = %cache_path.display(),
            error = %error,
            "preflight plugin wasm cache existence check failed"
        ),
    }

    Ok(())
}

fn registry_entry_unsupported_slot(
    registry_entry: &WasmRegistryEntry,
    slot: PluginSlotKind,
) -> bool {
    !registry_entry.is_builtin && !registry_entry.supported_slots.contains(&slot)
}

fn supported_slot_names(registry_entry: &WasmRegistryEntry) -> Vec<&'static str> {
    registry_entry
        .supported_slots
        .iter()
        .map(|slot| slot.as_str())
        .collect()
}

fn push_plugin_warning(report: &mut PreflightReport, warning: String) {
    report.plugin_blob_missing_count += 1;
    report.warnings.push(warning);
}

fn wasm_cache_path(data_dir: &Path, sha256: [u8; 32]) -> PathBuf {
    data_dir
        .join("plugins")
        .join("wasm")
        .join("cache")
        .join(format!("{}.wasm", hex_sha256(sha256)))
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in sha256 {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn storage_error(error: ApiStorageError) -> PreflightError {
    PreflightError::Storage(error.to_string())
}

fn decode_master_key(env_name: &str, value: &str) -> Result<[u8; 32], PreflightError> {
    let value = value.trim();
    if value.len() != 64 {
        return Err(PreflightError::MasterKeyBadLength {
            actual: value.len() / 2,
        });
    }
    let mut key = [0_u8; 32];
    for (index, &[high_byte, low_byte]) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = hex_nibble(high_byte)
            .ok_or_else(|| PreflightError::MasterKeyBadHex(env_name.to_owned()))?;
        let low = hex_nibble(low_byte)
            .ok_or_else(|| PreflightError::MasterKeyBadHex(env_name.to_owned()))?;
        key[index] = (high << 4) | low;
    }
    Ok(key)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn validate_sqlite_path(path: &Path) -> Result<(), PreflightError> {
    if path == Path::new(DEFAULT_SQLITE_PATH) && !path.exists() {
        return Ok(());
    }
    if path.exists() {
        let metadata = fs::metadata(path).map_err(|error| {
            PreflightError::Storage(format!("cannot inspect path {}: {error}", path.display()))
        })?;
        if !metadata.is_file() {
            return Err(PreflightError::Storage(format!(
                "not a file: {}",
                path.display()
            )));
        }
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_metadata = fs::metadata(parent).map_err(|_| {
        PreflightError::Storage(format!(
            "parent directory does not exist: {}",
            parent.display()
        ))
    })?;
    if !parent_metadata.is_dir() {
        return Err(PreflightError::Storage(format!(
            "parent path is not a directory: {}",
            parent.display()
        )));
    }
    Ok(())
}

fn active_tls_config(config: &Config) -> Option<&TlsConfig> {
    config.listener.tls.as_ref()
}

fn verify_tls(tls_config: &TlsConfig) -> Result<(), PreflightError> {
    let cert_path = tls_config
        .cert_path
        .as_ref()
        .ok_or_else(|| PreflightError::TlsCertMissing(PathBuf::from("listener.tls.cert_path")))?;
    let key_path = tls_config
        .key_path
        .as_ref()
        .ok_or_else(|| PreflightError::TlsCertMissing(PathBuf::from("listener.tls.key_path")))?;
    if !cert_path.exists() {
        return Err(PreflightError::TlsCertMissing(cert_path.clone()));
    }
    if !key_path.exists() {
        return Err(PreflightError::TlsCertMissing(key_path.clone()));
    }
    tls::load_certs(cert_path, key_path)
        .map(|_| ())
        .map_err(|error| PreflightError::TlsParse(error.to_string()))
}

async fn bind_addr(addr: SocketAddr) -> Result<(), PreflightError> {
    let listener =
        TcpListener::bind(addr)
            .await
            .map_err(|source| PreflightError::ListenerBind {
                addr,
                message: source.to_string(),
            })?;
    drop(listener);
    Ok(())
}
