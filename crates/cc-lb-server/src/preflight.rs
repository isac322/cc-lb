use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use cc_lb_config::{Config, PluginRef, TlsConfig};
use cc_lb_plugin_api::{PluginManifest, PluginRuntime};
use cc_lb_runtime_extism::ExtismRuntime;
use cc_lb_storage_redb::Storage;
use thiserror::Error;
use tokio::net::TcpListener;

use crate::tls;

#[derive(Debug, Default, Clone)]
pub struct PreflightReport {
    pub warnings: Vec<String>,
    pub successes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PreflightOptions {
    pub skip_bind: bool,
}

#[derive(Debug, Error)]
pub enum PreflightError {
    #[error("storage master key env {0} is missing")]
    MasterKeyMissing(String),
    #[error("storage master key must decode to 32 bytes; got {actual}")]
    MasterKeyBadLength { actual: usize },
    #[error("storage master key env {0} must be hex")]
    MasterKeyBadHex(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("plugin: {0}")]
    Plugin(String),
    #[error("plugin {name}: missing wasm_path")]
    PluginMissingArtifact { name: String },
    #[error("failed to bind {addr}: {message}")]
    ListenerBind { addr: SocketAddr, message: String },
    #[error("missing TLS certificate or key file: {0}")]
    TlsCertMissing(PathBuf),
    #[error("failed to parse TLS files: {0}")]
    TlsParse(String),
}

pub async fn run(
    cfg: &Config,
    options: PreflightOptions,
) -> Result<PreflightReport, PreflightError> {
    let mut report = PreflightReport::default();

    if let Some(redb_path) = &cfg.storage.redb_path {
        let key_name = &cfg.storage.oauth_aead_key_env;
        let key_hex =
            env::var(key_name).map_err(|_| PreflightError::MasterKeyMissing(key_name.clone()))?;
        let key = decode_master_key(key_name, &key_hex)?;
        report
            .successes
            .push(format!("storage master key resolved from {key_name}"));
        let _storage = Storage::open(redb_path, key)
            .map_err(|error| PreflightError::Storage(error.to_string()))?;
        report
            .successes
            .push(format!("storage opened: {}", redb_path.display()));
    }

    let runtime = ExtismRuntime::new();
    if let Some(plugin) = &cfg.plugins.authn_plugin {
        dry_load_plugin(&runtime, plugin, PluginLoadKind::Authn)?;
        report
            .successes
            .push(format!("plugin {} dry-loaded", plugin.name));
    }
    if let Some(plugin) = &cfg.plugins.router_plugin {
        dry_load_plugin(&runtime, plugin, PluginLoadKind::Router)?;
        report
            .successes
            .push(format!("plugin {} dry-loaded", plugin.name));
    }
    for plugin in &cfg.plugins.observability_hooks {
        dry_load_plugin(&runtime, plugin, PluginLoadKind::Observability)?;
        report
            .successes
            .push(format!("plugin {} dry-loaded", plugin.name));
    }

    for name in cfg.upstreams.keys() {
        report.warnings.push(format!(
            "upstream {name}: probe skipped (offline preflight)"
        ));
    }

    if !options.skip_bind {
        bind_addr(cfg.listener.proxy_addr).await?;
        bind_addr(cfg.listener.admin_addr).await?;
        bind_addr(cfg.listener.metrics_addr).await?;
        report
            .successes
            .push("listener bindability checked".to_owned());
    }

    if let Some((_, tls_config)) = active_tls_config(cfg) {
        verify_tls(tls_config)?;
        report.successes.push("tls files parsed".to_owned());
    }

    if let Some(warning) = ulimit_warning() {
        report.warnings.push(warning);
    } else {
        report.successes.push("ulimit checked".to_owned());
    }

    Ok(report)
}

fn decode_master_key(env_name: &str, value: &str) -> Result<[u8; 32], PreflightError> {
    if value.len() != 64 {
        return Err(PreflightError::MasterKeyBadLength {
            actual: value.len() / 2,
        });
    }

    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high = hex_nibble(chunk[0])
            .ok_or_else(|| PreflightError::MasterKeyBadHex(env_name.to_owned()))?;
        let low = hex_nibble(chunk[1])
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

enum PluginLoadKind {
    Authn,
    Router,
    Observability,
}

fn dry_load_plugin(
    runtime: &ExtismRuntime,
    plugin: &PluginRef,
    kind: PluginLoadKind,
) -> Result<(), PreflightError> {
    let manifest = manifest_from_plugin(plugin)?;
    let result = match kind {
        PluginLoadKind::Authn => runtime.instantiate(&manifest).map(|_| ()),
        PluginLoadKind::Router => runtime.instantiate_router(&manifest).map(|_| ()),
        PluginLoadKind::Observability => runtime.instantiate_observability(&manifest).map(|_| ()),
    };

    result.map_err(|error| PreflightError::Plugin(error.to_string()))
}

fn manifest_from_plugin(plugin: &PluginRef) -> Result<PluginManifest, PreflightError> {
    let artifact =
        plugin
            .wasm_path
            .as_ref()
            .ok_or_else(|| PreflightError::PluginMissingArtifact {
                name: plugin.name.clone(),
            })?;
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "observe_batch_count".to_owned(),
        serde_json::Value::from(plugin.batched_events_per_flush),
    );
    metadata.insert(
        "observe_flush_ms".to_owned(),
        serde_json::Value::from(plugin.batched_flush_ms),
    );
    Ok(PluginManifest {
        name: plugin.name.clone(),
        artifact: artifact.display().to_string(),
        config: plugin.config.clone(),
        metadata,
    })
}

fn active_tls_config(config: &Config) -> Option<(&'static str, &TlsConfig)> {
    if let Some(tls) = &config.listener.tls {
        Some(("listener.tls", tls))
    } else {
        config.tls.as_ref().map(|tls| ("tls", tls))
    }
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
        .map_err(|error| PreflightError::TlsParse(error.to_string()))?;
    Ok(())
}

async fn bind_addr(addr: SocketAddr) -> Result<(), PreflightError> {
    TcpListener::bind(addr)
        .await
        .map(drop)
        .map_err(|source| PreflightError::ListenerBind {
            addr,
            message: source.to_string(),
        })
}

fn ulimit_warning() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        use nix::sys::resource::{getrlimit, Resource};

        match getrlimit(Resource::RLIMIT_NOFILE) {
            Ok((soft, _hard)) if soft < 65_536 => Some(format!(
                "ulimit: RLIMIT_NOFILE soft limit {soft} is below 65536"
            )),
            Ok(_) => None,
            Err(error) => Some(format!("ulimit: could not read RLIMIT_NOFILE: {error}")),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
