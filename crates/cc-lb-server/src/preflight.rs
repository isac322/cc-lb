use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DEFAULT_REDB_PATH, StorageConfig, TlsConfig};
use thiserror::Error;
use tokio::net::TcpListener;

use crate::storage_factory;
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
    #[error("failed to bind {addr}: {message}")]
    ListenerBind { addr: SocketAddr, message: String },
    #[error("missing TLS certificate or key file: {0}")]
    TlsCertMissing(PathBuf),
    #[error("failed to parse TLS files: {0}")]
    TlsParse(String),
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
}

pub async fn run(
    cfg: &Config,
    options: PreflightOptions,
) -> Result<PreflightReport, PreflightError> {
    run_inner(cfg, options, true).await
}

pub async fn run_offline(
    cfg: &Config,
    options: PreflightOptions,
) -> Result<PreflightReport, PreflightError> {
    run_inner(cfg, options, false).await
}

async fn run_inner(
    cfg: &Config,
    options: PreflightOptions,
    probe_storage: bool,
) -> Result<PreflightReport, PreflightError> {
    let mut report = PreflightReport::default();
    if probe_storage {
        let key_name = &cfg.aead.key_env;
        let key_hex =
            env::var(key_name).map_err(|_| PreflightError::MasterKeyMissing(key_name.clone()))?;
        let key = decode_master_key(key_name, &key_hex)?;
        report
            .successes
            .push(format!("storage master key resolved from {key_name}"));
        match &cfg.storage {
            StorageConfig::Redb { path } => validate_redb_path(path)?,
            StorageConfig::Postgres { url, .. } => storage_factory::probe_postgres_connection(url)
                .await
                .map_err(|error| PreflightError::Storage(error.to_string()))?,
        }
        let aead = Arc::new(AeadService::from_master_key(key));
        let _storage = storage_factory::open_storage(&cfg.storage, aead, key)
            .await
            .map_err(|error| PreflightError::Storage(error.to_string()))?;
        report.successes.push(storage_open_success(&cfg.storage));
    }
    if !options.skip_bind {
        bind_addr(cfg.listener.proxy_addr).await?;
        bind_addr(cfg.listener.admin_addr).await?;
        bind_addr(cfg.listener.metrics_addr).await?;
        report
            .successes
            .push("listener bindability checked".to_owned());
    }
    if let Some((_label, tls_config)) = active_tls_config(cfg) {
        verify_tls(tls_config)?;
        report.successes.push("tls files parsed".to_owned());
    }
    report.successes.push("ulimit check skipped".to_owned());
    Ok(report)
}

fn decode_master_key(env_name: &str, value: &str) -> Result<[u8; 32], PreflightError> {
    let value = value.trim();
    if value.len() != 64 {
        return Err(PreflightError::MasterKeyBadLength {
            actual: value.len() / 2,
        });
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
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

fn storage_open_success(config: &StorageConfig) -> String {
    match config {
        StorageConfig::Redb { path } => format!("storage opened: {}", path.display()),
        StorageConfig::Postgres { url, .. } => format!("storage opened: {}", postgres_host(url)),
    }
}

fn postgres_host(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "<unknown host>".to_owned())
}

fn validate_redb_path(path: &Path) -> Result<(), PreflightError> {
    if path == Path::new(DEFAULT_REDB_PATH) && !path.exists() {
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

fn active_tls_config(config: &Config) -> Option<(&'static str, &TlsConfig)> {
    config
        .listener
        .tls
        .as_ref()
        .map(|tls| ("listener", tls))
        .or_else(|| config.tls.as_ref().map(|tls| ("legacy", tls)))
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
