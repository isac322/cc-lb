use std::fs;
use std::path::Path;

use thiserror::Error;

use crate::{Config, ConfigError, DEFAULT_REDB_PATH, PluginRef, StorageConfig, UpstreamKind};

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{field}: {message}")]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl ValidationError {
    fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

pub fn validate_config(config: &Config) -> Result<(), ConfigError> {
    validate_tls(config)?;
    validate_upstreams(config)?;
    validate_credentials_refs(config)?;
    validate_plugins(config)?;
    validate_storage(config)?;
    Ok(())
}

fn validate_tls(config: &Config) -> Result<(), ValidationError> {
    if let Some(tls) = &config.listener.tls {
        validate_tls_section("listener.tls", tls)?;
    }
    if let Some(tls) = &config.tls {
        validate_tls_section("tls", tls)?;
    }
    Ok(())
}

fn validate_tls_section(prefix: &str, tls: &crate::TlsConfig) -> Result<(), ValidationError> {
    let cert_field = format!("{prefix}.cert_path");
    let cert_path = tls
        .cert_path
        .as_deref()
        .ok_or_else(|| ValidationError::new(cert_field.clone(), "missing TLS certificate path"))?;
    ensure_existing_file(&cert_field, cert_path)?;

    let key_field = format!("{prefix}.key_path");
    let key_path = tls
        .key_path
        .as_deref()
        .ok_or_else(|| ValidationError::new(key_field.clone(), "missing TLS key path"))?;
    ensure_existing_file(&key_field, key_path)
}

fn validate_upstreams(config: &Config) -> Result<(), ValidationError> {
    for (name, upstream) in &config.upstreams {
        match upstream.kind {
            UpstreamKind::AnthropicDirect => {}
            UpstreamKind::BedrockRuntime | UpstreamKind::BedrockMantle => {
                require_non_empty(
                    &format!("upstreams.{name}.region"),
                    upstream.region.as_deref(),
                    "region is required for this upstream kind",
                )?;
            }
            UpstreamKind::Vertex => {
                require_non_empty(
                    &format!("upstreams.{name}.region"),
                    upstream.region.as_deref(),
                    "region is required for vertex upstreams",
                )?;
                require_non_empty(
                    &format!("upstreams.{name}.project"),
                    upstream.project.as_deref(),
                    "project is required for vertex upstreams",
                )?;
            }
            UpstreamKind::Custom => {
                if upstream.base_url.is_none() {
                    return Err(ValidationError::new(
                        format!("upstreams.{name}.base_url"),
                        "base_url is required for custom upstreams",
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_credentials_refs(config: &Config) -> Result<(), ValidationError> {
    for (name, upstream) in &config.upstreams {
        if let Some(reference) = &upstream.credentials_ref {
            validate_credentials_ref(
                config,
                &format!("upstreams.{name}.credentials_ref"),
                reference,
            )?;
        }
    }

    for (name, principal) in &config.principals {
        if let Some(reference) = &principal.credentials_ref {
            validate_credentials_ref(
                config,
                &format!("principals.{name}.credentials_ref"),
                reference,
            )?;
        }
    }

    Ok(())
}

fn validate_credentials_ref(
    config: &Config,
    field: &str,
    reference: &str,
) -> Result<(), ValidationError> {
    if config.principals.contains_key(reference) || config.upstreams.contains_key(reference) {
        Ok(())
    } else {
        Err(ValidationError::new(
            field,
            format!("dangling credentials_ref \"{reference}\""),
        ))
    }
}

fn validate_plugins(config: &Config) -> Result<(), ValidationError> {
    if let Some(plugin) = &config.plugins.authn_plugin {
        validate_plugin_ref("plugins.authn_plugin", plugin)?;
    }

    if let Some(plugin) = &config.plugins.router_plugin {
        validate_plugin_ref("plugins.router_plugin", plugin)?;
    }

    for (index, plugin) in config.plugins.observability_hooks.iter().enumerate() {
        validate_plugin_ref(&format!("plugins.observability_hooks.{index}"), plugin)?;
    }

    Ok(())
}

fn validate_plugin_ref(prefix: &str, plugin: &PluginRef) -> Result<(), ValidationError> {
    require_non_empty(
        &format!("{prefix}.name"),
        Some(plugin.name.as_str()),
        "plugin name is required",
    )?;

    let wasm_path = plugin.wasm_path.as_deref().ok_or_else(|| {
        ValidationError::new(format!("{prefix}.wasm_path"), "missing plugin wasm path")
    })?;
    ensure_existing_file(&format!("{prefix}.wasm_path"), wasm_path)
}

fn validate_storage(config: &Config) -> Result<(), ConfigError> {
    match &config.storage {
        StorageConfig::Redb { path } => {
            if path == Path::new(DEFAULT_REDB_PATH) && !path.exists() {
                return Ok(());
            }
            validate_redb_path(path)?;
        }
        StorageConfig::Postgres { url, pool } => {
            validate_postgres_url(url)?;
            if pool.statement_timeout_secs >= config.timeouts.upstream_total_secs {
                return Err(ConfigError::StatementTimeoutExceedsRequestTimeout {
                    statement: pool.statement_timeout_secs,
                    request: config.timeouts.upstream_total_secs,
                });
            }
        }
    }

    Ok(())
}

pub fn validate_postgres_url(url: &str) -> Result<(), ConfigError> {
    let parsed = url::Url::parse(url).map_err(|_| ConfigError::InvalidPostgresUrl {
        message: "invalid URL syntax".to_owned(),
    })?;

    if !matches!(parsed.scheme(), "postgres" | "postgresql") {
        return Err(ConfigError::InvalidPostgresUrl {
            message: format!(
                "expected postgres:// or postgresql:// scheme, got '{}'",
                parsed.scheme()
            ),
        });
    }

    if parsed.host_str().map(str::is_empty).unwrap_or(true) {
        return Err(ConfigError::InvalidPostgresUrl {
            message: "missing host".to_owned(),
        });
    }

    Ok(())
}

fn validate_redb_path(path: &Path) -> Result<(), ValidationError> {
    if path.exists() {
        let metadata = fs::metadata(path).map_err(|error| {
            ValidationError::new("storage.redb_path", format!("cannot inspect path: {error}"))
        })?;
        if !metadata.is_file() {
            return Err(ValidationError::new(
                "storage.redb_path",
                format!("not a file: {}", path.display()),
            ));
        }
        if metadata.permissions().readonly() {
            return Err(ValidationError::new(
                "storage.redb_path",
                format!("file is not writable: {}", path.display()),
            ));
        }
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_metadata = fs::metadata(parent).map_err(|_| {
        ValidationError::new(
            "storage.redb_path",
            format!("parent directory does not exist: {}", parent.display()),
        )
    })?;
    if !parent_metadata.is_dir() {
        return Err(ValidationError::new(
            "storage.redb_path",
            format!("parent path is not a directory: {}", parent.display()),
        ));
    }
    if parent_metadata.permissions().readonly() {
        return Err(ValidationError::new(
            "storage.redb_path",
            format!("parent directory is not writable: {}", parent.display()),
        ));
    }

    Ok(())
}

fn require_non_empty(
    field: &str,
    value: Option<&str>,
    message: &str,
) -> Result<(), ValidationError> {
    match value.map(str::trim) {
        Some(value) if !value.is_empty() => Ok(()),
        _ => Err(ValidationError::new(field, message)),
    }
}

fn ensure_existing_file(field: &str, path: &Path) -> Result<(), ValidationError> {
    let metadata = fs::metadata(path)
        .map_err(|_| ValidationError::new(field, format!("file not found: {}", path.display())))?;

    if metadata.is_file() {
        Ok(())
    } else {
        Err(ValidationError::new(
            field,
            format!("not a file: {}", path.display()),
        ))
    }
}
