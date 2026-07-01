use std::fs;
use std::path::Path;

use thiserror::Error;

mod wasmtime;

use crate::{
    Config, ConfigError, DEFAULT_SQLITE_PATH, DownstreamAuthMode, EventBusTransport, StorageConfig,
};

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
    validate_downstream_auth(config)?;
    validate_tls(config)?;
    validate_storage(config)?;
    validate_oauth(config)?;
    validate_event_bus(config)?;
    wasmtime::validate_wasmtime_runtime(config)?;
    Ok(())
}

fn validate_event_bus(config: &Config) -> Result<(), ValidationError> {
    if !matches!(config.event_bus.transport, EventBusTransport::PgNotify) {
        return Ok(());
    }
    if matches!(config.storage, StorageConfig::Sqlite { .. }) {
        return Err(ValidationError::new(
            "event_bus.transport",
            "pg_notify transport requires postgres storage",
        ));
    }
    if config
        .cluster
        .instance_url
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err(ValidationError::new(
            "cluster.instance_url",
            "cluster.instance_url is required when event_bus.transport=pg_notify",
        ));
    }
    Ok(())
}

pub fn validate_runtime_overlay(config: &Config) -> Result<(), ConfigError> {
    validate_downstream_auth(config)?;
    validate_oauth(config)?;
    Ok(())
}

fn validate_tls(config: &Config) -> Result<(), ValidationError> {
    if let Some(tls) = &config.listener.tls {
        validate_tls_section("listener.tls", tls)?;
    }
    Ok(())
}

fn validate_downstream_auth(config: &Config) -> Result<(), ValidationError> {
    let none_mode_is_some = config.downstream_auth.none_mode.is_some();
    let should_have_none_mode = matches!(config.downstream_auth.mode, DownstreamAuthMode::None);

    if none_mode_is_some != should_have_none_mode {
        return Err(ValidationError::new(
            "downstream_auth.none_mode",
            "downstream_auth.none_mode must be set iff mode=none",
        ));
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

fn validate_storage(config: &Config) -> Result<(), ConfigError> {
    match &config.storage {
        StorageConfig::Postgres { url, pool } => {
            validate_postgres_url(url)?;
            if pool.statement_timeout_secs >= config.timeouts.upstream_total_secs {
                return Err(ConfigError::StatementTimeoutExceedsRequestTimeout {
                    statement: pool.statement_timeout_secs,
                    request: config.timeouts.upstream_total_secs,
                });
            }
        }
        StorageConfig::Sqlite { path } => {
            if path == Path::new(DEFAULT_SQLITE_PATH) && !path.exists() {
                return Ok(());
            }
            validate_sqlite_path(path)?;
        }
    }

    Ok(())
}

fn validate_sqlite_path(path: &Path) -> Result<(), ValidationError> {
    if path.exists() {
        let metadata = fs::metadata(path).map_err(|error| {
            ValidationError::new("storage.path", format!("cannot inspect path: {error}"))
        })?;
        if !metadata.is_file() {
            return Err(ValidationError::new(
                "storage.path",
                format!("not a file: {}", path.display()),
            ));
        }
        if metadata.permissions().readonly() {
            return Err(ValidationError::new(
                "storage.path",
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
            "storage.path",
            format!("parent directory does not exist: {}", parent.display()),
        )
    })?;
    if !parent_metadata.is_dir() {
        return Err(ValidationError::new(
            "storage.path",
            format!("parent path is not a directory: {}", parent.display()),
        ));
    }
    if parent_metadata.permissions().readonly() {
        return Err(ValidationError::new(
            "storage.path",
            format!("parent directory is not writable: {}", parent.display()),
        ));
    }

    Ok(())
}

fn validate_oauth(config: &Config) -> Result<(), ValidationError> {
    if let Some(anthropic) = &config.oauth.anthropic
        && anthropic.client_id.trim().is_empty()
    {
        return Err(ValidationError::new(
            "oauth.anthropic.client_id",
            "client_id cannot be empty",
        ));
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
