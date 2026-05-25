use std::fs;
use std::path::Path;

use thiserror::Error;

use crate::{
    Config, ConfigError, DEFAULT_REDB_PATH, DownstreamAuthMode, PluginRef, StorageConfig,
    UpstreamKind,
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
    validate_upstreams(config)?;
    validate_credentials_refs(config)?;
    validate_plugins(config)?;
    validate_storage(config)?;
    Ok(())
}

pub fn validate_raw_toml(raw_toml: &str) -> Result<(), ValidationError> {
    let Ok(value) = raw_toml.parse::<toml::Value>() else {
        return Ok(());
    };

    if has_legacy_plugin(&value) || has_legacy_principal_quotas(&value) {
        return Err(ValidationError::new("config", legacy_removed_message()));
    }

    if let Some(storage) = value.get("storage").and_then(|v| v.as_table()) {
        let has_kind = storage.contains_key("kind");
        let has_legacy_redb = storage.contains_key("redb_path");
        let has_legacy_aead = storage.contains_key("oauth_aead_key_env");
        if has_kind && (has_legacy_redb || has_legacy_aead) {
            let mut keys = Vec::new();
            if has_legacy_redb {
                keys.push("redb_path");
            }
            if has_legacy_aead {
                keys.push("oauth_aead_key_env");
            }
            return Err(ValidationError::new(
                "storage",
                format!(
                    "conflicting [storage] keys: `kind` cannot be mixed with legacy [{}]",
                    keys.join(", ")
                ),
            ));
        }
    }

    Ok(())
}

pub fn migrate_legacy_storage_toml(raw_toml: &str) -> Result<String, ValidationError> {
    let Ok(mut value) = raw_toml.parse::<toml::Value>() else {
        return Ok(raw_toml.to_owned());
    };

    let root = match value.as_table_mut() {
        Some(table) => table,
        None => return Ok(raw_toml.to_owned()),
    };

    let mut legacy_redb_path: Option<toml::Value> = None;
    let mut legacy_aead_env: Option<toml::Value> = None;
    let mut storage_was_legacy_only = false;

    if let Some(storage) = root.get_mut("storage").and_then(|v| v.as_table_mut()) {
        let had_legacy = storage.contains_key("redb_path") || storage.contains_key("oauth_aead_key_env");
        let had_kind = storage.contains_key("kind");
        legacy_redb_path = storage.remove("redb_path");
        legacy_aead_env = storage.remove("oauth_aead_key_env");
        if had_legacy && !had_kind {
            storage_was_legacy_only = true;
        }
    }

    if storage_was_legacy_only
        && let Some(storage) = root.get_mut("storage").and_then(|v| v.as_table_mut())
    {
        storage.insert("kind".to_owned(), toml::Value::String("redb".to_owned()));
        if let Some(path) = legacy_redb_path.take() {
            storage.insert("path".to_owned(), path);
        }
    }

    if let Some(env) = legacy_aead_env {
        let aead = root
            .entry("aead".to_owned())
            .or_insert_with(|| toml::Value::Table(toml::map::Map::new()));
        if let Some(aead_table) = aead.as_table_mut()
            && !aead_table.contains_key("key_env")
        {
            aead_table.insert("key_env".to_owned(), env);
        }
    }

    toml::to_string(&value)
        .map_err(|err| ValidationError::new("storage", format!("failed to migrate legacy [storage] block: {err}")))
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

fn has_legacy_plugin(value: &toml::Value) -> bool {
    value
        .get("plugins")
        .and_then(toml::Value::as_table)
        .and_then(|plugins| plugins.get(&["authn", "_", "plugin"].concat()))
        .is_some()
}

fn has_legacy_principal_quotas(value: &toml::Value) -> bool {
    let Some(principals) = value.get("principals").and_then(toml::Value::as_table) else {
        return false;
    };

    principals.values().any(|principal| {
        principal
            .as_table()
            .map(|table| table.contains_key("quotas"))
            .unwrap_or(false)
    })
}

fn legacy_removed_message() -> String {
    [
        "v2 removed `plugins.",
        &[
            "authn",
            "_",
            "plugin",
        ]
        .concat(),
        "` / `principals.*.quotas`; use `downstream_auth.mode` + `principals.*.default_limits` (sk-cclb-* API keys)",
    ]
    .concat()
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
