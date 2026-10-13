use std::collections::HashSet;
use std::fs;
use std::path::Path;

use thiserror::Error;

mod wasmtime;

use crate::{AdminAuthProviderConfig, Config, ConfigError, DEFAULT_SQLITE_PATH, StorageConfig};

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
    validate_admin_auth(config)?;
    validate_tls_filesystem(config)?;
    validate_storage(config)?;
    validate_storage_filesystem(config)?;
    validate_event_bus(config)?;
    validate_oauth(config)?;
    validate_upstream_affinity(config)?;
    validate_api_key_usage_poll(config)?;
    validate_request_diagnostics(config)?;
    wasmtime::validate_wasmtime_runtime(config)?;
    Ok(())
}

pub fn validate_config_without_filesystem(config: &Config) -> Result<(), ConfigError> {
    validate_admin_auth(config)?;
    validate_tls(config)?;
    validate_storage(config)?;
    validate_event_bus(config)?;
    validate_oauth(config)?;
    validate_upstream_affinity(config)?;
    validate_api_key_usage_poll(config)?;
    validate_request_diagnostics(config)?;
    wasmtime::validate_wasmtime_runtime(config)?;
    Ok(())
}

fn validate_admin_auth(config: &Config) -> Result<(), ValidationError> {
    let mut provider_ids = HashSet::new();

    for (index, provider) in config.admin.auth.providers.iter().enumerate() {
        let prefix = format!("admin.auth.providers[{index}]");
        let id = match provider {
            AdminAuthProviderConfig::StaticToken { id, token_env } => {
                if token_env.trim().is_empty() {
                    return Err(ValidationError::new(
                        format!("{prefix}.token_env"),
                        "static token environment variable must not be empty",
                    ));
                }
                id
            }
            AdminAuthProviderConfig::CloudflareAccess {
                id,
                team_domain,
                audiences,
                header,
            } => {
                validate_cloudflare_team_domain(&prefix, team_domain)?;
                if audiences.is_empty() {
                    return Err(ValidationError::new(
                        format!("{prefix}.audiences"),
                        "at least one Cloudflare Access audience is required",
                    ));
                }
                if http::HeaderName::from_bytes(header.as_bytes()).is_err() {
                    return Err(ValidationError::new(
                        format!("{prefix}.header"),
                        "Cloudflare Access header must be a valid HTTP header name",
                    ));
                }
                id
            }
        };

        if id.trim().is_empty() {
            return Err(ValidationError::new(
                format!("{prefix}.id"),
                "admin authentication provider id must not be empty",
            ));
        }
        if !provider_ids.insert(id.as_str()) {
            return Err(ValidationError::new(
                format!("{prefix}.id"),
                "admin authentication provider id must be unique",
            ));
        }
    }

    Ok(())
}

fn validate_cloudflare_team_domain(prefix: &str, team_domain: &str) -> Result<(), ValidationError> {
    let field = format!("{prefix}.team_domain");
    let parsed = url::Url::parse(team_domain).map_err(|_| {
        ValidationError::new(
            field.clone(),
            "Cloudflare Access team domain must be a valid HTTPS URL",
        )
    })?;

    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return Err(ValidationError::new(
            field,
            "Cloudflare Access team domain must be an HTTPS origin without a path or query",
        ));
    }

    Ok(())
}
fn validate_upstream_affinity(config: &Config) -> Result<(), ValidationError> {
    if config.upstream_affinity.ttl_days == 0 {
        return Err(ValidationError::new(
            "upstream_affinity.ttl_days",
            "upstream affinity TTL must be greater than zero",
        ));
    }
    Ok(())
}

/// Smallest `api_key_usage_poll` interval: each tick sends up to two org
/// usage-report requests per upstream.
const API_KEY_USAGE_POLL_MIN_INTERVAL_SECS: u64 = 60;
/// Largest `api_key_usage_poll` interval (7 days): the hourly usage window
/// spans the last 7 days, so a longer interval leaves gaps. The bound also
/// keeps the scheduler's first tick (`interval + jitter`, jitter below the
/// interval) far inside the `DateTime` range.
const API_KEY_USAGE_POLL_MAX_INTERVAL_SECS: u64 = 7 * 24 * 60 * 60;

fn validate_api_key_usage_poll(config: &Config) -> Result<(), ValidationError> {
    let Some(job) = config.scheduler.recurring_jobs.get("api_key_usage_poll") else {
        return Ok(());
    };
    if !(API_KEY_USAGE_POLL_MIN_INTERVAL_SECS..=API_KEY_USAGE_POLL_MAX_INTERVAL_SECS)
        .contains(&job.interval_secs)
    {
        return Err(ValidationError::new(
            "scheduler.recurring_jobs.api_key_usage_poll.interval_secs",
            format!(
                "API-key usage poll interval must be between {API_KEY_USAGE_POLL_MIN_INTERVAL_SECS} and {API_KEY_USAGE_POLL_MAX_INTERVAL_SECS} seconds"
            ),
        ));
    }
    if job.jitter_secs >= job.interval_secs {
        return Err(ValidationError::new(
            "scheduler.recurring_jobs.api_key_usage_poll.jitter_secs",
            "API-key usage poll jitter must be less than its interval",
        ));
    }
    Ok(())
}

fn validate_event_bus(config: &Config) -> Result<(), ValidationError> {
    if matches!(config.storage, StorageConfig::Sqlite { .. }) {
        return Ok(());
    }
    if config
        .cluster
        .instance_url
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err(ValidationError::new(
            "cluster.instance_url",
            "cluster.instance_url is required when storage.kind=postgres (pg_notify fanout is always enabled)",
        ));
    }
    Ok(())
}

fn validate_tls(config: &Config) -> Result<(), ValidationError> {
    validate_tls_paths(config, |_, _| Ok(()))
}

fn validate_tls_filesystem(config: &Config) -> Result<(), ValidationError> {
    validate_tls_paths(config, ensure_existing_file)
}

fn validate_tls_paths(
    config: &Config,
    mut validate_path: impl FnMut(&str, &Path) -> Result<(), ValidationError>,
) -> Result<(), ValidationError> {
    if let Some(tls) = &config.listener.tls {
        let cert_path = tls.cert_path.as_deref().ok_or_else(|| {
            ValidationError::new("listener.tls.cert_path", "missing TLS certificate path")
        })?;
        validate_path("listener.tls.cert_path", cert_path)?;

        let key_path = tls
            .key_path
            .as_deref()
            .ok_or_else(|| ValidationError::new("listener.tls.key_path", "missing TLS key path"))?;
        validate_path("listener.tls.key_path", key_path)?;
    }
    Ok(())
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
        StorageConfig::Sqlite { .. } => {}
    }

    Ok(())
}

fn validate_storage_filesystem(config: &Config) -> Result<(), ConfigError> {
    if let StorageConfig::Sqlite { path } = &config.storage {
        if path == Path::new(DEFAULT_SQLITE_PATH) && !path.exists() {
            return Ok(());
        }
        validate_sqlite_path(path)?;
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

/// Stripping diagnostic fields from stored rows is only lossless when the
/// stdout diagnostics log carries them instead.
fn validate_request_diagnostics(config: &Config) -> Result<(), ValidationError> {
    if config.request_event_strip_diagnostics && !config.observability.request_diagnostics_log {
        return Err(ValidationError::new(
            "request_event_strip_diagnostics",
            "request_event_strip_diagnostics requires observability.request_diagnostics_log = true; otherwise diagnostic fields are discarded",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(strip: bool, log: bool) -> Config {
        let mut config = Config {
            request_event_strip_diagnostics: strip,
            ..Config::default()
        };
        config.observability.request_diagnostics_log = log;
        config
    }

    #[test]
    fn request_diagnostics_defaults_are_off_and_valid() {
        let config = Config::default();
        assert!(!config.request_event_strip_diagnostics);
        assert!(!config.observability.request_diagnostics_log);
        assert_eq!(validate_request_diagnostics(&config), Ok(()));
    }

    #[test]
    fn strip_without_diagnostics_log_is_rejected() {
        let config = config_with(true, false);
        let expected = Err(ValidationError::new(
            "request_event_strip_diagnostics",
            "request_event_strip_diagnostics requires observability.request_diagnostics_log = true; otherwise diagnostic fields are discarded",
        ));
        assert_eq!(validate_request_diagnostics(&config), expected);

        let entry_points = [
            ("validate_config", validate_config(&config)),
            (
                "validate_config_without_filesystem",
                validate_config_without_filesystem(&config),
            ),
        ];
        for (name, result) in entry_points {
            let error = match result {
                Ok(()) => panic!("{name} accepted strip without diagnostics log"),
                Err(error) => error.to_string(),
            };
            assert!(
                error.starts_with("request_event_strip_diagnostics: "),
                "{name} returned an unexpected error: {error}"
            );
        }
    }

    #[test]
    fn every_other_combination_is_accepted() {
        for (strip, log) in [(false, false), (false, true), (true, true)] {
            let config = config_with(strip, log);
            let label = format!("strip={strip} log={log}");
            assert_eq!(validate_request_diagnostics(&config), Ok(()), "{label}");
            assert!(
                validate_config_without_filesystem(&config).is_ok(),
                "{label}"
            );
        }
    }
}
