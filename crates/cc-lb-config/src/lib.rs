#![forbid(unsafe_code)]

mod types;
mod validation;

use std::env;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use thiserror::Error;

pub use types::{
    ADMIN_AUTH_PROVIDERS_JSON_ENV, AdminAuthConfig, AdminAuthProviderConfig, AdminConfig,
    AnthropicOAuthConfig, AntigravityOAuthConfig, BodyConfig, BulkheadConfig, CircuitBreakerConfig,
    Config, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES, DEFAULT_MAX_BUFFERED_RESPONSE_BYTES,
    DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV,
    DEFAULT_PGBOUNCER_STATEMENT_CACHE_CAPACITY, DEFAULT_SQLITE_PATH,
    DEFAULT_TLS_HANDSHAKE_TIMEOUT_SECS, DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS,
    DEFAULT_UPSTREAM_CONNECT_SECS, EventBusConfig, GeminiOAuthConfig,
    LONG_LIVED_ACCESS_TOKEN_EXPIRES_IN_SECS, LONG_LIVED_MIN_GRANT_SECS, ListenerConfig,
    ObservabilityConfig, OpenAiOAuthConfig, PluginWireBounds, PostgresPoolConfig,
    PriceCatalogConfig, PromptCacheShadowConfig, RecurringJobConfig, RuntimeConfig,
    SchedulerConfig, SchedulerPoolConfig, ShapeOriginPolicy, StorageConfig,
    SubscriptionQuotaConfig, TimeoutsConfig, TlsConfig, UnavailableOpenaiReasoningOwnerPolicy,
    UnknownOpenaiReasoningPolicy, UpstreamAffinityConfig, WarmupConfig, WasmtimeAllocationStrategy,
    WasmtimeConfig,
};
pub use validation::{ValidationError, validate_postgres_url};

pub const STORAGE_URL_REDACTION_SENTINEL: &str = "__CC_LB_STORAGE_URL_UNCHANGED__";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConfigEnvOverride {
    pub path: String,
    pub name: String,
    pub sensitive: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load config: {0}")]
    Figment(#[source] Box<figment::Error>),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("invalid postgres URL: {message}")]
    InvalidPostgresUrl { message: String },
    #[error("invalid {env}: {message}")]
    InvalidAdminAuthProvidersEnv { env: &'static str, message: String },
    #[error("postgres statement timeout {statement}s must be less than request timeout {request}s")]
    StatementTimeoutExceedsRequestTimeout { statement: u64, request: u64 },
    #[error("invalid TOML: {0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("failed to serialize TOML: {0}")]
    TomlSerialize(#[from] toml::ser::Error),
}

impl From<figment::Error> for ConfigError {
    fn from(error: figment::Error) -> Self {
        Self::Figment(Box::new(error))
    }
}

impl Config {
    pub fn load(toml_path: &Path) -> Result<Self, ConfigError> {
        let mut config: Config = Figment::new()
            .merge(Toml::file_exact(toml_path))
            .merge(Self::config_env())
            .extract()?;

        config.apply_admin_auth_env_override()?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_stored_toml(raw_toml: &str) -> Result<Self, ConfigError> {
        Ok(Figment::from(Toml::string(raw_toml)).extract()?)
    }

    pub fn from_stored_toml_with_env(raw_toml: &str) -> Result<Self, ConfigError> {
        let mut config: Config = Figment::new()
            .merge(Toml::string(raw_toml))
            .merge(Self::config_env())
            .extract()?;
        config.apply_admin_auth_env_override()?;
        Ok(config)
    }

    pub fn partial_toml_json(raw_toml: &str) -> Result<Value, ConfigError> {
        Ok(toml::from_str(raw_toml)?)
    }

    pub fn partial_json_toml(mut value: Value) -> Result<String, ConfigError> {
        normalize_json_unset(&mut value);
        Ok(toml::to_string_pretty(&value)?)
    }

    pub fn environment_overrides() -> Vec<ConfigEnvOverride> {
        let schema =
            serde_json::to_value(Self::json_schema()).expect("Config JSON schema must serialize");
        let mut overrides = env::vars_os()
            .filter_map(|(name, _)| name.into_string().ok())
            .filter_map(|name| {
                if matches!(
                    name.as_str(),
                    ADMIN_AUTH_PROVIDERS_JSON_ENV
                        | DEFAULT_ADMIN_TOKEN_ENV
                        | DEFAULT_OAUTH_AEAD_KEY_ENV
                ) {
                    return None;
                }
                let encoded_path = name.strip_prefix("CC_LB_")?;
                let segments = encoded_path
                    .split("__")
                    .map(|segment| segment.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                if segments.is_empty() || !schema_has_path(&schema, &schema, &segments) {
                    return None;
                }
                let path = segments.join(".");
                Some(ConfigEnvOverride {
                    sensitive: matches!(path.as_str(), "storage.url" | "storage.pool.listener_url"),
                    path,
                    name,
                })
            })
            .collect::<Vec<_>>();
        overrides
            .sort_by(|left, right| left.path.cmp(&right.path).then(left.name.cmp(&right.name)));
        overrides
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validation::validate_config(self)
    }

    pub fn validate_without_filesystem(&self) -> Result<(), ConfigError> {
        validation::validate_config_without_filesystem(self)
    }

    pub fn json_schema() -> schemars::Schema {
        schemars::schema_for!(Config)
    }

    fn config_env() -> Env {
        let schema =
            serde_json::to_value(Self::json_schema()).expect("Config JSON schema must serialize");
        let root = schema.clone();
        Env::prefixed("CC_LB_")
            .filter(move |key| {
                let segments = key
                    .as_str()
                    .split("__")
                    .map(|segment| segment.to_ascii_lowercase())
                    .collect::<Vec<_>>();
                schema_has_path(&root, &root, &segments)
            })
            .split("__")
    }

    fn apply_admin_auth_env_override(&mut self) -> Result<(), ConfigError> {
        let encoded = match env::var(ADMIN_AUTH_PROVIDERS_JSON_ENV) {
            Ok(encoded) => encoded,
            Err(env::VarError::NotPresent) => return Ok(()),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidAdminAuthProvidersEnv {
                    env: ADMIN_AUTH_PROVIDERS_JSON_ENV,
                    message: "value must be valid UTF-8 JSON".to_owned(),
                });
            }
        };
        let providers: Vec<AdminAuthProviderConfig> =
            serde_json::from_str(&encoded).map_err(|error| {
                ConfigError::InvalidAdminAuthProvidersEnv {
                    env: ADMIN_AUTH_PROVIDERS_JSON_ENV,
                    message: error.to_string(),
                }
            })?;
        if providers.is_empty() {
            return Err(ConfigError::InvalidAdminAuthProvidersEnv {
                env: ADMIN_AUTH_PROVIDERS_JSON_ENV,
                message: "provider list must not be empty".to_owned(),
            });
        }
        self.admin.auth.providers = providers;
        Ok(())
    }
}

fn normalize_json_unset(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.retain(|_, value| !value.is_null());
            for value in object.values_mut() {
                normalize_json_unset(value);
            }
        }
        Value::Array(values) => {
            values.retain(|value| !value.is_null());
            for value in values {
                normalize_json_unset(value);
            }
        }
        _ => {}
    }
}

fn schema_has_path(root: &Value, schema: &Value, segments: &[String]) -> bool {
    let Some((segment, remaining)) = segments.split_first() else {
        return true;
    };
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str)
        && let Some(target) = reference
            .strip_prefix("#/")
            .and_then(|path| path.split('/').try_fold(root, |value, key| value.get(key)))
    {
        return schema_has_path(root, target, segments);
    }
    for combinator in ["anyOf", "oneOf", "allOf"] {
        if schema
            .get(combinator)
            .and_then(Value::as_array)
            .is_some_and(|variants| {
                variants
                    .iter()
                    .any(|variant| schema_has_path(root, variant, segments))
            })
        {
            return true;
        }
    }
    if let Some(property) = schema
        .get("properties")
        .and_then(Value::as_object)
        .and_then(|properties| {
            properties
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(segment))
                .map(|(_, schema)| schema)
        })
    {
        return schema_has_path(root, property, remaining);
    }
    if let Some(additional) = schema
        .get("additionalProperties")
        .filter(|value| value.is_object())
    {
        return schema_has_path(root, additional, remaining);
    }
    schema
        .get("items")
        .is_some_and(|items| schema_has_path(root, items, remaining))
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    const LEGACY_CLUSTER_ENV_CHILD: &str = "CC_LB_TEST_LEGACY_CLUSTER_ENV_CHILD";

    #[test]
    fn legacy_cluster_env_vars_are_safely_ignored() {
        if std::env::var_os(LEGACY_CLUSTER_ENV_CHILD).is_some() {
            let dir = tempfile::tempdir().expect("temp dir");
            let config_path = dir.path().join("config.toml");
            std::fs::write(&config_path, "[listener]\n").expect("write config");

            let loaded = Config::load(&config_path).expect("legacy cluster envs must be ignored");
            assert_eq!(loaded, Config::default());

            let stored = Config::from_stored_toml_with_env("[listener]\n")
                .expect("stored load ignores legacy cluster envs");
            assert_eq!(stored, Config::default());

            let overrides = Config::environment_overrides();
            for legacy_env in [
                "CC_LB_CLUSTER__INSTANCE_URL",
                "CC_LB_CLUSTER__TOKEN_ENV",
                "CC_LB_CLUSTER_TOKEN",
            ] {
                assert!(
                    !overrides.iter().any(|entry| entry.name == legacy_env),
                    "{legacy_env} must not appear in environment_overrides"
                );
            }
            return;
        }

        let output = Command::new(std::env::current_exe().expect("current test exe"))
            .arg("--exact")
            .arg("tests::legacy_cluster_env_vars_are_safely_ignored")
            .env(LEGACY_CLUSTER_ENV_CHILD, "1")
            .env("CC_LB_CLUSTER__INSTANCE_URL", "http://cc-lb.example:9091")
            .env("CC_LB_CLUSTER__TOKEN_ENV", "CC_LB_CLUSTER_TOKEN")
            .env("CC_LB_CLUSTER_TOKEN", "legacy-cluster-secret")
            .output()
            .expect("spawn child test");
        assert!(
            output.status.success(),
            "child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
