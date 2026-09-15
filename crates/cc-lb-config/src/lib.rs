#![forbid(unsafe_code)]

mod types;
mod validation;

use std::env;
use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use thiserror::Error;

pub use types::{
    ADMIN_AUTH_PROVIDERS_JSON_ENV, AdminAuthConfig, AdminAuthProviderConfig, AdminConfig,
    AnthropicOAuthConfig, BodyConfig, BulkheadConfig, CircuitBreakerConfig, ClusterConfig, Config,
    ConfigOverrides, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES,
    DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_SQLITE_PATH, DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS,
    EventBusConfig, ListenerConfig, ListenerOverrides, ObservabilityConfig, PluginWireBounds,
    PostgresPoolConfig, PriceCatalogConfig, PromptCacheShadowConfig, RecurringJobConfig,
    RestartRequiredField, RuntimeConfig, SchedulerConfig, SchedulerPoolConfig, ShapeOriginPolicy,
    StorageConfig, SubscriptionQuotaConfig, TimeoutsConfig, TlsConfig, UpstreamAffinityConfig,
    WasmtimeAllocationStrategy, WasmtimeConfig,
};
pub use validation::{ValidationError, validate_postgres_url};

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
}

impl From<figment::Error> for ConfigError {
    fn from(error: figment::Error) -> Self {
        Self::Figment(Box::new(error))
    }
}

impl Config {
    pub fn load(toml_path: &Path) -> Result<Self, ConfigError> {
        Self::load_with_overrides(toml_path, ConfigOverrides::default())
    }

    pub fn load_with_overrides(
        toml_path: &Path,
        cli_overrides: ConfigOverrides,
    ) -> Result<Self, ConfigError> {
        let mut config: Config = Figment::new()
            .merge(Toml::file_exact(toml_path))
            .merge(Self::config_env())
            .merge(Serialized::defaults(cli_overrides))
            .extract()?;

        config.apply_admin_auth_env_override()?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_stored_toml(raw_toml: &str) -> Result<Self, ConfigError> {
        Ok(Figment::from(Toml::string(raw_toml)).extract()?)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validation::validate_config(self)
    }

    pub fn json_schema() -> schemars::Schema {
        schemars::schema_for!(Config)
    }

    fn config_env() -> Env {
        let roots = Self::json_schema()
            .get("properties")
            .and_then(serde_json::Value::as_object)
            .map(|properties| properties.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        Env::prefixed("CC_LB_")
            .filter(move |key| {
                let root = key.as_str().split("__").next().unwrap_or_default();
                roots
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(root))
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
