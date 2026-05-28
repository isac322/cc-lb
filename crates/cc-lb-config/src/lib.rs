#![forbid(unsafe_code)]

mod hot_reload;
mod types;
mod validation;

use std::env;
use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use types::{
    AdminConfig, AnthropicOAuthSignerConfig, ApiKeysConfig, AuthStrategy, BodyConfig,
    BulkheadConfig, CircuitBreakerConfig, Config, ConfigOverrides, DEFAULT_ADMIN_TOKEN_ENV,
    DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV,
    DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH, DEFAULT_PLUGIN_BATCHED_FLUSH_MS, DEFAULT_REDB_PATH,
    DnsConfig, DownstreamAuthConfig, DownstreamAuthMode, EgressConfig, Limit, LimitKind,
    ListenerConfig, ListenerOverrides, NoneModeConfig, NoneModeUpstreamKind, ObservabilityConfig,
    PluginRef, PluginsConfig, PostgresPoolConfig, PriceCatalogConfig, PrincipalSpec, PrincipalType,
    SignersConfig, StorageConfig, TimeoutsConfig, TlsConfig, UpstreamKind, UpstreamSpec,
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
    #[error("principal {principal_id} has invalid allowed_models glob {pattern}: {message}")]
    InvalidPrincipalAllowedModelsGlob {
        principal_id: String,
        pattern: String,
        message: String,
    },
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
        let raw_toml = std::fs::read_to_string(toml_path).ok();
        let migrated_toml = match raw_toml.as_deref() {
            Some(raw) => {
                validation::validate_raw_toml(raw)?;
                Some(validation::migrate_legacy_storage_toml(raw)?)
            }
            None => None,
        };

        let mut figment = Figment::from(Serialized::defaults(Config::default()));
        figment = match migrated_toml.as_deref() {
            Some(migrated) => figment.merge(Toml::string(migrated)),
            None => figment.merge(Toml::file_exact(toml_path)),
        };
        let mut config: Config = figment
            .merge(Env::prefixed("CC_LB_").split("__"))
            .merge(Serialized::defaults(cli_overrides))
            .extract()?;

        config.resolve_runtime_values();
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validation::validate_config(self)
    }

    pub fn watch_for_reload(path: &Path, tx: mpsc::Sender<Config>) -> JoinHandle<()> {
        hot_reload::watch_for_reload(path, tx)
    }

    pub fn json_schema() -> schemars::Schema {
        schemars::schema_for!(Config)
    }

    fn resolve_runtime_values(&mut self) {
        self.admin.token = if self.admin.token_env.trim().is_empty() {
            None
        } else {
            env::var(&self.admin.token_env)
                .ok()
                .filter(|token| !token.is_empty())
        };
    }
}
