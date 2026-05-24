#![forbid(unsafe_code)]

mod hot_reload;
mod types;
mod validation;

use std::env;
use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use types::{
    AdminConfig, AeadConfig, AnthropicOAuthSignerConfig, AuthStrategy, BodyConfig, BulkheadConfig,
    CircuitBreakerConfig, Config, ConfigOverrides, DEFAULT_ADMIN_TOKEN_ENV,
    DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV,
    DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH, DEFAULT_PLUGIN_BATCHED_FLUSH_MS, DEFAULT_REDB_PATH,
    DnsConfig, EgressConfig, ListenerConfig, ListenerOverrides, ObservabilityConfig, PluginRef,
    PluginsConfig, PostgresPoolConfig, PrincipalSpec, QuotasConfig, SignersConfig, StorageConfig,
    TimeoutsConfig, TlsConfig, UpstreamKind, UpstreamSpec,
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
    #[error(
        "storage.pool.statement_timeout_secs ({statement}) must be less than timeouts.upstream_total_secs ({request})"
    )]
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
        let legacy_aliases = LegacyConfigAliases::from_toml(toml_path)?;
        let mut config: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Toml::file_exact(toml_path))
            .merge(Serialized::defaults(legacy_aliases))
            .merge(Env::prefixed("CC_LB_").split("__"))
            .merge(Serialized::defaults(cli_overrides))
            .extract()?;

        config.validate_loaded()?;
        Ok(config)
    }

    pub fn validate_loaded(&mut self) -> Result<(), ConfigError> {
        self.resolve_runtime_values();
        self.validate()
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

#[derive(Debug, Default, Serialize)]
struct LegacyConfigAliases {
    #[serde(skip_serializing_if = "Option::is_none")]
    aead: Option<LegacyAeadAlias>,
}

impl LegacyConfigAliases {
    fn from_toml(toml_path: &Path) -> Result<Self, ConfigError> {
        let parsed: LegacyConfigFile = Figment::from(Toml::file_exact(toml_path)).extract()?;
        let legacy_key_env = parsed
            .storage
            .and_then(|storage| storage.oauth_aead_key_env);
        let has_aead_key_env = parsed.aead.and_then(|aead| aead.key_env).is_some();

        Ok(
            if let (Some(key_env), false) = (legacy_key_env, has_aead_key_env) {
                Self {
                    aead: Some(LegacyAeadAlias { key_env }),
                }
            } else {
                Self::default()
            },
        )
    }
}

#[derive(Debug, Serialize)]
struct LegacyAeadAlias {
    key_env: String,
}

#[derive(Debug, Default, Deserialize)]
struct LegacyConfigFile {
    storage: Option<LegacyStorageAliases>,
    aead: Option<LegacyAeadAliases>,
}

#[derive(Debug, Default, Deserialize)]
struct LegacyStorageAliases {
    oauth_aead_key_env: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LegacyAeadAliases {
    key_env: Option<String>,
}
