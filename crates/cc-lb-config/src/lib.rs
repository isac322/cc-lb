#![forbid(unsafe_code)]

mod hot_reload;
mod types;
mod validation;

use std::env;
use std::path::Path;

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub use types::{
    AdminConfig, AnthropicOAuthSignerConfig, AuthStrategy, BodyConfig, BulkheadConfig,
    CircuitBreakerConfig, Config, ConfigOverrides, DnsConfig, EgressConfig, ListenerConfig,
    ListenerOverrides, ObservabilityConfig, PluginRef, PluginsConfig, PrincipalSpec, QuotasConfig,
    SignersConfig, StorageConfig, TimeoutsConfig, TlsConfig, UpstreamKind, UpstreamSpec,
    DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES,
    DEFAULT_OAUTH_AEAD_KEY_ENV, DEFAULT_PLUGIN_BATCHED_EVENTS_PER_FLUSH,
    DEFAULT_PLUGIN_BATCHED_FLUSH_MS,
};
pub use validation::ValidationError;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load config: {0}")]
    Figment(#[source] Box<figment::Error>),
    #[error(transparent)]
    Validation(#[from] ValidationError),
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
        let mut config: Config = Figment::from(Serialized::defaults(Config::default()))
            .merge(Toml::file_exact(toml_path))
            .merge(Env::prefixed("CC_LB_").split("__"))
            .merge(Serialized::defaults(cli_overrides))
            .extract()?;

        config.validate_loaded()?;
        Ok(config)
    }

    pub fn validate_loaded(&mut self) -> Result<(), ValidationError> {
        self.resolve_runtime_values();
        self.validate()
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
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
