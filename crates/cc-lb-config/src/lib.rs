#![forbid(unsafe_code)]

#[cfg(not(loom))]
mod hot_reload;
mod types;
mod validation;

use std::env;
use std::path::Path;

use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use thiserror::Error;
#[cfg(not(loom))]
use tokio::sync::mpsc;
#[cfg(not(loom))]
use tokio::task::JoinHandle;

pub use types::{
    AdminConfig, AnthropicOAuthConfig, ApiKeysConfig, BodyConfig, BulkheadConfig,
    CircuitBreakerConfig, ClusterConfig, Config, ConfigOverrides, DEFAULT_ADMIN_TOKEN_ENV,
    DEFAULT_FILES_CAP_BYTES, DEFAULT_MESSAGES_CAP_BYTES, DEFAULT_OAUTH_AEAD_KEY_ENV,
    DEFAULT_SQLITE_PATH, DEFAULT_UPSTREAM_AFFINITY_TTL_DAYS, DnsConfig, DownstreamAuthConfig,
    DownstreamAuthMode, EgressConfig, EventBusConfig, ListenerConfig, ListenerOverrides,
    NoneModeConfig, NoneModeUpstreamKind, ObservabilityConfig, PluginFailurePolicy,
    PluginWireBounds, PostgresPoolConfig, PriceCatalogConfig, PromptCacheShadowConfig,
    RecurringJobConfig, RestartRequiredField, RuntimeConfig, SchedulerConfig,
    SchedulerIdempotencyConfig, SchedulerPoolConfig, SchedulerRetryClasses, SchedulerRetryConfig,
    SchedulerStalenessConfig, ShapeOriginPolicy, StorageConfig, SubscriptionQuotaConfig,
    TimeoutsConfig, TlsConfig, UpstreamAffinityConfig, WasmtimeAllocationStrategy, WasmtimeConfig,
};
pub use validation::{ValidationError, validate_postgres_url};

pub fn removed_prompt_cache_switch_warning(field: &str) -> String {
    format!(
        "removed prompt-cache disable switch `{field}` is ignored; cache-aware routing is always enabled"
    )
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to load config: {0}")]
    Figment(#[source] Box<figment::Error>),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("invalid postgres URL: {message}")]
    InvalidPostgresUrl { message: String },
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
        Self::load_with_warnings(toml_path).map(|(config, _warnings)| config)
    }

    pub fn load_with_warnings(toml_path: &Path) -> Result<(Self, Vec<String>), ConfigError> {
        Self::load_with_overrides_and_warnings(toml_path, ConfigOverrides::default())
    }

    pub fn load_with_overrides(
        toml_path: &Path,
        cli_overrides: ConfigOverrides,
    ) -> Result<Self, ConfigError> {
        Self::load_with_overrides_and_warnings(toml_path, cli_overrides)
            .map(|(config, _warnings)| config)
    }

    pub fn load_with_overrides_and_warnings(
        toml_path: &Path,
        cli_overrides: ConfigOverrides,
    ) -> Result<(Self, Vec<String>), ConfigError> {
        let raw_toml = std::fs::read_to_string(toml_path).ok();
        let mut warnings = raw_toml
            .as_deref()
            .map(validation::removed_prompt_cache_switches)
            .unwrap_or_default();
        let migrated_toml = match raw_toml.as_deref() {
            Some(raw) => {
                validation::validate_raw_toml(raw)?;
                Some(validation::migrate_legacy_storage_toml(raw)?)
            }
            None => None,
        };
        warnings.extend(validation::removed_prompt_cache_env_switches());

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
        Ok((config, warnings))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validation::validate_config(self)
    }

    #[cfg(not(loom))]
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
