//! Env-only boot config + storage-backed runtime overlay.
//!
//! `BootEnv` is the minimal slice of `Config` that must be set via process env
//! vars before storage can be opened (listener bind addrs, storage connection,
//! AEAD key env name, admin token env name, data dir). Everything else is
//! stored in the `effective_config_v1` row and edited from the dashboard.
//!
//! `Config::compose(&BootEnv, Option<Value>)` is the post-file-removal
//! replacement for `Config::load(path)`. The flow is:
//!   1. Start from `Config::default()`.
//!   2. Layer the JSON overlay (storage-backed runtime fields).
//!   3. Force boot fields from `BootEnv` (overlay attempts on these are ignored).
//!   4. Resolve admin token from the env var named by `boot.admin_token_env`.
//!   5. Validate.

use std::env;
use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Env, Serialized};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::ConfigError;
use crate::types::{
    AdminConfig, AeadConfig, Config, DEFAULT_ADMIN_TOKEN_ENV, DEFAULT_OAUTH_AEAD_KEY_ENV,
    ListenerConfig, StorageConfig, TlsConfig,
};
use crate::validation::validate_config;

/// Boot-only fields loaded from process env vars. Never reloaded.
#[derive(Clone, Debug)]
pub struct BootEnv {
    pub listener: ListenerConfig,
    pub tls: Option<TlsConfig>,
    pub storage: StorageConfig,
    pub aead_key_env: String,
    pub admin_token_env: String,
    pub data_dir: PathBuf,
}

#[derive(Debug, Error)]
pub enum BootEnvError {
    #[error("missing required env var: {var}")]
    MissingRequired { var: &'static str },
    #[error("invalid env var {var}: {detail}")]
    InvalidValue { var: &'static str, detail: String },
    #[error("failed to parse boot env: {0}")]
    Figment(#[source] Box<figment::Error>),
}

impl From<figment::Error> for BootEnvError {
    fn from(error: figment::Error) -> Self {
        Self::Figment(Box::new(error))
    }
}

/// Raw view used only by figment to absorb env vars. Boot-only top-level keys.
#[derive(Debug, Default, Serialize, Deserialize)]
struct BootEnvRaw {
    #[serde(default)]
    listener: Option<ListenerConfig>,
    #[serde(default)]
    tls: Option<TlsConfig>,
    #[serde(default)]
    storage: Option<StorageConfig>,
    #[serde(default)]
    aead: Option<AeadConfig>,
    #[serde(default)]
    admin: Option<BootAdminRaw>,
    #[serde(default)]
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct BootAdminRaw {
    #[serde(default)]
    token_env: Option<String>,
}

impl BootEnv {
    /// Parse the boot env contract from process env. Errors when a required
    /// field is missing or malformed.
    pub fn from_env() -> Result<Self, BootEnvError> {
        // `CC_LB_DATA_DIR` is a flat var (back-compat with existing usage);
        // `CC_LB_LISTENER__*`, `CC_LB_STORAGE__*`, `CC_LB_TLS__*`,
        // `CC_LB_AEAD__*`, `CC_LB_ADMIN__*` follow the figment "__" path split.
        let data_dir_from_flat = env::var("CC_LB_DATA_DIR").ok();

        let figment = Figment::new().merge(Env::prefixed("CC_LB_").split("__"));
        let raw: BootEnvRaw = figment.extract()?;

        // Listener must be fully specified (or rely on figment defaults).
        let listener = match raw.listener {
            Some(listener) => listener,
            None => {
                return Err(BootEnvError::MissingRequired {
                    var: "CC_LB_LISTENER__PROXY_ADDR",
                });
            }
        };

        // Per the contract, every listener bind addr is required.
        if env_unset("CC_LB_LISTENER__PROXY_ADDR") {
            return Err(BootEnvError::MissingRequired {
                var: "CC_LB_LISTENER__PROXY_ADDR",
            });
        }
        if env_unset("CC_LB_LISTENER__ADMIN_ADDR") {
            return Err(BootEnvError::MissingRequired {
                var: "CC_LB_LISTENER__ADMIN_ADDR",
            });
        }
        if env_unset("CC_LB_LISTENER__METRICS_ADDR") {
            return Err(BootEnvError::MissingRequired {
                var: "CC_LB_LISTENER__METRICS_ADDR",
            });
        }

        let storage = match raw.storage {
            Some(storage) => storage,
            None => {
                return Err(BootEnvError::MissingRequired {
                    var: "CC_LB_STORAGE__KIND",
                });
            }
        };

        let aead_key_env = raw
            .aead
            .and_then(|a| {
                if a.key_env.trim().is_empty() {
                    None
                } else {
                    Some(a.key_env)
                }
            })
            .unwrap_or_else(|| DEFAULT_OAUTH_AEAD_KEY_ENV.to_owned());

        let admin_token_env = raw
            .admin
            .and_then(|a| a.token_env)
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_ADMIN_TOKEN_ENV.to_owned());

        // data_dir: flat env var CC_LB_DATA_DIR wins, then figment-parsed `data_dir`,
        // then error.
        let data_dir = data_dir_from_flat
            .map(PathBuf::from)
            .or(raw.data_dir)
            .ok_or(BootEnvError::MissingRequired {
                var: "CC_LB_DATA_DIR",
            })?;

        Ok(Self {
            listener,
            tls: raw.tls,
            storage,
            aead_key_env,
            admin_token_env,
            data_dir,
        })
    }

    /// Convenience: clone the storage config (used by storage_factory).
    pub fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    /// Reconstruct a `BootEnv` from an already-composed `Config`. Used by the
    /// build pipeline to hand a `BootEnv` to `ConfigWatcher` without threading
    /// the original through every layer. `data_dir` falls back to the empty
    /// path when the config has none, which never happens after `compose`.
    pub fn from_config(config: &Config) -> Self {
        Self {
            listener: config.listener.clone(),
            tls: config.listener.tls.clone(),
            storage: config.storage.clone(),
            aead_key_env: config.aead.key_env.clone(),
            admin_token_env: config.admin.token_env.clone(),
            data_dir: config
                .runtime
                .data_dir
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(".")),
        }
    }

    /// JSON-safe view for the dashboard "read-only boot env" panel.
    /// Secret env-var names are kept; values never appear in this view.
    pub fn dashboard_view(&self) -> Value {
        serde_json::json!({
            "listener": self.listener,
            "tls": self.tls,
            "storage": redacted_storage(&self.storage),
            "aead_key_env": self.aead_key_env,
            "admin_token_env": self.admin_token_env,
            "data_dir": self.data_dir,
        })
    }
}

fn redacted_storage(storage: &StorageConfig) -> Value {
    match storage {
        StorageConfig::Sqlite { path } => serde_json::json!({
            "kind": "sqlite",
            "path": path,
        }),
        StorageConfig::Postgres { url, pool } => serde_json::json!({
            "kind": "postgres",
            "url": redact_postgres_url(url),
            "pool": pool,
        }),
    }
}

fn redact_postgres_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut parsed) => {
            if !parsed.username().is_empty() || parsed.password().is_some() {
                let _ = parsed.set_password(Some("***"));
            }
            parsed.to_string()
        }
        Err(_) => "<unparseable postgres url>".to_owned(),
    }
}

fn env_unset(name: &str) -> bool {
    match env::var(name) {
        Ok(value) => value.trim().is_empty(),
        Err(_) => true,
    }
}

impl Config {
    /// Compose a full Config from boot env + a storage-backed runtime overlay.
    ///
    /// Boot fields always win over overlay attempts.
    pub fn compose(boot: &BootEnv, overlay: Option<Value>) -> Result<Self, ConfigError> {
        let defaults = Config::default();
        let mut figment = Figment::from(Serialized::defaults(&defaults));
        if let Some(mut overlay) = overlay {
            strip_boot_only_keys(&mut overlay);
            figment = figment.merge(Serialized::defaults(overlay));
        }
        let mut config: Config = figment.extract()?;

        // Force boot fields.
        config.listener = boot.listener.clone();
        config.listener.tls = boot.tls.clone();
        config.storage = boot.storage.clone();
        config.aead = AeadConfig {
            key_env: boot.aead_key_env.clone(),
        };
        config.admin = AdminConfig {
            token_env: boot.admin_token_env.clone(),
            token: None,
        };
        // runtime.data_dir is boot-only — overlay's `runtime.data_dir` already stripped.
        config.runtime.data_dir = Some(boot.data_dir.clone());

        config.resolve_runtime_values();
        validate_config(&config)?;
        Ok(config)
    }

    /// JSON schema for the runtime overlay (boot-only top-level keys removed,
    /// secret env-var names removed from nested keys).
    pub fn runtime_overlay_schema() -> schemars::Schema {
        let mut schema =
            serde_json::to_value(schemars::schema_for!(Config)).expect("Config schema serializes");
        strip_boot_only_keys_from_schema(&mut schema);
        schemars::Schema::try_from(schema).expect("overlay schema serializes back")
    }
}

/// Strip boot-only top-level keys (`listener`, `tls`, `storage`, `aead`) plus
/// boot-only nested keys (`admin.token_env`, `runtime.data_dir`) from an
/// overlay JSON value. Used both internally by `Config::compose` and by the
/// server's seed-defaults path.
pub fn strip_boot_only_keys_for_seed(value: &mut Value) {
    strip_boot_only_keys(value);
}

fn strip_boot_only_keys(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    // Entire `admin` section is boot-only (only field is the `token_env`
    // env-var name, plus an in-memory `token` that is never serialized).
    for boot_key in ["listener", "tls", "storage", "aead", "admin"] {
        object.remove(boot_key);
    }
    if let Some(Value::Object(runtime)) = object.get_mut("runtime") {
        runtime.remove("data_dir");
    }
}

fn strip_boot_only_keys_from_schema(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(Value::Object(properties)) = object.get_mut("properties") {
        for boot_key in ["listener", "tls", "storage", "aead", "admin"] {
            properties.remove(boot_key);
        }
        strip_nested_property(properties, "runtime", &["data_dir"]);
    }
    if let Some(Value::Array(required)) = object.get_mut("required") {
        required.retain(|item| match item {
            Value::String(name) => !matches!(
                name.as_str(),
                "listener" | "tls" | "storage" | "aead" | "admin"
            ),
            _ => true,
        });
    }
    if let Some(Value::Object(defs)) = object.get_mut("$defs") {
        for boot_def in [
            "ListenerConfig",
            "TlsConfig",
            "StorageConfig",
            "AeadConfig",
            "AdminConfig",
        ] {
            defs.remove(boot_def);
        }
        if let Some(Value::Object(runtime_def)) = defs.get_mut("RuntimeConfig") {
            if let Some(Value::Object(props)) = runtime_def.get_mut("properties") {
                props.remove("data_dir");
            }
            if let Some(Value::Array(required)) = runtime_def.get_mut("required") {
                required.retain(|item| match item {
                    Value::String(name) => name.as_str() != "data_dir",
                    _ => true,
                });
            }
        }
    }
}

fn strip_nested_property(properties: &mut Map<String, Value>, parent: &str, drop: &[&str]) {
    if let Some(Value::Object(child)) = properties.get_mut(parent)
        && let Some(Value::Object(child_props)) = child.get_mut("properties")
    {
        for key in drop {
            child_props.remove(*key);
        }
    }
}
