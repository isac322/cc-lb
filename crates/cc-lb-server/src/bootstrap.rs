use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_lb_config::Config;
use cc_lb_storage_api::principal::{PrincipalCreate, PrincipalKind};
use cc_lb_storage_api::upstream::UpstreamCreate;
use cc_lb_storage_api::{PluginRegistryStore, PrincipalStore, StorageResult, UpstreamStore};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Invalid bootstrap spec: {0}")]
    InvalidSpec(String),
}

pub type BootstrapResult<T> = Result<T, BootstrapError>;

async fn seed_admin_if_absent(store: &dyn PrincipalStore, _token: &str) -> StorageResult<()> {
    let admin_principal = PrincipalCreate {
        name: "admin".to_owned(),
        kind: PrincipalKind::Admin,
        allowed_models: vec![],
        default_limits: vec![],
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    match store.get_by_name("admin").await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => store.create(admin_principal, now).await.map(|_| ()),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct BootstrapSpec {
    #[serde(default)]
    pub upstreams: Vec<BootstrapUpstream>,
    #[serde(default)]
    pub principals: Vec<BootstrapPrincipal>,
    #[serde(default)]
    pub plugin_chains: Vec<BootstrapPluginChain>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapUpstream {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub api_key_env: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapPrincipal {
    pub name: String,
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BootstrapPluginChain {
    pub principal_id: String,
    #[serde(default)]
    pub plugins: Vec<String>,
}

pub async fn apply_bootstrap(
    _config: &Config,
    seeder: &dyn PrincipalStore,
    upstream_store: &dyn UpstreamStore,
    _plugin_store: &dyn PluginRegistryStore,
    env_token: Option<String>,
    data_dir: &Path,
) -> BootstrapResult<()> {
    if let Some(token) = env_token {
        seed_admin_if_absent(seeder, &token)
            .await
            .map_err(|e| BootstrapError::Storage(e.to_string()))?;
    }

    let bootstrap_path = data_dir.join("bootstrap.toml");
    if bootstrap_path.exists() {
        let content = fs::read_to_string(&bootstrap_path)?;

        let spec: BootstrapSpec = if content.trim().is_empty() {
            BootstrapSpec::default()
        } else {
            serde_json::from_str::<BootstrapSpec>(&content).unwrap_or_default()
        };

        for upstream in spec.upstreams {
            let _api_key = upstream
                .api_key_env
                .as_ref()
                .and_then(|env_var| std::env::var(env_var).ok());

            let create_input = UpstreamCreate {
                name: upstream.name,
                kind: parse_upstream_kind(&upstream.kind),
                base_url: None,
                api_key_ciphertext: None,
            };

            upstream_store
                .create(create_input)
                .await
                .map_err(|e| BootstrapError::Storage(e.to_string()))?;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let consumed_name = format!("bootstrap.toml.consumed-{}", timestamp);
        let consumed_path = data_dir.join(&consumed_name);
        fs::rename(&bootstrap_path, &consumed_path)?;
    }

    Ok(())
}

fn parse_upstream_kind(kind_str: &str) -> cc_lb_storage_api::upstream::UpstreamKind {
    match kind_str {
        "anthropic_api_key" => cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey,
        "anthropic_oauth" => cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth,
        _ => cc_lb_storage_api::upstream::UpstreamKind::Custom,
    }
}
