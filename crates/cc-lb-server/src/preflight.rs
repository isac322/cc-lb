use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cc_lb_aead::AeadService;
use cc_lb_config::{Config, DEFAULT_REDB_PATH, PluginRef, StorageConfig, TlsConfig};
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_plugin_api::{PluginManifest, PluginRuntime};
use cc_lb_runtime_extism::ExtismRuntime;
use thiserror::Error;
use tokio::net::TcpListener;

use crate::storage_factory;
use crate::tls;

#[derive(Debug, Default, Clone)]
pub struct PreflightReport {
    pub warnings: Vec<String>,
    pub successes: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PreflightOptions {
    pub skip_bind: bool,
}

#[derive(Debug, Error)]
pub enum PreflightError {
    #[error("storage master key env {0} is missing")]
    MasterKeyMissing(String),
    #[error("storage master key must decode to 32 bytes; got {actual}")]
    MasterKeyBadLength { actual: usize },
    #[error("storage master key env {0} must be hex")]
    MasterKeyBadHex(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("plugin: {0}")]
    Plugin(String),
    #[error("plugin {name}: missing wasm_path")]
    PluginMissingArtifact { name: String },
    #[error("failed to bind {addr}: {message}")]
    ListenerBind { addr: SocketAddr, message: String },
    #[error("missing TLS certificate or key file: {0}")]
    TlsCertMissing(PathBuf),
    #[error("failed to parse TLS files: {0}")]
    TlsParse(String),
    #[error(transparent)]
    Config(#[from] cc_lb_config::ConfigError),
}

pub async fn run(
    cfg: &Config,
    options: PreflightOptions,
) -> Result<PreflightReport, PreflightError> {
    run_inner(cfg, options, true).await
}

pub async fn run_offline(
    cfg: &Config,
    options: PreflightOptions,
) -> Result<PreflightReport, PreflightError> {
    run_inner(cfg, options, false).await
}

async fn run_inner(
    cfg: &Config,
    options: PreflightOptions,
    probe_storage: bool,
) -> Result<PreflightReport, PreflightError> {
    let mut report = PreflightReport::default();

    if probe_storage {
        let key_name = &cfg.aead.key_env;
        let key_hex =
            env::var(key_name).map_err(|_| PreflightError::MasterKeyMissing(key_name.clone()))?;
        let key = decode_master_key(key_name, &key_hex)?;
        report
            .successes
            .push(format!("storage master key resolved from {key_name}"));
        match &cfg.storage {
            StorageConfig::Redb { path } => validate_redb_path(path)?,
            StorageConfig::Postgres { url, .. } => {
                probe_postgres_connection(url).await?;
            }
        }

        let aead = Arc::new(AeadService::from_master_key(key));
        let _storage = storage_factory::open_storage(&cfg.storage, aead, key)
            .await
            .map_err(|error| PreflightError::Storage(error.to_string()))?;
        report.successes.push(storage_open_success(&cfg.storage));
    }

    let runtime = ExtismRuntime::new();
    PrincipalView::from_config(cfg, std::collections::HashMap::new())?;
    report.successes.push("principal view built".to_owned());
    if let Some(plugin) = &cfg.plugins.router_plugin {
        dry_load_plugin(&runtime, plugin, PluginLoadKind::Router)?;
        report
            .successes
            .push(format!("plugin {} dry-loaded", plugin.name));
    }
    for plugin in &cfg.plugins.observability_hooks {
        dry_load_plugin(&runtime, plugin, PluginLoadKind::Observability)?;
        report
            .successes
            .push(format!("plugin {} dry-loaded", plugin.name));
    }

    let principals = cfg.principals.iter().collect::<BTreeMap<_, _>>();
    for (principal_name, principal) in principals {
        if let Some(plugin) = &principal.router_plugin {
            let path = format!("principals.{principal_name}.router_plugin");
            dry_load_plugin(&runtime, plugin, PluginLoadKind::Router)
                .map_err(|error| PreflightError::Plugin(format!("{path}: {error}")))?;
            report
                .successes
                .push(format!("plugin {} dry-loaded ({path})", plugin.name));
        }

        if let Some(plugins) = &principal.observability_hooks {
            for (index, plugin) in plugins.iter().enumerate() {
                let path = format!("principals.{principal_name}.observability_hooks.{index}");
                dry_load_plugin(&runtime, plugin, PluginLoadKind::Observability)
                    .map_err(|error| PreflightError::Plugin(format!("{path}: {error}")))?;
                report
                    .successes
                    .push(format!("plugin {} dry-loaded ({path})", plugin.name));
            }
        }
    }

    for name in cfg.upstreams.keys() {
        report.warnings.push(format!(
            "upstream {name}: probe skipped (offline preflight)"
        ));
    }

    if !options.skip_bind {
        bind_addr(cfg.listener.proxy_addr).await?;
        bind_addr(cfg.listener.admin_addr).await?;
        bind_addr(cfg.listener.metrics_addr).await?;
        report
            .successes
            .push("listener bindability checked".to_owned());
    }

    if let Some((_, tls_config)) = active_tls_config(cfg) {
        verify_tls(tls_config)?;
        report.successes.push("tls files parsed".to_owned());
    }

    if let Some(warning) = ulimit_warning() {
        report.warnings.push(warning);
    } else {
        report.successes.push("ulimit checked".to_owned());
    }

    Ok(report)
}

fn decode_master_key(env_name: &str, value: &str) -> Result<[u8; 32], PreflightError> {
    if value.len() != 64 {
        return Err(PreflightError::MasterKeyBadLength {
            actual: value.len() / 2,
        });
    }

    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks(2).enumerate() {
        let high = hex_nibble(chunk[0])
            .ok_or_else(|| PreflightError::MasterKeyBadHex(env_name.to_owned()))?;
        let low = hex_nibble(chunk[1])
            .ok_or_else(|| PreflightError::MasterKeyBadHex(env_name.to_owned()))?;
        key[index] = (high << 4) | low;
    }

    Ok(key)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn storage_open_success(config: &StorageConfig) -> String {
    match config {
        StorageConfig::Redb { path } => format!("storage opened: {}", path.display()),
        StorageConfig::Postgres { url, .. } => format!("storage opened: {}", postgres_host(url)),
    }
}

fn postgres_host(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "<unknown host>".to_owned())
}

async fn probe_postgres_connection(url: &str) -> Result<(), PreflightError> {
    match storage_factory::probe_postgres_connection(url).await {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::error!(
                host = %postgres_host(url),
                error = %error,
                "postgres connection probe failed"
            );
            Err(PreflightError::Storage(error.to_string()))
        }
    }
}

fn validate_redb_path(path: &Path) -> Result<(), PreflightError> {
    if path == Path::new(DEFAULT_REDB_PATH) && !path.exists() {
        return Ok(());
    }

    if path.exists() {
        let metadata = fs::metadata(path).map_err(|error| {
            PreflightError::Storage(format!("cannot inspect path {}: {error}", path.display()))
        })?;
        if !metadata.is_file() {
            return Err(PreflightError::Storage(format!(
                "not a file: {}",
                path.display()
            )));
        }
        if metadata.permissions().readonly() {
            return Err(PreflightError::Storage(format!(
                "file is not writable: {}",
                path.display()
            )));
        }
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent_metadata = fs::metadata(parent).map_err(|_| {
        PreflightError::Storage(format!(
            "parent directory does not exist: {}",
            parent.display()
        ))
    })?;
    if !parent_metadata.is_dir() {
        return Err(PreflightError::Storage(format!(
            "parent path is not a directory: {}",
            parent.display()
        )));
    }
    if parent_metadata.permissions().readonly() {
        return Err(PreflightError::Storage(format!(
            "parent directory is not writable: {}",
            parent.display()
        )));
    }

    Ok(())
}

enum PluginLoadKind {
    Router,
    Observability,
}

fn dry_load_plugin(
    runtime: &ExtismRuntime,
    plugin: &PluginRef,
    kind: PluginLoadKind,
) -> Result<(), PreflightError> {
    let manifest = manifest_from_plugin(plugin)?;
    let result = match kind {
        PluginLoadKind::Router => runtime.instantiate_router(&manifest).map(|_| ()),
        PluginLoadKind::Observability => runtime.instantiate_observability(&manifest).map(|_| ()),
    };

    result.map_err(|error| PreflightError::Plugin(error.to_string()))
}

fn manifest_from_plugin(plugin: &PluginRef) -> Result<PluginManifest, PreflightError> {
    let artifact =
        plugin
            .wasm_path
            .as_ref()
            .ok_or_else(|| PreflightError::PluginMissingArtifact {
                name: plugin.name.clone(),
            })?;
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "observe_batch_count".to_owned(),
        serde_json::Value::from(plugin.batched_events_per_flush),
    );
    metadata.insert(
        "observe_flush_ms".to_owned(),
        serde_json::Value::from(plugin.batched_flush_ms),
    );
    Ok(PluginManifest {
        name: plugin.name.clone(),
        artifact: artifact.display().to_string(),
        config: plugin.config.clone(),
        metadata,
    })
}

fn active_tls_config(config: &Config) -> Option<(&'static str, &TlsConfig)> {
    if let Some(tls) = &config.listener.tls {
        Some(("listener.tls", tls))
    } else {
        config.tls.as_ref().map(|tls| ("tls", tls))
    }
}

fn verify_tls(tls_config: &TlsConfig) -> Result<(), PreflightError> {
    let cert_path = tls_config
        .cert_path
        .as_ref()
        .ok_or_else(|| PreflightError::TlsCertMissing(PathBuf::from("listener.tls.cert_path")))?;
    let key_path = tls_config
        .key_path
        .as_ref()
        .ok_or_else(|| PreflightError::TlsCertMissing(PathBuf::from("listener.tls.key_path")))?;

    if !cert_path.exists() {
        return Err(PreflightError::TlsCertMissing(cert_path.clone()));
    }
    if !key_path.exists() {
        return Err(PreflightError::TlsCertMissing(key_path.clone()));
    }

    tls::load_certs(cert_path, key_path)
        .map_err(|error| PreflightError::TlsParse(error.to_string()))?;
    Ok(())
}

async fn bind_addr(addr: SocketAddr) -> Result<(), PreflightError> {
    TcpListener::bind(addr)
        .await
        .map(drop)
        .map_err(|source| PreflightError::ListenerBind {
            addr,
            message: source.to_string(),
        })
}

fn ulimit_warning() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        use nix::sys::resource::{Resource, getrlimit};

        match getrlimit(Resource::RLIMIT_NOFILE) {
            Ok((soft, _hard)) if soft < 65_536 => Some(format!(
                "ulimit: RLIMIT_NOFILE soft limit {soft} is below 65536"
            )),
            Ok(_) => None,
            Err(error) => Some(format!("ulimit: could not read RLIMIT_NOFILE: {error}")),
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use cc_lb_config::{Config, PluginRef, PrincipalSpec};
    use tempfile::TempDir;

    use super::{PreflightOptions, run_offline};

    const ROUTER_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
        0x03, 0x02, 0x01, 0x00, 0x07, 0x09, 0x01, 0x05, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x00, 0x00,
        0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x00, 0x0b,
    ];
    const OBSERVE_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f,
        0x03, 0x02, 0x01, 0x00, 0x07, 0x0b, 0x01, 0x07, 0x6f, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65,
        0x00, 0x00, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x00, 0x0b,
    ];

    struct PluginFixtures {
        dir: TempDir,
        router: PathBuf,
        observe: PathBuf,
    }

    impl PluginFixtures {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir is created");
            let router = write_plugin(dir.path(), "router.wasm", ROUTER_WASM);
            let observe = write_plugin(dir.path(), "observe.wasm", OBSERVE_WASM);
            Self {
                dir,
                router,
                observe,
            }
        }

        fn missing_path(&self, name: &str) -> PathBuf {
            self.dir.path().join(name)
        }
    }

    #[tokio::test]
    async fn preflight_aborts_on_principal_plugin_failure() {
        let fixtures = PluginFixtures::new();
        let mut config = config_with_global_plugins(&fixtures);
        config.principals.insert(
            "alice".to_owned(),
            principal(
                Some(plugin("alice-router", &fixtures.router)),
                Some(vec![plugin("alice-hook", &fixtures.observe)]),
            ),
        );
        config.principals.insert(
            "bob".to_owned(),
            principal(
                Some(plugin(
                    "bob-router",
                    &fixtures.missing_path("bob-router.wasm"),
                )),
                Some(vec![plugin("bob-hook", &fixtures.observe)]),
            ),
        );
        config.principals.insert(
            "carol".to_owned(),
            principal(
                Some(plugin("carol-router", &fixtures.router)),
                Some(vec![plugin("carol-hook", &fixtures.observe)]),
            ),
        );

        let error = run_offline(&config, PreflightOptions { skip_bind: true })
            .await
            .expect_err("bob's bad router plugin must abort preflight");
        let message = error.to_string();

        assert!(
            message.contains("principals.bob.router_plugin"),
            "error must include the failing principal plugin path, got: {message}"
        );
    }

    #[tokio::test]
    async fn preflight_all_valid() {
        let fixtures = PluginFixtures::new();
        let mut config = config_with_global_plugins(&fixtures);
        config.principals.insert(
            "alice".to_owned(),
            principal(
                Some(plugin("alice-router", &fixtures.router)),
                Some(vec![plugin("alice-hook", &fixtures.observe)]),
            ),
        );
        config.principals.insert(
            "bob".to_owned(),
            principal(
                Some(plugin("bob-router", &fixtures.router)),
                Some(vec![plugin("bob-hook-a", &fixtures.observe)]),
            ),
        );

        let report = run_offline(&config, PreflightOptions { skip_bind: true })
            .await
            .expect("global and per-principal plugins should all dry-load");

        for expected in [
            "principal view built",
            "plugin alice-router dry-loaded (principals.alice.router_plugin)",
            "plugin alice-hook dry-loaded (principals.alice.observability_hooks.0)",
            "plugin bob-router dry-loaded (principals.bob.router_plugin)",
            "plugin bob-hook-a dry-loaded (principals.bob.observability_hooks.0)",
        ] {
            assert!(
                report.successes.iter().any(|entry| entry == expected),
                "preflight should report success for {expected}: {:?}",
                report.successes
            );
        }
    }

    fn config_with_global_plugins(fixtures: &PluginFixtures) -> Config {
        let mut config = Config::default();
        config.plugins.router_plugin = Some(plugin("global-router", &fixtures.router));
        config.plugins.observability_hooks = vec![plugin("global-hook", &fixtures.observe)];
        config
    }

    fn principal(
        router_plugin: Option<PluginRef>,
        observability_hooks: Option<Vec<PluginRef>>,
    ) -> PrincipalSpec {
        PrincipalSpec {
            router_plugin,
            observability_hooks,
            ..PrincipalSpec::default()
        }
    }

    fn plugin(name: &str, wasm_path: &Path) -> PluginRef {
        PluginRef {
            name: name.to_owned(),
            wasm_path: Some(wasm_path.to_owned()),
            ..PluginRef::default()
        }
    }

    fn write_plugin(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, bytes).expect("wasm fixture is written");
        path
    }
}
