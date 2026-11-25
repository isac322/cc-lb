use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::task::JoinHandle;

/// Resolves the runtime data directory from env var, CLI flag, config value, or `./data`,
/// creating it with `0700` permissions on Unix when missing.
pub fn resolve_data_dir(
    cli: Option<&Path>,
    config_value: Option<&Path>,
    env_var: &str,
) -> io::Result<PathBuf> {
    let data_dir = if let Ok(env_path) = std::env::var(env_var) {
        PathBuf::from(env_path)
    } else if let Some(cli_path) = cli {
        cli_path.to_path_buf()
    } else if let Some(config_path) = config_value {
        config_path.to_path_buf()
    } else {
        PathBuf::from("./data")
    };

    if !data_dir.exists() {
        fs::create_dir_all(&data_dir)?;
        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&data_dir, Permissions::from_mode(0o700))?;
        }
    }

    Ok(data_dir)
}

/// Awaits `task` up to `timeout`, aborting and draining it if the deadline elapses.
pub async fn await_or_abort<T>(mut task: JoinHandle<T>, timeout: Duration) {
    if tokio::time::timeout(timeout, &mut task).await.is_err() {
        task.abort();
        let _ = task.await;
    }
}

/// Returns `true` when running inside a standard OCI/Docker/Podman container.
pub fn is_container_environment() -> bool {
    Path::new("/.dockerenv").exists() || Path::new("/run/.containerenv").exists()
}

/// Logs an actionable startup warning when a listener binds to loopback inside
/// a container, where published container ports cannot reach `127.0.0.1`.
pub fn warn_if_container_loopback_bind(listener: &str, addr: SocketAddr) {
    if is_container_environment() && addr.ip().is_loopback() {
        tracing::warn!(
            listener,
            %addr,
            "listener is bound to loopback inside a container; published ports will be unreachable from outside the container unless configured (for example CC_LB_LISTENER__PROXY_ADDR=0.0.0.0:8080 or CC_LB_LISTENER__ADMIN_ADDR=0.0.0.0:9090)",
        );
    }
}
