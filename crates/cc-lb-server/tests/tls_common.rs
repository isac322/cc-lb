#![allow(dead_code)]

use std::path::{Path, PathBuf};

use cc_lb_observability::{ObservabilityConfig, TracingGuard};
use tempfile::TempDir;

pub fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("tls")
        .join(name)
}

pub fn copy_pair(dir: &TempDir, cert_name: &str, key_name: &str) -> (PathBuf, PathBuf) {
    let cert_path = dir.path().join("server-cert.pem");
    let key_path = dir.path().join("server-key.pem");
    std::fs::copy(fixture(cert_name), &cert_path).unwrap();
    std::fs::copy(fixture(key_name), &key_path).unwrap();
    (cert_path, key_path)
}

pub fn overwrite_pair(cert_path: &Path, key_path: &Path, cert_name: &str, key_name: &str) {
    std::fs::copy(fixture(cert_name), cert_path).unwrap();
    std::fs::copy(fixture(key_name), key_path).unwrap();
}

pub fn init_metrics() -> TracingGuard {
    cc_lb_observability::init(&ObservabilityConfig {
        tracing_level: "info".to_owned(),
        otlp_endpoint: None,
        prometheus_endpoint: None,
        log_redaction: true,
        user_prompt_redaction: false,
        hook_channel_capacity: cc_lb_observability::DEFAULT_HOOK_CHANNEL_CAPACITY,
    })
    .unwrap()
}

pub fn tls_reload_counter(guard: &TracingGuard, outcome: &str) -> f64 {
    let Some(handle) = guard.prometheus_handle() else {
        return 0.0;
    };
    let needle = format!("cc_lb_tls_reload_total{{outcome=\"{outcome}\"}}");
    handle
        .render()
        .lines()
        .find_map(|line| {
            line.strip_prefix(&needle)
                .and_then(|value| value.trim().parse::<f64>().ok())
        })
        .unwrap_or(0.0)
}
