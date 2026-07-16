#![allow(dead_code)]

use std::path::{Path, PathBuf};

use metrics_exporter_prometheus::PrometheusHandle;
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

pub fn init_metrics() -> &'static PrometheusHandle {
    crate::common::install_prometheus()
}

pub fn tls_reload_counter(handle: &PrometheusHandle, outcome: &str) -> f64 {
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
