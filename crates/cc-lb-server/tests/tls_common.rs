#![allow(dead_code)]

use std::path::{Path, PathBuf};

use metrics_util::debugging::Snapshotter;
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

pub fn tls_reload_counter(snapshotter: &Snapshotter, outcome: &str) -> f64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, metric)| {
            (key.key().name() == "cc_lb_tls_reload_total"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == outcome))
            .then(|| format!("{metric:?}"))
            .and_then(|rendered| {
                rendered
                    .strip_prefix("Counter(")?
                    .strip_suffix(')')?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0) as f64
}
