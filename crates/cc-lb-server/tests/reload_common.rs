#![allow(dead_code)]

use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cc_lb_config::Config;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::fmt::MakeWriter;

pub fn write_config(path: &Path, messages_cap_bytes: u64, proxy_addr: SocketAddr) {
    let config = format!(
        r#"[listener]
proxy_addr = "{proxy_addr}"
admin_addr = "127.0.0.1:19090"
metrics_addr = "127.0.0.1:19091"

[body]
messages_cap_bytes = {messages_cap_bytes}
files_cap_bytes = 1048576
"#
    );
    std::fs::write(path, config).unwrap();
}

pub fn load_config(path: &Path) -> Config {
    Config::load(path).unwrap()
}

pub fn evidence_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(".omo")
        .join("evidence")
        .join(name)
}

pub fn install_prometheus() -> PrometheusHandle {
    PrometheusBuilder::new().install_recorder().unwrap()
}

pub fn counter_value(handle: &PrometheusHandle, name: &str) -> f64 {
    let prefix = format!("{name} ");
    handle
        .render()
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(0.0)
}

pub fn labeled_counter_value(
    handle: &PrometheusHandle,
    name: &str,
    label: &str,
    value: &str,
) -> f64 {
    let metric_prefix = format!("{name}{{");
    let label_fragment = format!(r#"{label}="{value}""#);
    handle
        .render()
        .lines()
        .find(|line| line.starts_with(&metric_prefix) && line.contains(&label_fragment))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

pub fn capture_warn_logs(run: impl FnOnce()) -> String {
    let captured = CapturedLogs::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(writer)
        .finish();

    tracing::subscriber::with_default(subscriber, run);
    captured.contents()
}

#[derive(Clone, Default)]
struct CapturedLogs {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn contents(&self) -> String {
        let bytes = self.bytes.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

struct CapturedWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Write for CapturedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
