#![allow(dead_code, deprecated)]

use std::fmt::Debug;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_lb_config::Config;
use cc_lb_domain::{Principal, Upstream, UpstreamCandidate};
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::{
    ApiKeyAwareSignerFactory, DynamicViewBuilder, DynamicViewHolder, UpstreamStatusSnapshot,
};
use cc_lb_observability::ObservabilityHook;
use cc_lb_routing::{RouteDecision, RouteError, RouterPlugin};
use cc_lb_upstream::SignerFactory;
use metrics_util::debugging::Snapshotter;
use tracing_subscriber::fmt::MakeWriter;

pub const ROUTER_WASM: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f, 0x03,
    0x02, 0x01, 0x00, 0x07, 0x09, 0x01, 0x05, 0x72, 0x6f, 0x75, 0x74, 0x65, 0x00, 0x00, 0x0a, 0x06,
    0x01, 0x04, 0x00, 0x41, 0x00, 0x0b,
];

pub const OBSERVE_WASM: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01, 0x7f, 0x03,
    0x02, 0x01, 0x00, 0x07, 0x0b, 0x01, 0x07, 0x6f, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x00, 0x00,
    0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x00, 0x0b,
];

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

pub fn write_config_with_principal_model(
    path: &Path,
    messages_cap_bytes: u64,
    proxy_addr: SocketAddr,
    _model: &str,
) {
    write_config(path, messages_cap_bytes, proxy_addr);
}

pub fn write_config_with_principal_plugins(
    path: &Path,
    messages_cap_bytes: u64,
    proxy_addr: SocketAddr,
    _router_path: &Path,
    _observe_path: &Path,
) {
    write_config(path, messages_cap_bytes, proxy_addr);
}

pub fn write_bytes(path: &Path, bytes: &[u8]) {
    std::fs::write(path, bytes).unwrap();
}

pub fn toml_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}

pub fn load_config(path: &Path) -> Config {
    Config::load(path).unwrap()
}

pub fn dynamic_view_holder(_config: &Config) -> Arc<DynamicViewHolder> {
    let principal_view = Arc::new(PrincipalView::from_db(
        &[],
        std::collections::HashMap::new(),
    ));
    Arc::new(DynamicViewHolder::new(
        DynamicViewBuilder::new(0)
            .signer_factory(Arc::new(NoopSignerFactory))
            .global_router(Arc::new(NoopRouter))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .principal_view(principal_view)
            .upstream_status_snapshot(Arc::new(UpstreamStatusSnapshot::default()))
            .build(),
    ))
}

struct NoopSignerFactory;

impl ApiKeyAwareSignerFactory for NoopSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        _router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        Arc::new(NoopSignerFactory)
    }
}

#[async_trait]
impl SignerFactory for NoopSignerFactory {
    async fn build(
        &self,
        _upstream: &Upstream,
    ) -> Result<Arc<dyn cc_lb_upstream::Signer>, cc_lb_upstream::SignerError> {
        Err(cc_lb_upstream::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &cc_lb_routing::RoutingContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "noop test router".to_owned(),
        })
    }
}

pub fn evidence_path(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(".omo")
        .join("evidence");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

pub fn counter_value(snapshotter: &Snapshotter, name: &str) -> f64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, metric)| {
            (key.key().name() == name)
                .then(|| debug_counter_value(&metric))
                .flatten()
        })
        .unwrap_or(0) as f64
}

pub fn labeled_counter_value(
    snapshotter: &Snapshotter,
    name: &str,
    label: &str,
    value: &str,
) -> f64 {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _, _, metric)| {
            (key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|item| item.key() == label && item.value() == value))
            .then(|| debug_counter_value(&metric))
            .flatten()
        })
        .unwrap_or(0) as f64
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ReloadFailureCounters {
    pub failed_total: f64,
    pub outcome_failure: f64,
}

pub fn reload_failure_counters(snapshotter: &Snapshotter) -> ReloadFailureCounters {
    let mut counters = ReloadFailureCounters::default();
    for (key, _, _, metric) in snapshotter.snapshot().into_vec() {
        let Some(value) = debug_counter_value(&metric).map(|value| value as f64) else {
            continue;
        };

        match key.key().name() {
            "cc_lb_config_reload_failed_total" => counters.failed_total = value,
            "cc_lb_config_reload_total"
                if key
                    .key()
                    .labels()
                    .any(|label| label.key() == "outcome" && label.value() == "failure") =>
            {
                counters.outcome_failure = value;
            }
            _ => {}
        }
    }
    counters
}

fn debug_counter_value(value: &impl Debug) -> Option<u64> {
    let rendered = format!("{value:?}");
    rendered
        .strip_prefix("Counter(")?
        .strip_suffix(')')?
        .parse()
        .ok()
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
