#![allow(dead_code)]

use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use cc_lb_config::Config;
use cc_lb_core::api_keys::principal_view::PrincipalView;
use cc_lb_core::{
    ApiKeyAwareSignerFactory, DispatchError, DynamicViewBuilder, DynamicViewHolder,
    ErrorNormalizer, UpstreamDispatch, UpstreamStatusSnapshot,
};
use cc_lb_plugin_api::{
    ObservabilityHook, Principal, RequestContext, RouteDecision, RouteError, RouterPlugin,
    SignedRequest, SignerFactory, Upstream, UpstreamCandidate,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
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
            .dispatcher(Arc::new(NoopDispatch))
            .global_observability_hooks(Vec::<Arc<dyn ObservabilityHook>>::new())
            .error_normalizer(Arc::new(ErrorNormalizer::new()))
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
    ) -> Result<Arc<dyn cc_lb_plugin_api::Signer>, cc_lb_plugin_api::SignerError> {
        Err(cc_lb_plugin_api::SignerError::MissingCredentials {
            reason: "noop test signer factory".to_owned(),
        })
    }
}

struct NoopRouter;

impl RouterPlugin for NoopRouter {
    fn route(
        &self,
        _ctx: &RequestContext,
        _principal: &Principal,
        _candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError> {
        Err(RouteError::NoRoute {
            reason: "noop test router".to_owned(),
        })
    }
}

struct NoopDispatch;

#[async_trait]
impl UpstreamDispatch for NoopDispatch {
    async fn dispatch(
        &self,
        _request: SignedRequest,
    ) -> Result<http::Response<Body>, DispatchError> {
        Ok(http::Response::new(Body::empty()))
    }
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
