mod common;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use cc_lb_plugin_api::{
    DialectError, ObserveEvent, PluginRuntime, Principal, RetryDecision, ShapedRequest,
    ShapedRequestBuilder, SignerError, Upstream, UpstreamDialect, UpstreamError, shape_request,
    sign_request,
};
use cc_lb_runtime_extism::ExtismRuntime;
use http::{HeaderMap, Method, StatusCode};
use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata as TracingMetadata, Subscriber};
use url::Url;

#[tokio::test(flavor = "current_thread")]
async fn dispatch_fallbacks_apply_per_function_policies_with_metrics_and_tracing() {
    sign_fail_request_fallback().await;
    build_signer_fail_request_fallback().await;
    shape_fail_request_fallback();
    observe_silent_skip_fallback();
    normalize_error_pass_through_fallback();
    on_unauthorized_pass_through_fallback().await;
}

async fn sign_fail_request_fallback() {
    let wat = module_with_panicking_export(
        "sign",
        &[("build_signer", &common::build_signer_response())],
    );
    let fixture = common::fixture("sign-fallback", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let factory = runtime
        .instantiate_signer_factory(&fixture.manifest)
        .expect("signer factory instantiates");
    let signer = factory
        .build(&Upstream::AnthropicDirect)
        .await
        .expect("build_signer succeeds before sign fallback");
    let shaped = shaped_request();

    capture().reset();
    let error = sign_request(signer.as_ref(), shaped)
        .await
        .expect_err("sign fallback fails request");

    assert_signing_failed(error, "plugin sign failed");
    capture().assert_dispatch_fallback("sign", "FailRequest");
}

async fn build_signer_fail_request_fallback() {
    let wat = module_with_panicking_export("build_signer", &[]);
    let fixture = common::fixture("build-signer-fallback", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let factory = runtime
        .instantiate_signer_factory(&fixture.manifest)
        .expect("signer factory instantiates");

    capture().reset();
    let error = match factory.build(&Upstream::AnthropicDirect).await {
        Ok(_) => panic!("build_signer fallback unexpectedly succeeded"),
        Err(error) => error,
    };

    assert_signing_failed(error, "plugin build_signer failed");
    capture().assert_dispatch_fallback("build_signer", "FailRequest");
}

fn shape_fail_request_fallback() {
    let wat = module_with_panicking_export("shape", &[]);
    let fixture = common::fixture("shape-fallback", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let dialect = runtime
        .instantiate_dialect(&fixture.manifest)
        .expect("dialect instantiates");

    capture().reset();
    let error = shape_request(
        dialect.as_ref(),
        &common::ctx(),
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect_err("shape fallback fails request");

    assert_unsupported_request(error, "plugin shape failed");
    capture().assert_dispatch_fallback("shape", "FailRequest");
}

fn observe_silent_skip_fallback() {
    let wat = module_with_panicking_export("observe", &[]);
    let fixture = common::fixture(
        "observe-fallback",
        &wat,
        common::metadata(&[("observe_batch_count", 1), ("observe_flush_ms", 10_000)]),
    );
    let runtime = ExtismRuntime::new();
    let hook = runtime
        .instantiate_observability(&fixture.manifest)
        .expect("observability hook instantiates");

    capture().reset();
    hook.observe(ObserveEvent::Chunk {
        batch_index: 0,
        event_count: 1,
        total_bytes: 8,
    })
    .expect("observe SilentSkip drops the batch without failing the request");

    capture().assert_dispatch_fallback("observe", "SilentSkip");
    capture().assert_counter_total("cc_lb_plugin_observe_batches_dropped_total", 1);
    capture().assert_counter_total("cc_lb_plugin_observe_events_dropped_total", 1);
}

fn normalize_error_pass_through_fallback() {
    let wat =
        module_with_panicking_export("normalize_error", &[("shape", &common::shape_response())]);
    let fixture = common::fixture("normalize-error-fallback", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let dialect = runtime
        .instantiate_dialect(&fixture.manifest)
        .expect("dialect instantiates");

    capture().reset();
    let normalized = dialect.normalize_error(
        StatusCode::BAD_GATEWAY,
        &Bytes::from_static(br#"{"error":"upstream"}"#),
    );

    assert_eq!(
        normalized, None,
        "PassThrough leaves original upstream error body in place"
    );
    capture().assert_dispatch_fallback("normalize_error", "PassThrough");
}

async fn on_unauthorized_pass_through_fallback() {
    let wat = module_with_panicking_export(
        "on_unauthorized",
        &[("build_signer", &common::build_signer_response())],
    );
    let fixture = common::fixture("on-unauthorized-fallback", &wat, common::metadata(&[]));
    let runtime = ExtismRuntime::new();
    let factory = runtime
        .instantiate_signer_factory(&fixture.manifest)
        .expect("signer factory instantiates");
    let signer = factory
        .build(&Upstream::AnthropicDirect)
        .await
        .expect("build_signer succeeds before on_unauthorized fallback");

    capture().reset();
    let decision = signer
        .on_unauthorized(&UpstreamError::Unauthorized {
            status: StatusCode::UNAUTHORIZED,
            body: Some(Bytes::from_static(b"original unauthorized")),
        })
        .await;

    assert!(matches!(decision, RetryDecision::Fail));
    capture().assert_dispatch_fallback("on_unauthorized", "PassThrough");
}

fn shaped_request() -> ShapedRequest {
    shape_request(
        &DirectDialect,
        &common::ctx(),
        &Upstream::AnthropicDirect,
        &common::principal(),
    )
    .expect("direct dialect shapes request")
}

struct DirectDialect;

impl UpstreamDialect for DirectDialect {
    fn shape(
        &self,
        _ctx: &cc_lb_plugin_api::RequestContext,
        _upstream: &Upstream,
        _principal: &Principal,
        builder: &mut ShapedRequestBuilder,
    ) -> Result<ShapedRequest, DialectError> {
        Ok(builder.shaped_request(
            Url::parse("https://api.anthropic.com/v1/messages").expect("test url parses"),
            Method::POST,
            HeaderMap::new(),
            Bytes::from_static(br#"{"model":"claude-test"}"#),
        ))
    }

    fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
        None
    }
}

fn assert_signing_failed(error: SignerError, expected_reason: &str) {
    match error {
        SignerError::SigningFailed { reason } => assert_eq!(reason, expected_reason),
        other => panic!("unexpected signer error: {other:?}"),
    }
}

fn assert_unsupported_request(error: DialectError, expected_reason: &str) {
    match error {
        DialectError::UnsupportedRequest { reason } => assert_eq!(reason, expected_reason),
        other => panic!("unexpected dialect error: {other:?}"),
    }
}

fn module_with_panicking_export(panicking_export: &str, outputs: &[(&str, &str)]) -> String {
    let mut helpers = String::new();
    let mut funcs = String::new();
    for (index, (export, output)) in outputs.iter().enumerate() {
        let helper = format!("bytes_{index}");
        helpers.push_str(&bytes_helper(&helper, output.as_bytes()));
        funcs.push_str(&format!(
            r#"
(func (export "{export}") (result i32)
  (call $output_set (call ${helper}) (i64.const {len}))
  (i32.const 0))
"#,
            len = output.len()
        ));
    }
    funcs.push_str(&format!(
        r#"
(func (export "{panicking_export}") (result i32)
  unreachable
  (i32.const 0))
"#
    ));

    format!(
        r#"
(module
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  {helpers}
  {funcs}
)
"#
    )
}

fn bytes_helper(name: &str, bytes: &[u8]) -> String {
    let mut stores = String::new();
    for (index, byte) in bytes.iter().enumerate() {
        stores.push_str(&format!(
            "  (call $store_u8 (i64.add (local.get $ptr) (i64.const {index})) (i32.const {byte}))\n"
        ));
    }
    format!(
        r#"
(func ${name} (result i64)
  (local $ptr i64)
  (local.set $ptr (call $alloc (i64.const {len})))
{stores}  (local.get $ptr))
"#,
        len = bytes.len()
    )
}

fn capture() -> &'static Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = Capture::default();
        metrics::set_global_recorder(capture.metrics.clone()).expect("install metrics recorder");
        tracing::subscriber::set_global_default(capture.tracing.clone())
            .expect("install tracing subscriber");
        capture
    })
}

#[derive(Clone, Default)]
struct Capture {
    metrics: CountingRecorder,
    tracing: CapturingSubscriber,
}

impl Capture {
    fn reset(&self) {
        self.metrics.reset();
        self.tracing.reset();
    }

    fn assert_dispatch_fallback(&self, function: &str, fallback: &str) {
        let dispatch_total = self.metrics.counter_total_matching(|key| {
            key.contains("cc_lb_plugin_dispatch_errors_total")
                && key.contains("function")
                && key.contains(function)
        });
        assert_eq!(
            dispatch_total,
            1,
            "expected exactly one dispatch error counter increment for {function}, counters: {:?}",
            self.metrics.snapshot()
        );

        let stage_total = self.metrics.counter_total_matching(|key| {
            key.contains("cc_lb_plugin_dispatch_errors_total")
                && key.contains(function)
                && key.contains("plugin_call")
        });
        assert_eq!(
            stage_total,
            1,
            "expected plugin_call stage label for {function}, counters: {:?}",
            self.metrics.snapshot()
        );

        let rendered_logs = self.tracing.rendered();
        assert!(
            rendered_logs.contains("target=cc_lb_plugin.dispatch"),
            "dispatch warning target missing for {function}: {rendered_logs}"
        );
        assert!(
            rendered_logs.contains(&format!("function=\"{function}\"")),
            "dispatch warning function missing for {function}: {rendered_logs}"
        );
        assert!(
            rendered_logs.contains("dispatch error, applying fallback"),
            "dispatch warning message missing for {function}: {rendered_logs}"
        );
        assert!(
            rendered_logs.contains(fallback),
            "dispatch warning fallback policy missing for {function}: {rendered_logs}"
        );
    }

    fn assert_counter_total(&self, metric_name: &str, expected: u64) {
        let total = self
            .metrics
            .counter_total_matching(|key| key.contains(metric_name));
        assert_eq!(
            total,
            expected,
            "unexpected total for {metric_name}, counters: {:?}",
            self.metrics.snapshot()
        );
    }
}

#[derive(Clone, Default)]
struct CountingRecorder {
    counts: Arc<Mutex<HashMap<String, u64>>>,
}

impl CountingRecorder {
    fn reset(&self) {
        self.counts.lock().expect("metric lock").clear();
    }

    fn snapshot(&self) -> HashMap<String, u64> {
        self.counts.lock().expect("metric lock").clone()
    }

    fn counter_total_matching(&self, matches: impl Fn(&str) -> bool) -> u64 {
        self.counts
            .lock()
            .expect("metric lock")
            .iter()
            .filter(|(key, _)| matches(key))
            .map(|(_, count)| *count)
            .sum()
    }
}

impl Recorder for CountingRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(CountingCounter {
            counts: Arc::clone(&self.counts),
            key: format!("{key:?}"),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

struct CountingCounter {
    counts: Arc<Mutex<HashMap<String, u64>>>,
    key: String,
}

impl CounterFn for CountingCounter {
    fn increment(&self, value: u64) {
        let mut counts = self.counts.lock().expect("metric lock");
        *counts.entry(self.key.clone()).or_insert(0) += value;
    }

    fn absolute(&self, value: u64) {
        self.counts
            .lock()
            .expect("metric lock")
            .insert(self.key.clone(), value);
    }
}

#[derive(Clone, Default)]
struct CapturingSubscriber {
    events: Arc<Mutex<Vec<String>>>,
}

impl CapturingSubscriber {
    fn reset(&self) {
        self.events.lock().expect("events lock").clear();
    }

    fn rendered(&self) -> String {
        self.events.lock().expect("events lock").join("\n")
    }
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, metadata: &TracingMetadata<'_>) -> bool {
        metadata.target() == "cc_lb_plugin.dispatch" && metadata.level() <= &Level::WARN
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = EventVisitor::default();
        event.record(&mut visitor);
        self.events.lock().expect("events lock").push(format!(
            "target={} {}",
            event.metadata().target(),
            visitor.fields
        ));
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Default)]
struct EventVisitor {
    fields: String,
}

impl Visit for EventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if !self.fields.is_empty() {
            self.fields.push(' ');
        }
        self.fields.push_str(&format!("{}={value:?}", field.name()));
    }
}
